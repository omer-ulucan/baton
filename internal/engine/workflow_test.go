package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// wfEnv uses definitions' own retry policies and timeouts.
func wfEnv(t *testing.T, mod func(*engine.Options)) *env {
	return newEnv(t, func(o *engine.Options) {
		o.RetryPolicy = nil
		if mod != nil {
			mod(o)
		}
	})
}

func (e *env) submit(yaml string, input string) uuid.UUID {
	e.t.Helper()
	sub := engine.Submission{Definition: []byte(yaml)}
	if input != "" {
		sub.Input = json.RawMessage(input)
	}
	out, err := e.eng.Submit(e.t.Context(), sub)
	if err != nil {
		e.t.Fatal(err)
	}
	return out.WorkflowID
}

func (e *env) steps(got []engine.Assignment) []string {
	var names []string
	for _, a := range got {
		names = append(names, a.Step)
	}
	sort.Strings(names)
	return names
}

func (e *env) complete(a engine.Assignment) {
	e.t.Helper()
	if err := e.eng.Complete(e.t.Context(), res(a, "w"), nil); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) workflow(wf uuid.UUID) *state.Snapshot {
	e.t.Helper()
	snap, err := e.store.Snapshot(e.t.Context(), wf)
	if err != nil {
		e.t.Fatal(err)
	}
	return snap
}

func (e *env) stepStatus(wf uuid.UUID, step string) state.TaskStatus {
	e.t.Helper()
	for _, t := range e.workflow(wf).Tasks {
		if t.StepName == step && t.Kind == state.KindStep {
			return t.Status
		}
	}
	e.t.Fatalf("no step %s", step)
	return ""
}

const diamond = `
name: diamond
version: 1
steps:
  - {name: a, handler: h, input: {from: a, shared: step}}
  - {name: b, handler: h, depends_on: [a]}
  - {name: c, handler: h, depends_on: [a]}
  - {name: d, handler: h, depends_on: [b, c], timeout: 7s, retry: {max_attempts: 2}}
`

func TestDiamondRunsInDependencyOrder(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(diamond, `{"order":42,"shared":"workflow"}`)

	got := e.claim("w", 10)
	if strings.Join(e.steps(got), ",") != "a" {
		t.Fatalf("first wave = %v", e.steps(got))
	}
	if string(got[0].Input) != `{"from":"a","order":42,"shared":"step"}` {
		t.Fatalf("input = %s (step input should merge over workflow input)", got[0].Input)
	}
	e.complete(got[0])

	got = e.claim("w", 10)
	if strings.Join(e.steps(got), ",") != "b,c" {
		t.Fatalf("second wave = %v", e.steps(got))
	}
	e.complete(got[0])
	if more := e.claim("w", 10); len(more) != 0 {
		t.Fatalf("d ready before both parents finished: %v", e.steps(more))
	}
	e.complete(got[1])

	got = e.claim("w", 10)
	if strings.Join(e.steps(got), ",") != "d" {
		t.Fatalf("third wave = %v", e.steps(got))
	}
	d := got[0]
	if d.MaxAttempts != 2 || time.Until(d.Deadline) > 8*time.Second || time.Until(d.Deadline) < 5*time.Second {
		t.Fatalf("d settings from definition: max_attempts %d, deadline in %s", d.MaxAttempts, time.Until(d.Deadline))
	}
	e.complete(d)
	if s := e.workflow(wf).Workflow.Status; s != state.WfSucceeded {
		t.Fatalf("workflow = %s", s)
	}
	e.verifyReplay()
}

const branches = `
name: branches
version: 1
steps:
  - {name: a, handler: h}
  - {name: b, handler: h, depends_on: [a]}
  - {name: c, handler: h}
`

func TestFailureWaitsForSiblingsThenSkipsBlockedSteps(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(branches, "")
	got := e.claim("w", 10)
	byStep := map[string]engine.Assignment{}
	for _, a := range got {
		byStep[a.Step] = a
	}
	no := false
	if _, err := e.eng.Fail(t.Context(), res(byStep["a"], "w"), engine.Failure{Message: "bad", Retryable: &no}); err != nil {
		t.Fatal(err)
	}
	if s := e.workflow(wf).Workflow.Status; s != state.WfRunning {
		t.Fatalf("workflow %s while sibling c still runs", s)
	}
	e.complete(byStep["c"])
	snap := e.workflow(wf)
	if snap.Workflow.Status != state.WfFailed || e.stepStatus(wf, "b") != state.TsSkipped || e.stepStatus(wf, "c") != state.TsSucceeded {
		t.Fatalf("after sibling finished: workflow %s, b %s", snap.Workflow.Status, e.stepStatus(wf, "b"))
	}
	e.verifyReplay()
}

func TestDLQRetryRevivesFailedWorkflow(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(branches, "")
	byStep := map[string]engine.Assignment{}
	for _, a := range e.claim("w", 10) {
		byStep[a.Step] = a
	}
	e.complete(byStep["c"])
	no := false
	if _, err := e.eng.Fail(t.Context(), res(byStep["a"], "w"), engine.Failure{Message: "bad", Retryable: &no}); err != nil {
		t.Fatal(err)
	}
	if e.workflow(wf).Workflow.Status != state.WfFailed {
		t.Fatal("workflow should have failed")
	}
	var aID uuid.UUID
	for _, tk := range e.workflow(wf).Tasks {
		if tk.StepName == "a" {
			aID = tk.ID
		}
	}
	if err := e.eng.RetryDLQ(t.Context(), aID, "ops"); err != nil {
		t.Fatal(err)
	}
	if s := e.workflow(wf).Workflow.Status; s != state.WfRunning || e.stepStatus(wf, "b") != state.TsPending {
		t.Fatalf("after DLQ retry: workflow %s, b %s", s, e.stepStatus(wf, "b"))
	}
	if err := e.eng.RetryDLQ(t.Context(), aID, "ops"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("retrying a task that is not dead-lettered: %v", err)
	}
	a := e.claim("w", 10)
	if len(a) != 1 || a[0].Attempt != 1 {
		t.Fatalf("requeued a = %+v", a)
	}
	e.complete(a[0])
	e.complete(e.claim("w", 10)[0])
	if s := e.workflow(wf).Workflow.Status; s != state.WfSucceeded {
		t.Fatalf("workflow = %s", s)
	}
	e.verifyReplay()
}

type fixedPolicy struct{ label string }

func (p fixedPolicy) Decide(_ context.Context, r decision.Request) (decision.Decision, error) {
	return decision.Decision{Label: p.label, Confidence: 0.9, Policy: "test", InputHash: r.Hash()}, nil
}

func TestCompensateDecisionFailsAtOnceAndStopsSiblings(t *testing.T) {
	e := wfEnv(t, func(o *engine.Options) { o.Policy = fixedPolicy{decision.Compensate} })
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(branches, "")
	byStep := map[string]engine.Assignment{}
	for _, a := range e.claim("w", 10) {
		byStep[a.Step] = a
	}
	if _, err := e.eng.Fail(t.Context(), res(byStep["a"], "w"), engine.Failure{Message: "corrupt ledger"}); err != nil {
		t.Fatal(err)
	}
	if s := e.workflow(wf).Workflow.Status; s != state.WfFailed {
		t.Fatalf("workflow = %s, want failed immediately", s)
	}
	if e.stepStatus(wf, "c") != state.TsCancelled || e.stepStatus(wf, "b") != state.TsSkipped {
		t.Fatalf("c %s, b %s", e.stepStatus(wf, "c"), e.stepStatus(wf, "b"))
	}
	hb, err := e.eng.Heartbeat(t.Context(), "w", []uuid.UUID{byStep["c"].AttemptID})
	if err != nil || hb.Revoked[byStep["c"].AttemptID] != engine.RevokedCancelled {
		t.Fatalf("sibling's lease not revoked: %+v %v", hb, err)
	}
	e.verifyReplay()
}

func TestCancelStopsEverything(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(diamond, "")
	a := e.claim("w", 1)[0]
	if err := e.eng.Cancel(t.Context(), wf, false, "user"); err != nil {
		t.Fatal(err)
	}
	snap := e.workflow(wf)
	if snap.Workflow.Status != state.WfCancelled {
		t.Fatalf("workflow = %s", snap.Workflow.Status)
	}
	for _, tk := range snap.Tasks {
		if tk.Status != state.TsCancelled {
			t.Errorf("step %s is %s", tk.StepName, tk.Status)
		}
	}
	hb, _ := e.eng.Heartbeat(t.Context(), "w", []uuid.UUID{a.AttemptID})
	if hb.Revoked[a.AttemptID] != engine.RevokedCancelled {
		t.Fatalf("heartbeat = %+v", hb)
	}
	if err := e.eng.Complete(t.Context(), res(a, "w"), nil); !errors.Is(err, engine.ErrCancelled) {
		t.Fatalf("late complete = %v", err)
	}
	if got := e.claim("w", 10); len(got) != 0 {
		t.Fatal("claimed a task of a cancelled workflow")
	}
	if err := e.eng.Cancel(t.Context(), wf, false, ""); !errors.Is(err, state.ErrInvalidTransition) {
		t.Fatalf("second cancel = %v", err)
	}
	if err := e.eng.Cancel(t.Context(), uuid.New(), false, ""); !errors.Is(err, engine.ErrWorkflowNotFound) {
		t.Fatalf("cancel unknown = %v", err)
	}
	assertNothingAfterCancel(t, e, wf)
	e.verifyReplay()
}

// assertNothingAfterCancel checks the invariant of SPEC 6.5: no claim and
// no recorded result after WorkflowCancelled.
func assertNothingAfterCancel(t *testing.T, e *env, wf uuid.UUID) {
	t.Helper()
	events, err := e.store.Events(t.Context(), wf)
	if err != nil {
		t.Fatal(err)
	}
	cancelled := false
	for _, ev := range events {
		switch ev.Payload.(type) {
		case state.WorkflowCancelled:
			cancelled = true
		case state.TaskClaimed, state.TaskSucceeded, state.TaskFailed:
			if cancelled {
				t.Fatalf("%s at seq %d after WorkflowCancelled", ev.Type(), ev.Seq)
			}
		}
	}
}

func TestPauseAndResume(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(diamond, "")
	a := e.claim("w", 1)[0]
	if err := e.eng.Pause(t.Context(), wf); err != nil {
		t.Fatal(err)
	}
	e.complete(a) // running attempts finish while paused
	if got := e.claim("w", 10); len(got) != 0 {
		t.Fatalf("claimed %v while paused", e.steps(got))
	}
	if e.stepStatus(wf, "b") != state.TsPending {
		t.Fatal("dependents must not become ready while paused")
	}
	if err := e.eng.Resume(t.Context(), wf); err != nil {
		t.Fatal(err)
	}
	if got := e.claim("w", 10); strings.Join(e.steps(got), ",") != "b,c" {
		t.Fatalf("after resume claimed %v", e.steps(got))
	}
	if err := e.eng.Resume(t.Context(), wf); !errors.Is(err, state.ErrInvalidTransition) {
		t.Fatalf("resume running = %v", err)
	}
	e.verifyReplay()
}

func TestSubmitIdempotencyAndValidation(t *testing.T) {
	e := wfEnv(t, nil)
	sub := engine.Submission{Definition: []byte(diamond), Key: "order-123"}
	first, err := e.eng.Submit(t.Context(), sub)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make([]engine.Submitted, 8)
	for i := range results {
		wg.Go(func() {
			r, err := e.eng.Submit(t.Context(), sub)
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		})
	}
	wg.Wait()
	for _, r := range results {
		if r.WorkflowID != first.WorkflowID || !r.Existing {
			t.Fatalf("repeat submission = %+v, want existing %s", r, first.WorkflowID)
		}
	}
	var defs int
	_ = e.pool.QueryRow(t.Context(), `SELECT count(*) FROM workflow_definitions WHERE name = 'diamond'`).Scan(&defs)
	if defs != 1 {
		t.Fatalf("%d copies of the definition stored", defs)
	}

	for name, s := range map[string]engine.Submission{
		"cycle":        {Definition: []byte("name: x\nversion: 1\nsteps: [{name: a, handler: h, depends_on: [a]}]")},
		"array input":  {Definition: []byte(diamond), Input: json.RawMessage(`[1]`)},
		"bad callback": {Definition: []byte(diamond), CallbackURL: "ftp://x"},
		"bad priority": {Definition: []byte(diamond), Priority: ptr(101)},
	} {
		var ve *workflow.ValidationError
		if _, err := e.eng.Submit(t.Context(), s); !errors.As(err, &ve) {
			t.Errorf("%s: err = %v, want ValidationError", name, err)
		}
	}
}

func ptr[T any](v T) *T { return &v }

type recorder struct {
	mu   sync.Mutex
	reqs []recorded
	fail int // answer 500 to this many requests first
}

type recorded struct {
	key, topic string
	body       map[string]any
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail > 0 {
		r.fail--
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	b, _ := io.ReadAll(req.Body)
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	r.reqs = append(r.reqs, recorded{key: req.Header.Get("Baton-Message-Key"), topic: req.Header.Get("Baton-Topic"), body: body})
}

func (r *recorder) received() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.reqs...)
}

func TestTerminalWebhookIsDeliveredThroughTheOutbox(t *testing.T) {
	rec := &recorder{fail: 1}
	hook := httptest.NewServer(rec)
	defer hook.Close()
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	out, err := e.eng.Submit(t.Context(), engine.Submission{
		Definition: []byte("name: one\nversion: 1\nsteps: [{name: a, handler: h}]"), CallbackURL: hook.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.complete(e.claim("w", 1)[0])

	if n, _ := e.eng.RelayOutbox(t.Context()); n != 0 {
		t.Fatal("first delivery should have failed")
	}
	if n, _ := e.eng.RelayOutbox(t.Context()); n != 0 {
		t.Fatal("redelivered before its backoff")
	}
	e.exec(`UPDATE outbox SET next_try_at = now()`)
	if n, err := e.eng.RelayOutbox(t.Context()); n != 1 || err != nil {
		t.Fatalf("redelivery: %d %v", n, err)
	}
	got := rec.received()
	if len(got) != 1 || got[0].topic != "workflow.terminal" || got[0].key == "" ||
		got[0].body["status"] != "succeeded" || got[0].body["workflow_id"] != out.WorkflowID.String() {
		t.Fatalf("webhook = %+v", got)
	}
	if n, _ := e.eng.RelayOutbox(t.Context()); n != 0 {
		t.Fatal("delivered twice")
	}
}

func TestEscalationGoesToTheOutbox(t *testing.T) {
	rec := &recorder{}
	hook := httptest.NewServer(rec)
	defer hook.Close()
	e := wfEnv(t, func(o *engine.Options) {
		o.Policy = fixedPolicy{decision.Escalate}
		o.EscalationURL = hook.URL
	})
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit("name: one\nversion: 1\nsteps: [{name: a, handler: h}]", "")
	next, err := e.eng.Fail(t.Context(), res(e.claim("w", 1)[0], "w"), engine.Failure{Message: "disk full on ledger host"})
	if err != nil || next != engine.NextDeadLetter {
		t.Fatalf("fail = %v %v", next, err)
	}
	if _, err := e.eng.RelayOutbox(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := rec.received()
	if len(got) != 1 || got[0].topic != "escalation" || got[0].body["error"] != "disk full on ledger host" {
		t.Fatalf("escalation = %+v", got)
	}
	if e.workflow(wf).Workflow.Status != state.WfFailed {
		t.Fatal("an escalated single-step workflow should fail")
	}
}

func TestConcurrentFirstSubmissionsOfOneKey(t *testing.T) {
	e := wfEnv(t, nil)
	sub := engine.Submission{Definition: []byte(diamond), Key: "race-key"}
	var wg sync.WaitGroup
	results := make([]engine.Submitted, 8)
	for i := range results {
		wg.Go(func() {
			r, err := e.eng.Submit(t.Context(), sub)
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		})
	}
	wg.Wait()
	fresh := 0
	for _, r := range results {
		if r.WorkflowID != results[0].WorkflowID {
			t.Fatalf("one key produced workflows %s and %s", r.WorkflowID, results[0].WorkflowID)
		}
		if !r.Existing {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d submissions claim to have created the workflow", fresh)
	}
	var n int
	_ = e.pool.QueryRow(t.Context(), `SELECT count(*) FROM workflows`).Scan(&n)
	if n != 1 {
		t.Fatalf("%d workflows", n)
	}
}

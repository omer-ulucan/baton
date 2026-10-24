package engine_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/state"
)

func (e *env) failPermanently(a engine.Assignment) {
	e.t.Helper()
	no := false
	if _, err := e.eng.Fail(e.t.Context(), res(a, "w"), engine.Failure{Message: "permanent", Retryable: &no}); err != nil {
		e.t.Fatal(err)
	}
}

// runCompensations claims and completes compensation tasks one by one,
// checking that only one is ever ready at a time, and returns the order.
func (e *env) runCompensations() []string {
	e.t.Helper()
	var order []string
	for {
		got := e.claim("w", 10)
		if len(got) == 0 {
			return order
		}
		if len(got) > 1 {
			e.t.Fatalf("compensations run in parallel: %v", e.steps(got))
		}
		if got[0].Kind != state.KindCompensation {
			e.t.Fatalf("claimed a %s task while compensating", got[0].Kind)
		}
		order = append(order, got[0].Step+":"+got[0].Handler)
		e.complete(got[0])
	}
}

func (e *env) status(wf uuid.UUID) state.WorkflowStatus {
	e.t.Helper()
	return e.workflow(wf).Workflow.Status
}

const linear = `
name: linear
version: 1
steps:
  - {name: a, handler: h, compensate: {handler: undo-a}}
  - {name: b, handler: h, depends_on: [a]}
  - {name: c, handler: h, depends_on: [b], compensate: {handler: undo-c, retry: {max_attempts: 7}}}
  - {name: d, handler: h, depends_on: [c]}
`

func TestFailureCompensatesCompletedStepsInReverseOrder(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h", "undo-a", "undo-c"})
	wf := e.submit(linear, `{"order":7}`)
	for range 3 { // a, b, c succeed
		e.complete(e.claim("w", 1)[0])
	}
	d := e.claim("w", 1)[0]
	e.failPermanently(d)
	if s := e.status(wf); s != state.WfCompensating {
		t.Fatalf("workflow = %s, want compensating", s)
	}
	// b declares no compensate; d never succeeded.
	if got := strings.Join(e.runCompensations(), ","); got != "c:undo-c,a:undo-a" {
		t.Fatalf("compensation order = %s", got)
	}
	if s := e.status(wf); s != state.WfCompensated {
		t.Fatalf("workflow = %s, want compensated", s)
	}
	for _, tk := range e.workflow(wf).Tasks {
		if tk.Kind == state.KindCompensation && tk.StepName == "c" && tk.MaxAttempts != 7 {
			t.Fatalf("compensation retry policy not applied: max_attempts %d", tk.MaxAttempts)
		}
	}
	e.verifyReplay()
}

func TestCompensationGetsStepInputAndOwnKey(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h", "undo"})
	wf := e.submit(`
name: keys
version: 1
steps:
  - {name: a, handler: h, input: {sku: x1}, compensate: {handler: undo}}
  - {name: b, handler: h, depends_on: [a]}
`, `{"order":9}`)
	e.complete(e.claim("w", 1)[0])
	e.failPermanently(e.claim("w", 1)[0])
	comp := e.claim("w", 1)[0]
	if comp.Kind != state.KindCompensation || string(comp.Input) != `{"order":9,"sku":"x1"}` ||
		comp.IdempotencyKey != wf.String()+"/a/compensation" {
		t.Fatalf("compensation assignment = %+v", comp)
	}
}

const diamondComp = `
name: diamond-comp
version: 1
steps:
  - {name: a, handler: h, compensate: {handler: undo}}
  - {name: b, handler: h, depends_on: [a], compensate: {handler: undo}}
  - {name: c, handler: h, depends_on: [a], compensate: {handler: undo}}
  - {name: d, handler: h, depends_on: [b, c], compensate: {handler: undo}}
`

func TestDiamondCompensatesInReverseTopologicalOrder(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h", "undo"})
	e.submit(diamondComp, "")
	e.complete(e.claim("w", 1)[0])
	for _, a := range e.claim("w", 10) {
		e.complete(a)
	}
	e.failPermanently(e.claim("w", 1)[0])
	if got := strings.Join(e.runCompensations(), ","); got != "c:undo,b:undo,a:undo" {
		t.Fatalf("order = %s, want reverse of canonical a,b,c,d restricted to succeeded steps", got)
	}
	e.verifyReplay()
}

func TestFailedCompensationStopsChainUntilDLQRetry(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h", "undo-a", "undo-c"})
	wf := e.submit(linear, "")
	for range 3 {
		e.complete(e.claim("w", 1)[0])
	}
	e.failPermanently(e.claim("w", 1)[0])
	undoC := e.claim("w", 1)[0]
	e.failPermanently(undoC)
	if s := e.status(wf); s != state.WfCompensationFailed {
		t.Fatalf("workflow = %s", s)
	}
	if got := e.claim("w", 10); len(got) != 0 {
		t.Fatalf("chain continued past a failed compensation: %v", e.steps(got))
	}
	for _, tk := range e.workflow(wf).Tasks {
		if tk.Kind == state.KindCompensation && tk.StepName == "a" && tk.Status != state.TsPending {
			t.Fatalf("remaining compensation is %s, want pending", tk.Status)
		}
	}
	if err := e.eng.RetryDLQ(t.Context(), undoC.TaskID, "ops"); err != nil {
		t.Fatal(err)
	}
	if s := e.status(wf); s != state.WfCompensating {
		t.Fatalf("after DLQ retry workflow = %s", s)
	}
	if got := strings.Join(e.runCompensations(), ","); got != "c:undo-c,a:undo-a" {
		t.Fatalf("resumed chain = %s", got)
	}
	if s := e.status(wf); s != state.WfCompensated {
		t.Fatalf("workflow = %s", s)
	}
	e.verifyReplay()
}

func TestCancelWithCompensation(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h", "undo-a", "undo-c"})
	wf := e.submit(linear, "")
	e.complete(e.claim("w", 1)[0]) // a
	b := e.claim("w", 1)[0]
	if err := e.eng.Cancel(t.Context(), wf, true, "customer cancelled"); err != nil {
		t.Fatal(err)
	}
	if s := e.status(wf); s != state.WfCompensating {
		t.Fatalf("workflow = %s, want compensating", s)
	}
	hb, _ := e.eng.Heartbeat(t.Context(), "w", []uuid.UUID{b.AttemptID})
	if hb.Revoked[b.AttemptID] != engine.RevokedCancelled {
		t.Fatal("running step not stopped by cancel")
	}
	if got := strings.Join(e.runCompensations(), ","); got != "a:undo-a" {
		t.Fatalf("compensations = %s", got)
	}
	if s := e.status(wf); s != state.WfCancelled {
		t.Fatalf("workflow = %s, want cancelled after its compensations", s)
	}
	assertNoStepClaimedAfterCancelRequest(t, e, wf)
	e.verifyReplay()
}

func TestCancelWithCompensationButNothingToUndo(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(linear, "")
	if err := e.eng.Cancel(t.Context(), wf, true, ""); err != nil {
		t.Fatal(err)
	}
	if s := e.status(wf); s != state.WfCancelled {
		t.Fatalf("workflow = %s", s)
	}
}

func TestCancelledChainSkipsRemainingAfterAFailedCompensation(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h", "undo-a", "undo-c"})
	wf := e.submit(linear, "")
	for range 3 {
		e.complete(e.claim("w", 1)[0])
	}
	if err := e.eng.Cancel(t.Context(), wf, true, ""); err != nil {
		t.Fatal(err)
	}
	e.failPermanently(e.claim("w", 1)[0]) // undo-c fails
	snap := e.workflow(wf)
	if snap.Workflow.Status != state.WfCancelled {
		t.Fatalf("workflow = %s, want cancelled even though a compensation failed", snap.Workflow.Status)
	}
	for _, tk := range snap.Tasks {
		if tk.Kind == state.KindCompensation && tk.StepName == "a" && tk.Status != state.TsSkipped {
			t.Fatalf("remaining compensation is %s, want skipped", tk.Status)
		}
	}
	e.verifyReplay()
}

func TestCompensateDecisionStartsSaga(t *testing.T) {
	e := wfEnv(t, func(o *engine.Options) { o.Policy = fixedPolicy{decision.Compensate} })
	e.register("w", []string{"default"}, []string{"h", "undo"})
	wf := e.submit(`
name: fanout
version: 1
steps:
  - {name: a, handler: h, compensate: {handler: undo}}
  - {name: b, handler: h, depends_on: [a]}
  - {name: c, handler: h, depends_on: [a], compensate: {handler: undo}}
`, "")
	e.complete(e.claim("w", 1)[0])
	byStep := map[string]engine.Assignment{}
	for _, a := range e.claim("w", 10) {
		byStep[a.Step] = a
	}
	if _, err := e.eng.Fail(t.Context(), res(byStep["b"], "w"), engine.Failure{Message: "ledger corrupt"}); err != nil {
		t.Fatal(err)
	}
	// c was still running: cancelled, so not compensated.
	if got := strings.Join(e.runCompensations(), ","); got != "a:undo" {
		t.Fatalf("compensations = %s", got)
	}
	if s := e.status(wf); s != state.WfCompensated {
		t.Fatalf("workflow = %s", s)
	}
	e.verifyReplay()
}

func TestFailureWithoutCompensationJustFails(t *testing.T) {
	e := wfEnv(t, nil)
	e.register("w", []string{"default"}, []string{"h"})
	wf := e.submit(branches, "")
	byStep := map[string]engine.Assignment{}
	for _, a := range e.claim("w", 10) {
		byStep[a.Step] = a
	}
	e.complete(byStep["c"])
	e.failPermanently(byStep["a"])
	if s := e.status(wf); s != state.WfFailed {
		t.Fatalf("workflow = %s", s)
	}
}

// assertNoStepClaimedAfterCancelRequest: once cancel is requested, only
// compensation tasks may be claimed.
func assertNoStepClaimedAfterCancelRequest(t *testing.T, e *env, wf uuid.UUID) {
	t.Helper()
	events, err := e.store.Events(t.Context(), wf)
	if err != nil {
		t.Fatal(err)
	}
	snap := e.workflow(wf)
	requested := false
	for _, ev := range events {
		switch ev.Payload.(type) {
		case state.WorkflowCancelRequested:
			requested = true
		case state.TaskClaimed:
			if requested && snap.Tasks[ev.TaskID].Kind == state.KindStep {
				t.Fatalf("step %s claimed after cancel was requested", snap.Tasks[ev.TaskID].StepName)
			}
		}
	}
}

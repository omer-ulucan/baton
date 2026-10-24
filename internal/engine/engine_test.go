package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/retry"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
)

var pg *pgtest.Server

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		ctx := context.Background()
		var err error
		if pg, err = pgtest.Start(ctx, store.Migrate); err != nil {
			fmt.Fprintln(os.Stderr, "pgtest:", err)
			os.Exit(1)
		}
		code := m.Run()
		_ = pg.Close(ctx)
		os.Exit(code)
	}
	os.Exit(m.Run())
}

var defID = uuid.MustParse("00000000-0000-7000-8000-0000000000aa")

type env struct {
	t     *testing.T
	pool  *pgxpool.Pool
	store *store.Store
	eng   *engine.Engine
}

func fastRetry(*state.Task) retry.Policy {
	return retry.Policy{MaxAttempts: 10, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, Multiplier: 2, Jitter: retry.JitterFull}
}

func newEnv(t *testing.T, mod func(*engine.Options)) *env {
	t.Helper()
	pool, _ := pg.NewDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO workflow_definitions (id, name, version, sha256, spec, created_at)
		VALUES ($1, 'test', 1, '\x01', '{}', now())`, defID); err != nil {
		t.Fatal(err)
	}
	s := store.New(pool)
	o := engine.Options{Store: s, RetryPolicy: fastRetry, Config: engine.Config{ClaimPoll: 20 * time.Millisecond}}
	if mod != nil {
		mod(&o)
	}
	return &env{t: t, pool: pool, store: s, eng: engine.New(o)}
}

// seed creates a running workflow with n ready tasks.
func (e *env) seed(n int, queue, handler string, maxAttempts int) (uuid.UUID, []uuid.UUID) {
	e.t.Helper()
	wf := uuid.New()
	ids := make([]uuid.UUID, n)
	changes := []state.Change{{Payload: state.WorkflowSubmitted{DefinitionID: defID, Priority: 50}}}
	for i := range ids {
		ids[i] = uuid.New()
		step := fmt.Sprintf("s%03d", i)
		changes = append(changes, state.Change{TaskID: ids[i], Payload: state.TaskCreated{
			Step: step, Kind: state.KindStep, Handler: handler, Queue: queue, Priority: 50,
			MaxAttempts: maxAttempts, IdempotencyKey: wf.String() + "/" + step + "/step",
		}})
	}
	for _, id := range ids {
		changes = append(changes, state.Change{TaskID: id, Payload: state.TaskReady{}})
	}
	if _, _, err := e.store.Update(e.t.Context(), store.Fence{}, wf, func(*state.Snapshot, time.Time) ([]state.Change, error) {
		return changes, nil
	}); err != nil {
		e.t.Fatal(err)
	}
	return wf, ids
}

func (e *env) register(id string, queues, handlers []string) {
	e.t.Helper()
	if err := e.eng.Register(e.t.Context(), engine.Worker{ID: id, Queues: queues, Handlers: handlers}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) claim(worker string, n int) []engine.Assignment {
	e.t.Helper()
	got, err := e.eng.Claim(e.t.Context(), engine.ClaimRequest{WorkerID: worker, Max: n})
	if err != nil {
		e.t.Fatal(err)
	}
	return got
}

func (e *env) task(wf, id uuid.UUID) *state.Task {
	e.t.Helper()
	snap, err := e.store.Snapshot(e.t.Context(), wf)
	if err != nil {
		e.t.Fatal(err)
	}
	return snap.Tasks[id]
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(e.t.Context(), sql, args...); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) expireLeases() {
	e.exec(`UPDATE task_leases SET lease_until = now() - interval '1 second'`)
}

func (e *env) verifyReplay() {
	e.t.Helper()
	mm, err := e.store.VerifyReplay(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	for _, m := range mm {
		e.t.Error(m)
	}
}

func res(a engine.Assignment, worker string) engine.Result {
	return engine.Result{WorkerID: worker, TaskID: a.TaskID, AttemptID: a.AttemptID}
}

func TestClaimAndComplete(t *testing.T) {
	e := newEnv(t, nil)
	wf, ids := e.seed(1, "q", "h", 3)
	e.register("w1", []string{"q"}, []string{"h"})
	got := e.claim("w1", 5)
	if len(got) != 1 {
		t.Fatalf("claimed %d", len(got))
	}
	a := got[0]
	if a.TaskID != ids[0] || a.Attempt != 1 || a.IdempotencyKey != wf.String()+"/s000/step" || !a.LeaseUntil.After(time.Now().Add(20*time.Second)) {
		t.Fatalf("assignment = %+v", a)
	}
	if more := e.claim("w1", 5); len(more) != 0 {
		t.Fatal("a running task was claimed again")
	}
	if err := e.eng.Complete(t.Context(), res(a, "w1"), json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if tk := e.task(wf, ids[0]); tk.Status != state.TsSucceeded {
		t.Fatalf("status = %s", tk.Status)
	}
	// Repeating the accepted report is harmless.
	if err := e.eng.Complete(t.Context(), res(a, "w1"), nil); err != nil {
		t.Fatalf("duplicate complete: %v", err)
	}
	var leases int
	_ = e.pool.QueryRow(t.Context(), `SELECT count(*) FROM task_leases`).Scan(&leases)
	if leases != 0 {
		t.Fatalf("%d leases left", leases)
	}
	e.verifyReplay()
}

func TestClaimFilters(t *testing.T) {
	e := newEnv(t, nil)
	e.seed(1, "q1", "h1", 1)
	e.seed(1, "q2", "h2", 1)
	if _, err := e.eng.Claim(t.Context(), engine.ClaimRequest{WorkerID: "ghost"}); !errors.Is(err, engine.ErrUnknownWorker) {
		t.Fatalf("unknown worker: %v", err)
	}
	e.register("w1", []string{"q1", "q2"}, []string{"h1"})
	if got := e.claim("w1", 10); len(got) != 1 || got[0].Handler != "h1" {
		t.Fatalf("claimed %+v; want only the h1 task", got)
	}
	e.register("w2", []string{"q1"}, []string{"h2"})
	if got := e.claim("w2", 10); len(got) != 0 {
		t.Fatalf("w2 claimed %+v from a queue it does not serve", got)
	}
	if _, err := e.eng.Claim(t.Context(), engine.ClaimRequest{WorkerID: "w2", Queues: []string{"q9"}}); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("unregistered queue: %v", err)
	}
}

func TestClaimOrderAndPausedWorkflows(t *testing.T) {
	e := newEnv(t, nil)
	paused, _ := e.seed(1, "q", "h", 1)
	if _, _, err := e.store.Update(t.Context(), store.Fence{}, paused, func(*state.Snapshot, time.Time) ([]state.Change, error) {
		return []state.Change{{Payload: state.WorkflowPaused{}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	wf, ids := e.seed(3, "q", "h", 1)
	e.exec(`UPDATE tasks SET priority = 90 WHERE id = $1`, ids[2])
	e.register("w", []string{"q"}, []string{"h"})
	got := e.claim("w", 10)
	if len(got) != 3 {
		t.Fatalf("claimed %d, want 3 (paused workflow excluded)", len(got))
	}
	if got[0].TaskID != ids[2] {
		t.Fatalf("highest priority not first: %v", got[0].Step)
	}
	for _, a := range got {
		if a.WorkflowID != wf {
			t.Fatal("claimed a task of a paused workflow")
		}
	}
}

func TestLongPollWakesOnRelease(t *testing.T) {
	e := newEnv(t, func(o *engine.Options) { o.Config.ClaimPoll = 10 * time.Second })
	e.seed(1, "q", "h", 3)
	e.register("a", []string{"q"}, []string{"h"})
	e.register("b", []string{"q"}, []string{"h"})
	held := e.claim("a", 1)
	done := make(chan []engine.Assignment, 1)
	start := time.Now()
	go func() {
		got, _ := e.eng.Claim(t.Context(), engine.ClaimRequest{WorkerID: "b", Wait: 5 * time.Second})
		done <- got
	}()
	time.Sleep(100 * time.Millisecond)
	if err := e.eng.Release(t.Context(), "a", []uuid.UUID{held[0].AttemptID}); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if len(got) != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("long poll got %d after %s", len(got), time.Since(start))
	}
	if got[0].Attempt != 1 {
		t.Fatalf("release consumed an attempt: attempt %d", got[0].Attempt)
	}
}

func TestLongPollTimesOut(t *testing.T) {
	e := newEnv(t, nil)
	e.register("w", []string{"q"}, []string{"h"})
	start := time.Now()
	got, err := e.eng.Claim(t.Context(), engine.ClaimRequest{WorkerID: "w", Wait: 200 * time.Millisecond})
	if err != nil || len(got) != 0 || time.Since(start) < 200*time.Millisecond {
		t.Fatalf("got %v %v after %s", got, err, time.Since(start))
	}
}

func TestHeartbeat(t *testing.T) {
	e := newEnv(t, nil)
	e.seed(3, "q", "h", 3)
	e.register("w", []string{"q"}, []string{"h"})
	got := e.claim("w", 3)
	live, expired, cancelled := got[0], got[1], got[2]
	e.exec(`UPDATE task_leases SET lease_until = now() - interval '1 second' WHERE attempt_id = $1`, expired.AttemptID)
	e.exec(`UPDATE task_leases SET revoked = true WHERE attempt_id = $1`, cancelled.AttemptID)
	unknown := uuid.New()

	r, err := e.eng.Heartbeat(t.Context(), "w", []uuid.UUID{live.AttemptID, expired.AttemptID, cancelled.AttemptID, unknown})
	if err != nil {
		t.Fatal(err)
	}
	if until, ok := r.Extended[live.AttemptID]; !ok || !until.After(live.LeaseUntil) {
		t.Fatalf("live lease not extended: %v", r.Extended)
	}
	want := map[uuid.UUID]string{expired.AttemptID: engine.RevokedExpired, cancelled.AttemptID: engine.RevokedCancelled, unknown: engine.RevokedSuperseded}
	for a, reason := range want {
		if r.Revoked[a] != reason {
			t.Errorf("attempt revoked as %q, want %q", r.Revoked[a], reason)
		}
	}
	if _, err := e.eng.Heartbeat(t.Context(), "ghost", nil); !errors.Is(err, engine.ErrUnknownWorker) {
		t.Fatalf("ghost heartbeat: %v", err)
	}
	// Someone else's attempt is not extendable.
	e.register("other", []string{"q"}, []string{"h"})
	r, _ = e.eng.Heartbeat(t.Context(), "other", []uuid.UUID{live.AttemptID})
	if r.Revoked[live.AttemptID] != engine.RevokedSuperseded {
		t.Fatalf("foreign attempt: %v", r.Revoked)
	}
}

func TestLeaseExpiryRetriesThenDeadLetters(t *testing.T) {
	e := newEnv(t, func(o *engine.Options) {
		o.RetryPolicy = func(*state.Task) retry.Policy {
			return retry.Policy{MaxAttempts: 2, InitialBackoff: time.Hour, MaxBackoff: time.Hour, Multiplier: 1, Jitter: retry.JitterNone}
		}
	})
	wf, ids := e.seed(1, "q", "h", 2)
	e.register("w", []string{"q"}, []string{"h"})

	first := e.claim("w", 1)[0]
	e.expireLeases()
	if err := e.eng.Complete(t.Context(), res(first, "w"), nil); !errors.Is(err, engine.ErrLeaseExpired) {
		t.Fatalf("complete on expired lease: %v", err)
	}
	if n, err := e.eng.ReapExpired(t.Context()); err != nil || n != 1 {
		t.Fatalf("reaped %d, %v", n, err)
	}
	if tk := e.task(wf, ids[0]); tk.Status != state.TsRetryWait || *tk.LastError != "lease expired" {
		t.Fatalf("after reap: %+v", tk)
	}
	if err := e.eng.Complete(t.Context(), res(first, "w"), nil); !errors.Is(err, engine.ErrAttemptSuperseded) {
		t.Fatalf("late complete: %v", err)
	}
	if n, _ := e.eng.PromoteRetries(t.Context()); n != 0 {
		t.Fatal("promoted before run_at")
	}
	e.exec(`UPDATE tasks SET run_at = now() - interval '1 second'`)
	if n, _ := e.eng.PromoteRetries(t.Context()); n != 1 {
		t.Fatal("not promoted")
	}
	second := e.claim("w", 1)[0]
	if second.Attempt != 2 {
		t.Fatalf("attempt = %d", second.Attempt)
	}
	e.expireLeases()
	if n, _ := e.eng.ReapExpired(t.Context()); n != 1 {
		t.Fatal("second expiry not reaped")
	}
	if tk := e.task(wf, ids[0]); tk.Status != state.TsDeadLettered {
		t.Fatalf("exhausted task is %s", tk.Status)
	}
	e.verifyReplay()
}

func TestFailTriage(t *testing.T) {
	e := newEnv(t, nil)
	wf, ids := e.seed(2, "q", "h", 3)
	e.register("w", []string{"q"}, []string{"h"})
	got := e.claim("w", 2)
	byTask := map[uuid.UUID]engine.Assignment{got[0].TaskID: got[0], got[1].TaskID: got[1]}
	transient, permanent := byTask[ids[0]], byTask[ids[1]]

	next, err := e.eng.Fail(t.Context(), res(transient, "w"), engine.Failure{Message: "connection reset", Type: "IOError"})
	if err != nil || next != engine.NextRetry {
		t.Fatalf("transient: %v %v", next, err)
	}
	no := false
	next, err = e.eng.Fail(t.Context(), res(permanent, "w"), engine.Failure{Message: "invalid card", Retryable: &no})
	if err != nil || next != engine.NextDeadLetter {
		t.Fatalf("permanent: %v %v", next, err)
	}
	// A repeated report returns the original outcome without new events.
	next, err = e.eng.Fail(t.Context(), res(permanent, "w"), engine.Failure{Message: "invalid card"})
	if err != nil || next != engine.NextDeadLetter {
		t.Fatalf("duplicate fail: %v %v", next, err)
	}
	if tk := e.task(wf, ids[0]); tk.Status != state.TsRetryWait || tk.RunAt == nil {
		t.Fatalf("transient task: %+v", tk)
	}

	events, _ := e.store.Events(t.Context(), wf)
	var decisions []state.DecisionMade
	for _, ev := range events {
		if d, ok := ev.Payload.(state.DecisionMade); ok {
			decisions = append(decisions, d)
		}
	}
	if len(decisions) != 2 || decisions[0].Policy != "rule" || decisions[0].InputHash == "" {
		t.Fatalf("decisions = %+v", decisions)
	}
	e.verifyReplay()
}

type slowPolicy struct{}

func (slowPolicy) Decide(ctx context.Context, _ decision.Request) (decision.Decision, error) {
	<-ctx.Done()
	return decision.Decision{}, ctx.Err()
}

func TestFailFallsBackWhenPolicyTimesOut(t *testing.T) {
	e := newEnv(t, func(o *engine.Options) {
		o.Policy = slowPolicy{}
		o.Config.DecisionTimeout = 50 * time.Millisecond
	})
	wf, _ := e.seed(1, "q", "h", 3)
	e.register("w", []string{"q"}, []string{"h"})
	a := e.claim("w", 1)[0]
	start := time.Now()
	next, err := e.eng.Fail(t.Context(), res(a, "w"), engine.Failure{Message: "x"})
	if err != nil || next != engine.NextRetry || time.Since(start) > time.Second {
		t.Fatalf("fail = %v %v after %s", next, err, time.Since(start))
	}
	events, _ := e.store.Events(t.Context(), wf)
	for _, ev := range events {
		if d, ok := ev.Payload.(state.DecisionMade); ok && (d.Fallback != "timeout" || d.Policy != "rule") {
			t.Fatalf("decision = %+v", d)
		}
	}
}

func TestReportsAreOwnedAndSanitized(t *testing.T) {
	e := newEnv(t, nil)
	wf, ids := e.seed(1, "q", "h", 3)
	e.register("w", []string{"q"}, []string{"h"})
	a := e.claim("w", 1)[0]
	if err := e.eng.Complete(t.Context(), res(a, "intruder"), nil); !errors.Is(err, engine.ErrAttemptSuperseded) {
		t.Fatalf("foreign complete: %v", err)
	}
	if err := e.eng.Complete(t.Context(), engine.Result{WorkerID: "w", TaskID: uuid.New(), AttemptID: a.AttemptID}, nil); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("unknown task: %v", err)
	}
	_, err := e.eng.Fail(t.Context(), res(a, "w"), engine.Failure{Message: "bad\x00bytes \xff here", Stack: "s\x00"})
	if err != nil {
		t.Fatalf("fail with NUL/invalid UTF-8: %v", err)
	}
	if got := *e.task(wf, ids[0]).LastError; got != "badbytes � here" {
		t.Fatalf("last_error = %q", got)
	}
}

func TestReleaseIgnoresForeignAttempts(t *testing.T) {
	e := newEnv(t, nil)
	wf, ids := e.seed(1, "q", "h", 1)
	e.register("w", []string{"q"}, []string{"h"})
	a := e.claim("w", 1)[0]
	if err := e.eng.Release(t.Context(), "someone-else", []uuid.UUID{a.AttemptID}); err != nil {
		t.Fatal(err)
	}
	if tk := e.task(wf, ids[0]); tk.Status != state.TsRunning {
		t.Fatalf("foreign release changed task to %s", tk.Status)
	}
	if err := e.eng.Release(t.Context(), "w", []uuid.UUID{a.AttemptID}); err != nil {
		t.Fatal(err)
	}
	if tk := e.task(wf, ids[0]); tk.Status != state.TsReady || tk.Attempt != 0 {
		t.Fatalf("after release: %+v", tk)
	}
}

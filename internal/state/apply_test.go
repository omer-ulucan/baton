package state

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// fixture drives a snapshot through events, assigning seq and time.
type fixture struct {
	t    *testing.T
	s    *Snapshot
	wf   uuid.UUID
	tick int
}

func newFixture(t *testing.T) *fixture {
	return &fixture{t: t, s: NewSnapshot(), wf: uuid.New()}
}

func (f *fixture) event(task uuid.UUID, p Payload) Event {
	f.tick++
	seq := 1
	if f.s.Workflow != nil {
		seq = f.s.Workflow.LastSeq + 1
	}
	return Event{WorkflowID: f.wf, Seq: seq, TaskID: task, OccurredAt: t0.Add(time.Duration(f.tick) * time.Second), Payload: p}
}

func (f *fixture) ok(task uuid.UUID, p Payload) {
	f.t.Helper()
	if err := Apply(f.s, f.event(task, p)); err != nil {
		f.t.Fatalf("%s: %v", p.Type(), err)
	}
}

// bad asserts rejection and that the snapshot is untouched.
func (f *fixture) bad(task uuid.UUID, p Payload) {
	f.t.Helper()
	before := f.s.Clone()
	err := Apply(f.s, f.event(task, p))
	if !errors.Is(err, ErrInvalidTransition) {
		f.t.Fatalf("%s: err = %v, want ErrInvalidTransition", p.Type(), err)
	}
	if !reflect.DeepEqual(before, f.s) {
		f.t.Fatalf("%s: rejected event mutated the snapshot", p.Type())
	}
}

func (f *fixture) submit() {
	f.ok(uuid.Nil, WorkflowSubmitted{DefinitionID: uuid.New(), Priority: 50})
}

func (f *fixture) step(name string, maxAttempts int) uuid.UUID {
	return f.task(name, KindStep, maxAttempts)
}

func (f *fixture) task(name string, kind TaskKind, maxAttempts int) uuid.UUID {
	id := uuid.New()
	f.ok(id, TaskCreated{Step: name, Kind: kind, Handler: "h." + name, Queue: "default", Priority: 50, MaxAttempts: maxAttempts, IdempotencyKey: f.wf.String() + "/" + name + "/" + string(kind)})
	return id
}

func (f *fixture) claim(task uuid.UUID) uuid.UUID {
	f.t.Helper()
	att := uuid.New()
	f.ok(task, TaskClaimed{AttemptID: att, WorkerID: "w1", Attempt: f.s.Tasks[task].Attempt + 1})
	return att
}

// run takes a ready task through a successful attempt.
func (f *fixture) run(task uuid.UUID) {
	f.t.Helper()
	att := f.claim(task)
	f.ok(task, TaskSucceeded{AttemptID: att})
}

func (f *fixture) deadLetter(task uuid.UUID) {
	f.t.Helper()
	att := f.claim(task)
	f.ok(task, TaskFailed{AttemptID: att, Message: "boom"})
	f.ok(task, TaskDeadLettered{Reason: "decision"})
}

func (f *fixture) wfStatus(want WorkflowStatus) {
	f.t.Helper()
	if got := f.s.Workflow.Status; got != want {
		f.t.Fatalf("workflow status = %s, want %s", got, want)
	}
}

func (f *fixture) taskStatus(task uuid.UUID, want TaskStatus) {
	f.t.Helper()
	if got := f.s.Tasks[task].Status; got != want {
		f.t.Fatalf("task %s status = %s, want %s", f.s.Tasks[task].StepName, got, want)
	}
}

func TestHappyPath(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b := f.step("a", 3), f.step("b", 3)
	f.ok(a, TaskReady{})
	f.run(a)
	f.bad(uuid.Nil, WorkflowSucceeded{}) // b not done
	f.ok(b, TaskReady{})
	f.run(b)
	f.ok(uuid.Nil, WorkflowSucceeded{})
	f.wfStatus(WfSucceeded)
	w := f.s.Workflow
	if w.LastSeq != 10 || w.FinishedAt == nil || !w.FinishedAt.Equal(w.UpdatedAt) {
		t.Fatalf("workflow = %+v", w)
	}
	if tk := f.s.Tasks[a]; tk.Attempt != 1 || tk.CurrentAttempt != nil || tk.WorkerID != nil {
		t.Fatalf("task a = %+v", tk)
	}
}

func TestSubmitRules(t *testing.T) {
	f := newFixture(t)
	f.bad(uuid.Nil, WorkflowPaused{}) // no workflow yet
	f.submit()
	f.bad(uuid.Nil, WorkflowSubmitted{})
	f.bad(uuid.Nil, WorkflowSucceeded{}) // no steps

	e := f.event(uuid.Nil, WorkflowPaused{})
	e.Seq += 5
	if err := Apply(f.s, e); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("seq gap accepted: %v", err)
	}
	e = f.event(uuid.Nil, WorkflowPaused{})
	e.WorkflowID = uuid.New()
	if err := Apply(f.s, e); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("foreign workflow accepted: %v", err)
	}
	f.bad(uuid.New(), TaskReady{})      // unknown task
	f.bad(uuid.New(), WorkflowPaused{}) // workflow event with task id
	f.bad(uuid.Nil, TaskReady{})        // task event without task id
	f.step("a", 1)
	f.bad(uuid.New(), TaskCreated{Step: "a", Kind: KindStep, Handler: "h", Queue: "q", MaxAttempts: 1, IdempotencyKey: "k"})
	f.bad(uuid.New(), TaskCreated{Step: "c", Kind: KindCompensation, Handler: "h", Queue: "q", MaxAttempts: 1, IdempotencyKey: "k"})
	f.bad(uuid.New(), TaskCreated{Step: "d", Kind: KindStep, Handler: "h", Queue: "q", MaxAttempts: 0, IdempotencyKey: "k"})
}

func TestRetryUntilExhausted(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 2)
	f.ok(a, TaskReady{})
	readyAt := *f.s.Tasks[a].ReadyAt

	att := f.claim(a)
	f.bad(a, TaskRetryScheduled{RunAt: t0}) // attempt still in flight
	f.ok(a, TaskFailed{AttemptID: att, Message: "transient"})
	f.bad(a, TaskSucceeded{AttemptID: att}) // attempt is over
	runAt := t0.Add(time.Minute).Add(123 * time.Nanosecond)
	f.ok(a, TaskRetryScheduled{RunAt: runAt, DelayMS: 60000})
	f.taskStatus(a, TsRetryWait)
	if got := *f.s.Tasks[a].RunAt; !got.Equal(runAt.Truncate(time.Microsecond)) {
		t.Fatalf("run_at = %v, not truncated to µs", got)
	}
	f.ok(a, TaskReady{})
	if !f.s.Tasks[a].ReadyAt.Equal(readyAt) || f.s.Tasks[a].RunAt != nil {
		t.Fatal("retry must keep ready_at and clear run_at")
	}

	att = f.claim(a)
	f.ok(a, TaskLeaseExpired{AttemptID: att})
	f.bad(a, TaskRetryScheduled{RunAt: t0}) // 2/2 used
	f.ok(a, TaskDeadLettered{Reason: "exhausted"})
	f.taskStatus(a, TsDeadLettered)
	if *f.s.Tasks[a].LastError != "lease expired" {
		t.Fatalf("last_error = %q", *f.s.Tasks[a].LastError)
	}
	f.bad(a, TaskClaimed{AttemptID: uuid.New(), WorkerID: "w", Attempt: 3})
}

func TestClaimRules(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 3)
	f.bad(a, TaskClaimed{AttemptID: uuid.New(), WorkerID: "w", Attempt: 1}) // pending
	f.ok(a, TaskReady{})
	f.bad(a, TaskClaimed{AttemptID: uuid.New(), WorkerID: "w", Attempt: 2}) // skips attempt 1
	f.bad(a, TaskClaimed{AttemptID: uuid.Nil, WorkerID: "w", Attempt: 1})   // no attempt id
	f.bad(a, TaskClaimed{AttemptID: uuid.New(), WorkerID: "", Attempt: 1})  // no worker
	att := f.claim(a)
	f.bad(a, TaskClaimed{AttemptID: uuid.New(), WorkerID: "w2", Attempt: 2}) // double claim
	f.bad(a, TaskSucceeded{AttemptID: uuid.New()})                           // stale attempt id
	f.ok(a, TaskSucceeded{AttemptID: att})
	f.bad(a, TaskSucceeded{AttemptID: att}) // duplicate result
}

func TestReleaseReturnsAttempt(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 1)
	f.ok(a, TaskReady{})
	readyAt := *f.s.Tasks[a].ReadyAt
	att := f.claim(a)
	f.ok(a, TaskReleased{AttemptID: att})
	tk := f.s.Tasks[a]
	if tk.Status != TsReady || tk.Attempt != 0 || !tk.ReadyAt.Equal(readyAt) || tk.CurrentAttempt != nil {
		t.Fatalf("after release: %+v", tk)
	}
	f.run(a) // the single attempt is still available
}

func TestPauseBlocksClaimsNotCompletions(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b := f.step("a", 3), f.step("b", 3)
	f.ok(a, TaskReady{})
	f.ok(b, TaskReady{})
	att := f.claim(a)
	f.ok(uuid.Nil, WorkflowPaused{})
	f.bad(uuid.Nil, WorkflowPaused{})
	f.bad(b, TaskClaimed{AttemptID: uuid.New(), WorkerID: "w", Attempt: 1})
	f.ok(a, TaskSucceeded{AttemptID: att})
	f.ok(uuid.Nil, WorkflowResumed{})
	f.run(b)
	f.ok(uuid.Nil, WorkflowSucceeded{})
}

func TestRetryWaitStaysWhilePaused(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 3)
	f.ok(a, TaskReady{})
	att := f.claim(a)
	f.ok(a, TaskFailed{AttemptID: att, Message: "x"})
	f.ok(a, TaskRetryScheduled{RunAt: t0})
	f.ok(uuid.Nil, WorkflowPaused{})
	f.bad(a, TaskReady{})
	f.ok(uuid.Nil, WorkflowResumed{})
	f.ok(a, TaskReady{})
}

func TestDeferAndAdmit(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 1)
	f.ok(uuid.Nil, WorkflowDeferred{})
	f.bad(a, TaskReady{})
	f.bad(uuid.Nil, WorkflowCancelRequested{Compensate: true})
	f.ok(uuid.Nil, WorkflowAdmitted{})
	f.ok(a, TaskReady{})
	f.bad(uuid.Nil, WorkflowDeferred{}) // no longer fresh
}

func TestCancel(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b, c := f.step("a", 3), f.step("b", 3), f.step("c", 3)
	f.ok(a, TaskReady{})
	f.run(a)
	f.ok(b, TaskReady{})
	att := f.claim(b)
	f.ok(c, TaskReady{})

	f.bad(c, TaskCancelled{}) // not requested
	f.bad(uuid.Nil, WorkflowCancelled{})
	f.ok(uuid.Nil, WorkflowCancelRequested{Reason: "user"})
	f.bad(uuid.Nil, WorkflowCancelRequested{})
	f.bad(uuid.Nil, WorkflowCancelled{}) // b and c still live
	f.bad(a, TaskCancelled{})            // already succeeded
	f.ok(b, TaskCancelled{})
	f.ok(c, TaskCancelled{})
	f.ok(uuid.Nil, WorkflowCancelled{})
	f.wfStatus(WfCancelled)

	f.bad(b, TaskSucceeded{AttemptID: att}) // late result
	f.bad(c, TaskReady{})
	f.bad(uuid.Nil, WorkflowResumed{})
	f.bad(uuid.Nil, WorkflowRetried{})
}

func TestFailureCompensationAndRetry(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b, c := f.step("a", 1), f.step("b", 1), f.step("c", 1)
	f.ok(a, TaskReady{})
	f.run(a)
	f.ok(b, TaskReady{})
	f.deadLetter(b)
	f.bad(uuid.Nil, WorkflowFailed{CauseTaskID: a}) // a succeeded
	f.bad(c, TaskSkipped{})                         // workflow still running
	f.ok(uuid.Nil, WorkflowFailed{CauseTaskID: b})
	f.ok(c, TaskSkipped{})
	f.wfStatus(WfFailed)

	f.bad(uuid.Nil, CompensationStarted{})
	f.ok(uuid.Nil, CompensationStarted{Order: []string{"a"}})
	f.wfStatus(WfCompensating)
	if f.s.Workflow.FinishedAt != nil {
		t.Fatal("compensating workflow has finished_at")
	}
	ca := f.task("a", KindCompensation, 1)
	f.bad(uuid.Nil, CompensationCompleted{})
	f.ok(ca, TaskReady{})
	f.deadLetter(ca)
	f.bad(ca, TaskSkipped{})
	f.ok(uuid.Nil, CompensationFailed{})
	f.wfStatus(WfCompensationFailed)

	// DLQ retry resumes the chain, not the forward DAG.
	f.bad(ca, TaskRequeued{})
	f.ok(uuid.Nil, WorkflowRetried{})
	f.wfStatus(WfCompensating)
	f.ok(ca, TaskRequeued{By: "ops"})
	if f.s.Tasks[ca].Attempt != 0 {
		t.Fatal("requeue must reset attempts")
	}
	f.run(ca)
	f.ok(uuid.Nil, CompensationCompleted{})
	f.wfStatus(WfCompensated)
}

func TestRetryFromFailedRevivesDAG(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b := f.step("a", 1), f.step("b", 1)
	f.ok(a, TaskReady{})
	f.deadLetter(a)
	f.ok(uuid.Nil, WorkflowFailed{CauseTaskID: a})
	f.ok(b, TaskSkipped{})
	f.ok(uuid.Nil, WorkflowRetried{})
	f.wfStatus(WfRunning)
	f.taskStatus(b, TsPending)
	f.ok(a, TaskRequeued{})
	f.run(a)
	f.ok(b, TaskReady{})
	f.run(b)
	f.ok(uuid.Nil, WorkflowSucceeded{})
}

func TestCancelWithCompensation(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b := f.step("a", 1), f.step("b", 1)
	f.ok(a, TaskReady{})
	f.run(a)
	f.ok(uuid.Nil, WorkflowCancelRequested{Compensate: true})
	f.bad(uuid.Nil, WorkflowFailed{CauseTaskID: a})
	f.ok(b, TaskCancelled{})
	f.ok(uuid.Nil, CompensationStarted{Order: []string{"a"}})
	ca := f.task("a", KindCompensation, 1)
	cx := f.task("x", KindCompensation, 1)
	f.ok(ca, TaskReady{})
	f.deadLetter(ca)
	f.ok(uuid.Nil, CompensationFailed{})
	f.wfStatus(WfCompensating) // cancel decides the end state
	f.bad(uuid.Nil, WorkflowCancelled{})
	f.ok(cx, TaskSkipped{})
	f.ok(uuid.Nil, WorkflowCancelled{})
	f.wfStatus(WfCancelled)
}

func TestCompensateDecisionCancelsSiblings(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b, c := f.step("a", 1), f.step("b", 1), f.step("c", 1)
	f.ok(a, TaskReady{})
	f.run(a)
	f.ok(b, TaskReady{})
	f.ok(c, TaskReady{})
	att := f.claim(c)
	f.deadLetter(b)
	f.ok(uuid.Nil, WorkflowFailed{CauseTaskID: b})
	f.ok(c, TaskCancelled{})
	f.bad(c, TaskSucceeded{AttemptID: att})
	f.ok(uuid.Nil, CompensationStarted{Order: []string{"a"}})
}

func TestDecisionMadeIsRecordedOnly(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 1)
	before := *f.s.Tasks[a]
	f.ok(a, DecisionMade{Kind: "priority", Label: "high", Confidence: 0.9, Policy: "rule", InputHash: "h"})
	if !reflect.DeepEqual(before, *f.s.Tasks[a]) {
		t.Fatal("DecisionMade changed the task")
	}
	f.bad(a, DecisionMade{Kind: "priority"})
}

func TestLastErrorIsTruncatedAtRuneBoundary(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a := f.step("a", 2)
	f.ok(a, TaskReady{})
	att := f.claim(a)
	msg := string(make([]byte, MaxErrorLen-1)) + "é" // 2-byte rune straddles the cut
	f.ok(a, TaskFailed{AttemptID: att, Message: msg})
	if got := len(*f.s.Tasks[a].LastError); got != MaxErrorLen-1 {
		t.Fatalf("len(last_error) = %d", got)
	}
}

func TestTouchedTracksChangedTasks(t *testing.T) {
	f := newFixture(t)
	f.submit()
	a, b := f.step("a", 1), f.step("b", 1)
	f.s.ResetTouched()
	f.ok(a, TaskReady{})
	if got := f.s.Touched(); len(got) != 1 || got[0] != a {
		t.Fatalf("touched = %v", got)
	}
	_ = b
}

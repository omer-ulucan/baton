package state_test

import (
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/state/statetest"
)

// TestRandomHistories drives many workflows through random valid
// histories, interleaved with random (mostly invalid) events, and checks
// invariants after every accepted event.
func TestRandomHistories(t *testing.T) {
	const workflows, maxBatches = 400, 60
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	r := rand.New(rand.NewPCG(seed, 0))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	reachedWf := map[state.WorkflowStatus]bool{}
	reachedTask := map[state.TaskStatus]bool{}
	for range workflows {
		wfID := uuid.New()
		s := state.NewSnapshot()
		for range maxBatches {
			now = now.Add(time.Duration(r.IntN(1000)) * time.Millisecond)
			b := statetest.Next(r, s, wfID, now)
			if b == nil {
				break
			}
			for _, ev := range statetest.Events(s, wfID, b, now) {
				if err := state.Apply(s, ev); err != nil {
					t.Fatalf("seed %d: generated batch rejected: %v", seed, err)
				}
				checkInvariants(t, s)
			}
			fuzzReject(t, r, s, wfID, now)
			reachedWf[s.Workflow.Status] = true
			for _, tk := range s.Tasks {
				reachedTask[tk.Status] = true
			}
		}
	}

	// The generator must actually explore the state space.
	for _, st := range []state.WorkflowStatus{state.WfRunning, state.WfDeferred, state.WfPaused, state.WfSucceeded,
		state.WfFailed, state.WfCompensating, state.WfCompensated, state.WfCompensationFailed, state.WfCancelled} {
		if !reachedWf[st] {
			t.Errorf("seed %d: workflow status %s never reached", seed, st)
		}
	}
	for _, st := range []state.TaskStatus{state.TsPending, state.TsReady, state.TsRunning, state.TsRetryWait,
		state.TsSucceeded, state.TsDeadLettered, state.TsCancelled, state.TsSkipped} {
		if !reachedTask[st] {
			t.Errorf("seed %d: task status %s never reached", seed, st)
		}
	}
}

// fuzzReject throws a random event at the snapshot. If Apply rejects it
// the snapshot must be unchanged; if it accepts it, invariants must hold.
func fuzzReject(t *testing.T, r *rand.Rand, s *state.Snapshot, wfID uuid.UUID, now time.Time) {
	t.Helper()
	var task uuid.UUID
	if r.IntN(2) == 0 {
		for id := range s.Tasks {
			task = id
			break
		}
	}
	payloads := []state.Payload{
		state.TaskReady{}, state.TaskClaimed{AttemptID: uuid.New(), WorkerID: "x", Attempt: r.IntN(4)},
		state.TaskSucceeded{AttemptID: uuid.New()}, state.TaskRetryScheduled{RunAt: now},
		state.TaskDeadLettered{}, state.TaskCancelled{}, state.TaskSkipped{}, state.TaskRequeued{},
		state.WorkflowSucceeded{}, state.WorkflowCancelled{}, state.WorkflowResumed{}, state.WorkflowRetried{},
		state.CompensationCompleted{}, state.WorkflowFailed{CauseTaskID: task},
	}
	c := s.Clone()
	ev := statetest.Events(c, wfID, statetest.Batch{{TaskID: task, Payload: payloads[r.IntN(len(payloads))]}}, now)[0]
	if err := state.Apply(c, ev); err != nil {
		if !errors.Is(err, state.ErrInvalidTransition) {
			t.Fatalf("rejection is not ErrInvalidTransition: %v", err)
		}
		if !reflect.DeepEqual(c, s) {
			t.Fatalf("rejected %s mutated the snapshot", ev.Type())
		}
		return
	}
	checkInvariants(t, c)
}

func checkInvariants(t *testing.T, s *state.Snapshot) {
	t.Helper()
	w := s.Workflow
	if (w.FinishedAt != nil) != w.Status.Terminal() {
		t.Fatalf("workflow %s: finished_at set = %t", w.Status, w.FinishedAt != nil)
	}
	for _, tk := range s.Tasks {
		if tk.Attempt < 0 || tk.Attempt > tk.MaxAttempts {
			t.Fatalf("task %s: attempt %d of %d", tk.StepName, tk.Attempt, tk.MaxAttempts)
		}
		if tk.CurrentAttempt != nil && tk.Status != state.TsRunning {
			t.Fatalf("task %s: %s with an attempt in flight", tk.StepName, tk.Status)
		}
		if (tk.CurrentAttempt == nil) != (tk.WorkerID == nil) {
			t.Fatalf("task %s: attempt and worker out of sync", tk.StepName)
		}
		if (tk.RunAt != nil) != (tk.Status == state.TsRetryWait) {
			t.Fatalf("task %s: %s with run_at set = %t", tk.StepName, tk.Status, tk.RunAt != nil)
		}
		if tk.Status != state.TsPending && tk.Status != state.TsSkipped && tk.Status != state.TsCancelled && tk.ReadyAt == nil {
			t.Fatalf("task %s: %s but never ready", tk.StepName, tk.Status)
		}
		if w.Status == state.WfCancelled && !tk.Status.Terminal() {
			t.Fatalf("cancelled workflow has %s task %s", tk.Status, tk.StepName)
		}
		if w.Status == state.WfSucceeded && tk.Kind == state.KindStep && tk.Status != state.TsSucceeded {
			t.Fatalf("succeeded workflow has %s step %s", tk.Status, tk.StepName)
		}
	}
}

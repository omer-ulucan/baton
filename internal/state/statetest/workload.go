// Package statetest generates random but valid workflow histories, for
// property tests of the state machine and replay tests of the store.
package statetest

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/state"
)

// Step is one event of a batch, before seq and time are assigned.
type Step = state.Change

// Batch is a set of events that must be applied atomically, as one
// transaction of the real write path would.
type Batch []Step

// Next proposes a valid batch for the workflow in s, or nil when the
// workflow is terminal and the generator chose not to retry it. now is
// used for retry run_at values only.
func Next(r *rand.Rand, s *state.Snapshot, wfID uuid.UUID, now time.Time) Batch {
	if s.Workflow == nil {
		return submit(r, wfID)
	}
	cands := candidates(r, s, now)
	r.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	for _, b := range cands {
		if Valid(s, wfID, b) {
			return b
		}
	}
	return nil
}

// Valid reports whether b applies cleanly to a copy of s.
func Valid(s *state.Snapshot, wfID uuid.UUID, b Batch) bool {
	c := s.Clone()
	for _, ev := range Events(c, wfID, b, time.Unix(1, 0)) {
		if err := state.Apply(c, ev); err != nil {
			return false
		}
	}
	return true
}

// Events turns a batch into events following s, all stamped at.
func Events(s *state.Snapshot, wfID uuid.UUID, b Batch, at time.Time) []state.Event {
	seq := 0
	if s.Workflow != nil {
		seq = s.Workflow.LastSeq
	}
	evs := make([]state.Event, len(b))
	for i, st := range b {
		evs[i] = state.Event{WorkflowID: wfID, Seq: seq + i + 1, TaskID: st.TaskID, OccurredAt: at, Payload: st.Payload}
	}
	return evs
}

func submit(r *rand.Rand, wfID uuid.UUID) Batch {
	b := Batch{{Payload: state.WorkflowSubmitted{DefinitionID: DefinitionID, Priority: r.IntN(101)}}}
	for i := range 1 + r.IntN(5) {
		b = append(b, created(wfID, fmt.Sprintf("s%d", i), state.KindStep, 1+r.IntN(3)))
	}
	if r.IntN(10) == 0 {
		b = append(b, Step{Payload: state.WorkflowDeferred{}})
	}
	return b
}

// DefinitionID is the definition every generated workflow refers to.
// Store tests insert it once.
var DefinitionID = uuid.MustParse("00000000-0000-7000-8000-000000000001")

func created(wfID uuid.UUID, step string, kind state.TaskKind, maxAttempts int) Step {
	return Step{TaskID: uuid.New(), Payload: state.TaskCreated{
		Step: step, Kind: kind, Handler: "h." + step, Queue: "default", Priority: 50,
		MaxAttempts: maxAttempts, IdempotencyKey: fmt.Sprintf("%s/%s/%s", wfID, step, kind),
	}}
}

func candidates(r *rand.Rand, s *state.Snapshot, now time.Time) []Batch {
	w := s.Workflow
	tasks := sortedTasks(s)
	var out []Batch
	add := func(b ...Step) { out = append(out, b) }

	// Per-task progress. Apply rejects whatever the workflow state forbids.
	for _, t := range tasks {
		switch t.Status {
		case state.TsPending, state.TsRetryWait:
			add(Step{TaskID: t.ID, Payload: state.TaskReady{}})
		case state.TsReady:
			add(Step{TaskID: t.ID, Payload: state.TaskClaimed{AttemptID: uuid.New(), WorkerID: fmt.Sprintf("w%d", r.IntN(4)), Attempt: t.Attempt + 1}})
		case state.TsRunning:
			att := *t.CurrentAttempt
			add(Step{TaskID: t.ID, Payload: state.TaskSucceeded{AttemptID: att}})
			add(Step{TaskID: t.ID, Payload: state.TaskReleased{AttemptID: att}})
			for _, end := range []state.Payload{
				state.TaskFailed{AttemptID: att, Message: "error " + t.StepName},
				state.TaskLeaseExpired{AttemptID: att},
			} {
				decision := Step{TaskID: t.ID, Payload: state.DecisionMade{Kind: "triage", Label: "retry", Confidence: 1, Policy: "rule", InputHash: "x"}}
				if t.Attempt < t.MaxAttempts {
					delay := time.Duration(r.IntN(5000)) * time.Millisecond
					add(Step{TaskID: t.ID, Payload: end}, decision, Step{TaskID: t.ID, Payload: state.TaskRetryScheduled{RunAt: now.Add(delay), DelayMS: delay.Milliseconds()}})
				}
				add(Step{TaskID: t.ID, Payload: end}, decision, Step{TaskID: t.ID, Payload: state.TaskDeadLettered{Reason: "decision"}})
			}
		}
	}

	switch w.Status {
	case state.WfDeferred:
		add(Step{Payload: state.WorkflowAdmitted{}})
	case state.WfRunning:
		add(Step{Payload: state.WorkflowPaused{}})
		add(Step{Payload: state.WorkflowSucceeded{}})
		if cause := find(tasks, state.KindStep, state.TsDeadLettered); cause != nil && none(tasks, state.TsReady, state.TsRunning, state.TsRetryWait) {
			add(fail(s, tasks, cause)...)
		}
	case state.WfPaused:
		add(Step{Payload: state.WorkflowResumed{}})
	case state.WfCompensating:
		out = append(out, finishCompensation(tasks, w.CancelRequested)...)
	case state.WfFailed, state.WfCompensationFailed:
		if r.IntN(3) == 0 {
			kind := state.KindStep
			if w.Status == state.WfCompensationFailed {
				kind = state.KindCompensation
			}
			if t := find(tasks, kind, state.TsDeadLettered); t != nil {
				add(Step{Payload: state.WorkflowRetried{}}, Step{TaskID: t.ID, Payload: state.TaskRequeued{By: "test"}})
			}
		}
	}

	if !w.CancelRequested && (w.Status == state.WfRunning || w.Status == state.WfPaused || w.Status == state.WfDeferred) && r.IntN(4) == 0 {
		out = append(out, cancel(s, tasks, false))
		if w.Status != state.WfDeferred {
			out = append(out, cancel(s, tasks, true))
		}
	}
	return out
}

func fail(s *state.Snapshot, tasks []*state.Task, cause *state.Task) Batch {
	b := Batch{{Payload: state.WorkflowFailed{CauseTaskID: cause.ID}}}
	for _, t := range tasks {
		if t.Kind == state.KindStep && t.Status == state.TsPending {
			b = append(b, Step{TaskID: t.ID, Payload: state.TaskSkipped{}})
		}
	}
	return append(b, startCompensation(s, tasks)...)
}

func cancel(s *state.Snapshot, tasks []*state.Task, compensate bool) Batch {
	b := Batch{{Payload: state.WorkflowCancelRequested{Compensate: compensate, Reason: "test"}}}
	for _, t := range tasks {
		if !t.Status.Terminal() {
			b = append(b, Step{TaskID: t.ID, Payload: state.TaskCancelled{}})
		}
	}
	if comp := startCompensation(s, tasks); compensate && len(comp) > 0 {
		return append(b, comp...)
	}
	return append(b, Step{Payload: state.WorkflowCancelled{}})
}

// startCompensation returns the CompensationStarted batch tail for every
// succeeded step (reverse name order stands in for reverse topological
// order), or nil when there is nothing to compensate.
func startCompensation(s *state.Snapshot, tasks []*state.Task) Batch {
	var done []string
	for _, t := range tasks {
		if t.Kind == state.KindStep && t.Status == state.TsSucceeded {
			done = append(done, t.StepName)
		}
	}
	if len(done) == 0 {
		return nil
	}
	slices.Reverse(done)
	b := Batch{{Payload: state.CompensationStarted{Order: done}}}
	for _, name := range done {
		b = append(b, created(s.Workflow.ID, name, state.KindCompensation, 2))
	}
	return b
}

func finishCompensation(tasks []*state.Task, cancelRequested bool) []Batch {
	var out []Batch
	if find(tasks, state.KindCompensation, state.TsDeadLettered) != nil {
		b := Batch{{Payload: state.CompensationFailed{}}}
		if cancelRequested {
			for _, t := range tasks {
				if t.Kind == state.KindCompensation && t.Status == state.TsPending {
					b = append(b, Step{TaskID: t.ID, Payload: state.TaskSkipped{}})
				}
			}
			b = append(b, Step{Payload: state.WorkflowCancelled{}})
		}
		out = append(out, b)
	}
	b := Batch{{Payload: state.CompensationCompleted{}}}
	if cancelRequested {
		b = append(b, Step{Payload: state.WorkflowCancelled{}})
	}
	return append(out, b)
}

func sortedTasks(s *state.Snapshot) []*state.Task {
	ts := make([]*state.Task, 0, len(s.Tasks))
	for _, t := range s.Tasks {
		ts = append(ts, t)
	}
	slices.SortFunc(ts, func(a, b *state.Task) int {
		if a.Kind != b.Kind {
			if a.Kind == state.KindStep {
				return -1
			}
			return 1
		}
		if a.StepName < b.StepName {
			return -1
		}
		if a.StepName > b.StepName {
			return 1
		}
		return 0
	})
	return ts
}

func find(tasks []*state.Task, kind state.TaskKind, status state.TaskStatus) *state.Task {
	for _, t := range tasks {
		if t.Kind == kind && t.Status == status {
			return t
		}
	}
	return nil
}

func none(tasks []*state.Task, statuses ...state.TaskStatus) bool {
	for _, t := range tasks {
		if t.Kind == state.KindStep && slices.Contains(statuses, t.Status) {
			return false
		}
	}
	return true
}

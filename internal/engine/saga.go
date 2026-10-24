package engine

import (
	"slices"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// Saga compensation (SPEC 3.3, ADR 0010): when a workflow fails, or is
// cancelled with compensate, every succeeded step that declares a
// compensate action is undone, one at a time, in reverse canonical
// topological order. Compensation tasks are ordinary tasks with their own
// retry policy, timeout, lease and idempotency key.

// compensationOrder lists the steps to compensate: succeeded steps that
// declare compensate, last-completed-first by the canonical order.
func compensationOrder(s *state.Snapshot, def *workflow.Definition) []string {
	var order []string
	for _, name := range slices.Backward(def.Order) {
		st, _ := def.Step(name)
		t := findTask(s, name, state.KindStep)
		if st.Compensate != nil && t != nil && t.Status == state.TsSucceeded {
			order = append(order, name)
		}
	}
	return order
}

// startCompensation begins the chain if there is anything to compensate,
// creating every compensation task up front (pending) and readying the
// first. It reports whether it started one.
func startCompensation(sb *sandbox, def *workflow.Definition) bool {
	order := compensationOrder(sb.s, def)
	if len(order) == 0 {
		return false
	}
	sb.add(uuid.Nil, state.CompensationStarted{Order: order})
	for _, name := range order {
		st, _ := def.Step(name)
		fwd := findTask(sb.s, name, state.KindStep)
		id, err := uuid.NewV7()
		if err != nil {
			sb.err = err
			return true
		}
		sb.add(id, state.TaskCreated{
			Step: name, Kind: state.KindCompensation, Handler: st.Compensate.Handler, Queue: fwd.Queue,
			Priority: fwd.Priority, MaxAttempts: st.Compensate.Retry.MaxAttempts,
			IdempotencyKey: idempotencyKey(sb.wfID, name, state.KindCompensation),
		})
	}
	advanceCompensation(sb, def)
	return true
}

// advanceCompensation moves a compensating workflow along its chain: ready
// the next compensation once all earlier ones succeeded, and end the chain
// when one is dead-lettered or all have succeeded. A cancel-initiated
// chain always ends the workflow cancelled (SPEC 3.2).
func advanceCompensation(sb *sandbox, def *workflow.Definition) {
	w := sb.s.Workflow
	chain := compensationTasks(sb.s, def)
	for _, t := range chain {
		switch t.Status {
		case state.TsSucceeded:
			continue
		case state.TsPending:
			sb.add(t.ID, state.TaskReady{})
			return
		case state.TsDeadLettered:
			sb.add(uuid.Nil, state.CompensationFailed{})
			if w.CancelRequested {
				for _, rest := range chain {
					if rest.Status == state.TsPending {
						sb.add(rest.ID, state.TaskSkipped{})
					}
				}
				sb.add(uuid.Nil, state.WorkflowCancelled{})
			}
			return
		default: // ready, running, retry_wait: wait for it
			return
		}
	}
	if len(chain) > 0 {
		sb.add(uuid.Nil, state.CompensationCompleted{})
		if w.CancelRequested {
			sb.add(uuid.Nil, state.WorkflowCancelled{})
		}
	}
}

// compensationTasks returns the compensation tasks in chain order.
func compensationTasks(s *state.Snapshot, def *workflow.Definition) []*state.Task {
	rank := map[string]int{}
	for i, n := range def.Order {
		rank[n] = i
	}
	var ts []*state.Task
	for _, t := range s.Tasks {
		if t.Kind == state.KindCompensation {
			ts = append(ts, t)
		}
	}
	slices.SortFunc(ts, func(a, b *state.Task) int { return rank[b.StepName] - rank[a.StepName] })
	return ts
}

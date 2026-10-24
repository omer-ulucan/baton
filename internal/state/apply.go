package state

import (
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// ErrInvalidTransition is matched (errors.Is) by every rejection from Apply.
var ErrInvalidTransition = errors.New("invalid transition")

// TransitionError explains why Apply rejected an event.
type TransitionError struct {
	Event  EventType
	Seq    int
	Reason string
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("invalid transition: %s (seq %d): %s", e.Event, e.Seq, e.Reason)
}

func (e *TransitionError) Is(target error) bool { return target == ErrInvalidTransition }

// MaxErrorLen bounds tasks.last_error; longer messages are cut at a rune
// boundary. The full message stays in the TaskFailed payload.
const MaxErrorLen = 4096

// Apply applies e to s. It validates first and mutates only if the event
// is valid, so on error s is unchanged.
func Apply(s *Snapshot, e Event) error {
	reject := func(format string, args ...any) error {
		return &TransitionError{Event: e.Type(), Seq: e.Seq, Reason: fmt.Sprintf(format, args...)}
	}
	if e.Payload == nil {
		return &TransitionError{Seq: e.Seq, Reason: "nil payload"}
	}
	if e.OccurredAt.IsZero() {
		return reject("occurred_at is zero")
	}
	at := normTime(e.OccurredAt)

	if sub, ok := e.Payload.(WorkflowSubmitted); ok {
		if s.Workflow != nil {
			return reject("workflow already exists")
		}
		if e.Seq != 1 || e.TaskID != uuid.Nil {
			return reject("WorkflowSubmitted must be seq 1 without a task")
		}
		s.Workflow = &Workflow{
			ID:           e.WorkflowID,
			DefinitionID: sub.DefinitionID,
			Status:       WfRunning,
			Priority:     sub.Priority,
			SubmitKey:    clonePtr(sub.SubmitKey),
			LastSeq:      1,
			CreatedAt:    at,
			UpdatedAt:    at,
		}
		return nil
	}

	w := s.Workflow
	if w == nil {
		return reject("workflow does not exist")
	}
	if e.WorkflowID != w.ID {
		return reject("event is for workflow %s, snapshot is %s", e.WorkflowID, w.ID)
	}
	if e.Seq != w.LastSeq+1 {
		return reject("seq %d does not follow %d", e.Seq, w.LastSeq)
	}

	var err error
	if isTaskEvent(e.Type()) {
		err = applyTask(s, e, at, reject)
	} else {
		if e.TaskID != uuid.Nil {
			return reject("workflow event carries task id")
		}
		err = applyWorkflow(s, e.Payload, at, reject)
	}
	if err != nil {
		return err
	}
	w.LastSeq = e.Seq
	w.UpdatedAt = at
	return nil
}

func isTaskEvent(t EventType) bool {
	switch t {
	case EvTaskCreated, EvTaskReady, EvTaskClaimed, EvTaskSucceeded, EvTaskFailed,
		EvTaskLeaseExpired, EvTaskReleased, EvTaskRetryScheduled, EvTaskDeadLettered,
		EvTaskRequeued, EvTaskCancelled, EvTaskSkipped, EvDecisionMade:
		return true
	}
	return false
}

type rejectFunc func(format string, args ...any) error

func applyWorkflow(s *Snapshot, p Payload, at time.Time, reject rejectFunc) error {
	w := s.Workflow
	switch p := p.(type) {
	case WorkflowDeferred:
		if w.Status != WfRunning {
			return reject("workflow is %s", w.Status)
		}
		for _, t := range s.Tasks {
			if t.Status != TsPending {
				return reject("task %s is %s; only a fresh workflow can be deferred", t.StepName, t.Status)
			}
		}
		w.Status = WfDeferred

	case WorkflowAdmitted:
		if w.Status != WfDeferred {
			return reject("workflow is %s", w.Status)
		}
		w.Status = WfRunning

	case WorkflowPaused:
		if w.Status != WfRunning {
			return reject("workflow is %s", w.Status)
		}
		w.Status = WfPaused

	case WorkflowResumed:
		if w.Status != WfPaused {
			return reject("workflow is %s", w.Status)
		}
		w.Status = WfRunning

	case WorkflowCancelRequested:
		switch {
		case w.CancelRequested:
			return reject("cancel already requested")
		case w.Status != WfRunning && w.Status != WfPaused && w.Status != WfDeferred:
			return reject("workflow is %s", w.Status)
		case p.Compensate && w.Status == WfDeferred:
			return reject("a deferred workflow has nothing to compensate")
		}
		w.CancelRequested = true

	case WorkflowCancelled:
		if !w.CancelRequested {
			return reject("cancel was not requested")
		}
		switch w.Status {
		case WfRunning, WfPaused, WfDeferred, WfCompensating:
		default:
			return reject("workflow is %s", w.Status)
		}
		if t := firstTask(s, func(t *Task) bool { return !t.Status.Terminal() }); t != nil {
			return reject("task %s/%s is still %s", t.StepName, t.Kind, t.Status)
		}
		w.Status = WfCancelled
		w.FinishedAt = &at

	case WorkflowSucceeded:
		if w.Status != WfRunning {
			return reject("workflow is %s", w.Status)
		}
		steps := 0
		for _, t := range s.Tasks {
			if t.Kind != KindStep {
				continue
			}
			steps++
			if t.Status != TsSucceeded {
				return reject("step %s is %s", t.StepName, t.Status)
			}
		}
		if steps == 0 {
			return reject("workflow has no steps")
		}
		w.Status = WfSucceeded
		w.FinishedAt = &at

	case WorkflowFailed:
		if w.Status != WfRunning && w.Status != WfPaused {
			return reject("workflow is %s", w.Status)
		}
		if w.CancelRequested {
			return reject("cancel requested; cancellation, not failure, ends this workflow")
		}
		cause, ok := s.Tasks[p.CauseTaskID]
		if !ok || cause.Kind != KindStep || cause.Status != TsDeadLettered {
			return reject("cause %s is not a dead-lettered step", p.CauseTaskID)
		}
		w.Status = WfFailed
		w.FinishedAt = &at

	case WorkflowRetried:
		switch w.Status {
		case WfFailed:
			// The DAG must be able to run again past the retried task.
			for id, t := range s.Tasks {
				if t.Kind == KindStep && (t.Status == TsSkipped || t.Status == TsCancelled) {
					t.Status = TsPending
					t.UpdatedAt = at
					s.touch(id)
				}
			}
			w.Status = WfRunning
		case WfCompensationFailed:
			w.Status = WfCompensating
		default:
			return reject("workflow is %s", w.Status)
		}
		w.FinishedAt = nil

	case CompensationStarted:
		failed := w.Status == WfFailed && !w.CancelRequested
		cancelled := (w.Status == WfRunning || w.Status == WfPaused) && w.CancelRequested
		if !failed && !cancelled {
			return reject("workflow is %s (cancel requested: %t)", w.Status, w.CancelRequested)
		}
		if len(p.Order) == 0 {
			return reject("empty compensation order")
		}
		w.Status = WfCompensating
		w.FinishedAt = nil

	case CompensationCompleted:
		if w.Status != WfCompensating {
			return reject("workflow is %s", w.Status)
		}
		n := 0
		for _, t := range s.Tasks {
			if t.Kind != KindCompensation {
				continue
			}
			n++
			if t.Status != TsSucceeded {
				return reject("compensation %s is %s", t.StepName, t.Status)
			}
		}
		if n == 0 {
			return reject("no compensation tasks")
		}
		if !w.CancelRequested {
			w.Status = WfCompensated
			w.FinishedAt = &at
		}

	case CompensationFailed:
		if w.Status != WfCompensating {
			return reject("workflow is %s", w.Status)
		}
		if firstTask(s, func(t *Task) bool { return t.Kind == KindCompensation && t.Status == TsDeadLettered }) == nil {
			return reject("no dead-lettered compensation task")
		}
		if !w.CancelRequested {
			w.Status = WfCompensationFailed
			w.FinishedAt = &at
		}

	default:
		return reject("unhandled workflow event")
	}
	return nil
}

func applyTask(s *Snapshot, e Event, at time.Time, reject rejectFunc) error {
	w := s.Workflow
	if e.TaskID == uuid.Nil {
		return reject("task event without task id")
	}

	if c, ok := e.Payload.(TaskCreated); ok {
		if _, exists := s.Tasks[e.TaskID]; exists {
			return reject("task %s already exists", e.TaskID)
		}
		switch {
		case c.Kind == KindStep && w.Status != WfRunning:
			return reject("step tasks are created at submission; workflow is %s", w.Status)
		case c.Kind == KindCompensation && w.Status != WfCompensating:
			return reject("compensation tasks need a compensating workflow; workflow is %s", w.Status)
		case c.Kind != KindStep && c.Kind != KindCompensation:
			return reject("unknown task kind %q", c.Kind)
		case c.Step == "" || c.Handler == "" || c.Queue == "" || c.IdempotencyKey == "":
			return reject("step, handler, queue and idempotency key are required")
		case c.MaxAttempts < 1:
			return reject("max_attempts must be >= 1")
		}
		for _, t := range s.Tasks {
			if t.StepName == c.Step && t.Kind == c.Kind {
				return reject("duplicate %s task for step %s", c.Kind, c.Step)
			}
		}
		s.Tasks[e.TaskID] = &Task{
			ID:             e.TaskID,
			WorkflowID:     w.ID,
			StepName:       c.Step,
			Kind:           c.Kind,
			Handler:        c.Handler,
			Queue:          c.Queue,
			Priority:       c.Priority,
			Status:         TsPending,
			MaxAttempts:    c.MaxAttempts,
			IdempotencyKey: c.IdempotencyKey,
			UpdatedAt:      at,
		}
		s.touch(e.TaskID)
		return nil
	}

	t, ok := s.Tasks[e.TaskID]
	if !ok {
		return reject("task %s does not exist", e.TaskID)
	}
	// active: the workflow state in which this task may be scheduled.
	active := w.Status == WfRunning
	if t.Kind == KindCompensation {
		active = w.Status == WfCompensating
	}
	attemptIs := func(id uuid.UUID) bool {
		return t.Status == TsRunning && t.CurrentAttempt != nil && *t.CurrentAttempt == id
	}
	awaitingDisposition := t.Status == TsRunning && t.CurrentAttempt == nil

	switch p := e.Payload.(type) {
	case TaskReady:
		if t.Status != TsPending && t.Status != TsRetryWait {
			return reject("task is %s", t.Status)
		}
		if !active {
			return reject("workflow is %s", w.Status)
		}
		if t.Status == TsPending {
			t.ReadyAt = &at
		}
		t.Status = TsReady
		t.RunAt = nil

	case TaskClaimed:
		switch {
		case t.Status != TsReady:
			return reject("task is %s", t.Status)
		case !active:
			return reject("workflow is %s", w.Status)
		case p.AttemptID == uuid.Nil || p.WorkerID == "":
			return reject("attempt id and worker id are required")
		case p.Attempt != t.Attempt+1:
			return reject("attempt %d does not follow %d", p.Attempt, t.Attempt)
		case p.Attempt > t.MaxAttempts:
			return reject("attempt %d exceeds max_attempts %d", p.Attempt, t.MaxAttempts)
		}
		t.Status = TsRunning
		t.Attempt = p.Attempt
		t.CurrentAttempt = &p.AttemptID
		t.WorkerID = &p.WorkerID

	case TaskSucceeded:
		if !attemptIs(p.AttemptID) {
			return reject("attempt %s is not current", p.AttemptID)
		}
		t.Status = TsSucceeded
		endAttempt(t)

	case TaskFailed:
		if !attemptIs(p.AttemptID) {
			return reject("attempt %s is not current", p.AttemptID)
		}
		msg := truncate(p.Message, MaxErrorLen)
		t.LastError = &msg
		endAttempt(t)

	case TaskLeaseExpired:
		if !attemptIs(p.AttemptID) {
			return reject("attempt %s is not current", p.AttemptID)
		}
		msg := "lease expired"
		t.LastError = &msg
		endAttempt(t)

	case TaskReleased:
		if !attemptIs(p.AttemptID) {
			return reject("attempt %s is not current", p.AttemptID)
		}
		t.Status = TsReady
		t.Attempt--
		endAttempt(t)

	case TaskRetryScheduled:
		switch {
		case !awaitingDisposition:
			return reject("task is %s with attempt in flight %t", t.Status, t.CurrentAttempt != nil)
		case t.Attempt >= t.MaxAttempts:
			return reject("attempts exhausted (%d/%d)", t.Attempt, t.MaxAttempts)
		case p.RunAt.IsZero():
			return reject("run_at is zero")
		}
		runAt := normTime(p.RunAt)
		t.Status = TsRetryWait
		t.RunAt = &runAt

	case TaskDeadLettered:
		if !awaitingDisposition {
			return reject("task is %s with attempt in flight %t", t.Status, t.CurrentAttempt != nil)
		}
		t.Status = TsDeadLettered

	case TaskRequeued:
		if t.Status != TsDeadLettered {
			return reject("task is %s", t.Status)
		}
		if !active {
			return reject("workflow is %s", w.Status)
		}
		t.Status = TsReady
		t.Attempt = 0
		t.ReadyAt = &at

	case TaskCancelled:
		switch {
		case t.Kind != KindStep:
			return reject("compensation tasks are never cancelled")
		case t.Status.Terminal():
			return reject("task is %s", t.Status)
		case !w.CancelRequested && w.Status != WfFailed && w.Status != WfCompensating:
			return reject("workflow is %s and cancel was not requested", w.Status)
		}
		t.Status = TsCancelled
		t.RunAt = nil
		endAttempt(t)

	case TaskSkipped:
		if t.Status != TsPending {
			return reject("task is %s", t.Status)
		}
		if t.Kind == KindStep && w.Status != WfFailed && w.Status != WfCompensating {
			return reject("workflow is %s", w.Status)
		}
		if t.Kind == KindCompensation && (w.Status != WfCompensating || !w.CancelRequested) {
			return reject("remaining compensations are skipped only when a cancel ends the chain")
		}
		t.Status = TsSkipped

	case DecisionMade:
		if p.Label == "" || p.Policy == "" {
			return reject("label and policy are required")
		}
		// Recorded for audit and replay; the transition it drives is a
		// separate event in the same transaction.
		return nil

	default:
		return reject("unhandled task event")
	}
	t.UpdatedAt = at
	s.touch(t.ID)
	return nil
}

func endAttempt(t *Task) {
	t.CurrentAttempt = nil
	t.WorkerID = nil
}

func firstTask(s *Snapshot, pred func(*Task) bool) *Task {
	for _, t := range s.Tasks {
		if pred(t) {
			return t
		}
	}
	return nil
}

// normTime matches what timestamptz stores: UTC, microsecond precision.
// Without it the in-memory result of Apply would differ from the same
// state read back from Postgres.
func normTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

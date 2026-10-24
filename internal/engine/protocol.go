package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// Worker is a registration request.
type Worker struct {
	ID       string
	Queues   []string
	Handlers []string
	Version  string
}

// Register records a worker and its capabilities.
func (e *Engine) Register(ctx context.Context, w Worker) error {
	if w.ID == "" || len(w.Queues) == 0 || len(w.Handlers) == 0 {
		return fmt.Errorf("%w: worker id, queues and handlers are required", ErrInvalid)
	}
	return e.inTx(ctx, func(tx *store.Tx) error {
		return tx.UpsertWorker(ctx, clean(w.ID), w.Queues, w.Handlers, clean(w.Version))
	})
}

// ClaimRequest asks for up to Max tasks, waiting up to Wait for some.
type ClaimRequest struct {
	WorkerID string
	Queues   []string // subset of the registered queues; empty means all
	Max      int
	Wait     time.Duration
}

// Assignment is one claimed attempt handed to a worker.
type Assignment struct {
	TaskID         uuid.UUID
	AttemptID      uuid.UUID
	WorkflowID     uuid.UUID
	Step           string
	Kind           state.TaskKind
	Handler        string
	Queue          string
	Input          json.RawMessage
	IdempotencyKey string
	Attempt        int
	MaxAttempts    int
	Deadline       time.Time
	LeaseUntil     time.Time
}

// Claim grants leases on ready tasks the worker can run. With Wait > 0 it
// long-polls: it rechecks when tasks become ready on this replica, and
// every ClaimPoll otherwise, until it has tasks, Wait passes or ctx ends.
func (e *Engine) Claim(ctx context.Context, req ClaimRequest) ([]Assignment, error) {
	if req.Max <= 0 {
		req.Max = 1
	}
	req.Max = min(req.Max, e.cfg.MaxClaimBatch)
	wait := min(req.Wait, e.cfg.MaxClaimWait)
	deadline := time.Now().Add(wait)
	for {
		woken := e.ready.wait() // subscribe before checking, so no wakeup is lost
		got, err := e.claimOnce(ctx, req)
		if err != nil || len(got) > 0 {
			return got, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return nil, nil
		}
		t := time.NewTimer(min(left, e.cfg.ClaimPoll))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-woken:
		case <-t.C:
		}
		t.Stop()
	}
}

func (e *Engine) claimOnce(ctx context.Context, req ClaimRequest) ([]Assignment, error) {
	var out []Assignment
	err := e.inTx(ctx, func(tx *store.Tx) error {
		out = out[:0]
		queues, handlers, ok, err := tx.Worker(ctx, req.WorkerID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknownWorker
		}
		if len(req.Queues) > 0 {
			queues = slices.DeleteFunc(slices.Clone(req.Queues), func(q string) bool { return !slices.Contains(queues, q) })
			if len(queues) == 0 {
				return fmt.Errorf("%w: none of the requested queues are registered for this worker", ErrInvalid)
			}
		}
		if _, err := tx.TouchWorker(ctx, req.WorkerID); err != nil {
			return err
		}
		refs, err := tx.ClaimCandidates(ctx, queues, handlers, req.Max)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			snap, err := tx.Load(ctx, ref.WorkflowID)
			if err != nil {
				return err
			}
			t := snap.Tasks[ref.TaskID]
			def, err := e.defs.get(ctx, snap.Workflow.DefinitionID)
			if err != nil {
				return err
			}
			sub, err := tx.Submission(ctx, ref.WorkflowID)
			if err != nil {
				return err
			}
			_, timeout := e.stepSettings(def, t)
			att, err := uuid.NewV7()
			if err != nil {
				return err
			}
			if _, err := tx.Append(ctx, snap, ref.WorkflowID, []state.Change{{
				TaskID: t.ID, Payload: state.TaskClaimed{AttemptID: att, WorkerID: req.WorkerID, Attempt: t.Attempt + 1},
			}}); err != nil {
				return err
			}
			until, err := tx.InsertLease(ctx, ref, att, req.WorkerID, e.cfg.LeaseTTL)
			if err != nil {
				return err
			}
			out = append(out, Assignment{
				TaskID: t.ID, AttemptID: att, WorkflowID: t.WorkflowID, Step: t.StepName, Kind: t.Kind,
				Handler: t.Handler, Queue: t.Queue, Input: handlerInput(def, sub, t), IdempotencyKey: t.IdempotencyKey,
				Attempt: t.Attempt, MaxAttempts: t.MaxAttempts,
				Deadline: tx.Now().Add(timeout), LeaseUntil: until,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Revocation reasons reported by Heartbeat.
const (
	RevokedCancelled  = "cancelled"
	RevokedSuperseded = "superseded"
	RevokedExpired    = "expired"
)

// HeartbeatResult says which attempts may continue and which must stop.
type HeartbeatResult struct {
	Extended map[uuid.UUID]time.Time
	Revoked  map[uuid.UUID]string
}

// Heartbeat extends the worker's live leases. Attempts it cannot extend are
// returned as revoked with a reason; the worker must abandon them.
func (e *Engine) Heartbeat(ctx context.Context, workerID string, attempts []uuid.UUID) (HeartbeatResult, error) {
	res := HeartbeatResult{Extended: map[uuid.UUID]time.Time{}, Revoked: map[uuid.UUID]string{}}
	err := e.inTx(ctx, func(tx *store.Tx) error {
		ok, err := tx.TouchWorker(ctx, workerID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrUnknownWorker
		}
		if len(attempts) == 0 {
			return nil
		}
		if res.Extended, err = tx.ExtendLeases(ctx, workerID, attempts, e.cfg.LeaseTTL); err != nil {
			return err
		}
		leases, err := tx.Leases(ctx, attempts)
		if err != nil {
			return err
		}
		for _, a := range attempts {
			if _, ok := res.Extended[a]; ok {
				continue
			}
			l, ok := leases[a]
			switch {
			case !ok || l.WorkerID != workerID:
				res.Revoked[a] = RevokedSuperseded
			case l.Revoked:
				res.Revoked[a] = RevokedCancelled
			default:
				res.Revoked[a] = RevokedExpired
			}
		}
		return nil
	})
	return res, err
}

// Result identifies the attempt a worker reports on.
type Result struct {
	WorkerID  string
	TaskID    uuid.UUID
	AttemptID uuid.UUID
}

// Complete records a successful attempt. Repeating an accepted report is
// a no-op; a report for an attempt that is no longer current is rejected.
func (e *Engine) Complete(ctx context.Context, r Result, output json.RawMessage) error {
	wf, err := e.workflowOf(ctx, r.TaskID)
	if err != nil {
		return err
	}
	err = e.inTx(ctx, func(tx *store.Tx) error {
		snap, err := tx.Load(ctx, wf)
		if err != nil {
			return err
		}
		if err := e.checkLease(ctx, tx, snap, r, state.EvTaskSucceeded); err != nil {
			return err
		}
		if err := e.commit(ctx, tx, snap, wf, []state.Change{{
			TaskID: r.TaskID, Payload: state.TaskSucceeded{AttemptID: r.AttemptID, OutputDigest: digest(output)},
		}}); err != nil {
			return err
		}
		return tx.DeleteLease(ctx, r.TaskID)
	})
	if errors.Is(err, errDuplicate) {
		return nil
	}
	return err
}

// Failure is a worker's error report.
type Failure struct {
	Message   string
	Type      string
	Stack     string
	Retryable *bool // worker's hint; nil means unknown
}

// MaxStackLen bounds the stack trace passed to triage (most recent frames
// are kept, which are at the top of Go and most other traces).
const MaxStackLen = 4096

// Next is what happens to a task after a failure report.
type Next string

const (
	NextRetry      Next = "retry"
	NextDeadLetter Next = "dead_letter"
)

// Fail records a failed attempt, triages it and schedules a retry or
// dead-letters the task. Triage runs before the transaction (SPEC 10.2),
// bounded by Config.DecisionTimeout, and falls back to the rule policy on
// error or timeout.
func (e *Engine) Fail(ctx context.Context, r Result, f Failure) (Next, error) {
	f.Message, f.Type, f.Stack = clean(f.Message), clean(f.Type), clean(f.Stack)
	if len(f.Stack) > MaxStackLen {
		f.Stack = f.Stack[:MaxStackLen]
	}
	wf, err := e.workflowOf(ctx, r.TaskID)
	if err != nil {
		return "", err
	}
	snap, err := e.store.Snapshot(ctx, wf)
	if err != nil {
		return "", err
	}
	t, ok := snap.Tasks[r.TaskID]
	if !ok {
		return "", ErrNotFound
	}
	d, latency := e.triage(ctx, decision.Request{
		Kind: decision.KindTriage, TaskID: t.ID, Handler: t.Handler, ErrorType: f.Type, ErrorMessage: f.Message,
		Stack: f.Stack, Attempt: t.Attempt, MaxAttempts: t.MaxAttempts, Retryable: f.Retryable,
	})

	var next Next
	err = e.inTx(ctx, func(tx *store.Tx) error {
		snap, err := tx.Load(ctx, wf)
		if err != nil {
			return err
		}
		if err := e.checkLease(ctx, tx, snap, r, state.EvTaskFailed); err != nil {
			if errors.Is(err, errDuplicate) {
				next = nextOf(snap.Tasks[r.TaskID])
			}
			return err
		}
		t := snap.Tasks[r.TaskID]
		def, err := e.defs.get(ctx, snap.Workflow.DefinitionID)
		if err != nil {
			return err
		}
		changes := []state.Change{
			{TaskID: t.ID, Payload: state.TaskFailed{AttemptID: r.AttemptID, ErrorClass: f.Type, Message: f.Message, StackDigest: digest([]byte(f.Stack))}},
			{TaskID: t.ID, Payload: decisionEvent(d, r.AttemptID, latency)},
		}
		var disposition state.Change
		disposition, next = e.dispose(def, t, d.Label, tx.Now())
		changes = append(changes, disposition)
		if next == NextDeadLetter && d.Label == decision.Compensate && t.Kind == state.KindStep {
			// Fail now rather than waiting for siblings (SPEC 3.2).
			sb := newSandbox(snap, wf)
			sb.addAll(changes)
			if snap.Workflow.Status == state.WfRunning || snap.Workflow.Status == state.WfPaused {
				failWorkflow(sb, t.ID)
			}
			if sb.err != nil {
				return sb.err
			}
			changes = sb.changes
		}
		if next == NextDeadLetter && d.Label == decision.Escalate {
			if err := e.queueEscalation(ctx, tx, t, f, d); err != nil {
				return err
			}
		}
		if err := e.commit(ctx, tx, snap, wf, changes); err != nil {
			return err
		}
		return tx.DeleteLease(ctx, t.ID)
	})
	if errors.Is(err, errDuplicate) {
		return next, nil
	}
	return next, err
}

// dispose turns a triage label into the task's next event. Every label
// but the two retry ones dead-letters the task; the workflow-level effects
// of compensate and escalate are applied by the workflow engine.
func (e *Engine) dispose(def *workflow.Definition, t *state.Task, label string, now time.Time) (state.Change, Next) {
	if (label == decision.Retry || label == decision.RetryWithBackoff) && t.Attempt < t.MaxAttempts {
		p, _ := e.stepSettings(def, t)
		delay := p.InitialBackoff
		if label == decision.RetryWithBackoff {
			delay = p.Backoff(t.Attempt, e.rand)
		}
		return state.Change{TaskID: t.ID, Payload: state.TaskRetryScheduled{RunAt: now.Add(delay), DelayMS: delay.Milliseconds()}}, NextRetry
	}
	reason := label
	if t.Attempt >= t.MaxAttempts {
		reason = "attempts exhausted"
	}
	return state.Change{TaskID: t.ID, Payload: state.TaskDeadLettered{Reason: reason}}, NextDeadLetter
}

func (e *Engine) triage(ctx context.Context, req decision.Request) (decision.Decision, time.Duration) {
	start := time.Now()
	dctx, cancel := context.WithTimeout(ctx, e.cfg.DecisionTimeout)
	defer cancel()
	d, err := e.policy.Decide(dctx, req)
	if err != nil {
		reason := "error"
		if dctx.Err() != nil {
			reason = "timeout"
		}
		e.log.WarnContext(ctx, "triage policy failed, using rules", "err", err, "task", req.TaskID)
		d, _ = decision.RulePolicy{}.Decide(ctx, req)
		d.Fallback = reason
	}
	return d, time.Since(start)
}

func decisionEvent(d decision.Decision, attempt uuid.UUID, latency time.Duration) state.DecisionMade {
	ev := state.DecisionMade{
		Kind: string(decision.KindTriage), AttemptID: &attempt, InputHash: d.InputHash, Label: d.Label,
		Confidence: d.Confidence, Policy: d.Policy, ModelRevision: d.ModelRevision, Fallback: d.Fallback,
		LatencyMS: latency.Milliseconds(),
	}
	if d.Primary != nil {
		ev.Primary = &state.PrimaryDecision{Policy: d.Primary.Policy, Label: d.Primary.Label, Confidence: d.Primary.Confidence}
	}
	return ev
}

func nextOf(t *state.Task) Next {
	if t.Status == state.TsDeadLettered {
		return NextDeadLetter
	}
	return NextRetry
}

// Release hands unfinished attempts back without consuming them (worker
// shutdown, SPEC 8.2). Attempts the worker no longer owns are ignored.
func (e *Engine) Release(ctx context.Context, workerID string, attempts []uuid.UUID) error {
	var leases map[uuid.UUID]store.Lease
	err := e.inTx(ctx, func(tx *store.Tx) error {
		var err error
		leases, err = tx.Leases(ctx, attempts)
		return err
	})
	if err != nil {
		return err
	}
	released := false
	for _, l := range leases {
		if l.WorkerID != workerID {
			continue
		}
		err := e.inTx(ctx, func(tx *store.Tx) error {
			snap, err := tx.Load(ctx, l.WorkflowID)
			if err != nil {
				return err
			}
			cur, err := tx.LockLease(ctx, l.AttemptID)
			if err != nil || cur == nil {
				return err
			}
			if cur.Revoked {
				return tx.DeleteLease(ctx, l.TaskID) // task already cancelled
			}
			t := snap.Tasks[l.TaskID]
			if t.Status != state.TsRunning || t.CurrentAttempt == nil || *t.CurrentAttempt != l.AttemptID {
				return tx.DeleteLease(ctx, l.TaskID)
			}
			if err := e.commit(ctx, tx, snap, l.WorkflowID, []state.Change{{
				TaskID: l.TaskID, Payload: state.TaskReleased{AttemptID: l.AttemptID},
			}}); err != nil {
				return err
			}
			released = true
			return tx.DeleteLease(ctx, l.TaskID)
		})
		if err != nil {
			return err
		}
	}
	if released {
		e.ready.notify()
	}
	return nil
}

// errDuplicate marks a repeated report of an already accepted result.
var errDuplicate = errors.New("duplicate report")

// checkLease verifies that r is the current, live attempt of its task and
// that the reporting worker holds its lease. A repeat of an already
// accepted report of type typ returns errDuplicate.
func (e *Engine) checkLease(ctx context.Context, tx *store.Tx, snap *state.Snapshot, r Result, typ state.EventType) error {
	t, ok := snap.Tasks[r.TaskID]
	if !ok {
		return ErrNotFound
	}
	l, err := tx.LockLease(ctx, r.AttemptID)
	if err != nil {
		return err
	}
	if l == nil || l.TaskID != r.TaskID {
		if dup, err := e.isDuplicate(ctx, tx, t, r, typ); err != nil || dup {
			if err != nil {
				return err
			}
			return errDuplicate
		}
		if t.Status == state.TsCancelled {
			return ErrCancelled
		}
		return ErrAttemptSuperseded
	}
	switch {
	case l.WorkerID != r.WorkerID:
		return ErrAttemptSuperseded
	case l.Revoked:
		return ErrCancelled
	case !l.LeaseUntil.After(tx.Now()):
		return ErrLeaseExpired
	}
	return nil
}

func (e *Engine) isDuplicate(ctx context.Context, tx *store.Tx, t *state.Task, r Result, typ state.EventType) (bool, error) {
	raw, err := tx.LastAttemptEvent(ctx, store.TaskRef{TaskID: t.ID, WorkflowID: t.WorkflowID}, string(typ))
	if err != nil || raw == nil {
		return false, err
	}
	var p struct {
		AttemptID uuid.UUID `json:"attempt_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return false, err
	}
	return p.AttemptID == r.AttemptID, nil
}

func (e *Engine) workflowOf(ctx context.Context, taskID uuid.UUID) (uuid.UUID, error) {
	wf, ok, err := e.store.TaskWorkflow(ctx, taskID)
	if err != nil {
		return uuid.Nil, err
	}
	if !ok {
		return uuid.Nil, ErrNotFound
	}
	return wf, nil
}

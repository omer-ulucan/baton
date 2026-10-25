package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/telemetry"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// ErrWorkflowNotFound is returned for an unknown workflow id.
var ErrWorkflowNotFound = errors.New("workflow not found")

// Submission is a request to run a workflow.
type Submission struct {
	Definition  []byte          // YAML
	Input       json.RawMessage // JSON object, optional
	Priority    *int            // overrides the definition's priority
	Key         string          // client idempotency key, optional
	CallbackURL string          // receives a workflow.terminal webhook, optional
}

// Submitted is the outcome of Submit.
type Submitted struct {
	WorkflowID uuid.UUID
	Status     state.WorkflowStatus
	// Existing is true when Key matched an earlier submission, which is
	// returned unchanged.
	Existing bool
}

// Submit validates a definition and starts a workflow: one task per step,
// with the steps that have no dependencies ready at once.
func (e *Engine) Submit(ctx context.Context, sub Submission) (Submitted, error) {
	ctx, sp := e.tracer.Start(ctx, "workflow.submit")
	defer sp.End()
	if sub.Key != "" {
		if out, ok, err := e.existing(ctx, sub.Key); err != nil || ok {
			return out, err
		}
	}
	parsed, err := workflow.Parse(sub.Definition)
	if err != nil {
		return Submitted{}, err
	}
	if err := validateSubmission(sub); err != nil {
		return Submitted{}, err
	}
	def := parsed.Definition
	priority := def.Priority
	if sub.Priority != nil {
		priority = *sub.Priority
	}
	wfID, err := uuid.NewV7()
	if err != nil {
		return Submitted{}, err
	}
	taskIDs := make(map[string]uuid.UUID, len(def.Order))
	for _, name := range def.Order {
		if taskIDs[name], err = uuid.NewV7(); err != nil {
			return Submitted{}, err
		}
	}
	// Decide before the transaction, never inside it (SPEC 10.2).
	priorities := e.classifyPriorities(ctx, &def, taskIDs)

	var status state.WorkflowStatus
	err = e.inTx(ctx, func(tx *store.Tx) error {
		defID, err := tx.InsertDefinition(ctx, def.Name, def.Version, parsed.SHA256[:], parsed.Spec)
		if err != nil {
			return err
		}
		var key *string
		if sub.Key != "" {
			key = &sub.Key
		}
		changes := []state.Change{{Payload: state.WorkflowSubmitted{
			DefinitionID: defID, Priority: priority, Input: sub.Input, SubmitKey: key, CallbackURL: sub.CallbackURL,
			TraceParent: telemetry.TraceParent(ctx),
		}}}
		for _, name := range def.Order {
			st, _ := def.Step(name)
			p := priority
			cl, classified := priorities[name]
			switch {
			case st.PriorityExplicit:
				p = st.Priority
			case classified:
				p = decision.PriorityValue(cl.d.Label, priority)
			}
			id := taskIDs[name]
			changes = append(changes, state.Change{TaskID: id, Payload: state.TaskCreated{
				Step: st.Name, Kind: state.KindStep, Handler: st.Handler, Queue: st.Queue, Priority: p,
				MaxAttempts: st.Retry.MaxAttempts, IdempotencyKey: idempotencyKey(wfID, st.Name, state.KindStep),
			}})
			if classified {
				changes = append(changes, state.Change{TaskID: id, Payload: decisionEvent(decision.KindPriority, cl.d, nil, cl.latency)})
			}
		}
		// Admission control (SPEC 9.3), with the queues locked so racing
		// submissions cannot both squeeze in.
		cfgs, err := tx.QueueConfigs(ctx)
		if err != nil {
			return err
		}
		adding := queueCounts(&def)
		qs := limited(cfgs, keys(adding), hasDepthLimit)
		if err := tx.LockQueues(ctx, qs); err != nil {
			return err
		}
		depths, err := tx.Depths(ctx, qs)
		if err != nil {
			return err
		}
		deferIt, err := admission(cfgs, depths, adding)
		if err != nil {
			return err
		}
		if deferIt {
			changes = append(changes, state.Change{Payload: state.WorkflowDeferred{}})
		}
		snap := state.NewSnapshot()
		e.defs.put(defID, &def)
		if err := e.commit(ctx, tx, snap, wfID, changes); err != nil {
			return err
		}
		status = snap.Workflow.Status
		return nil
	})
	if isUniqueViolation(err) && sub.Key != "" {
		// Lost a race with a concurrent submission of the same key.
		if out, ok, err := e.existing(ctx, sub.Key); err != nil || ok {
			return out, err
		}
	}
	if err != nil {
		return Submitted{}, err
	}
	sp.SetAttributes(attribute.String("workflow.id", wfID.String()), attribute.String("workflow.name", def.Name))
	return Submitted{WorkflowID: wfID, Status: status}, nil
}

func (e *Engine) existing(ctx context.Context, key string) (Submitted, bool, error) {
	id, ok, err := e.store.WorkflowBySubmitKey(ctx, key)
	if err != nil || !ok {
		return Submitted{}, false, err
	}
	snap, err := e.store.Snapshot(ctx, id)
	if err != nil {
		return Submitted{}, false, err
	}
	return Submitted{WorkflowID: id, Status: snap.Workflow.Status, Existing: true}, true, nil
}

func validateSubmission(sub Submission) error {
	var problems []string
	if len(sub.Input) > 0 {
		var obj map[string]json.RawMessage
		if json.Unmarshal(sub.Input, &obj) != nil || obj == nil {
			problems = append(problems, "input must be a JSON object")
		}
		if len(sub.Input) > workflow.MaxInputLen {
			problems = append(problems, fmt.Sprintf("input is %d bytes, limit %d", len(sub.Input), workflow.MaxInputLen))
		}
	}
	if sub.Priority != nil && (*sub.Priority < 0 || *sub.Priority > 100) {
		problems = append(problems, "priority must be in [0,100]")
	}
	if sub.CallbackURL != "" {
		u, err := url.Parse(sub.CallbackURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			problems = append(problems, "callback_url must be an absolute http(s) URL")
		}
	}
	if len(sub.Key) > 200 {
		problems = append(problems, "idempotency key is longer than 200 characters")
	}
	if len(problems) > 0 {
		return &workflow.ValidationError{Problems: problems}
	}
	return nil
}

type classified struct {
	d       decision.Decision
	latency time.Duration
}

// classifyPriorities asks the decision layer for the priority of every
// step that has a description and no explicit priority, concurrently, so
// a submission waits at most one decision timeout.
func (e *Engine) classifyPriorities(ctx context.Context, def *workflow.Definition, ids map[string]uuid.UUID) map[string]classified {
	out := map[string]classified{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range def.Steps {
		st := &def.Steps[i]
		if st.PriorityExplicit || st.Description == "" {
			continue
		}
		wg.Go(func() {
			start := time.Now()
			ctx, sp := e.tracer.Start(ctx, "decision")
			defer sp.End()
			d, err := e.policy.Decide(ctx, decision.Request{Kind: decision.KindPriority, TaskID: ids[st.Name], Description: st.Description})
			sp.AddEvent("decided", decisionAttrs(decision.KindPriority, d))
			if err != nil {
				e.log.WarnContext(ctx, "priority classification failed; step keeps its inherited priority", "step", st.Name, "err", err)
				return
			}
			mu.Lock()
			out[st.Name] = classified{d: d, latency: time.Since(start)}
			mu.Unlock()
		})
	}
	wg.Wait()
	return out
}

func idempotencyKey(wf uuid.UUID, step string, kind state.TaskKind) string {
	return fmt.Sprintf("%s/%s/%s", wf, step, kind)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Cancel ends a workflow (SPEC 3.2). Queued and running tasks are
// cancelled in the same transaction and running attempts' leases revoked,
// so their next heartbeat stops them. With compensate, completed steps are
// compensated first (SPEC 3.3).
func (e *Engine) Cancel(ctx context.Context, wfID uuid.UUID, compensate bool, reason string) error {
	return e.updateWorkflow(ctx, wfID, func(tx *store.Tx, snap *state.Snapshot) ([]state.Change, error) {
		def, err := e.defs.get(ctx, tx, snap.Workflow.DefinitionID)
		if err != nil {
			return nil, err
		}
		sb := newSandbox(snap, wfID)
		sb.add(uuid.Nil, state.WorkflowCancelRequested{Compensate: compensate, Reason: clean(reason)})
		for _, t := range stepTasks(sb.s, nil) {
			if !t.Status.Terminal() {
				sb.add(t.ID, state.TaskCancelled{})
			}
		}
		if !compensate || !startCompensation(sb, def) {
			sb.add(uuid.Nil, state.WorkflowCancelled{})
		}
		return sb.changes, sb.err
	})
}

// Pause stops new claims of a workflow's tasks; running attempts finish.
func (e *Engine) Pause(ctx context.Context, wfID uuid.UUID) error {
	return e.updateWorkflow(ctx, wfID, func(*store.Tx, *state.Snapshot) ([]state.Change, error) {
		return []state.Change{{Payload: state.WorkflowPaused{}}}, nil
	})
}

// Resume lets a paused workflow continue. Steps whose dependencies
// finished while it was paused become ready at once; retries whose time
// passed are picked up by the retry timer.
func (e *Engine) Resume(ctx context.Context, wfID uuid.UUID) error {
	return e.updateWorkflow(ctx, wfID, func(*store.Tx, *state.Snapshot) ([]state.Change, error) {
		return []state.Change{{Payload: state.WorkflowResumed{}}}, nil
	})
}

// RetryDLQ requeues a dead-lettered task with a fresh attempt budget. If
// its workflow had failed, the workflow runs again (SPEC 3.2).
func (e *Engine) RetryDLQ(ctx context.Context, taskID uuid.UUID, by string) error {
	wf, err := e.workflowOf(ctx, taskID)
	if err != nil {
		return err
	}
	return e.updateWorkflow(ctx, wf, func(_ *store.Tx, snap *state.Snapshot) ([]state.Change, error) {
		t := snap.Tasks[taskID]
		if t.Status != state.TsDeadLettered {
			return nil, fmt.Errorf("%w: task is %s, not dead_lettered", ErrInvalid, t.Status)
		}
		var changes []state.Change
		switch snap.Workflow.Status {
		case state.WfFailed, state.WfCompensationFailed:
			changes = append(changes, state.Change{Payload: state.WorkflowRetried{}})
		}
		return append(changes, state.Change{TaskID: taskID, Payload: state.TaskRequeued{By: clean(by)}}), nil
	})
}

func (e *Engine) updateWorkflow(ctx context.Context, wfID uuid.UUID, decide func(*store.Tx, *state.Snapshot) ([]state.Change, error)) error {
	return e.inTx(ctx, func(tx *store.Tx) error {
		snap, err := tx.Load(ctx, wfID)
		if err != nil {
			return err
		}
		if snap.Workflow == nil {
			return ErrWorkflowNotFound
		}
		changes, err := decide(tx, snap)
		if err != nil {
			return err
		}
		err = e.commit(ctx, tx, snap, wfID, changes)
		return err
	})
}

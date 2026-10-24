package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/retry"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// progress computes what follows from a workflow's state under its
// definition (the DAG engine): ready the pending steps whose dependencies
// all succeeded, then finish the workflow if every step succeeded, or fail
// it once a step is dead-lettered and nothing that could still run is
// left, starting compensation if any is due; a compensating workflow
// advances its chain. It is a function of the snapshot and the definition
// only (new task ids aside).
func progress(s *state.Snapshot, def *workflow.Definition) []state.Change {
	w := s.Workflow
	if w == nil {
		return nil
	}
	sb := newSandbox(s, w.ID)
	if w.Status == state.WfCompensating {
		advanceCompensation(sb, def)
		return sb.changes
	}
	if w.Status != state.WfRunning || w.CancelRequested {
		return nil
	}
	steps := stepTasks(sb.s, def)

	for _, t := range steps {
		if t.Status == state.TsPending && depsSucceeded(sb.s, def, t.StepName) {
			sb.add(t.ID, state.TaskReady{})
		}
	}

	allDone, anyActive := true, false
	var cause *state.Task
	for _, t := range steps {
		switch t.Status {
		case state.TsSucceeded:
			continue
		case state.TsReady, state.TsRunning, state.TsRetryWait:
			anyActive = true
		case state.TsDeadLettered:
			if cause == nil {
				cause = t
			}
		}
		allDone = false
	}
	switch {
	case len(steps) > 0 && allDone:
		sb.add(uuid.Nil, state.WorkflowSucceeded{})
	case cause != nil && !anyActive:
		failWorkflow(sb, def, cause.ID)
	}
	return sb.changes
}

// failWorkflow fails a running workflow: cancel any step still in flight
// or queued (only the compensate decision fails a workflow with siblings
// active), skip the steps that can no longer run, and start compensating
// the steps that completed.
func failWorkflow(sb *sandbox, def *workflow.Definition, cause uuid.UUID) {
	sb.add(uuid.Nil, state.WorkflowFailed{CauseTaskID: cause})
	for _, t := range stepTasks(sb.s, nil) {
		switch t.Status {
		case state.TsReady, state.TsRunning, state.TsRetryWait:
			sb.add(t.ID, state.TaskCancelled{})
		case state.TsPending:
			sb.add(t.ID, state.TaskSkipped{})
		}
	}
	startCompensation(sb, def)
}

func depsSucceeded(s *state.Snapshot, def *workflow.Definition, step string) bool {
	st, ok := def.Step(step)
	if !ok {
		return true
	}
	for _, dep := range st.DependsOn {
		t := findTask(s, dep, state.KindStep)
		if t == nil || t.Status != state.TsSucceeded {
			return false
		}
	}
	return true
}

// stepTasks returns the step tasks in canonical topological order (by
// name for steps the definition does not know, or with def nil).
func stepTasks(s *state.Snapshot, def *workflow.Definition) []*state.Task {
	rank := map[string]int{}
	if def != nil {
		for i, n := range def.Order {
			rank[n] = i
		}
	}
	var ts []*state.Task
	for _, t := range s.Tasks {
		if t.Kind == state.KindStep {
			ts = append(ts, t)
		}
	}
	slices.SortFunc(ts, func(a, b *state.Task) int {
		ra, aok := rank[a.StepName]
		rb, bok := rank[b.StepName]
		switch {
		case aok && bok && ra != rb:
			return ra - rb
		case aok != bok:
			if aok {
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

func findTask(s *state.Snapshot, step string, kind state.TaskKind) *state.Task {
	for _, t := range s.Tasks {
		if t.StepName == step && t.Kind == kind {
			return t
		}
	}
	return nil
}

// sandbox applies proposed changes to a copy of a snapshot, so later
// decisions in the same transaction see the effect of earlier ones.
type sandbox struct {
	s       *state.Snapshot
	wfID    uuid.UUID
	changes []state.Change
	err     error
}

func newSandbox(s *state.Snapshot, wfID uuid.UUID) *sandbox {
	return &sandbox{s: s.Clone(), wfID: wfID}
}

func (sb *sandbox) add(task uuid.UUID, p state.Payload) {
	if sb.err != nil {
		return
	}
	seq := 1
	if sb.s.Workflow != nil {
		seq = sb.s.Workflow.LastSeq + 1
	}
	ev := state.Event{WorkflowID: sb.wfID, Seq: seq, TaskID: task, OccurredAt: time.Unix(1, 0), Payload: p}
	if err := state.Apply(sb.s, ev); err != nil {
		sb.err = err
		return
	}
	sb.changes = append(sb.changes, state.Change{TaskID: task, Payload: p})
}

func (sb *sandbox) addAll(cs []state.Change) {
	for _, c := range cs {
		sb.add(c.TaskID, c.Payload)
	}
}

// definitions caches parsed definitions; they are immutable once stored.
type definitions struct {
	store *store.Store
	mu    sync.Mutex
	byID  map[uuid.UUID]*workflow.Definition
}

func (d *definitions) get(ctx context.Context, id uuid.UUID) (*workflow.Definition, error) {
	d.mu.Lock()
	def, ok := d.byID[id]
	d.mu.Unlock()
	if ok {
		return def, nil
	}
	spec, err := d.store.Definition(ctx, id)
	if err != nil {
		return nil, err
	}
	if def, err = workflow.Decode(spec); err != nil {
		return nil, err
	}
	d.mu.Lock()
	if len(d.byID) > 10_000 { // bound memory; definitions are cheap to reload
		clear(d.byID)
	}
	d.byID[id] = def
	d.mu.Unlock()
	return def, nil
}

func (d *definitions) put(id uuid.UUID, def *workflow.Definition) {
	d.mu.Lock()
	d.byID[id] = def
	d.mu.Unlock()
}

// commit appends changes plus everything progress derives from them, in
// the caller's transaction, and performs the transaction's side effects:
// revoking leases of cancelled tasks, queueing outbox messages for
// terminal workflows, and waking claimers when tasks became ready.
func (e *Engine) commit(ctx context.Context, tx *store.Tx, snap *state.Snapshot, wfID uuid.UUID, changes []state.Change) error {
	sb := newSandbox(snap, wfID)
	sb.addAll(changes)
	if sb.err != nil {
		return sb.err
	}
	def, err := e.defs.get(ctx, sb.s.Workflow.DefinitionID)
	if err != nil {
		return err
	}
	all := append(slices.Clone(changes), progress(sb.s, def)...)
	events, err := tx.Append(ctx, snap, wfID, all)
	if err != nil {
		return err
	}
	var cancelled []uuid.UUID
	for _, ev := range events {
		switch ev.Payload.(type) {
		case state.TaskCancelled:
			cancelled = append(cancelled, ev.TaskID)
		case state.TaskReady, state.TaskRequeued:
			tx.AfterCommit(e.ready.notify)
		case state.WorkflowSucceeded, state.WorkflowFailed, state.WorkflowCancelled,
			state.CompensationCompleted, state.CompensationFailed:
			if snap.Workflow.Status.Terminal() {
				if err := e.queueTerminal(ctx, tx, snap.Workflow); err != nil {
					return err
				}
			}
		}
	}
	if len(cancelled) > 0 {
		return tx.RevokeLeases(ctx, cancelled)
	}
	return nil
}

// TerminalMessage is the body of a workflow.terminal webhook.
type TerminalMessage struct {
	WorkflowID uuid.UUID            `json:"workflow_id"`
	Status     state.WorkflowStatus `json:"status"`
	FinishedAt *time.Time           `json:"finished_at,omitempty"`
}

func (e *Engine) queueTerminal(ctx context.Context, tx *store.Tx, w *state.Workflow) error {
	sub, err := tx.Submission(ctx, w.ID)
	if err != nil || sub.CallbackURL == "" {
		return err
	}
	body, err := json.Marshal(TerminalMessage{WorkflowID: w.ID, Status: w.Status, FinishedAt: w.FinishedAt})
	if err != nil {
		return err
	}
	return tx.InsertOutbox(ctx, "workflow.terminal", fmt.Sprintf("%s:%d", w.ID, w.LastSeq), sub.CallbackURL, body)
}

// stepSettings returns the retry policy and per-attempt timeout of a task.
func (e *Engine) stepSettings(def *workflow.Definition, t *state.Task) (retry.Policy, time.Duration) {
	p, timeout := retry.DefaultPolicy(), e.cfg.DefaultTimeout
	if st, ok := def.Step(t.StepName); ok {
		p, timeout = st.Retry, st.Timeout
		if t.Kind == state.KindCompensation && st.Compensate != nil {
			p, timeout = st.Compensate.Retry, st.Compensate.Timeout
		}
	}
	if e.retryPolicy != nil {
		p = e.retryPolicy(t)
	}
	if e.timeout != nil {
		timeout = e.timeout(t)
	}
	return p, timeout
}

// handlerInput merges the workflow input with the step's own input.
func handlerInput(def *workflow.Definition, sub state.WorkflowSubmitted, t *state.Task) json.RawMessage {
	var stepInput json.RawMessage
	if st, ok := def.Step(t.StepName); ok {
		stepInput = st.Input
	}
	in, err := workflow.MergeInput(sub.Input, stepInput)
	if err != nil {
		return stepInput // validated at submission; unreachable in practice
	}
	return in
}

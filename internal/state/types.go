// Package state is the workflow and task state machine (SPEC 3).
//
// Apply is the only way state changes. It is a pure function of the
// current snapshot and one event: no clocks, randomness, I/O or decision
// policies. The live write path and replay both use it, which is what
// makes replay reproduce the materialized projections exactly.
package state

import (
	"maps"
	"time"

	"github.com/google/uuid"
)

// WorkflowStatus is a workflow's state (SPEC 3.2).
type WorkflowStatus string

const (
	WfRunning            WorkflowStatus = "running"
	WfDeferred           WorkflowStatus = "deferred"
	WfPaused             WorkflowStatus = "paused"
	WfSucceeded          WorkflowStatus = "succeeded"
	WfFailed             WorkflowStatus = "failed"
	WfCompensating       WorkflowStatus = "compensating"
	WfCompensated        WorkflowStatus = "compensated"
	WfCompensationFailed WorkflowStatus = "compensation_failed"
	WfCancelled          WorkflowStatus = "cancelled"
)

// Terminal reports whether no further transitions happen without a
// manual DLQ retry.
func (s WorkflowStatus) Terminal() bool {
	switch s {
	case WfSucceeded, WfFailed, WfCompensated, WfCompensationFailed, WfCancelled:
		return true
	}
	return false
}

// TaskStatus is a task's state (SPEC 3.1).
type TaskStatus string

const (
	TsPending      TaskStatus = "pending"
	TsReady        TaskStatus = "ready"
	TsRunning      TaskStatus = "running"
	TsRetryWait    TaskStatus = "retry_wait"
	TsSucceeded    TaskStatus = "succeeded"
	TsDeadLettered TaskStatus = "dead_lettered"
	TsCancelled    TaskStatus = "cancelled"
	TsSkipped      TaskStatus = "skipped"
)

// Terminal reports whether the task will not be scheduled again without
// a manual DLQ retry.
func (s TaskStatus) Terminal() bool {
	switch s {
	case TsSucceeded, TsDeadLettered, TsCancelled, TsSkipped:
		return true
	}
	return false
}

// TaskKind distinguishes forward steps from saga compensations.
type TaskKind string

const (
	KindStep         TaskKind = "step"
	KindCompensation TaskKind = "compensation"
)

// Workflow is the projection of one workflow (table workflows).
type Workflow struct {
	ID              uuid.UUID
	DefinitionID    uuid.UUID
	Status          WorkflowStatus
	Priority        int
	SubmitKey       *string
	CancelRequested bool
	LastSeq         int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	FinishedAt      *time.Time
}

// Task is the projection of one task (table tasks).
type Task struct {
	ID             uuid.UUID
	WorkflowID     uuid.UUID
	StepName       string
	Kind           TaskKind
	Handler        string
	Queue          string
	Priority       int
	Status         TaskStatus
	Attempt        int
	MaxAttempts    int
	CurrentAttempt *uuid.UUID
	WorkerID       *string
	RunAt          *time.Time
	ReadyAt        *time.Time
	IdempotencyKey string
	LastError      *string
	UpdatedAt      time.Time
}

// Snapshot is the full state of one workflow: the workflow row and all
// of its tasks. Workflow is nil before WorkflowSubmitted.
type Snapshot struct {
	Workflow *Workflow
	Tasks    map[uuid.UUID]*Task

	touched map[uuid.UUID]struct{}
}

// NewSnapshot returns an empty snapshot, the state before any event.
func NewSnapshot() *Snapshot {
	return &Snapshot{Tasks: map[uuid.UUID]*Task{}}
}

// Touched returns the ids of tasks changed by Apply since the last
// ResetTouched, so writers persist only what changed.
func (s *Snapshot) Touched() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(s.touched))
	for id := range s.touched {
		ids = append(ids, id)
	}
	return ids
}

// ResetTouched clears the set returned by Touched.
func (s *Snapshot) ResetTouched() { s.touched = nil }

func (s *Snapshot) touch(id uuid.UUID) {
	if s.touched == nil {
		s.touched = map[uuid.UUID]struct{}{}
	}
	s.touched[id] = struct{}{}
}

// Clone returns a deep copy, so a batch of events can be applied
// tentatively and discarded on error.
func (s *Snapshot) Clone() *Snapshot {
	c := &Snapshot{Tasks: make(map[uuid.UUID]*Task, len(s.Tasks)), touched: maps.Clone(s.touched)}
	if s.Workflow != nil {
		w := *s.Workflow
		w.SubmitKey = clonePtr(w.SubmitKey)
		w.FinishedAt = clonePtr(w.FinishedAt)
		c.Workflow = &w
	}
	for id, t := range s.Tasks {
		c.Tasks[id] = t.clone()
	}
	return c
}

func (t *Task) clone() *Task {
	c := *t
	c.CurrentAttempt = clonePtr(t.CurrentAttempt)
	c.WorkerID = clonePtr(t.WorkerID)
	c.RunAt = clonePtr(t.RunAt)
	c.ReadyAt = clonePtr(t.ReadyAt)
	c.LastError = clonePtr(t.LastError)
	return &c
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

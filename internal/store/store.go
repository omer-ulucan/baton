package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/state"
)

// Store is the event log plus projections, in one Postgres database.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store over pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Fence identifies the writer. Its token is stored on every event; from
// phase 5 a trigger rejects tokens that are not the current leader's.
type Fence struct {
	Token int64
}

// DecideFunc computes the changes to make to a workflow, given its current
// state and the database's clock. It runs inside the transaction while the
// workflow row is locked, so it must be fast and must not mutate s. It
// must never do slow work such as inference (SPEC 10.2).
type DecideFunc func(s *state.Snapshot, now time.Time) ([]state.Change, error)

// Update runs one state change of one workflow (SPEC 5.1): it locks the
// workflow, loads its snapshot, asks decide for changes, applies them,
// appends the events and writes the touched projection rows, all in one
// transaction. It returns the new snapshot and the appended events. If
// decide returns no changes, nothing is written.
func (s *Store) Update(ctx context.Context, fence Fence, wfID uuid.UUID, decide DecideFunc) (*state.Snapshot, []state.Event, error) {
	var (
		snap   *state.Snapshot
		events []state.Event
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return fmt.Errorf("read clock: %w", err)
		}
		var err error
		if snap, err = loadSnapshot(ctx, tx, wfID, true); err != nil {
			return err
		}
		changes, err := decide(snap, now)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			return nil
		}
		snap.ResetTouched()
		seq := 0
		if snap.Workflow != nil {
			seq = snap.Workflow.LastSeq
		}
		events = make([]state.Event, len(changes))
		for i, c := range changes {
			events[i] = state.Event{WorkflowID: wfID, Seq: seq + i + 1, TaskID: c.TaskID, OccurredAt: now, Payload: c.Payload}
			if err := state.Apply(snap, events[i]); err != nil {
				return err
			}
		}
		if err := appendEvents(ctx, tx, fence, events); err != nil {
			return err
		}
		return writeProjection(ctx, tx, snap)
	})
	if err != nil {
		return nil, nil, err
	}
	return snap, events, nil
}

// Snapshot reads a workflow's projection. A workflow that does not exist
// yields an empty snapshot.
func (s *Store) Snapshot(ctx context.Context, wfID uuid.UUID) (*state.Snapshot, error) {
	return loadSnapshot(ctx, s.pool, wfID, false)
}

// Events returns a workflow's history in order.
func (s *Store) Events(ctx context.Context, wfID uuid.UUID) ([]state.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, type, task_id, payload, occurred_at
		  FROM workflow_events WHERE workflow_id = $1 ORDER BY seq`, wfID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	var events []state.Event
	for rows.Next() {
		var (
			ev      = state.Event{WorkflowID: wfID}
			typ     string
			taskID  *uuid.UUID
			payload []byte
		)
		if err := rows.Scan(&ev.Seq, &typ, &taskID, &payload, &ev.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if taskID != nil {
			ev.TaskID = *taskID
		}
		if ev.Payload, err = state.DecodePayload(state.EventType(typ), payload); err != nil {
			return nil, fmt.Errorf("workflow %s seq %d: %w", wfID, ev.Seq, err)
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const workflowColumns = `id, definition_id, status, priority, submit_key, cancel_requested,
	last_seq, created_at, updated_at, finished_at`

const taskColumns = `id, workflow_id, step_name, kind, handler, queue, priority, status,
	attempt, max_attempts, current_attempt, worker_id, run_at, ready_at, idempotency_key,
	last_error, updated_at`

func loadSnapshot(ctx context.Context, q querier, wfID uuid.UUID, lock bool) (*state.Snapshot, error) {
	sql := `SELECT ` + workflowColumns + ` FROM workflows WHERE id = $1`
	if lock {
		// Serializes every writer of this workflow (SPEC 5.1).
		sql += ` FOR UPDATE`
	}
	snap := state.NewSnapshot()
	var w state.Workflow
	err := q.QueryRow(ctx, sql, wfID).Scan(&w.ID, &w.DefinitionID, &w.Status, &w.Priority, &w.SubmitKey,
		&w.CancelRequested, &w.LastSeq, &w.CreatedAt, &w.UpdatedAt, &w.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return snap, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load workflow %s: %w", wfID, err)
	}
	w.CreatedAt, w.UpdatedAt, w.FinishedAt = w.CreatedAt.UTC(), w.UpdatedAt.UTC(), utcPtr(w.FinishedAt)
	snap.Workflow = &w

	rows, err := q.Query(ctx, `SELECT `+taskColumns+` FROM tasks WHERE workflow_id = $1`, wfID)
	if err != nil {
		return nil, fmt.Errorf("load tasks of %s: %w", wfID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var t state.Task
		if err := rows.Scan(&t.ID, &t.WorkflowID, &t.StepName, &t.Kind, &t.Handler, &t.Queue, &t.Priority,
			&t.Status, &t.Attempt, &t.MaxAttempts, &t.CurrentAttempt, &t.WorkerID, &t.RunAt, &t.ReadyAt,
			&t.IdempotencyKey, &t.LastError, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		t.RunAt, t.ReadyAt, t.UpdatedAt = utcPtr(t.RunAt), utcPtr(t.ReadyAt), t.UpdatedAt.UTC()
		snap.Tasks[t.ID] = &t
	}
	return snap, rows.Err()
}

func appendEvents(ctx context.Context, tx pgx.Tx, fence Fence, events []state.Event) error {
	var b pgx.Batch
	for _, ev := range events {
		payload, err := state.EncodePayload(ev.Payload)
		if err != nil {
			return err
		}
		var taskID *uuid.UUID
		if ev.TaskID != uuid.Nil {
			taskID = &ev.TaskID
		}
		b.Queue(`INSERT INTO workflow_events
			(workflow_id, seq, type, task_id, payload, occurred_at, fencing_token)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			ev.WorkflowID, ev.Seq, string(ev.Type()), taskID, payload, ev.OccurredAt, fence.Token)
	}
	if err := tx.SendBatch(ctx, &b).Close(); err != nil {
		return fmt.Errorf("append events: %w", err)
	}
	return nil
}

func writeProjection(ctx context.Context, tx pgx.Tx, snap *state.Snapshot) error {
	var b pgx.Batch
	w := snap.Workflow
	b.Queue(`INSERT INTO workflows (`+workflowColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status, priority = EXCLUDED.priority,
			cancel_requested = EXCLUDED.cancel_requested, last_seq = EXCLUDED.last_seq,
			updated_at = EXCLUDED.updated_at, finished_at = EXCLUDED.finished_at`,
		w.ID, w.DefinitionID, string(w.Status), w.Priority, w.SubmitKey, w.CancelRequested,
		w.LastSeq, w.CreatedAt, w.UpdatedAt, w.FinishedAt)
	for _, id := range snap.Touched() {
		t := snap.Tasks[id]
		b.Queue(`INSERT INTO tasks (`+taskColumns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT (id) DO UPDATE SET
				status = EXCLUDED.status, priority = EXCLUDED.priority, attempt = EXCLUDED.attempt,
				current_attempt = EXCLUDED.current_attempt, worker_id = EXCLUDED.worker_id,
				run_at = EXCLUDED.run_at, ready_at = EXCLUDED.ready_at,
				last_error = EXCLUDED.last_error, updated_at = EXCLUDED.updated_at`,
			t.ID, t.WorkflowID, t.StepName, string(t.Kind), t.Handler, t.Queue, t.Priority, string(t.Status),
			t.Attempt, t.MaxAttempts, t.CurrentAttempt, t.WorkerID, t.RunAt, t.ReadyAt, t.IdempotencyKey,
			t.LastError, t.UpdatedAt)
	}
	if err := tx.SendBatch(ctx, &b).Close(); err != nil {
		return fmt.Errorf("write projection: %w", err)
	}
	return nil
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

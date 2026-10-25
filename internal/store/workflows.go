package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/omer-ulucan/baton/internal/state"
)

// InsertDefinition stores a definition, deduplicated by its hash, and
// returns its id.
//
// The lookup after a conflict must be a separate statement: a statement's
// snapshot predates a concurrent inserter's commit, so a combined
// INSERT ... ON CONFLICT DO NOTHING / SELECT can see neither row. Under
// READ COMMITTED the second statement takes a fresh snapshot.
func (t *Tx) InsertDefinition(ctx context.Context, name string, version int, sha []byte, spec []byte) (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	var got uuid.UUID
	err = t.tx.QueryRow(ctx, `
		INSERT INTO workflow_definitions (id, name, version, sha256, spec, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (sha256) DO NOTHING
		RETURNING id`, id, name, version, sha, spec, t.now).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		err = t.tx.QueryRow(ctx, `SELECT id FROM workflow_definitions WHERE sha256 = $1`, sha).Scan(&got)
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert definition: %w", err)
	}
	return got, nil
}

// Definition returns a stored definition's spec, read in the transaction.
// Code holding a transaction must never take a second connection from the
// pool: with the pool exhausted by transactions waiting on locks the
// holder has, that wait never ends and Postgres cannot see it as a deadlock.
func (t *Tx) Definition(ctx context.Context, id uuid.UUID) ([]byte, error) {
	var spec []byte
	if err := t.tx.QueryRow(ctx, `SELECT spec FROM workflow_definitions WHERE id = $1`, id).Scan(&spec); err != nil {
		return nil, fmt.Errorf("definition %s: %w", id, err)
	}
	return spec, nil
}

// Submission returns what a workflow was submitted with: its input and
// callback URL, from the WorkflowSubmitted event.
func (t *Tx) Submission(ctx context.Context, wfID uuid.UUID) (state.WorkflowSubmitted, error) {
	var raw []byte
	err := t.tx.QueryRow(ctx, `SELECT payload FROM workflow_events WHERE workflow_id = $1 AND seq = 1`, wfID).Scan(&raw)
	if err != nil {
		return state.WorkflowSubmitted{}, fmt.Errorf("submission of %s: %w", wfID, err)
	}
	p, err := state.DecodePayload(state.EvWorkflowSubmitted, raw)
	if err != nil {
		return state.WorkflowSubmitted{}, err
	}
	return p.(state.WorkflowSubmitted), nil
}

// WorkflowBySubmitKey finds a workflow by its client idempotency key.
func (s *Store) WorkflowBySubmitKey(ctx context.Context, key string) (uuid.UUID, bool, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT id FROM workflows WHERE submit_key = $1`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("workflow by submit key: %w", err)
	}
	return id, true, nil
}

// ListWorkflows returns up to limit workflows with id greater than after
// (keyset pagination; ids are UUIDv7 so this is submission order),
// optionally filtered by status.
func (s *Store) ListWorkflows(ctx context.Context, status string, after uuid.UUID, limit int) ([]state.Workflow, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+workflowColumns+` FROM workflows
		WHERE ($1 = '' OR status = $1) AND id > $2 ORDER BY id LIMIT $3`, status, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	defer rows.Close()
	var out []state.Workflow
	for rows.Next() {
		var w state.Workflow
		if err := rows.Scan(&w.ID, &w.DefinitionID, &w.Status, &w.Priority, &w.SubmitKey,
			&w.CancelRequested, &w.LastSeq, &w.CreatedAt, &w.UpdatedAt, &w.FinishedAt); err != nil {
			return nil, err
		}
		w.CreatedAt, w.UpdatedAt, w.FinishedAt = w.CreatedAt.UTC(), w.UpdatedAt.UTC(), utcPtr(w.FinishedAt)
		out = append(out, w)
	}
	return out, rows.Err()
}

// DLQEntry is a dead-lettered task with the last decision about it.
type DLQEntry struct {
	Task         state.Task
	Decision     *state.DecisionMade // nil if the task was never triaged (e.g. lease expiry)
	DeadLettered time.Time
}

// ListDLQ returns dead-lettered tasks, keyset-paginated by task id.
func (s *Store) ListDLQ(ctx context.Context, queue string, after uuid.UUID, limit int) ([]DLQEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+prefixed("t.", taskColumns)+`,
		       (SELECT payload FROM workflow_events e
		         WHERE e.workflow_id = t.workflow_id AND e.task_id = t.id AND e.type = 'DecisionMade'
		         ORDER BY e.seq DESC LIMIT 1)
		  FROM tasks t
		 WHERE t.status = 'dead_lettered' AND ($1 = '' OR t.queue = $1) AND t.id > $2
		 ORDER BY t.id LIMIT $3`, queue, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}
	defer rows.Close()
	var out []DLQEntry
	for rows.Next() {
		var (
			e   DLQEntry
			t   = &e.Task
			raw []byte
		)
		if err := rows.Scan(&t.ID, &t.WorkflowID, &t.StepName, &t.Kind, &t.Handler, &t.Queue, &t.Priority,
			&t.Status, &t.Attempt, &t.MaxAttempts, &t.CurrentAttempt, &t.WorkerID, &t.RunAt, &t.ReadyAt,
			&t.IdempotencyKey, &t.LastError, &t.UpdatedAt, &raw); err != nil {
			return nil, err
		}
		t.UpdatedAt = t.UpdatedAt.UTC()
		e.DeadLettered = t.UpdatedAt
		if raw != nil {
			var d state.DecisionMade
			if err := json.Unmarshal(raw, &d); err == nil {
				e.Decision = &d
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func prefixed(prefix, columns string) string {
	cols := strings.Split(columns, ",")
	for i, c := range cols {
		cols[i] = prefix + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Lease is a worker's exclusive right to run one attempt (table task_leases).
type Lease struct {
	TaskID     uuid.UUID
	WorkflowID uuid.UUID
	AttemptID  uuid.UUID
	WorkerID   string
	LeaseUntil time.Time
	Revoked    bool
}

// TaskRef locates a task.
type TaskRef struct {
	TaskID     uuid.UUID
	WorkflowID uuid.UUID
}

// UpsertWorker records a worker's registration.
func (t *Tx) UpsertWorker(ctx context.Context, id string, queues, handlers []string, version string) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO workers (id, queues, handlers, version, registered_at, last_seen_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (id) DO UPDATE SET queues = EXCLUDED.queues, handlers = EXCLUDED.handlers,
			version = EXCLUDED.version, last_seen_at = EXCLUDED.last_seen_at`,
		id, queues, handlers, version, t.now)
	if err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}
	return nil
}

// TouchWorker marks a worker as seen and reports whether it is registered.
func (t *Tx) TouchWorker(ctx context.Context, id string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `UPDATE workers SET last_seen_at = $2 WHERE id = $1`, id, t.now)
	if err != nil {
		return false, fmt.Errorf("touch worker: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Worker returns a registered worker's queues and handlers.
func (t *Tx) Worker(ctx context.Context, id string) (queues, handlers []string, ok bool, err error) {
	err = t.tx.QueryRow(ctx, `SELECT queues, handlers FROM workers WHERE id = $1`, id).Scan(&queues, &handlers)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("load worker: %w", err)
	}
	return queues, handlers, true, nil
}

// Candidate is a claimable task.
type Candidate struct {
	TaskRef
	Queue string
}

// ClaimCandidates locks up to limit ready tasks a worker may run, together
// with their workflow rows, best first by aged priority (SPEC 9.2). It
// locks task before workflow, against the usual order, so it uses SKIP
// LOCKED for both: it never waits, and a workflow being written by someone
// else is simply skipped this round.
func (t *Tx) ClaimCandidates(ctx context.Context, queues, handlers []string, limit, agingSeconds int) ([]Candidate, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT t.id, t.workflow_id, t.queue
		  FROM tasks t JOIN workflows w ON w.id = t.workflow_id
		 WHERE t.status = 'ready'
		   AND t.queue = ANY($2) AND t.handler = ANY($3)
		   AND ((t.kind = 'step' AND w.status = 'running')
		     OR (t.kind = 'compensation' AND w.status = 'compensating'))
		 ORDER BY `+effectivePriority(agingSeconds)+` DESC, t.ready_at, t.id
		 LIMIT $4
		 FOR UPDATE OF t, w SKIP LOCKED`, t.now, queues, handlers, limit)
	if err != nil {
		return nil, fmt.Errorf("claim candidates: %w", err)
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var c Candidate
		if err := rows.Scan(&c.TaskID, &c.WorkflowID, &c.Queue); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// InsertLease grants a lease for a claimed attempt, valid for ttl from now.
func (t *Tx) InsertLease(ctx context.Context, ref TaskRef, attemptID uuid.UUID, workerID string, ttl time.Duration) (time.Time, error) {
	until := t.now.Add(ttl)
	_, err := t.tx.Exec(ctx, `INSERT INTO task_leases (task_id, attempt_id, worker_id, lease_until)
		VALUES ($1, $2, $3, $4)`, ref.TaskID, attemptID, workerID, until)
	if err != nil {
		return time.Time{}, fmt.Errorf("insert lease: %w", err)
	}
	return until, nil
}

// LockLease locks the lease of an attempt. Callers must hold the task's
// workflow lock first. It returns nil when the attempt has no lease.
func (t *Tx) LockLease(ctx context.Context, attemptID uuid.UUID) (*Lease, error) {
	var l Lease
	err := t.tx.QueryRow(ctx, `
		SELECT l.task_id, tk.workflow_id, l.attempt_id, l.worker_id, l.lease_until, l.revoked
		  FROM task_leases l JOIN tasks tk ON tk.id = l.task_id
		 WHERE l.attempt_id = $1 FOR UPDATE OF l`, attemptID).
		Scan(&l.TaskID, &l.WorkflowID, &l.AttemptID, &l.WorkerID, &l.LeaseUntil, &l.Revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock lease: %w", err)
	}
	l.LeaseUntil = l.LeaseUntil.UTC()
	return &l, nil
}

// DeleteLease ends the lease of a task.
func (t *Tx) DeleteLease(ctx context.Context, taskID uuid.UUID) error {
	if _, err := t.tx.Exec(ctx, `DELETE FROM task_leases WHERE task_id = $1`, taskID); err != nil {
		return fmt.Errorf("delete lease: %w", err)
	}
	return nil
}

// ExtendLeases moves lease_until to now+ttl for the worker's live,
// unrevoked leases among attemptIDs and returns the extended ones.
// Expired leases are not revived: the reaper owns them.
func (t *Tx) ExtendLeases(ctx context.Context, workerID string, attemptIDs []uuid.UUID, ttl time.Duration) (map[uuid.UUID]time.Time, error) {
	rows, err := t.tx.Query(ctx, `
		UPDATE task_leases SET lease_until = $3
		 WHERE attempt_id = ANY($1) AND worker_id = $2 AND NOT revoked AND lease_until > $4
		RETURNING attempt_id, lease_until`, attemptIDs, workerID, t.now.Add(ttl), t.now)
	if err != nil {
		return nil, fmt.Errorf("extend leases: %w", err)
	}
	defer rows.Close()
	out := map[uuid.UUID]time.Time{}
	for rows.Next() {
		var id uuid.UUID
		var until time.Time
		if err := rows.Scan(&id, &until); err != nil {
			return nil, err
		}
		out[id] = until.UTC()
	}
	return out, rows.Err()
}

// Leases returns the current leases among attemptIDs, without locking.
func (t *Tx) Leases(ctx context.Context, attemptIDs []uuid.UUID) (map[uuid.UUID]Lease, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT l.task_id, tk.workflow_id, l.attempt_id, l.worker_id, l.lease_until, l.revoked
		  FROM task_leases l JOIN tasks tk ON tk.id = l.task_id
		 WHERE l.attempt_id = ANY($1)`, attemptIDs)
	if err != nil {
		return nil, fmt.Errorf("leases: %w", err)
	}
	defer rows.Close()
	out := map[uuid.UUID]Lease{}
	for rows.Next() {
		var l Lease
		if err := rows.Scan(&l.TaskID, &l.WorkflowID, &l.AttemptID, &l.WorkerID, &l.LeaseUntil, &l.Revoked); err != nil {
			return nil, err
		}
		l.LeaseUntil = l.LeaseUntil.UTC()
		out[l.AttemptID] = l
	}
	return out, rows.Err()
}

// ExpiredLeases locks up to limit unrevoked leases past lease_until, with
// their workflow rows. Lease is locked before workflow, so SKIP LOCKED.
func (t *Tx) ExpiredLeases(ctx context.Context, limit int) ([]Lease, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT l.task_id, tk.workflow_id, l.attempt_id, l.worker_id, l.lease_until, l.revoked
		  FROM task_leases l
		  JOIN tasks tk ON tk.id = l.task_id
		  JOIN workflows w ON w.id = tk.workflow_id
		 WHERE NOT l.revoked AND l.lease_until <= $1
		 ORDER BY l.lease_until
		 LIMIT $2
		 FOR UPDATE OF l, w SKIP LOCKED`, t.now, limit)
	if err != nil {
		return nil, fmt.Errorf("expired leases: %w", err)
	}
	defer rows.Close()
	var out []Lease
	for rows.Next() {
		var l Lease
		if err := rows.Scan(&l.TaskID, &l.WorkflowID, &l.AttemptID, &l.WorkerID, &l.LeaseUntil, &l.Revoked); err != nil {
			return nil, err
		}
		l.LeaseUntil = l.LeaseUntil.UTC()
		out = append(out, l)
	}
	return out, rows.Err()
}

// RevokeLeases marks the leases of the given tasks revoked, so the next
// heartbeat tells their workers to stop (SPEC 6.5). Callers hold the
// workflow lock.
func (t *Tx) RevokeLeases(ctx context.Context, taskIDs []uuid.UUID) error {
	if _, err := t.tx.Exec(ctx, `UPDATE task_leases SET revoked = true WHERE task_id = ANY($1)`, taskIDs); err != nil {
		return fmt.Errorf("revoke leases: %w", err)
	}
	return nil
}

// DueRetries locks up to limit tasks whose retry time has come, with their
// workflow rows (SKIP LOCKED, task before workflow).
func (t *Tx) DueRetries(ctx context.Context, limit int) ([]TaskRef, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT t.id, t.workflow_id
		  FROM tasks t JOIN workflows w ON w.id = t.workflow_id
		 WHERE t.status = 'retry_wait' AND t.run_at <= $1
		   AND ((t.kind = 'step' AND w.status = 'running')
		     OR (t.kind = 'compensation' AND w.status = 'compensating'))
		 ORDER BY t.run_at
		 LIMIT $2
		 FOR UPDATE OF t, w SKIP LOCKED`, t.now, limit)
	if err != nil {
		return nil, fmt.Errorf("due retries: %w", err)
	}
	return collectRefs(rows)
}

// TaskWorkflow returns the workflow of a task, without locking. Writers use
// it to find which workflow to Load before touching the task.
func (s *Store) TaskWorkflow(ctx context.Context, taskID uuid.UUID) (uuid.UUID, bool, error) {
	var wf uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT workflow_id FROM tasks WHERE id = $1`, taskID).Scan(&wf)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("task workflow: %w", err)
	}
	return wf, true, nil
}

// LastAttemptEvent returns the payload of the most recent event of type typ
// for a task, used to answer duplicate result reports idempotently.
func (t *Tx) LastAttemptEvent(ctx context.Context, ref TaskRef, typ string) ([]byte, error) {
	var payload []byte
	err := t.tx.QueryRow(ctx, `
		SELECT payload FROM workflow_events
		 WHERE workflow_id = $1 AND task_id = $2 AND type = $3
		 ORDER BY seq DESC LIMIT 1`, ref.WorkflowID, ref.TaskID, typ).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("last %s: %w", typ, err)
	}
	return payload, nil
}

func collectRefs(rows pgx.Rows) ([]TaskRef, error) {
	defer rows.Close()
	var out []TaskRef
	for rows.Next() {
		var r TaskRef
		if err := rows.Scan(&r.TaskID, &r.WorkflowID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PurgeRevokedLeases deletes revoked leases whose time has passed. They
// are kept until then so heartbeats can tell workers why they must stop.
func (t *Tx) PurgeRevokedLeases(ctx context.Context) error {
	if _, err := t.tx.Exec(ctx, `DELETE FROM task_leases WHERE revoked AND lease_until <= $1`, t.now); err != nil {
		return fmt.Errorf("purge revoked leases: %w", err)
	}
	return nil
}

// Package chaos checks Baton's invariants (SPEC 13) against a database,
// and, behind the chaos build tag, runs the fault-injection suite that
// kills workers and leaders, partitions replicas from Postgres, slows
// Postgres down and breaks the decision model.
package chaos

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
)

// Violation is one broken invariant.
type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) String() string { return v.Invariant + ": " + v.Detail }

// Check verifies every invariant over the whole database. It reads only;
// the system should be quiescent (all workflows terminal) when it runs.
func Check(ctx context.Context, pool *pgxpool.Pool) ([]Violation, error) {
	var out []Violation
	add := func(inv, format string, args ...any) {
		out = append(out, Violation{Invariant: inv, Detail: fmt.Sprintf(format, args...)})
	}

	// Termination: every workflow reached a terminal state.
	rows, err := pool.Query(ctx, `SELECT id, status FROM workflows
		WHERE status NOT IN ('succeeded', 'failed', 'compensated', 'compensation_failed', 'cancelled')`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id uuid.UUID
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			rows.Close()
			return nil, err
		}
		add("termination", "workflow %s is %s", id, status)
	}
	rows.Close()

	// Gapless history and last_seq agreement.
	var gaps int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM (
			SELECT workflow_id FROM workflow_events GROUP BY workflow_id
			HAVING min(seq) <> 1 OR max(seq) <> count(*)) g`).Scan(&gaps); err != nil {
		return nil, err
	}
	if gaps > 0 {
		add("gapless history", "%d workflows have gaps in seq", gaps)
	}

	// Fencing: tokens never decrease in commit (id) order.
	var regress int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM (
			SELECT fencing_token < lag(fencing_token) OVER (ORDER BY id) AS back FROM workflow_events) x WHERE back`).Scan(&regress); err != nil {
		return nil, err
	}
	if regress > 0 {
		add("fencing", "%d events carry a lower token than an earlier one", regress)
	}

	// Attempt budget.
	var over int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE attempt > max_attempts OR attempt < 0`).Scan(&over); err != nil {
		return nil, err
	}
	if over > 0 {
		add("attempts bounded", "%d tasks exceed their attempt budget", over)
	}

	// Replay equality.
	s := store.New(pool)
	mm, err := s.VerifyReplay(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range mm {
		add("replay", "%s", m)
	}

	// Per-workflow event-stream invariants.
	ids, err := allWorkflows(ctx, pool)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		events, err := s.Events(ctx, id)
		if err != nil {
			return nil, err
		}
		checkStream(id, events, add)
	}
	return out, nil
}

// checkStream: no overlapping attempts of a task; nothing claimed after
// WorkflowCancelled; no step claimed after cancel was requested (only
// compensations may run then, SPEC 3.2).
func checkStream(id uuid.UUID, events []state.Event, add func(inv, format string, args ...any)) {
	kind := map[uuid.UUID]state.TaskKind{}
	open := map[uuid.UUID]uuid.UUID{}
	cancelRequested, cancelled := false, false
	for _, ev := range events {
		switch p := ev.Payload.(type) {
		case state.TaskCreated:
			kind[ev.TaskID] = p.Kind
		case state.WorkflowCancelRequested:
			cancelRequested = true
		case state.WorkflowCancelled:
			cancelled = true
		case state.TaskClaimed:
			if prev, busy := open[ev.TaskID]; busy {
				add("no double claim", "workflow %s task %s claimed by %s while %s was in flight (seq %d)", id, ev.TaskID, p.AttemptID, prev, ev.Seq)
			}
			open[ev.TaskID] = p.AttemptID
			if cancelled {
				add("no run after cancel", "workflow %s: claim at seq %d after WorkflowCancelled", id, ev.Seq)
			}
			if cancelRequested && kind[ev.TaskID] == state.KindStep {
				add("no run after cancel", "workflow %s: step claimed at seq %d after cancel was requested", id, ev.Seq)
			}
		case state.TaskSucceeded, state.TaskFailed:
			if cancelled {
				add("no run after cancel", "workflow %s: %s recorded at seq %d after WorkflowCancelled", id, ev.Type(), ev.Seq)
			}
			delete(open, ev.TaskID)
		case state.TaskLeaseExpired, state.TaskReleased, state.TaskCancelled:
			delete(open, ev.TaskID)
		}
	}
}

func allWorkflows(ctx context.Context, pool *pgxpool.Pool) ([]uuid.UUID, error) {
	rows, err := pool.Query(ctx, `SELECT id FROM workflows ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

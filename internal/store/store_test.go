package store_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/state/statetest"
	"github.com/omer-ulucan/baton/internal/store"
)

// fence is the token pgtest.Lead returns on a fresh database.
var fence = store.Fence{Token: 1}

func newStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	pool, _ := pg.NewDB(t)
	if f := pgtest.Lead(t, pool); f != fence {
		t.Fatalf("fence = %v, want %v", f, fence)
	}
	_, err := pool.Exec(t.Context(), `INSERT INTO workflow_definitions (id, name, version, sha256, spec, created_at)
		VALUES ($1, 'test', 1, '\x00', '{}', now())`, statetest.DefinitionID)
	if err != nil {
		t.Fatal(err)
	}
	return store.New(pool), pool
}

func changes(c ...state.Change) store.DecideFunc {
	return func(*state.Snapshot, time.Time) ([]state.Change, error) { return c, nil }
}

func submitChanges(wfID uuid.UUID, steps ...string) []state.Change {
	c := []state.Change{{Payload: state.WorkflowSubmitted{DefinitionID: statetest.DefinitionID, Priority: 10}}}
	for _, s := range steps {
		c = append(c, state.Change{TaskID: uuid.New(), Payload: state.TaskCreated{
			Step: s, Kind: state.KindStep, Handler: "h", Queue: "q", Priority: 10, MaxAttempts: 3,
			IdempotencyKey: wfID.String() + "/" + s + "/step",
		}})
	}
	return c
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestUpdateWritesEventsAndProjection(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	wf := uuid.New()
	snap, events, err := s.Update(ctx, fence, wf, changes(submitChanges(wf, "a", "b")...))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Seq != 1 || events[2].Seq != 3 {
		t.Fatalf("events = %+v", events)
	}
	if !events[0].OccurredAt.Equal(events[2].OccurredAt) {
		t.Fatal("events of one transaction must share occurred_at")
	}
	if snap.Workflow.LastSeq != 3 || len(snap.Tasks) != 2 {
		t.Fatalf("snapshot = %+v", snap.Workflow)
	}
	if n := count(t, pool, `SELECT count(*) FROM tasks WHERE workflow_id = $1 AND status = 'pending'`, wf); n != 2 {
		t.Fatalf("%d pending tasks stored", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM workflow_events WHERE fencing_token = 1`); n != 3 {
		t.Fatalf("%d events with fencing token", n)
	}

	stored, err := s.Snapshot(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	if d := store.DiffSnapshots(snap, stored); len(d) > 0 {
		t.Fatalf("returned snapshot differs from stored: %v", d)
	}
}

func TestUpdateRollsBackRejectedBatch(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	wf := uuid.New()
	if _, _, err := s.Update(ctx, fence, wf, changes(submitChanges(wf, "a")...)); err != nil {
		t.Fatal(err)
	}
	// First change is valid, second is not: the whole batch must vanish.
	_, _, err := s.Update(ctx, fence, wf, changes(
		state.Change{Payload: state.WorkflowPaused{}},
		state.Change{Payload: state.WorkflowSucceeded{}},
	))
	if !errors.Is(err, state.ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM workflow_events WHERE workflow_id = $1`, wf); n != 2 {
		t.Fatalf("%d events after rollback, want 2", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM workflows WHERE id = $1 AND status = 'running' AND last_seq = 2`, wf); n != 1 {
		t.Fatal("workflow row changed by a rolled-back batch")
	}
}

func TestUpdateDecideErrorAndNoop(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	wf := uuid.New()
	boom := errors.New("boom")
	_, _, err := s.Update(ctx, fence, wf, func(*state.Snapshot, time.Time) ([]state.Change, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	snap, events, err := s.Update(ctx, fence, wf, changes())
	if err != nil || len(events) != 0 || snap.Workflow != nil {
		t.Fatalf("noop update: %v %v %+v", err, events, snap)
	}
	if n := count(t, pool, `SELECT count(*) FROM workflow_events`); n != 0 {
		t.Fatalf("%d events written", n)
	}
}

// TestConcurrentWritersSerialize races many writers on one workflow. Each
// toggles pause/resume based on what it reads; if the row lock did not
// serialize them, two would read the same state and one would fail or
// leave a gap.
func TestConcurrentWritersSerialize(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	wf := uuid.New()
	if _, _, err := s.Update(ctx, fence, wf, changes(submitChanges(wf, "a")...)); err != nil {
		t.Fatal(err)
	}
	const writers = 40
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for range writers {
		wg.Go(func() {
			_, _, err := s.Update(ctx, fence, wf, func(snap *state.Snapshot, _ time.Time) ([]state.Change, error) {
				if snap.Workflow.Status == state.WfPaused {
					return []state.Change{{Payload: state.WorkflowResumed{}}}, nil
				}
				return []state.Change{{Payload: state.WorkflowPaused{}}}, nil
			})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, pool, `SELECT last_seq FROM workflows WHERE id = $1`, wf); n != 2+writers {
		t.Fatalf("last_seq = %d, want %d", n, 2+writers)
	}
	assertGapless(t, pool)
}

func assertGapless(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	n := count(t, pool, `
		SELECT count(*) FROM (
			SELECT workflow_id FROM workflow_events GROUP BY workflow_id
			HAVING min(seq) <> 1 OR max(seq) <> count(*)
		) gaps`)
	if n != 0 {
		t.Fatalf("%d workflows with gaps in seq", n)
	}
	n = count(t, pool, `SELECT count(*) FROM workflows w
		WHERE last_seq <> (SELECT max(seq) FROM workflow_events e WHERE e.workflow_id = w.id)`)
	if n != 0 {
		t.Fatalf("%d workflows whose last_seq disagrees with their events", n)
	}
}

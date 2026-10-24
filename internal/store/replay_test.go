package store_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/state/statetest"
	"github.com/omer-ulucan/baton/internal/store"
)

// TestReplayMatchesProjection is the replay determinism test (SPEC 5.3):
// a concurrent random workload over many workflows, then every workflow
// is rebuilt from its events and compared column by column with the
// workflows and tasks tables.
func TestReplayMatchesProjection(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	const workflows, writers, maxBatches = 150, 8, 40
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)

	ids := make(chan uuid.UUID, workflows)
	for range workflows {
		ids <- uuid.New()
	}
	close(ids)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		r := rand.New(rand.NewPCG(seed, uint64(w)))
		wg.Go(func() {
			for wf := range ids {
				if err := drive(ctx, s, r, wf, maxBatches); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("seed %d: %v", seed, err)
	}

	if n := count(t, pool, `SELECT count(*) FROM workflows`); n != workflows {
		t.Fatalf("%d workflows stored", n)
	}
	if n := count(t, pool, `SELECT count(DISTINCT type) FROM workflow_events`); n < 20 {
		t.Fatalf("workload produced only %d event types", n)
	}
	mismatches, err := s.VerifyReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range mismatches {
		t.Errorf("seed %d: %s", seed, m)
	}
	assertGapless(t, pool)
}

// drive runs one workflow through random batches, each a separate
// transaction deciding from the state it reads back from Postgres.
func drive(ctx context.Context, s *store.Store, r *rand.Rand, wf uuid.UUID, maxBatches int) error {
	for range maxBatches {
		snap, events, err := s.Update(ctx, fence, wf, func(snap *state.Snapshot, now time.Time) ([]state.Change, error) {
			return statetest.Next(r, snap, wf, now), nil
		})
		if err != nil {
			return err
		}
		if len(events) == 0 || snap.Workflow.Status.Terminal() && r.IntN(2) == 0 {
			return nil
		}
	}
	return nil
}

func TestVerifyReplayDetectsDrift(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	wf := uuid.New()
	if _, _, err := s.Update(ctx, fence, wf, changes(submitChanges(wf, "a")...)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET attempt = 2, last_error = 'x' WHERE workflow_id = $1`, wf); err != nil {
		t.Fatal(err)
	}
	mismatches, err := s.VerifyReplay(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 1 || len(mismatches[0].Diffs) != 2 {
		t.Fatalf("mismatches = %v, want one workflow with 2 diffs", mismatches)
	}
	t.Log(mismatches[0])
}

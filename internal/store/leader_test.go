package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/state/statetest"
	"github.com/omer-ulucan/baton/internal/store"
)

func acquire(t *testing.T, s *store.Store, holder string, ttl time.Duration) (int64, bool) {
	t.Helper()
	token, ok, err := s.AcquireLeadership(t.Context(), holder, "http://"+holder, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return token, ok
}

func newLeaderStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	pool, _ := pg.NewDB(t)
	_, err := pool.Exec(t.Context(), `INSERT INTO workflow_definitions (id, name, version, sha256, spec, created_at)
		VALUES ($1, 'test', 1, '\x00', '{}', now())`, statetest.DefinitionID)
	if err != nil {
		t.Fatal(err)
	}
	return store.New(pool), pool
}

func TestLeaseSemantics(t *testing.T) {
	s, _ := newLeaderStore(t)
	a1, ok := acquire(t, s, "a", time.Hour)
	if !ok || a1 != 1 {
		t.Fatalf("first acquire = %d %t", a1, ok)
	}
	if a2, ok := acquire(t, s, "a", time.Hour); !ok || a2 != a1 {
		t.Fatalf("renewal changed the token: %d -> %d", a1, a2)
	}
	if _, ok := acquire(t, s, "b", time.Hour); ok {
		t.Fatal("b took a live lease")
	}
	li, err := s.Leader(t.Context())
	if err != nil || li.Holder != "a" || li.Addr != "http://a" || !li.Live {
		t.Fatalf("leader = %+v %v", li, err)
	}
	if err := s.ReleaseLeadership(t.Context(), "a", a1); err != nil {
		t.Fatal(err)
	}
	b, ok := acquire(t, s, "b", time.Hour)
	if !ok || b != a1+1 {
		t.Fatalf("takeover after release = %d %t", b, ok)
	}
	// A stale release must not drop the new leader's lease.
	if err := s.ReleaseLeadership(t.Context(), "a", a1); err != nil {
		t.Fatal(err)
	}
	if li, _ := s.Leader(t.Context()); li.Holder != "b" || !li.Live {
		t.Fatalf("stale release affected new leader: %+v", li)
	}
}

func TestExpiredLeaseIsRetakenWithNewToken(t *testing.T) {
	s, _ := newLeaderStore(t)
	a, _ := acquire(t, s, "a", 100*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	// Even the same holder gets a new token once its lease lapsed: it may
	// have been partitioned and must treat this as a new term.
	again, ok := acquire(t, s, "a", time.Hour)
	if !ok || again != a+1 {
		t.Fatalf("re-acquire after expiry = %d %t (was %d)", again, ok, a)
	}
}

func submitWith(t *testing.T, s *store.Store, fence store.Fence) error {
	t.Helper()
	wf := uuid.New()
	_, _, err := s.Update(t.Context(), fence, wf, changes(submitChanges(wf, "a")...))
	return err
}

func TestStaleLeaderWritesAreRejected(t *testing.T) {
	s, _ := newLeaderStore(t)
	old, _ := acquire(t, s, "a", 100*time.Millisecond)
	if err := submitWith(t, s, store.Fence{Token: old}); err != nil {
		t.Fatalf("leader write: %v", err)
	}

	// The lease lapses: even before anyone takes over, writes are refused.
	time.Sleep(150 * time.Millisecond)
	if err := submitWith(t, s, store.Fence{Token: old}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("write on expired lease: %v", err)
	}

	newer, _ := acquire(t, s, "b", time.Hour)
	if err := submitWith(t, s, store.Fence{Token: old}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale leader write after takeover: %v", err)
	}
	if err := submitWith(t, s, store.Fence{Token: newer}); err != nil {
		t.Fatalf("new leader write: %v", err)
	}
	if err := submitWith(t, s, store.Fence{Token: newer + 1}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("future token accepted: %v", err)
	}
}

// TestTriggerFencesWritesThatBypassTheStore proves the rejection lives in
// the database: raw SQL with a stale token fails too.
func TestTriggerFencesWritesThatBypassTheStore(t *testing.T) {
	pool, _ := pg.NewDB(t)
	s := store.New(pool)
	old, _ := acquire(t, s, "a", 100*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	newer, _ := acquire(t, s, "b", time.Hour)
	insert := func(token int64) error {
		_, err := pool.Exec(t.Context(), `INSERT INTO workflow_events (workflow_id, seq, type, payload, occurred_at, fencing_token)
			VALUES (gen_random_uuid(), 1, 'WorkflowSubmitted', '{}', now(), $1)`, token)
		return err
	}
	var pgErr *pgconn.PgError
	if err := insert(old); !errors.As(err, &pgErr) || pgErr.Code != "BT001" {
		t.Fatalf("stale raw insert: %v", err)
	}
	if err := insert(newer); err != nil {
		t.Fatalf("current raw insert: %v", err)
	}
}

// TestTakeoverWaitsForInFlightLeaderTransaction checks the FOR SHARE
// interlock: a follower cannot take over while the old leader's
// transaction is still open, and once it has, the old leader's next
// transaction fails.
func TestTakeoverWaitsForInFlightLeaderTransaction(t *testing.T) {
	s, pool := newLeaderStore(t)
	old, _ := acquire(t, s, "a", 200*time.Millisecond)
	fence := store.Fence{Token: old}

	inTx := make(chan struct{})
	// closing is when the old leader's transaction stops being open from
	// the client's side: just before COMMIT is sent. The server can finish
	// the commit, and release the waiting takeover, before the client sees
	// the reply, so "after the reply" would be the wrong boundary.
	closing := make(chan time.Time, 1)
	go func() {
		wf := uuid.New()
		err := s.InTx(context.Background(), fence, func(tx *store.Tx) error {
			snap, err := tx.Load(context.Background(), wf)
			if err != nil {
				return err
			}
			if _, err := tx.Append(context.Background(), snap, wf, submitChanges(wf, "a")); err != nil {
				return err
			}
			close(inTx)
			time.Sleep(600 * time.Millisecond) // a GC pause, holding the transaction open
			closing <- time.Now()
			return nil
		})
		if err != nil {
			t.Errorf("old leader's open transaction: %v", err)
		}
	}()
	<-inTx
	time.Sleep(300 * time.Millisecond) // lease has lapsed; the transaction is still open
	newer, ok := acquire(t, s, "b", time.Hour)
	tookOver := time.Now()
	if !ok || newer != old+1 {
		t.Fatalf("takeover = %d %t", newer, ok)
	}
	if openUntil := <-closing; tookOver.Before(openUntil) {
		t.Fatalf("takeover completed %s while the old leader's transaction was still open", openUntil.Sub(tookOver))
	}
	if err := submitWith(t, s, fence); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("old leader after takeover: %v", err)
	}
	// History stays ordered by term: tokens never decrease in commit order.
	if err := submitWith(t, s, store.Fence{Token: newer}); err != nil {
		t.Fatal(err)
	}
	var regressions int
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*) FROM (SELECT fencing_token < lag(fencing_token) OVER (ORDER BY id) AS back FROM workflow_events) x
		 WHERE back`).Scan(&regressions); err != nil {
		t.Fatal(err)
	}
	if regressions != 0 {
		t.Fatalf("%d events carry a lower token than an earlier event", regressions)
	}
}

func TestUpdateIsFencedEvenWithoutEvents(t *testing.T) {
	s, _ := newLeaderStore(t)
	err := s.InTx(t.Context(), store.Fence{Token: 42}, func(*store.Tx) error {
		t.Fatal("body ran for a fenced writer")
		return nil
	})
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("err = %v", err)
	}
}

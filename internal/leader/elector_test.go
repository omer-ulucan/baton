package leader_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/leader"
	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/store"
)

var pg *pgtest.Server

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		ctx := context.Background()
		var err error
		if pg, err = pgtest.Start(ctx, store.Migrate); err != nil {
			fmt.Fprintln(os.Stderr, "pgtest:", err)
			os.Exit(1)
		}
		code := m.Run()
		_ = pg.Close(ctx)
		os.Exit(code)
	}
	os.Exit(m.Run())
}

const ttl = 900 * time.Millisecond

// term records one period of leadership as seen by the lead function.
type term struct {
	replica    string
	token      int64
	start, end time.Time
}

type recorder struct {
	mu    sync.Mutex
	terms []*term
}

func (r *recorder) lead(id string) func(context.Context, store.Fence) {
	return func(ctx context.Context, f store.Fence) {
		tm := &term{replica: id, token: f.Token, start: time.Now()}
		r.mu.Lock()
		r.terms = append(r.terms, tm)
		r.mu.Unlock()
		<-ctx.Done()
		r.mu.Lock()
		tm.end = time.Now()
		r.mu.Unlock()
	}
}

func (r *recorder) snapshot() []term {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]term, len(r.terms))
	for i, t := range r.terms {
		out[i] = *t
	}
	return out
}

// waitTerm waits until the n-th term has started.
func (r *recorder) waitTerm(t *testing.T, n int, within time.Duration) term {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ts := r.snapshot(); len(ts) >= n {
			return ts[n-1]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("term %d did not start within %s", n, within)
	return term{}
}

type replica struct {
	id     string
	pool   *pgxpool.Pool
	el     *leader.Elector
	cancel context.CancelFunc
	done   chan struct{}
}

func startReplica(t *testing.T, url, id string, rec *recorder) *replica {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	el := leader.New(store.New(pool), leader.Config{ID: id, Addr: "http://" + id, TTL: ttl}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{id: id, pool: pool, el: el, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		el.Run(ctx, rec.lead(id))
	}()
	t.Cleanup(func() { cancel(); <-r.done })
	return r
}

func assertNoOverlap(t *testing.T, terms []term) {
	t.Helper()
	for i := 1; i < len(terms); i++ {
		prev, cur := terms[i-1], terms[i]
		if prev.end.IsZero() || cur.start.Before(prev.end) {
			t.Fatalf("term %d (%s, token %d) started while term %d (%s, token %d) was still leading",
				i+1, cur.replica, cur.token, i, prev.replica, prev.token)
		}
		if cur.token <= prev.token {
			t.Fatalf("token did not increase: %d -> %d", prev.token, cur.token)
		}
	}
}

func TestOneLeaderAndFailoverAfterCrash(t *testing.T) {
	_, url := pg.NewDB(t)
	rec := &recorder{}
	a := startReplica(t, url, "a", rec)
	b := startReplica(t, url, "b", rec)
	first := rec.waitTerm(t, 1, 2*ttl)
	time.Sleep(ttl) // both campaign for a while: still exactly one term
	if n := len(rec.snapshot()); n != 1 {
		t.Fatalf("%d terms while the leader was healthy", n)
	}
	leaderRep, follower := a, b
	if first.replica == "b" {
		leaderRep, follower = b, a
	}
	if !leaderRep.el.IsLeader() || follower.el.IsLeader() {
		t.Fatal("IsLeader disagrees with the elected term")
	}
	if addr, ok := follower.el.LeaderAddr(); !ok || addr != "http://"+leaderRep.id {
		t.Fatalf("follower thinks the leader is at %q (%t)", addr, ok)
	}

	// Crash: the leader loses its database connection and dies without
	// releasing the lease, so the follower must wait for it to expire.
	crashed := time.Now()
	leaderRep.pool.Close()
	leaderRep.cancel()
	second := rec.waitTerm(t, 2, 3*ttl)
	if second.replica != follower.id {
		t.Fatalf("term 2 went to %s", second.replica)
	}
	if took := second.start.Sub(crashed); took > ttl+ttl/3+300*time.Millisecond {
		t.Fatalf("failover took %s, more than TTL plus one renewal period", took)
	}
	assertNoOverlap(t, rec.snapshot())
}

func TestGracefulShutdownHandsOverQuickly(t *testing.T) {
	_, url := pg.NewDB(t)
	rec := &recorder{}
	a := startReplica(t, url, "a", rec)
	rec.waitTerm(t, 1, 2*ttl)
	b := startReplica(t, url, "b", rec)
	time.Sleep(50 * time.Millisecond)
	stopped := time.Now()
	a.cancel()
	<-a.done
	second := rec.waitTerm(t, 2, 2*ttl)
	if second.replica != b.id {
		t.Fatalf("term 2 went to %s", second.replica)
	}
	// Released, not expired: one renewal period, not a whole TTL.
	if took := second.start.Sub(stopped); took > ttl/3+300*time.Millisecond {
		t.Fatalf("handover took %s", took)
	}
	assertNoOverlap(t, rec.snapshot())
}

// TestPartitionedLeaderStepsDownBeforeTakeover cuts the leader off from
// the database while it keeps running: it must stop leading on its own
// (self-fencing) before the follower can be elected.
func TestPartitionedLeaderStepsDownBeforeTakeover(t *testing.T) {
	_, url := pg.NewDB(t)
	rec := &recorder{}
	a := startReplica(t, url, "a", rec)
	rec.waitTerm(t, 1, 2*ttl)
	b := startReplica(t, url, "b", rec)
	a.pool.Close() // partition: renewals fail, the process lives on
	second := rec.waitTerm(t, 2, 4*ttl)
	terms := rec.snapshot()
	if second.replica != b.id || terms[0].end.IsZero() {
		t.Fatalf("terms = %+v", terms)
	}
	if gap := second.start.Sub(terms[0].end); gap <= 0 {
		t.Fatalf("follower elected %s before the partitioned leader stepped down", -gap)
	}
	if a.el.IsLeader() {
		t.Fatal("partitioned replica still believes it leads")
	}
	assertNoOverlap(t, terms)
}

func TestDemoteStopsLeadingAtOnce(t *testing.T) {
	_, url := pg.NewDB(t)
	rec := &recorder{}
	a := startReplica(t, url, "a", rec)
	rec.waitTerm(t, 1, 2*ttl)
	a.el.Demote("fenced write")
	if a.el.IsLeader() {
		t.Fatal("still leading after Demote")
	}
	// It campaigns again and, holding a live lease, renews it: same term
	// token, new local term.
	second := rec.waitTerm(t, 2, 2*ttl)
	if second.replica != "a" {
		t.Fatalf("term 2 went to %s", second.replica)
	}
	assertNoOverlap(t, rec.snapshot()[:1])
}

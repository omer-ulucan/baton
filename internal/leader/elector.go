// Package leader elects one orchestrator replica as leader through the
// Postgres lease row (SPEC 6, ADR 0002) and runs the leader-only work
// while, and only while, this replica holds the lease.
package leader

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/omer-ulucan/baton/internal/store"
)

// Config tunes an Elector.
type Config struct {
	ID   string        // replica id, the lease holder
	Addr string        // address followers forward writes to
	TTL  time.Duration // lease duration, default 10s
}

// Elector campaigns for the leader lease and tracks who leads.
type Elector struct {
	store *store.Store
	cfg   Config
	log   *slog.Logger

	renewEvery    time.Duration // TTL/3
	stepDownAfter time.Duration // TTL/2 without a successful renewal

	mu        sync.Mutex
	leading   bool
	token     int64
	lastRenew time.Time // local monotonic time a successful renewal was sent
	known     store.LeaderInfo
	cancel    context.CancelFunc
	done      chan struct{}
	changes   int
}

// New builds an Elector.
func New(s *store.Store, cfg Config, log *slog.Logger) *Elector {
	if cfg.TTL <= 0 {
		cfg.TTL = 10 * time.Second
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Elector{
		store: s, cfg: cfg, log: log.With("replica", cfg.ID),
		renewEvery: cfg.TTL / 3, stepDownAfter: cfg.TTL / 2,
	}
}

// Run campaigns until ctx ends. While this replica leads, lead runs with a
// context that is cancelled the moment leadership is lost or doubted.
// On return, leadership has been released so a follower can take over at
// once.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, fence store.Fence)) {
	var wg sync.WaitGroup
	wg.Go(func() { e.watchdog(ctx) })
	tick := time.NewTicker(e.renewEvery)
	defer tick.Stop()
	for {
		e.campaign(ctx, lead)
		select {
		case <-ctx.Done():
			wg.Wait()
			e.resign(ctx)
			return
		case <-tick.C:
		}
	}
}

func (e *Elector) campaign(ctx context.Context, lead func(context.Context, store.Fence)) {
	sent := time.Now()
	rctx, cancel := context.WithTimeout(ctx, e.renewEvery)
	token, ok, err := e.store.AcquireLeadership(rctx, e.cfg.ID, e.cfg.Addr, e.cfg.TTL)
	cancel()
	switch {
	case err != nil:
		if ctx.Err() == nil {
			e.log.WarnContext(ctx, "lease renewal failed", "err", err)
		}
		// The watchdog steps down once renewals have failed for too long.
	case !ok:
		e.demote("another replica holds the lease")
	default:
		e.mu.Lock()
		renewedTerm := e.leading && e.token == token
		e.lastRenew = sent
		e.mu.Unlock()
		if !renewedTerm {
			e.demote("new term") // no-op unless the token changed under us
			e.promote(ctx, token, lead)
		}
	}
	if li, err := e.store.Leader(ctx); err == nil {
		e.mu.Lock()
		e.known = li
		e.mu.Unlock()
	}
}

// watchdog enforces self-fencing: a leader whose last successful renewal
// was sent stepDownAfter ago stops leading, well before the lease (TTL)
// can expire and a follower can take over.
func (e *Elector) watchdog(ctx context.Context) {
	tick := time.NewTicker(max(e.cfg.TTL/20, 10*time.Millisecond))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		e.mu.Lock()
		stale := e.leading && time.Since(e.lastRenew) >= e.stepDownAfter
		e.mu.Unlock()
		if stale {
			e.demote("lease renewals failing; stepping down before the lease can expire")
		}
	}
}

func (e *Elector) promote(ctx context.Context, token int64, lead func(context.Context, store.Fence)) {
	lctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	e.mu.Lock()
	e.leading, e.token, e.cancel, e.done = true, token, cancel, done
	e.changes++
	e.mu.Unlock()
	e.log.Info("elected leader", "token", token)
	go func() {
		defer close(done)
		lead(lctx, store.Fence{Token: token})
	}()
}

// Demote stops leading immediately, e.g. after a write was fenced.
func (e *Elector) Demote(reason string) { e.demote(reason) }

func (e *Elector) demote(reason string) {
	e.mu.Lock()
	if !e.leading {
		e.mu.Unlock()
		return
	}
	e.leading = false
	cancel, done, token := e.cancel, e.done, e.token
	e.changes++
	e.mu.Unlock()
	e.log.Warn("stepped down", "token", token, "reason", reason)
	cancel()
	select {
	case <-done:
	case <-time.After(e.cfg.TTL):
		e.log.Error("leader work did not stop within the lease TTL")
	}
}

// resign gives the lease up on shutdown. ctx is already done, so the
// release runs on a detached context with its own short deadline.
func (e *Elector) resign(ctx context.Context) {
	e.mu.Lock()
	leading, token := e.leading, e.token
	e.mu.Unlock()
	if !leading {
		return
	}
	e.demote("shutting down")
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := e.store.ReleaseLeadership(rctx, e.cfg.ID, token); err != nil {
		e.log.Warn("could not release the lease; followers wait for it to expire", "err", err)
		return
	}
	e.log.Info("released leadership", "token", token)
}

// IsLeader reports whether this replica currently leads.
func (e *Elector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leading
}

// Fence is the token this replica's writes carry. A replica that is not
// leading still returns its last token: the database rejects it.
func (e *Elector) Fence() store.Fence {
	e.mu.Lock()
	defer e.mu.Unlock()
	return store.Fence{Token: e.token}
}

// LeaderAddr is where writes should go: this replica's address when it
// leads, otherwise the last known live leader's.
func (e *Elector) LeaderAddr() (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.leading {
		return e.cfg.Addr, true
	}
	if e.known.Live && e.known.Holder != e.cfg.ID && e.known.Addr != "" {
		return e.known.Addr, true
	}
	return "", false
}

// Changes counts leadership transitions of this replica (for metrics).
func (e *Elector) Changes() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.changes
}

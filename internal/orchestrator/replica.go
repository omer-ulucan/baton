// Package orchestrator assembles one baton replica: store, elector,
// engine and HTTP server. cmd/baton runs one; failover and chaos tests run
// several against one database.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/laya"
	"github.com/omer-ulucan/baton/internal/laya/model"
	"github.com/omer-ulucan/baton/internal/leader"
	"github.com/omer-ulucan/baton/internal/server"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/telemetry"
)

// Options configure a replica.
type Options struct {
	Config   config.Orchestrator
	Pool     *pgxpool.Pool
	Listener net.Listener // serves HTTP; its address need not match Config.HTTPAddr
	Log      *slog.Logger
	// Engine, when set, adjusts engine options (tests use it for fast
	// retries and short polls).
	Engine func(*engine.Options)
	// Metrics and Tracer default to a fresh registry and a no-op tracer.
	Metrics *telemetry.Metrics
	Tracer  trace.TracerProvider
}

// Replica is one orchestrator process.
type Replica struct {
	cfg     config.Orchestrator
	pool    *pgxpool.Pool
	ln      net.Listener
	log     *slog.Logger
	store   *store.Store
	elector *leader.Elector
	engine  *engine.Engine
	loader  *laya.Loader // nil with the rule policy
	http    *http.Server

	killOnce sync.Once
	killed   chan struct{}
}

// New wires a replica; Run starts it.
func New(o Options) *Replica {
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("replica", o.Config.ReplicaID)
	if o.Metrics == nil {
		o.Metrics = telemetry.NewMetrics()
	}
	if o.Tracer == nil {
		o.Tracer = noop.NewTracerProvider()
	}
	st := store.New(o.Pool)
	el := leader.New(st, leader.Config{ID: o.Config.ReplicaID, Addr: o.Config.AdvertiseAddr, TTL: o.Config.LeaderTTL}, log)
	policy, loader := decisionPolicy(o.Config, log, o.Metrics.Decision)
	sources := telemetry.Sources{QueueStats: st.QueueStats, OutboxPending: st.OutboxPending, IsLeader: el.IsLeader, LeaderChanges: el.Changes}
	if loader != nil {
		sources.ModelLoaded = loader.Loaded
	}
	o.Metrics.Observe(sources)
	eo := engine.Options{
		Store: st, Log: log, Fence: el.Fence, EscalationURL: o.Config.EscalationWebhook,
		OnFenced: func() { el.Demote("write rejected by fencing") },
		Policy:   policy, Observer: o.Metrics, Tracer: o.Tracer.Tracer("baton"),
		Config: engine.Config{
			LeaseTTL: o.Config.LeaseTTL, HeartbeatInterval: o.Config.HeartbeatInterval,
			// An outer guard only: the fallback policy enforces the real deadline.
			DecisionTimeout: o.Config.TriageTimeout + time.Second,
		},
	}
	if o.Engine != nil {
		o.Engine(&eo)
	}
	eng := engine.New(eo)
	handler := server.New(server.Deps{
		DB: o.Pool, Worker: eng, Commands: eng, Reader: st,
		Leadership: el, ReplicaID: o.Config.ReplicaID, Log: log,
		Metrics: o.Metrics.Handler(), Tracer: o.Tracer,
	})
	return &Replica{
		cfg: o.Config, pool: o.Pool, ln: o.Listener, log: log, store: st, elector: el, engine: eng, loader: loader,
		http:   &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second},
		killed: make(chan struct{}),
	}
}

// Run serves until ctx ends, then shuts down gracefully: it stops leading
// and releases the lease first, so a follower takes over at once, then
// drains HTTP requests.
func (r *Replica) Run(ctx context.Context) error {
	srvCtx, stopSrv := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSrv()
	r.http.BaseContext = func(net.Listener) context.Context { return srvCtx }

	if r.loader != nil {
		// Every replica loads the model: any of them may become leader.
		go r.loader.Run(srvCtx)
		defer func() { _ = r.loader.Close() }()
	}
	errc := make(chan error, 1)
	go func() { errc <- r.http.Serve(r.ln) }()
	r.log.Info("listening", "addr", r.ln.Addr().String(), "advertise", r.cfg.AdvertiseAddr)

	elCtx, stopEl := context.WithCancel(ctx)
	defer stopEl()
	elDone := make(chan struct{})
	go func() {
		defer close(elDone)
		r.elector.Run(elCtx, func(lctx context.Context, f store.Fence) {
			r.log.Info("running leader loops", "token", f.Token)
			r.engine.Run(lctx)
		})
	}()

	select {
	case err := <-errc:
		stopEl()
		<-elDone
		return err
	case <-ctx.Done():
	case <-r.killed:
		stopEl()
		<-elDone
		return nil
	}
	stopEl()
	<-elDone // released leadership
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.ShutdownTimeout)
	defer cancel()
	stopSrv() // end long polls so Shutdown does not wait for them
	if err := r.http.Shutdown(shutdownCtx); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("shutdown: %w", err)
		}
		// Connections that never sent a request count as active for up
		// to 5s in net/http; past the drain deadline, cut what is left.
		r.log.Warn("drain deadline passed; closing remaining connections")
		_ = r.http.Close()
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// decisionPolicy builds the policy the engine uses: always the fallback
// policy, with Laya as primary when configured (SPEC 10.1).
func decisionPolicy(c config.Orchestrator, log *slog.Logger, observe func(decision.Request, decision.Decision, time.Duration)) (decision.Policy, *laya.Loader) {
	var primary decision.Policy = decision.RulePolicy{}
	var loader *laya.Loader
	if c.DecisionPolicy == "laya" {
		dir := c.ModelDir
		if dir == "" {
			var err error
			if dir, err = model.DefaultDir(); err != nil {
				log.Error("no model cache dir; decisions use the rule policy", "err", err)
			}
		}
		lp := &laya.Policy{}
		loader = laya.NewLoader(laya.LoaderConfig{
			Session: laya.SessionConfig{
				LibPath: c.ONNXRuntimeLib, ModelDir: dir, Variant: model.Variant(c.LayaVariant), MaxInflight: c.LayaMaxInflight,
			},
			Download: c.ModelDownload, Log: log,
		}, lp)
		primary = lp
	}
	return decision.FallbackPolicy{
		Primary: primary, Fallback: decision.RulePolicy{}, Threshold: c.DecisionThreshold,
		Timeouts: map[decision.Kind]time.Duration{decision.KindTriage: c.TriageTimeout, decision.KindPriority: c.PriorityTimeout},
		Observe:  observe,
	}, loader
}

// Kill stops the replica abruptly, like a crash: connections are cut and
// the database pool closed before anything is released, so the lease
// stays held until it expires. For tests and chaos runs.
func (r *Replica) Kill() {
	r.killOnce.Do(func() {
		r.elector.SkipRelease() // before anything can trigger a graceful exit
		close(r.killed)
		_ = r.http.Close()
		r.pool.Close()
	})
}

// IsLeader reports whether this replica leads.
func (r *Replica) IsLeader() bool { return r.elector.IsLeader() }

// LeaderChanges counts this replica's leadership transitions.
func (r *Replica) LeaderChanges() int { return r.elector.Changes() }

// Engine exposes the replica's engine (tests and chaos tooling).
func (r *Replica) Engine() *engine.Engine { return r.engine }

// Store exposes the replica's store.
func (r *Replica) Store() *store.Store { return r.store }

// Addr is the replica's listening address.
func (r *Replica) Addr() string { return r.ln.Addr().String() }

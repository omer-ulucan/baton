// Package engine is the orchestrator's core: the worker protocol (claim,
// heartbeat, complete, fail, release), the lease reaper and the retry
// timer. Every state change goes through store transactions and
// state.Apply; the engine only decides which events to propose.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/retry"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// Protocol errors. The HTTP layer maps them to status codes.
var (
	ErrUnknownWorker     = errors.New("worker is not registered")
	ErrNotFound          = errors.New("task not found")
	ErrAttemptSuperseded = errors.New("attempt is no longer current")
	ErrCancelled         = errors.New("workflow was cancelled")
	ErrLeaseExpired      = errors.New("lease expired")
	ErrInvalid           = errors.New("invalid request")
)

// Config tunes the engine. Zero values take the defaults in New.
type Config struct {
	LeaseTTL          time.Duration // default 30s
	HeartbeatInterval time.Duration // default 10s
	ClaimPoll         time.Duration // long-poll recheck period, default 250ms
	MaxClaimWait      time.Duration // default 30s
	MaxClaimBatch     int           // default 32
	LoopBatch         int           // reaper and retry timer batch, default 100
	ReapInterval      time.Duration // default 1s
	RetryInterval     time.Duration // default 500ms
	DecisionTimeout   time.Duration // triage budget, default 1500ms
	DefaultTimeout    time.Duration // per-attempt timeout without a step setting, default 5m
	AgingSeconds      int           // one priority point per this many seconds ready, default 30
	AdmitInterval     time.Duration // deferred-workflow admission, default 1s
}

// Options are the engine's collaborators.
type Options struct {
	Store  *store.Store
	Log    *slog.Logger
	Config Config
	// Fence returns the writer's current fencing token (leader election
	// supplies it from phase 5).
	Fence func() store.Fence
	// Policy triages failures; defaults to decision.RulePolicy.
	Policy decision.Policy
	// RetryPolicy overrides the retry policy from the task's definition
	// (tests use it for fast retries).
	RetryPolicy func(*state.Task) retry.Policy
	// Timeout overrides the per-attempt timeout from the definition.
	Timeout func(*state.Task) time.Duration
	// Rand returns uniform values in [0,1) for jitter; defaults to math/rand/v2.
	Rand func() float64
	// EscalationURL receives escalation messages via the outbox; empty
	// disables them (the task is still dead-lettered).
	EscalationURL string
	// HTTPClient delivers outbox messages; defaults to a 5s-timeout client.
	HTTPClient *http.Client
	// OnFenced is called when the database rejects a write as coming from
	// a stale leader; the replica must step down at once (SPEC 6.2).
	OnFenced func()
	// Observer receives events for metrics; defaults to none.
	Observer Observer
	// Tracer creates spans; defaults to a no-op tracer.
	Tracer trace.Tracer
}

// Engine implements the worker protocol and the leader's timer loops.
type Engine struct {
	store       *store.Store
	log         *slog.Logger
	cfg         Config
	fence       func() store.Fence
	policy      decision.Policy
	retryPolicy func(*state.Task) retry.Policy
	timeout     func(*state.Task) time.Duration
	rand        func() float64
	ready       *notifier
	defs        *definitions
	escalateURL string
	http        *http.Client
	onFenced    func()
	rates       buckets
	obs         Observer
	tracer      trace.Tracer
}

// New builds an Engine, filling defaults.
func New(o Options) *Engine {
	c := o.Config
	c.LeaseTTL = orDefault(c.LeaseTTL, 30*time.Second)
	c.HeartbeatInterval = orDefault(c.HeartbeatInterval, 10*time.Second)
	c.ClaimPoll = orDefault(c.ClaimPoll, 250*time.Millisecond)
	c.MaxClaimWait = orDefault(c.MaxClaimWait, 30*time.Second)
	c.ReapInterval = orDefault(c.ReapInterval, time.Second)
	c.RetryInterval = orDefault(c.RetryInterval, 500*time.Millisecond)
	c.DecisionTimeout = orDefault(c.DecisionTimeout, 1500*time.Millisecond)
	c.DefaultTimeout = orDefault(c.DefaultTimeout, 5*time.Minute)
	if c.MaxClaimBatch <= 0 {
		c.MaxClaimBatch = 32
	}
	if c.LoopBatch <= 0 {
		c.LoopBatch = 100
	}
	if c.AgingSeconds <= 0 {
		c.AgingSeconds = 30
	}
	c.AdmitInterval = orDefault(c.AdmitInterval, time.Second)
	e := &Engine{
		store: o.Store, log: o.Log, cfg: c, fence: o.Fence, policy: o.Policy,
		retryPolicy: o.RetryPolicy, timeout: o.Timeout, rand: o.Rand, ready: newNotifier(),
		defs:        &definitions{store: o.Store, byID: map[uuid.UUID]*workflow.Definition{}},
		escalateURL: o.EscalationURL, http: o.HTTPClient, onFenced: o.OnFenced,
		obs: o.Observer, tracer: o.Tracer,
	}
	if e.obs == nil {
		e.obs = noopObserver{}
	}
	if e.tracer == nil {
		e.tracer = noop.NewTracerProvider().Tracer("baton")
	}
	if e.http == nil {
		e.http = &http.Client{Timeout: 5 * time.Second}
	}
	if e.log == nil {
		e.log = slog.New(slog.DiscardHandler)
	}
	if e.fence == nil {
		e.fence = func() store.Fence { return store.Fence{} }
	}
	if e.policy == nil {
		e.policy = decision.RulePolicy{}
	}
	if e.rand == nil {
		e.rand = rand.Float64
	}
	return e
}

// Config returns the effective configuration.
func (e *Engine) Config() Config { return e.cfg }

// Run executes the timer loops until ctx ends.
func (e *Engine) Run(ctx context.Context) {
	reap := time.NewTicker(e.cfg.ReapInterval)
	defer reap.Stop()
	retries := time.NewTicker(e.cfg.RetryInterval)
	defer retries.Stop()
	outbox := time.NewTicker(500 * time.Millisecond)
	defer outbox.Stop()
	admit := time.NewTicker(e.cfg.AdmitInterval)
	defer admit.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-reap.C:
			if _, err := e.ReapExpired(ctx); err != nil && ctx.Err() == nil {
				e.log.ErrorContext(ctx, "lease reaper", "err", err)
			}
		case <-retries.C:
			if _, err := e.PromoteRetries(ctx); err != nil && ctx.Err() == nil {
				e.log.ErrorContext(ctx, "retry timer", "err", err)
			}
		case <-outbox.C:
			if _, err := e.RelayOutbox(ctx); err != nil && ctx.Err() == nil {
				e.log.ErrorContext(ctx, "outbox relay", "err", err)
			}
		case <-admit.C:
			if _, err := e.AdmitDeferred(ctx); err != nil && ctx.Err() == nil {
				e.log.ErrorContext(ctx, "admission", "err", err)
			}
		}
	}
}

// inTx runs a fenced write transaction and reports fencing rejections.
func (e *Engine) inTx(ctx context.Context, fn func(*store.Tx) error) error {
	err := e.store.InTx(ctx, e.fence(), fn)
	if errors.Is(err, store.ErrFenced) {
		e.obs.Fenced()
		if e.onFenced != nil {
			e.onFenced()
		}
	}
	return err
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// clean makes worker-supplied text storable: Postgres text and jsonb
// reject NUL bytes and invalid UTF-8.
func clean(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
}

func digest(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

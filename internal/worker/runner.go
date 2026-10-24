package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/protocol"
)

// Task is what a handler receives.
type Task struct {
	TaskID     uuid.UUID
	AttemptID  uuid.UUID
	WorkflowID uuid.UUID
	Step       string
	Handler    string
	Input      json.RawMessage
	// IdempotencyKey is stable across all attempts of the task. Handlers
	// use it to make their side effects effectively-once (SPEC 7).
	IdempotencyKey string
	Attempt        int
	MaxAttempts    int
}

// Handler runs one attempt. ctx ends at the attempt's deadline, when the
// orchestrator revokes the lease (e.g. the workflow was cancelled), or
// when the worker gives up draining at shutdown; handlers must return
// promptly when it does.
type Handler func(ctx context.Context, t Task) (json.RawMessage, error)

// permanent marks an error that retrying cannot fix.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent wraps err so the orchestrator is told not to retry
// (retryable=false). The decision layer still makes the final call.
func Permanent(err error) error { return permanent{err} }

// Config tunes a Runner.
type Config struct {
	ID           string
	Queues       []string
	Concurrency  int           // parallel attempts, default 4
	DrainTimeout time.Duration // at shutdown, default 25s
	ClaimWait    time.Duration // long-poll duration, default 20s
	Version      string
}

// Runner claims tasks and runs their handlers until its context ends,
// then drains: it stops claiming, lets in-flight attempts finish for up to
// DrainTimeout while heartbeating, and releases whatever is left (SPEC 8.2).
type Runner struct {
	client   *Client
	handlers map[string]Handler
	cfg      Config
	log      *slog.Logger

	mu       sync.Mutex
	inflight map[uuid.UUID]*attempt
	hbEvery  time.Duration
}

type attempt struct {
	task   protocol.Task
	cancel context.CancelFunc
	// abandoned is set when the attempt must not be reported: its lease
	// was revoked or it was released at shutdown.
	abandoned bool
}

// NewRunner builds a Runner. handlers maps handler names to functions; the
// names are registered as the worker's capabilities.
func NewRunner(client *Client, handlers map[string]Handler, cfg Config, log *slog.Logger) *Runner {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 25 * time.Second
	}
	if cfg.ClaimWait <= 0 {
		cfg.ClaimWait = 20 * time.Second
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Runner{client: client, handlers: handlers, cfg: cfg, log: log.With("worker", cfg.ID), inflight: map[uuid.UUID]*attempt{}}
}

// Run blocks until ctx ends and in-flight attempts are drained.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.handlers) == 0 {
		return errors.New("worker: no handlers")
	}
	if err := r.register(ctx); err != nil {
		return err
	}

	// Attempts and their reports outlive ctx so that draining can finish
	// them; work is cancelled only when draining gives up.
	work, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
	hbCtx, stopHB := context.WithCancel(work)
	var hbWG sync.WaitGroup
	hbWG.Go(func() { r.heartbeatLoop(hbCtx) })

	slots := make(chan struct{}, r.cfg.Concurrency)
	var running sync.WaitGroup
	backoff := newBackoff()
	for ctx.Err() == nil {
		free := cap(slots) - len(slots)
		if free == 0 {
			select {
			case slots <- struct{}{}: // wait for one slot, then give it back
				<-slots
			case <-ctx.Done():
			}
			continue
		}
		tasks, err := r.client.Claim(ctx, protocol.ClaimRequest{
			WorkerID: r.cfg.ID, Queues: r.cfg.Queues, Max: free, WaitMS: r.cfg.ClaimWait.Milliseconds(),
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if IsCode(err, protocol.CodeUnknownWorker) {
				_ = r.register(ctx)
				continue
			}
			r.log.WarnContext(ctx, "claim failed", "err", err)
			backoff.sleep(ctx)
			continue
		}
		backoff.reset()
		for _, t := range tasks {
			slots <- struct{}{}
			running.Go(func() {
				defer func() { <-slots }()
				r.execute(work, t)
			})
		}
	}

	r.log.Info("draining", "inflight", r.inflightCount(), "timeout", r.cfg.DrainTimeout)
	drained := make(chan struct{})
	go func() {
		running.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(r.cfg.DrainTimeout):
		r.releaseAll(work)
		<-drained
	}
	stopHB()
	hbWG.Wait()
	r.log.Info("stopped")
	return nil
}

func (r *Runner) register(ctx context.Context) error {
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	backoff := newBackoff()
	for {
		resp, err := r.client.Register(ctx, protocol.RegisterRequest{WorkerID: r.cfg.ID, Queues: r.cfg.Queues, Handlers: names, Version: r.cfg.Version})
		if err == nil {
			r.mu.Lock()
			r.hbEvery = time.Duration(resp.HeartbeatIntervalMS) * time.Millisecond
			r.mu.Unlock()
			r.log.InfoContext(ctx, "registered", "queues", r.cfg.Queues, "handlers", names)
			return nil
		}
		r.log.WarnContext(ctx, "register failed", "err", err)
		if !backoff.sleep(ctx) {
			return ctx.Err()
		}
	}
}

func (r *Runner) execute(work context.Context, t protocol.Task) {
	ctx, cancel := context.WithDeadline(work, t.Deadline)
	defer cancel()
	a := &attempt{task: t, cancel: cancel}
	r.mu.Lock()
	r.inflight[t.AttemptID] = a
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.inflight, t.AttemptID)
		r.mu.Unlock()
	}()

	log := r.log.With("task", t.TaskID, "attempt", t.AttemptID, "handler", t.Handler)
	out, stack, err := r.call(ctx, t)

	r.mu.Lock()
	abandoned := a.abandoned
	r.mu.Unlock()
	if abandoned {
		log.InfoContext(work, "attempt abandoned; not reporting")
		return
	}
	if err == nil {
		err = retryReport(work, func(ctx context.Context) error {
			return r.client.Complete(ctx, protocol.CompleteRequest{WorkerID: r.cfg.ID, TaskID: t.TaskID, AttemptID: t.AttemptID, Output: out})
		})
		if err != nil {
			log.WarnContext(work, "complete rejected", "err", err)
		}
		return
	}
	te := protocol.TaskError{Message: err.Error(), Type: errorType(err), Stack: stack}
	var p permanent
	if errors.As(err, &p) {
		no := false
		te.Retryable = &no
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		te.Type = "DeadlineExceeded"
	}
	err = retryReport(work, func(ctx context.Context) error {
		_, err := r.client.Fail(ctx, protocol.FailRequest{WorkerID: r.cfg.ID, TaskID: t.TaskID, AttemptID: t.AttemptID, Error: te})
		return err
	})
	if err != nil {
		log.WarnContext(work, "fail rejected", "err", err)
	}
}

// call runs the handler, turning a panic into an error with its stack.
func (r *Runner) call(ctx context.Context, t protocol.Task) (out json.RawMessage, stack string, err error) {
	h, ok := r.handlers[t.Handler]
	if !ok {
		return nil, "", Permanent(fmt.Errorf("no handler %q on this worker", t.Handler))
	}
	defer func() {
		if p := recover(); p != nil {
			err, stack = fmt.Errorf("panic: %v", p), string(debug.Stack())
		}
	}()
	out, err = h(ctx, Task{
		TaskID: t.TaskID, AttemptID: t.AttemptID, WorkflowID: t.WorkflowID, Step: t.Step, Handler: t.Handler,
		Input: t.Input, IdempotencyKey: t.IdempotencyKey, Attempt: t.Attempt, MaxAttempts: t.MaxAttempts,
	})
	return out, "", err
}

func (r *Runner) heartbeatLoop(ctx context.Context) {
	r.mu.Lock()
	every := r.hbEvery
	r.mu.Unlock()
	if every <= 0 {
		every = 10 * time.Second
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		ids := r.inflightIDs()
		resp, err := r.client.Heartbeat(ctx, protocol.HeartbeatRequest{WorkerID: r.cfg.ID, Attempts: ids})
		if err != nil {
			if ctx.Err() == nil {
				r.log.WarnContext(ctx, "heartbeat failed", "err", err)
				if IsCode(err, protocol.CodeUnknownWorker) {
					_ = r.register(ctx)
				}
			}
			continue
		}
		for _, rv := range resp.Revoked {
			r.abandon(rv.AttemptID, rv.Reason)
		}
	}
}

// abandon stops an attempt that must not continue or be reported.
func (r *Runner) abandon(id uuid.UUID, reason string) {
	r.mu.Lock()
	a, ok := r.inflight[id]
	if ok {
		a.abandoned = true
	}
	r.mu.Unlock()
	if ok {
		r.log.Info("lease revoked; stopping attempt", "attempt", id, "reason", reason)
		a.cancel()
	}
}

// releaseAll hands every in-flight attempt back and stops its handler.
func (r *Runner) releaseAll(ctx context.Context) {
	ids := r.inflightIDs()
	if len(ids) == 0 {
		return
	}
	for _, id := range ids {
		r.abandon(id, "drain timeout")
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.client.Release(rctx, protocol.ReleaseRequest{WorkerID: r.cfg.ID, Attempts: ids}); err != nil {
		r.log.WarnContext(ctx, "release failed; leases will expire instead", "err", err)
		return
	}
	r.log.InfoContext(ctx, "released unfinished attempts", "count", len(ids))
}

func (r *Runner) inflightIDs() []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uuid.UUID, 0, len(r.inflight))
	for id, a := range r.inflight {
		if !a.abandoned {
			ids = append(ids, id)
		}
	}
	return ids
}

func (r *Runner) inflightCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.inflight)
}

// retryReport retries a result report on transport errors and 5xx, a few
// times, so a brief orchestrator failover does not turn a finished attempt
// into a lease expiry. 4xx answers are final.
func retryReport(ctx context.Context, report func(context.Context) error) error {
	b := newBackoff()
	var err error
	for range 5 {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = report(rctx)
		cancel()
		var ae *APIError
		if err == nil || (errors.As(err, &ae) && ae.Status < 500) {
			return err
		}
		if !b.sleep(ctx) {
			return err
		}
	}
	return err
}

func errorType(err error) string {
	for {
		u := errors.Unwrap(err)
		if u == nil {
			break
		}
		err = u
	}
	return fmt.Sprintf("%T", err)
}

// backoff is a small capped exponential sleep for client-side retries.
type backoff struct{ d time.Duration }

func newBackoff() *backoff { return &backoff{d: 100 * time.Millisecond} }

func (b *backoff) reset() { b.d = 100 * time.Millisecond }

// sleep waits the current delay and doubles it; false if ctx ended.
func (b *backoff) sleep(ctx context.Context) bool {
	t := time.NewTimer(b.d)
	defer t.Stop()
	b.d = min(b.d*2, 5*time.Second)
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

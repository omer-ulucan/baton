package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sync"
	"time"

	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// ErrQueueFull rejects a submission by admission control (SPEC 9.3).
type ErrQueueFull struct {
	Queue      string
	Depth      int
	MaxDepth   int
	RetryAfter time.Duration
}

func (e *ErrQueueFull) Error() string {
	return fmt.Sprintf("queue %s is full: depth %d, limit %d", e.Queue, e.Depth, e.MaxDepth)
}

// admission decides whether a workflow touching these queues may start:
// admit, defer, or reject. It must run with the queues locked.
func admission(cfgs map[string]store.QueueConfig, depths map[string]int, adding map[string]int) (deferIt bool, err error) {
	for q, n := range adding {
		c, ok := cfgs[q]
		if !ok || c.MaxDepth == nil {
			continue
		}
		if depth := depths[q]; depth+n > *c.MaxDepth {
			if c.AdmissionMode != "defer" {
				return false, &ErrQueueFull{Queue: q, Depth: depth, MaxDepth: *c.MaxDepth, RetryAfter: 5 * time.Second}
			}
			deferIt = true
		}
	}
	return deferIt, nil
}

// queueCounts counts a definition's step tasks per queue.
func queueCounts(def *workflow.Definition) map[string]int {
	out := map[string]int{}
	for _, st := range def.Steps {
		out[st.Queue]++
	}
	return out
}

func limited(cfgs map[string]store.QueueConfig, queues []string, pick func(store.QueueConfig) bool) []string {
	var out []string
	for _, q := range queues {
		if c, ok := cfgs[q]; ok && pick(c) {
			out = append(out, q)
		}
	}
	return out
}

func hasDepthLimit(c store.QueueConfig) bool { return c.MaxDepth != nil }
func hasClaimLimit(c store.QueueConfig) bool { return c.MaxConcurrency != nil || c.RatePerSec != nil }

// AdmitDeferred starts deferred workflows, oldest first, whose queues have
// room again. It returns how many it admitted.
func (e *Engine) AdmitDeferred(ctx context.Context) (int, error) {
	n := 0
	err := e.inTx(ctx, func(tx *store.Tx) error {
		n = 0
		ids, err := tx.DeferredWorkflows(ctx, e.cfg.LoopBatch)
		if err != nil || len(ids) == 0 {
			return err
		}
		cfgs, err := tx.QueueConfigs(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			snap, err := tx.Load(ctx, id)
			if err != nil {
				return err
			}
			def, err := e.defs.get(ctx, snap.Workflow.DefinitionID)
			if err != nil {
				return err
			}
			adding := queueCounts(def)
			qs := limited(cfgs, keys(adding), hasDepthLimit)
			if err := tx.LockQueues(ctx, qs); err != nil {
				return err
			}
			depths, err := tx.Depths(ctx, qs)
			if err != nil {
				return err
			}
			if deferIt, err := admission(cfgs, depths, adding); deferIt || err != nil {
				continue // still no room; a younger workflow on other queues may fit
			}
			if err := e.commit(ctx, tx, snap, id, []state.Change{{Payload: state.WorkflowAdmitted{}}}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// buckets are the leader's per-queue token buckets (SPEC 9.1). They live in
// memory because only the leader claims; a failover resets them, which can
// allow at most one extra burst.
type buckets struct {
	mu sync.Mutex
	m  map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
}

// take spends one token for queue if its rate allows, refilling by time.
func (b *buckets) take(queue string, rate float64, burst int, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.m == nil {
		b.m = map[string]*bucket{}
	}
	limit := float64(max(1, burst))
	k, ok := b.m[queue]
	if !ok || k.rate != rate || k.burst != limit {
		k = &bucket{tokens: limit, last: now, rate: rate, burst: limit}
		b.m[queue] = k
	}
	k.tokens = math.Min(k.burst, k.tokens+now.Sub(k.last).Seconds()*k.rate)
	k.last = now
	if k.tokens < 1 {
		return false
	}
	k.tokens--
	return true
}

var queueName = regexp.MustCompile(`^[a-z0-9-]{1,63}$`)

// SetQueue stores a queue's backpressure settings.
func (e *Engine) SetQueue(ctx context.Context, c store.QueueConfig) error {
	var problems []error
	if !queueName.MatchString(c.Queue) {
		problems = append(problems, fmt.Errorf("queue %q must match [a-z0-9-]{1,63}", c.Queue))
	}
	if c.AdmissionMode == "" {
		c.AdmissionMode = "reject"
	}
	if c.AdmissionMode != "reject" && c.AdmissionMode != "defer" {
		problems = append(problems, fmt.Errorf("admission_mode must be reject or defer"))
	}
	for name, v := range map[string]*int{"max_concurrency": c.MaxConcurrency, "burst": c.Burst, "max_depth": c.MaxDepth} {
		if v != nil && *v <= 0 {
			problems = append(problems, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.RatePerSec != nil && (*c.RatePerSec <= 0 || math.IsInf(*c.RatePerSec, 0) || math.IsNaN(*c.RatePerSec)) {
		problems = append(problems, fmt.Errorf("rate_per_sec must be positive"))
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalid, errors.Join(problems...))
	}
	return e.inTx(ctx, func(tx *store.Tx) error { return tx.UpsertQueueConfig(ctx, c) })
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func burstOf(c store.QueueConfig) int {
	if c.Burst != nil {
		return *c.Burst
	}
	return 1
}

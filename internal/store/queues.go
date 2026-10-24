package store

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// QueueConfig is a queue's backpressure settings (SPEC 9). Nil fields are
// unlimited.
type QueueConfig struct {
	Queue          string   `json:"queue"`
	MaxConcurrency *int     `json:"max_concurrency,omitempty"`
	RatePerSec     *float64 `json:"rate_per_sec,omitempty"`
	Burst          *int     `json:"burst,omitempty"`
	MaxDepth       *int     `json:"max_depth,omitempty"`
	AdmissionMode  string   `json:"admission_mode"` // reject | defer
}

// QueueStat is a queue's current load.
type QueueStat struct {
	Queue        string      `json:"queue"`
	Pending      int         `json:"pending"`
	Ready        int         `json:"ready"`
	Running      int         `json:"running"`
	RetryWait    int         `json:"retry_wait"`
	DeadLettered int         `json:"dead_lettered"`
	Config       QueueConfig `json:"config"`
}

const queueConfigColumns = `queue, max_concurrency, rate_per_sec, burst, max_depth, admission_mode`

func scanQueueConfigs(rows pgx.Rows) (map[string]QueueConfig, error) {
	defer rows.Close()
	out := map[string]QueueConfig{}
	for rows.Next() {
		var c QueueConfig
		if err := rows.Scan(&c.Queue, &c.MaxConcurrency, &c.RatePerSec, &c.Burst, &c.MaxDepth, &c.AdmissionMode); err != nil {
			return nil, err
		}
		out[c.Queue] = c
	}
	return out, rows.Err()
}

// QueueConfigs returns the configured queues.
func (t *Tx) QueueConfigs(ctx context.Context) (map[string]QueueConfig, error) {
	rows, err := t.tx.Query(ctx, `SELECT `+queueConfigColumns+` FROM queue_config`)
	if err != nil {
		return nil, fmt.Errorf("queue configs: %w", err)
	}
	return scanQueueConfigs(rows)
}

// UpsertQueueConfig stores a queue's settings.
func (t *Tx) UpsertQueueConfig(ctx context.Context, c QueueConfig) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO queue_config (`+queueConfigColumns+`, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (queue) DO UPDATE SET max_concurrency = EXCLUDED.max_concurrency,
			rate_per_sec = EXCLUDED.rate_per_sec, burst = EXCLUDED.burst, max_depth = EXCLUDED.max_depth,
			admission_mode = EXCLUDED.admission_mode, updated_at = EXCLUDED.updated_at`,
		c.Queue, c.MaxConcurrency, c.RatePerSec, c.Burst, c.MaxDepth, c.AdmissionMode, t.now)
	if err != nil {
		return fmt.Errorf("upsert queue config: %w", err)
	}
	return nil
}

// LockQueues takes a transaction-scoped advisory lock per queue, in sorted
// order so two transactions locking overlapping sets cannot deadlock. It
// serializes the count-then-act of concurrency limits and admission.
func (t *Tx) LockQueues(ctx context.Context, queues []string) error {
	qs := slices.Clone(queues)
	slices.Sort(qs)
	qs = slices.Compact(qs)
	for _, q := range qs {
		if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('baton-queue:' || $1, 0))`, q); err != nil {
			return fmt.Errorf("lock queue %s: %w", q, err)
		}
	}
	return nil
}

// RunningCounts counts running tasks per queue.
func (t *Tx) RunningCounts(ctx context.Context, queues []string) (map[string]int, error) {
	return t.countByQueue(ctx, `SELECT queue, count(*) FROM tasks WHERE status = 'running' AND queue = ANY($1) GROUP BY queue`, queues)
}

// Depths counts each queue's backlog: tasks not yet finished (pending,
// ready, retry_wait) of workflows that were admitted.
func (t *Tx) Depths(ctx context.Context, queues []string) (map[string]int, error) {
	return t.countByQueue(ctx, `
		SELECT t.queue, count(*) FROM tasks t JOIN workflows w ON w.id = t.workflow_id
		 WHERE t.status IN ('pending', 'ready', 'retry_wait') AND w.status <> 'deferred' AND t.queue = ANY($1)
		 GROUP BY t.queue`, queues)
}

func (t *Tx) countByQueue(ctx context.Context, sql string, queues []string) (map[string]int, error) {
	rows, err := t.tx.Query(ctx, sql, queues)
	if err != nil {
		return nil, fmt.Errorf("count by queue: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var q string
		var n int
		if err := rows.Scan(&q, &n); err != nil {
			return nil, err
		}
		out[q] = n
	}
	return out, rows.Err()
}

// DeferredWorkflows locks up to limit deferred workflows, oldest first.
func (t *Tx) DeferredWorkflows(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := t.tx.Query(ctx, `SELECT id FROM workflows WHERE status = 'deferred' ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("deferred workflows: %w", err)
	}
	return collectUUIDs(rows)
}

// QueueStats reports load and settings for every queue that has tasks or
// settings.
func (s *Store) QueueStats(ctx context.Context) ([]QueueStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue,
		       count(*) FILTER (WHERE status = 'pending'),
		       count(*) FILTER (WHERE status = 'ready'),
		       count(*) FILTER (WHERE status = 'running'),
		       count(*) FILTER (WHERE status = 'retry_wait'),
		       count(*) FILTER (WHERE status = 'dead_lettered')
		  FROM tasks GROUP BY queue`)
	if err != nil {
		return nil, fmt.Errorf("queue stats: %w", err)
	}
	stats := map[string]*QueueStat{}
	for rows.Next() {
		var st QueueStat
		if err := rows.Scan(&st.Queue, &st.Pending, &st.Ready, &st.Running, &st.RetryWait, &st.DeadLettered); err != nil {
			rows.Close()
			return nil, err
		}
		stats[st.Queue] = &st
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	crows, err := s.pool.Query(ctx, `SELECT `+queueConfigColumns+` FROM queue_config`)
	if err != nil {
		return nil, fmt.Errorf("queue configs: %w", err)
	}
	cfgs, err := scanQueueConfigs(crows)
	if err != nil {
		return nil, err
	}
	for q, c := range cfgs {
		if stats[q] == nil {
			stats[q] = &QueueStat{Queue: q}
		}
		stats[q].Config = c
	}
	out := make([]QueueStat, 0, len(stats))
	for _, st := range stats {
		if st.Config.Queue == "" {
			st.Config = QueueConfig{Queue: st.Queue, AdmissionMode: "reject"}
		}
		out = append(out, *st)
	}
	slices.SortFunc(out, func(a, b QueueStat) int {
		if a.Queue < b.Queue {
			return -1
		}
		return 1
	})
	return out, nil
}

// effectivePriority is the claim ordering of SPEC 9.2: static priority
// plus one point per aging interval spent ready, capped at 200, so no
// ready task waits forever behind higher-priority arrivals.
func effectivePriority(agingSeconds int) string {
	return fmt.Sprintf(`LEAST(200, t.priority + floor(extract(epoch FROM ($1::timestamptz - t.ready_at)) / %d))`, max(1, agingSeconds))
}

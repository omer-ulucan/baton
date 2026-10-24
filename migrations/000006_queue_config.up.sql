-- Per-queue backpressure (SPEC 9). A queue without a row is unlimited.
CREATE TABLE queue_config (
  queue           text PRIMARY KEY,
  max_concurrency int CHECK (max_concurrency > 0),
  rate_per_sec    double precision CHECK (rate_per_sec > 0),
  burst           int CHECK (burst > 0),
  max_depth       int CHECK (max_depth > 0),
  admission_mode  text NOT NULL DEFAULT 'reject' CHECK (admission_mode IN ('reject', 'defer')),
  updated_at      timestamptz NOT NULL
);

-- Depth and running counts per queue.
CREATE INDEX tasks_queue_status_idx ON tasks (queue, status);
-- Deferred workflows are admitted oldest first.
CREATE INDEX workflows_deferred_idx ON workflows (id) WHERE status = 'deferred';

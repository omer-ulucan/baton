-- Transactional outbox (SPEC 7, ADR 0006): messages for the outside world,
-- written in the same transaction as the state change that causes them.
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  topic        text NOT NULL,          -- 'workflow.terminal' | 'escalation'
  key          text NOT NULL,          -- dedupe key for consumers
  url          text NOT NULL,          -- destination
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL,
  attempts     int NOT NULL DEFAULT 0,
  next_try_at  timestamptz NOT NULL,
  last_error   text,
  published_at timestamptz,
  failed_at    timestamptz             -- gave up after the retry window
);
CREATE INDEX outbox_pending_idx ON outbox (next_try_at) WHERE published_at IS NULL AND failed_at IS NULL;

-- Keyset pagination of the DLQ and per-queue views.
CREATE INDEX tasks_status_idx ON tasks (status, queue, id);

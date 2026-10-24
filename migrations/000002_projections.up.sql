-- Projections of workflow_events (SPEC 4, 5). Every column is derived from
-- event payloads and occurred_at so replay reproduces them exactly.
CREATE TABLE workflows (
  id               uuid PRIMARY KEY,
  definition_id    uuid NOT NULL REFERENCES workflow_definitions(id),
  status           text NOT NULL,
  priority         int  NOT NULL,
  submit_key       text UNIQUE,
  cancel_requested bool NOT NULL DEFAULT false,
  last_seq         int  NOT NULL,
  created_at       timestamptz NOT NULL,
  updated_at       timestamptz NOT NULL,
  finished_at      timestamptz
);
CREATE INDEX workflows_status_idx ON workflows (status, id);

CREATE TABLE tasks (
  id              uuid PRIMARY KEY,
  workflow_id     uuid NOT NULL REFERENCES workflows(id),
  step_name       text NOT NULL,
  kind            text NOT NULL CHECK (kind IN ('step', 'compensation')),
  handler         text NOT NULL,
  queue           text NOT NULL,
  priority        int  NOT NULL,
  status          text NOT NULL,
  attempt         int  NOT NULL DEFAULT 0,
  max_attempts    int  NOT NULL,
  current_attempt uuid,
  worker_id       text,
  run_at          timestamptz,
  ready_at        timestamptz,
  idempotency_key text NOT NULL,
  last_error      text,
  updated_at      timestamptz NOT NULL,
  UNIQUE (workflow_id, step_name, kind)
);
CREATE INDEX tasks_claim_idx ON tasks (queue, priority DESC, ready_at) WHERE status = 'ready';
CREATE INDEX tasks_retry_idx ON tasks (run_at) WHERE status = 'retry_wait';

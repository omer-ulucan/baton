-- Immutable workflow definitions, deduplicated by content hash.
CREATE TABLE workflow_definitions (
  id         uuid PRIMARY KEY,
  name       text NOT NULL,
  version    int  NOT NULL,
  sha256     bytea NOT NULL UNIQUE,
  spec       jsonb NOT NULL,
  created_at timestamptz NOT NULL
);

-- Source of truth for workflow and task state (SPEC 5).
CREATE TABLE workflow_events (
  id            bigserial PRIMARY KEY,
  workflow_id   uuid NOT NULL,
  seq           int  NOT NULL CHECK (seq > 0),
  type          text NOT NULL,
  task_id       uuid,
  payload       jsonb NOT NULL,
  occurred_at   timestamptz NOT NULL,
  fencing_token bigint NOT NULL,
  UNIQUE (workflow_id, seq)
);

CREATE FUNCTION workflow_events_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'workflow_events is append-only (% rejected)', TG_OP
    USING ERRCODE = 'BT002';
END;
$$;

CREATE TRIGGER workflow_events_no_update
  BEFORE UPDATE OR DELETE ON workflow_events
  FOR EACH ROW EXECUTE FUNCTION workflow_events_append_only();

CREATE TRIGGER workflow_events_no_truncate
  BEFORE TRUNCATE ON workflow_events
  FOR EACH STATEMENT EXECUTE FUNCTION workflow_events_append_only();

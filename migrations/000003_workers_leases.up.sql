-- Operational state (SPEC 4): not event-sourced and not part of replay.
-- Lease outcomes (claim, expiry, release, revoke) are events; heartbeats
-- that only move lease_until are not.
CREATE TABLE workers (
  id            text PRIMARY KEY,
  queues        text[] NOT NULL,
  handlers      text[] NOT NULL,
  version       text NOT NULL DEFAULT '',
  registered_at timestamptz NOT NULL,
  last_seen_at  timestamptz NOT NULL
);

CREATE TABLE task_leases (
  task_id     uuid PRIMARY KEY REFERENCES tasks(id),
  attempt_id  uuid NOT NULL UNIQUE,
  worker_id   text NOT NULL,
  lease_until timestamptz NOT NULL,
  revoked     bool NOT NULL DEFAULT false
);
CREATE INDEX task_leases_expiry_idx ON task_leases (lease_until) WHERE NOT revoked;
CREATE INDEX task_leases_worker_idx ON task_leases (worker_id);

-- Leader election and fencing (SPEC 6, ADR 0002).
--
-- Two rows, so renewal and fencing do not contend: every write transaction
-- locks leader_fence FOR SHARE, and only a takeover (a token change)
-- updates it; renewals only move leader_lease.expires_at. Were they one
-- row, each renewal would wait behind all in-flight writes, and a steady
-- stream of share lockers can starve it until the leader steps down.
CREATE TABLE leader_lease (
  name        text PRIMARY KEY,
  holder_id   text,
  holder_addr text,
  expires_at  timestamptz NOT NULL DEFAULT '-infinity'
);
INSERT INTO leader_lease (name) VALUES ('leader');

CREATE TABLE leader_fence (
  name          text PRIMARY KEY,
  fencing_token bigint NOT NULL DEFAULT 0
);
INSERT INTO leader_fence (name) VALUES ('leader');

-- Every event insert must carry the current token while the lease is live.
-- FOR SHARE conflicts with the takeover's token UPDATE: a takeover waits
-- for in-flight leader transactions, and once it commits every later
-- insert with the old token fails. Since every state change appends an
-- event in the same transaction, a stale leader cannot commit any state.
CREATE FUNCTION workflow_events_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  cur_token bigint;
  cur_exp   timestamptz;
BEGIN
  SELECT fencing_token INTO cur_token FROM leader_fence WHERE name = 'leader' FOR SHARE;
  SELECT expires_at INTO cur_exp FROM leader_lease WHERE name = 'leader';
  IF NEW.fencing_token IS DISTINCT FROM cur_token OR cur_exp <= clock_timestamp() THEN
    RAISE EXCEPTION 'stale fencing token % (current %, lease expires %)', NEW.fencing_token, cur_token, cur_exp
      USING ERRCODE = 'BT001';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER workflow_events_fence
  BEFORE INSERT ON workflow_events
  FOR EACH ROW EXECUTE FUNCTION workflow_events_fence();

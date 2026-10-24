-- Leader election and fencing (SPEC 6, ADR 0002).
CREATE TABLE leader_lease (
  name          text PRIMARY KEY,
  holder_id     text,
  holder_addr   text,
  fencing_token bigint NOT NULL DEFAULT 0,
  expires_at    timestamptz NOT NULL DEFAULT '-infinity'
);
INSERT INTO leader_lease (name) VALUES ('leader');

-- Every event insert must carry the current leader's token while its lease
-- is live. FOR SHARE conflicts with the takeover UPDATE: a takeover waits
-- for in-flight leader transactions, and once it commits every later
-- insert with the old token fails. Since every state change appends an
-- event in the same transaction, a stale leader cannot commit any state.
CREATE FUNCTION workflow_events_fence() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  cur_token bigint;
  cur_exp   timestamptz;
BEGIN
  SELECT fencing_token, expires_at INTO cur_token, cur_exp
    FROM leader_lease WHERE name = 'leader' FOR SHARE;
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

# 0002. Leader election via a Postgres lease with fencing tokens, not Raft

Status: Accepted

## Context

The scheduler, lease reaper, retry timer, saga coordinator and outbox relay
must run on exactly one replica. Two concurrently active leaders could
double-schedule retries, double-reap leases, or run a compensation twice.
Leases alone are not enough: a leader paused by GC or a network partition
can wake up after its lease expired and still issue writes.

## Decision

A single `leader_lease` row holds `holder_id`, `fencing_token` and
`expires_at`. Replicas acquire or renew with one conditional `UPDATE` using
the database clock; the token increments whenever the holder changes or a
lease is re-acquired after expiry. Every event insert carries the writer's
token, and a `BEFORE INSERT` trigger on `workflow_events` takes `FOR SHARE`
on the lease row and rejects the insert (SQLSTATE `BT001`) unless the token
is current and unexpired. Since every state-changing transaction inserts an
event, a stale leader cannot commit anything. A leader that cannot renew
within TTL/2 steps down on its own before its lease can expire.

## Consequences

- Fencing is enforced by the storage, not by the leader's good behaviour,
  which is the only enforcement that holds under process pauses.
- The `FOR SHARE` lock serializes takeover against in-flight leader
  transactions: takeover waits for them, and they fail after it.
- Failover time is bounded by the lease TTL (default 10s) plus one renew period.
- Leader election availability equals Postgres availability, which we
  already depend on (ADR 0001); no new failure domain.
- Every event insert does one extra indexed row read with a share lock on a
  hot row. Measured in phase 10; acceptable because all writes already go
  through one leader (ADR 0003).

## Alternatives considered

- **Embedded Raft (hashicorp/raft, etcd/raft):** gives election and a
  replicated log, but the state of record is Postgres, so Raft would elect a
  leader for a store it does not own. We would still need fencing at the
  Postgres level, plus a quorum of 3+ replicas, membership changes, snapshot
  handling and another failure domain to test. The cost buys nothing that
  the lease row does not already provide when Postgres is the source of truth.
- **Postgres advisory locks:** session-scoped, so they vanish with the
  connection, but carry no token; a leader whose connection was dropped
  cannot tell, and old transactions are not fenced. Also awkward with pooled
  connections.
- **etcd or Consul election:** another stateful system to run, and the
  fencing token would still have to be checked inside Postgres.

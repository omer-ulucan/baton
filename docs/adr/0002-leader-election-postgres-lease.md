# 0002. Leader election via a Postgres lease with fencing tokens, not Raft

Status: Accepted

## Context

The scheduler, lease reaper, retry timer, saga coordinator and outbox relay
must run on exactly one replica. Two concurrently active leaders could
double-schedule retries, double-reap leases, or run a compensation twice.
Leases alone are not enough: a leader paused by GC or a network partition
can wake up after its lease expired and still issue writes.

## Decision

A `leader_lease` row holds `holder_id` and `expires_at`; a separate
`leader_fence` row holds `fencing_token`. Replicas acquire or renew with a
conditional `UPDATE` of the lease using the database clock; the token
increments (an `UPDATE` of the fence row) only when the holder changes or a
lease is re-acquired after expiry. Every write transaction locks the fence
row `FOR SHARE` and checks the token, and every event insert carries the
token, where a `BEFORE INSERT` trigger on `workflow_events` does the same
check and rejects the insert (SQLSTATE `BT001`) unless the token is current
and the lease unexpired. Since every state-changing transaction inserts an
event, a stale leader cannot commit anything. A leader that cannot renew
within TTL/2 steps down on its own before its lease can expire.

## Consequences

- Fencing is enforced by the storage, not by the leader's good behaviour,
  which is the only enforcement that holds under process pauses.
- The `FOR SHARE` lock serializes takeover against in-flight leader
  transactions: takeover waits for them, and they fail after it.
- Failover time is bounded by the lease TTL (default 10s) plus one renew
  period (TTL/6).
- Renewal timing leaves slack on purpose: renew every TTL/6 with a TTL/3
  deadline, step down after TTL/2 without success. The first version renewed
  every TTL/3, which left a renewal only TTL/6 to complete before the
  step-down deadline; a loaded test machine demoted a healthy leader.
- Leader election availability equals Postgres availability, which we
  already depend on (ADR 0001); no new failure domain.
- Every write transaction takes a share lock on one hot row. Shared locks
  do not conflict with each other; only a takeover waits on them.
- Lease and fence are separate rows on purpose. In a first version they
  were one row, and the failover test showed the new leader stepping down
  under load: each renewal `UPDATE` had to wait for all in-flight writes
  holding `FOR SHARE`, and a steady stream of share lockers can starve it
  past the self-fencing deadline. With the split, renewals never touch the
  locked row, and the test asserts the new leader is promoted exactly once.

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

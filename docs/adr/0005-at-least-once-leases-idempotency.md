# 0005. At-least-once delivery with leases and idempotency keys

Status: Accepted

## Context

Workers can crash after doing work but before reporting it. No orchestrator
can tell "did the work, then crashed" from "crashed before the work" without
cooperation from the system the work touched. Exactly-once execution is
therefore not something the orchestrator can promise.

## Decision

- Execution is at-least-once. Claims grant a lease (`lease_until`, default
  30s); heartbeats (default every 10s) extend it; the reaper returns expired
  attempts to retry or the DLQ. Lease expiry consumes an attempt so a task
  that kills its workers ends in the DLQ instead of looping.
- Each attempt has an `attempt_id`. Results are accepted only for the
  current, unrevoked attempt; late results from superseded attempts get `409`.
- Each task has a stable `idempotency_key` (`workflow_id/step/kind`)
  exposed to handlers. Effectively-once is the handler's job, by passing the
  key to idempotent downstream APIs or by recording it with a unique
  constraint in the same transaction as the side effect.
- Graceful shutdown releases unfinished attempts (`TaskReleased`) without
  consuming an attempt.

## Consequences

- Handlers must be idempotent or use the key; documented in
  `docs/IDEMPOTENCY.md` with examples.
- Lease TTL trades failure-detection latency against false expiry under GC
  pauses or slow networks; `lease_ttl >= 3 * heartbeat_interval` is enforced.

## Alternatives considered

- **At-most-once (mark done on claim):** loses work on every crash.
- **Two-phase commit with handlers:** not generally available, and turns
  every handler into a transaction participant.

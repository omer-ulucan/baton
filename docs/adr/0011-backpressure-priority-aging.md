# 0011. Backpressure: claim-time limits, priority aging, admission control

Status: Accepted

## Context

Queues share workers and the database. Without limits, a burst on one queue
can starve others, high-priority work can starve low-priority work forever,
and unbounded submissions grow the backlog without bound.

## Decision

- **Concurrency limits** are enforced at claim time: count running tasks of
  the queue under `pg_advisory_xact_lock(hashtext(queue))`, so concurrent
  claims cannot overshoot.
- **Rate limits** are token buckets in leader memory (single writer, ADR 0003).
- **Aging:** `effective_priority = priority + floor(wait / BATON_AGING_SECONDS)`,
  capped at 200, computed in the claim query. Ties by `ready_at`, then `id`.
- **Admission control** on submission: if a target queue's depth exceeds
  `max_depth`, reject with `429` and `Retry-After`, or, in `defer` mode,
  accept as `deferred` and admit FIFO as depth falls.

## Consequences

- The per-queue advisory lock serializes claims within a queue (not across
  queues). Claim latency under contention is measured in phase 10.
- Rate-limit state resets on failover, allowing at most one extra burst.
- Computing aging in the query prevents a plain index-only ordering; the
  partial index on `ready` tasks keeps the scanned set small. Revisit with a
  stored, periodically bumped priority if benchmarks show a problem.

## Alternatives considered

- **Weighted fair queuing across queues:** better isolation, more state;
  not needed until there are tenants.
- **Rate limits in Postgres:** survives failover, but adds a hot row
  update to every claim.

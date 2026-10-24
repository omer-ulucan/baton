# 0001. PostgreSQL as the single source of truth

Status: Accepted

## Context

Baton needs durable workflow state, exclusive task claims, leader election
and an outbox. Each could live in a different system (Redis for queues,
etcd for election, Kafka for events), but every extra stateful system adds
a failure mode and makes cross-system atomicity impossible without
distributed transactions.

## Decision

PostgreSQL 16 is the only durable state. Queues are tables claimed with
`FOR UPDATE SKIP LOCKED`; the event log, projections, leases, leader lease
and outbox are tables in the same database. Access via `pgx/v5`; schema via
golang-migrate. All time-based decisions (leases, leader expiry) use the
database clock.

## Consequences

- Every state change, its event, and its outbound message commit atomically.
- One clock for correctness, so replica clock skew does not matter.
- Throughput is bounded by one Postgres primary. Fine for the target (thousands
  of tasks per second), not for millions.
- Postgres availability is Baton's availability. HA Postgres is the
  deployment's job and is out of scope.

## Alternatives considered

- **Redis/RabbitMQ for queues:** faster dispatch, but claims and state
  changes could no longer be atomic; a crash between them loses or
  duplicates work.
- **Kafka as the event log:** strong ordering, but projections would be
  eventually consistent and exactly-once projection needs extra machinery.

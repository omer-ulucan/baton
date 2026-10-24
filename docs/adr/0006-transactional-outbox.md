# 0006. Transactional outbox for external side effects

Status: Accepted

## Context

Baton sends messages outside the database: terminal-state webhooks and
escalation notifications. Sending them inside the state transaction ties
DB locks to remote latency and still loses the message if the commit fails
after sending; sending after commit loses it if the process dies in between.

## Decision

Outbound messages are rows in `outbox`, inserted in the same transaction as
the state change. The leader's relay publishes pending rows, marks
`published_at` on success, retries with exponential backoff, and sets a
`Baton-Message-Key` header for consumer deduplication.

## Consequences

- Messages are published at least once and never for a transaction that
  rolled back.
- Consumers must dedupe on the message key.
- Delivery latency is the relay period (default 500ms) plus the request.
- Outbox writes are fenced like everything else (they share the event
  transaction).

## Alternatives considered

- **Logical decoding / CDC (Debezium):** no polling, but another component
  to run for a handful of message types.
- **LISTEN/NOTIFY only:** not durable.

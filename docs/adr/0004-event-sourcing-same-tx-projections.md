# 0004. Event sourcing with same-transaction projections

Status: Accepted

## Context

We need an audit trail of every workflow, the ability to rebuild state, and
a deterministic record of non-deterministic inputs (jitter, model
decisions). We also need fast queries for scheduling.

## Decision

`workflow_events` is append-only and per-workflow gapless (`seq`). The
`workflows` and `tasks` tables are projections, updated in the same
transaction as the event insert by the same pure `Apply(state, event)`
function used by replay. Projection columns derive only from event payloads
and `occurred_at`. Random draws and decisions are stored in events.

Operational, high-churn state is not event-sourced: `task_leases`
(`lease_until` changes on every heartbeat) and `workers` (heartbeats,
registration). Their meaningful transitions (claim, lease expiry, release,
revoke) are events. Replay equality is defined over `workflows` and `tasks`.

## Consequences

- Projections are never stale, so the scheduler queries them directly; no
  async projector to lag or fail.
- Replay test: rebuild from events, compare column by column. Any
  non-determinism in `Apply` fails it.
- One `FOR UPDATE` on the workflow row per transaction serializes writers of
  one workflow. Contention is per workflow, which is fine.
- Events are never deleted; retention/archival is a future concern.

## Alternatives considered

- **Async projections:** decouple write latency from projection cost, but
  the scheduler would read stale state and claims would need compensating logic.
- **Heartbeats as events:** full fidelity, but roughly
  `duration / heartbeat_interval` events per attempt with nothing to recover
  from them.

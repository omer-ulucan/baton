# 0010. YAML DAG workflows with sequential saga compensation

Status: Accepted

## Context

Workflows are DAGs of steps with dependencies, timeouts, retry policies,
queues, priorities and optional compensation. Failures after partial
progress need undo semantics without distributed transactions.

## Decision

- Definitions are YAML, decoded strictly (unknown keys rejected), validated
  fully before any write (all errors reported at once), and stored
  immutably, deduplicated by content hash.
- A canonical topological order (Kahn's algorithm, ties by step name) is
  used everywhere, so ordering is a pure function of the definition.
- Saga: when a workflow fails terminally (or is cancelled with
  `compensate: true`), compensation runs for `succeeded` steps that declare
  `compensate`, in reverse canonical topological order, **one at a time**.
  Compensation tasks are ordinary tasks with their own retry policy and
  idempotency key. A dead-lettered compensation stops the chain
  (`compensation_failed`) until manually retried.

## Consequences

- Sequential compensation is slower than reverse-DAG-parallel compensation
  but has an obvious order to reason about and to test.
- Compensations must be idempotent and must tolerate the forward step having
  been partly applied (at-least-once, ADR 0005).
- Steps without `compensate` are assumed not to need undo.

## Alternatives considered

- **Parallel compensation by reverse DAG levels:** faster, harder to reason
  about when a compensation fails mid-level. Possible later as an opt-in.
- **Code-defined workflows (Temporal style):** more expressive, but needs
  deterministic workflow code in workers and versioning machinery; out of
  scope.

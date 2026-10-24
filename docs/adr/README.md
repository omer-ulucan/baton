# Architecture decision records

One file per decision, numbered, never renumbered. A superseded ADR stays in
place with its status changed to `Superseded by NNNN`.

Format: Status, Context, Decision, Consequences, Alternatives considered.

| # | Title | Status |
|---|---|---|
| [0001](0001-postgres-single-source-of-truth.md) | PostgreSQL as the single source of truth | Accepted |
| [0002](0002-leader-election-postgres-lease.md) | Leader election via a Postgres lease with fencing tokens, not Raft | Accepted |
| [0003](0003-single-writer-leader.md) | Single writer: followers proxy writes to the leader | Accepted |
| [0004](0004-event-sourcing-same-tx-projections.md) | Event sourcing with same-transaction projections | Accepted |
| [0005](0005-at-least-once-leases-idempotency.md) | At-least-once delivery with leases and idempotency keys | Accepted |
| [0006](0006-transactional-outbox.md) | Transactional outbox for external side effects | Accepted |
| [0007](0007-onnx-runtime-cgo.md) | ONNX Runtime via cgo for Laya, not hugot | Accepted |
| [0008](0008-laya-model-artifact.md) | Laya model artifact: pinned revision, checksums, fp32 default | Accepted |
| [0009](0009-decision-layer-fallback.md) | Decision layer: policy interface, bounded inference, fallback, recorded decisions | Accepted |
| [0010](0010-yaml-dag-and-saga.md) | YAML DAG workflows with sequential saga compensation | Accepted |
| [0011](0011-backpressure-priority-aging.md) | Backpressure: claim-time limits, priority aging, admission control | Accepted |

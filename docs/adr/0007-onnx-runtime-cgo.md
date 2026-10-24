# 0007. ONNX Runtime via cgo for Laya, not hugot

Status: Accepted

## Context

The decision layer runs Laya in-process with no Python at runtime. Laya
(ONNX export `onnx-community/laya-ONNX`) is a ModernBERT-large encoder
(about 395M parameters, 1.69 GB fp32) with a decision head. Its graph takes
five inputs, `input_ids`, `attention_mask`, `marker_pos`, `marker_mask`
(bool) and `qtype`, and returns `logits` over option markers plus
`act_logits`. Inputs are built by Laya's own sequence builder, not by a
standard text-classification pipeline.

## Decision

Use `github.com/yalue/onnxruntime_go`, a cgo binding to the official ONNX
Runtime C API, with `libonnxruntime.so` at a pinned version. Tensors are
created directly from Go slices, so the custom inputs map one to one.

Requirements this imposes:
- `CGO_ENABLED=1` for `cmd/baton` and its tests that touch inference.
- `libonnxruntime.so` (matching the binding's supported API version) must be
  present at runtime; path from `ONNXRUNTIME_LIB_PATH`. The orchestrator
  Docker image installs the pinned release from Microsoft's GitHub releases
  with a checksum check.
- The build uses a glibc base image (not `scratch`/static musl).
- `internal/laya` sits behind the `DecisionPolicy` interface; packages other
  than `internal/laya` do not import the binding, and the rest of the
  system builds and tests with the rule policy if the library is absent.
  Workers and `batonctl` stay pure Go (`CGO_ENABLED=0`).

## Consequences

- Production-grade CPU kernels and threading, the same runtime the model
  card's parity numbers were measured on.
- cgo complicates cross-compilation and adds a shared-library deployment step.
- A missing or incompatible library is a startup-time policy failure handled
  by fallback (ADR 0009), not a crash.

## Alternatives considered

- **hugot, pure Go backend (GoMLX simplego):** no cgo, but (1) a pure Go
  backend is far slower than ONNX Runtime on a 395M-parameter encoder, and
  at 1.69 GB the latency would blow any useful decision timeout; (2) hugot's
  pipelines are built around standard tasks (feature extraction,
  classification, token classification) with `input_ids` /
  `attention_mask` inputs, while Laya needs `marker_pos`, `marker_mask` and
  `qtype` tensors and post-processing over marker logits. Using hugot would
  mean bypassing its pipelines and its value. hugot's ORT backend is itself
  cgo, so it would not remove the cgo requirement either.
- **Python sidecar:** violates the "no Python at runtime" constraint.
- **Remote inference service:** adds network failure modes to every decision
  and another deployable; the in-process design with fallback is simpler.

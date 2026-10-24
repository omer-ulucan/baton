# 0008. Laya model artifact: pinned revision, checksums, fp32 default

Status: Accepted (int8 question open, see below)

## Context

Model files are large (fp32: 4.4 MB graph + 1.69 GB external data; fp16:
4.5 MB + 0.84 GB) and must never be committed. The model must be
reproducible: the same revision and bytes in every environment, and
decisions recorded with the revision that made them.

## Decision

- Source `onnx-community/laya-ONNX` pinned to revision
  `42e2a6e3b3708c8ff5f61f4aa75c314b1449199b`. Downloads use
  `https://huggingface.co/<repo>/resolve/<revision>/<file>`, never `main`.
- Each file (`onnx/model.onnx`, `onnx/model.onnx_data`, `tokenizer.json`,
  `config.json`) has a SHA-256 pinned in code (`internal/laya/manifest.go`).
  Download goes to a temp file, is hashed while streaming, and is renamed
  into the cache only on a match. A mismatch is an error that makes the
  policy unavailable (fallback), never a silent load.
- Cache dir `BATON_MODEL_DIR`, default `$XDG_CACHE_HOME/baton/models/<revision>`.
- The orchestrator image bakes the model in a separate build stage (same
  verification), so containers do not download at start. Without a baked
  model, first start downloads in the background while the rule policy serves.
- Default variant **fp32** on CPU. fp16 is selectable
  (`BATON_LAYA_VARIANT=fp16`); the model card reports fp16 max |Δp| of 0.007
  against the reference, which fails our 1e-4 parity bar, so parity tests
  run against fp32 only.
- `model_revision` in every `DecisionMade` event is the pinned revision plus
  variant, e.g. `42e2a6e3…@fp32`.

## Consequences

- Upgrading the model is a code change (new revision and hashes), reviewed
  like any other.
- The orchestrator image is about 1.8 GB larger.

## Open question: int8

The pinned revision ships no int8 export. Options, decided in phase 7 after
measuring fp32 latency:
1. Stay on fp32 (simplest, parity holds).
2. Produce an int8 graph offline with ONNX Runtime dynamic quantization
   (tool under `tools/`, never at runtime), host it under our own Hugging
   Face repo with a pinned revision and checksum, and run the eval on it.
   Quantization changes outputs, so it would get its own accuracy numbers in
   EVAL.md rather than the 1e-4 parity bar.

# 0008. Laya model artifact: pinned revision, checksums, fp32 default

Status: Accepted (int8 question resolved in phase 7: stay on fp32)

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

## int8 (resolved)

The pinned revision ships no int8 export. Options were to stay on fp32, or
to quantize offline with ONNX Runtime and host the result ourselves.
Decision: stay on fp32. It is the only variant that meets the 1e-4 parity
bar against laya, its measured CPU latency (~830ms p50 per triage) fits the
1.5s decision timeout, and hosting a derived artifact would add a
supply-chain surface for a model whose accuracy on triage is still unproven
(SPEC 14). Revisit if the eval shows Laya is worth running and latency
becomes the limit.

The non-LFS files are pinned too: `tokenizer.json`
`6c8aaa9a…3c08d30`, `config.json` `1a888f13…2993bb0c` (full hashes in
`internal/laya/model`), computed on downloads whose git blob ids matched
the Hub.

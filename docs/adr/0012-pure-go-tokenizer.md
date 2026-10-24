# 0012. A pure-Go tokenizer for Laya, verified against Hugging Face

Status: Accepted

## Context

Laya's encoder (ModernBERT-large) uses a GPT-NeoX-style `tokenizer.json`:
NFC normalization, the byte-level GPT-2 pre-tokenizer, 50,009 BPE merges,
and 116 added tokens (seven special tokens, one of them `[MASK]` with
`lstrip`, and 109 normalized tokens, most of them runs of spaces). The
model only works on the exact ids the reference tokenizer produces, and
error messages and stack traces, the text Baton triages, are full of the
whitespace patterns where tokenizers differ.

The brief asks for a Go library that loads `tokenizer.json` with exact
parity, and to stop before choosing a cgo tokenizer if none exists. Two
pure-Go libraries were checked against reference ids from Hugging Face
`tokenizers` 0.23.3 on a 36-case corpus:

| Library | Result | Why |
|---|---|---|
| `github.com/sugarme/tokenizer` v0.3.0 | 25/36 | ignores added tokens (space runs, `[CLS]`...), no NFC, wrong merge order on long input |
| `github.com/gomlx/go-huggingface` v0.4.15 | 33/36 | merges `\n\t` and `\n\n` because Go's RE2 lacks the `\s+(?!\S)` lookahead, so every stack trace differs; ignores `lstrip`; requires Go 1.27 |

## Decision

Implement the pipeline in `internal/laya/tokenizer`, supporting exactly
the components this `tokenizer.json` uses and refusing to load anything
else: leftmost-longest added-token extraction (raw, then normalized) with
`lstrip`, NFC via `golang.org/x/text`, the GPT-2 split pattern hand-coded
with its lookahead, GPT-2's byte-to-rune map and rank-ordered BPE. Chosen
over a cgo binding to the Rust library (`daulet/tokenizers`), which would
add a second cgo dependency and a Rust artifact to the build, and over
patching and forking `gomlx`. Decided with the project owner after the
evaluation above.

Parity is a test, not an assumption: `tools/parity/tokenizer_fuzz.py`
generates a seeded corpus aimed at every pipeline stage (all Unicode
whitespace, long runs, combining marks and decomposed Hangul, several
scripts, emoji sequences, contractions, added tokens whole and partial,
filled error templates and stack traces up to 2,000 characters), records
Hugging Face's ids, and `TestParityWithHuggingFace` requires every case to
match exactly. The committed corpus has 3,036 cases; a 20,036-case corpus
from a different seed also matches in full.

## Consequences

- No Rust toolchain or second cgo dependency; the tokenizer builds and
  tests with `CGO_ENABLED=0`.
- About 35µs to encode a typical stack trace, negligible next to inference.
- The risk is behaviour the corpus does not reach. The loader rejects
  unsupported components outright, and a changed `tokenizer.json` (new
  model revision) requires regenerating the corpus.
- `tokenizer.json` is a model file and is not committed; the parity test
  reads it from the model cache and skips with instructions when absent.

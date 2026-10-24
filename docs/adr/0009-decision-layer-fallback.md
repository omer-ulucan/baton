# 0009. Decision layer: policy interface, bounded inference, fallback, recorded decisions

Status: Accepted

## Context

Failure triage and priority classification can use Laya, but the model is
slow on CPU (hundreds of milliseconds), may be absent or corrupt, and its
accuracy on our domain is unknown (the model card reports 36.4% zero-shot on
its own benchmark). Decisions also feed the event-sourced state machine,
which must replay deterministically.

## Decision

- One small interface, `DecisionPolicy.Decide(ctx, Request) (Decision, error)`.
  `RulePolicy` is deterministic, never fails, and is the default.
  `LayaPolicy` runs the ONNX model. The engine uses a `FallbackPolicy` that
  wraps the primary with a timeout and a confidence threshold and falls back
  to `RulePolicy` on error, timeout, low confidence, or unavailability.
- Inference never runs inside a database transaction, and never on the claim
  path, the reaper, the retry timer, or leader renewal. A semaphore bounds
  concurrent ONNX runs; acquiring it honours the request deadline. Concurrent
  requests are micro-batched (max 8, max wait 5ms). The session is loaded
  once per process.
- Every decision is recorded as a `DecisionMade` event in the transaction it
  drives, with input hash, label, confidence, policy, model revision,
  fallback reason and, on fallback, the primary's answer. Replay reads
  decisions from history and never runs a policy.

## Consequences

- The worst added latency on the failure and submission paths is the
  configured timeout. Timed-out ONNX runs finish in the background and are
  discarded (the C API run cannot be cancelled), which the semaphore keeps
  bounded.
- Events carry both answers on fallback, so the eval can compare policies on
  live traffic, not just the offline dataset.
- If Laya does not beat the rules, the system loses nothing: the default stays
  `rule` and EVAL.md says so.

## Alternatives considered

- **Async triage (fail now, decide later):** keeps `POST /fail` fast but
  adds a `triaging` state, a sweeper for decisions lost to crashes, and
  more replay cases. Revisit if the timeout proves too costly.
- **Re-running the model on replay:** non-deterministic across model
  versions and hardware; rejected outright.

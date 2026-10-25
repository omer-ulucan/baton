## What this means

**Recommendation: keep `BATON_DECISION_POLICY=rule` (the default). Laya is not
recommended for failure triage at revision `42e2a6e`.**

- **Laya alone is worse than the rules** (31.9% vs 47.5% exact). It leans hard
  towards `dead_letter`: more than half of the failures that would have
  recovered are dead-lettered, which strands workflows. It never predicts
  `escalate` and almost never `compensate`. This matches the model card's
  36.4% on its own benchmark and the fact that error triage is outside its
  training distribution.
- **Its confidence carries little information.** Accuracy rises from the 0.2
  to the 0.4-0.5 band, then falls: the 13 answers above 0.5 are right 15% of
  the time. A confidence threshold therefore cannot select good answers;
  raising it just hands more decisions back to the rules.
- **The production setting is close to the rules alone.** At the default
  threshold 0.5 the model answers 6% of cases, and the result is within two
  points of rules-only, with slightly fewer wrongful retries and a few more
  wrongful dead-letters. Running Laya buys nothing measurable here, at about
  0.8s and 1.4 GB per replica.
- **The rules have a clear weakness: the cautious default.** Anything they do
  not recognise is retried with backoff, so failures that need compensation
  or a human (78 cases) burn their remaining attempts first. They recognise
  only a few phrasings of partial side effects (`compensate` recall 5%) and
  of operational danger (`escalate` recall 21%). They never dead-letter a
  recoverable failure, and with a worker's `retryable: false` hint or
  exhausted attempts they always dead-letter.

## Caveats

- The dataset and the rule table were written by the same author, which
  biases the comparison towards the rules; the messages were written to
  look like what real systems emit (gRPC, AWS, Stripe, Kafka, Postgres
  SQLSTATEs, kernel and Kubernetes errors), not to match the rules, and the
  rules' 47.5% suggests they were not tuned to it. The rules were not
  changed after seeing these results; improving them needs a separate,
  held-out set, or this one stops being an evaluation.
- Gold labels involve judgement, mostly between `retry` and
  `retry_with_backoff` (hence the coarse accuracy column) and between
  `dead_letter` and `escalate` for configuration and permission errors.
- 204 cases is small: a 1-point difference is two cases.
- Laya is evaluated zero-shot, as Baton runs it. A checkpoint fine-tuned on
  triage data would need its own evaluation.

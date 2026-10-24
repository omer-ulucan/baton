package decision

import "context"

// RulePolicy is the deterministic Policy: always available, never errors,
// and the default (SPEC 10.1). The rule table grows in phase 7; the rules
// here are the ones every later rule set must keep.
type RulePolicy struct{}

// Decide implements Policy.
func (RulePolicy) Decide(_ context.Context, req Request) (Decision, error) {
	d := Decision{Policy: "rule", InputHash: req.Hash(), Confidence: 1}
	switch req.Kind {
	case KindPriority:
		d.Label = PriorityNormal
	default:
		switch {
		case req.Retryable != nil && !*req.Retryable:
			// The worker knows best that retrying cannot help.
			d.Label = DeadLetter
		case req.MaxAttempts > 0 && req.Attempt >= req.MaxAttempts:
			d.Label = DeadLetter
		default:
			d.Label = RetryWithBackoff
			d.Confidence = 0.6
		}
	}
	return d, nil
}

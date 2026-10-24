package decision

import (
	"context"
	"regexp"
	"strings"
)

// RulePolicy is the deterministic Policy: always available, never errors,
// and the default (SPEC 10.1). Rules are tried in order and the first
// match decides; its confidence is fixed per rule.
type RulePolicy struct{}

type rule struct {
	label      string
	confidence float64
	re         *regexp.Regexp
}

// Triage rules, matched case-insensitively against "type: message". Order
// matters: damage and danger first, then permanent errors, then the two
// kinds of transient failure.
var triageRules = []rule{
	{Compensate, 0.85, regexp.MustCompile(`(?i)\b(partial(ly)? (applied|committed|post)|inconsisten|corrupt(ed)? (state|ledger|record|data)|left in an? (bad|unknown) state|balance mismatch|double[- ]?(charged|posted)|half[- ]applied)`)},
	{Escalate, 0.85, regexp.MustCompile(`(?i)(no space left|disk (is )?full|out of memory|\boom\b|oomkilled|data loss|security|breach|certificate (has )?expired|x509|misconfigur|invalid configuration|license expired|quota exceeded for (project|account))`)},
	{DeadLetter, 0.9, regexp.MustCompile(`(?i)(invalid|validation|malformed|bad request|\b40[0134]\b|\b422\b|not found|no such|unauthori[sz]ed|forbidden|permission denied|access denied|declined|unsupported|cannot (parse|unmarshal|decode)|parse error|syntax error|unknown (field|column|key)|keyerror|valueerror|typeerror|attributeerror|nil pointer|null ?pointer|index out of range|division by zero|unique constraint|duplicate key|23505|already exists)`)},
	{RetryWithBackoff, 0.9, regexp.MustCompile(`(?i)(time[d ]?out|deadline exceeded|\b429\b|too many requests|rate[- ]?limit|throttl|\b503\b|service unavailable|overload|try again later|busy|deadlock|lock (wait )?timeout|could not obtain lock|serialization failure|40001|40p01|connection pool exhausted|too many connections)`)},
	{Retry, 0.8, regexp.MustCompile(`(?i)(connection (reset|refused|closed|aborted)|broken pipe|econnreset|unexpected eof|\beof\b|temporary failure|network is unreachable|no route to host|\b50[24]\b|bad gateway|gateway timeout|tls handshake|i/o error)`)},
}

// Priority rules, matched against the step description.
var priorityRules = []rule{
	{PriorityCritical, 0.85, regexp.MustCompile(`(?i)(outage|incident|\bdown\b|security|breach|data loss|revenue|checkout|payment (fail|outage)|p0\b|sev ?1)`)},
	{PriorityHigh, 0.8, regexp.MustCompile(`(?i)(refund|customer|user[- ]facing|urgent|asap|\bsla\b|deadline|charged twice|escalat)`)},
	{PriorityLow, 0.8, regexp.MustCompile(`(?i)(nightly|weekly|monthly|digest|newsletter|report|cleanup|clean up|backfill|analytics|re-?index|rebuild .*index|archiv|housekeeping|best effort)`)},
}

// Decide implements Policy.
func (RulePolicy) Decide(_ context.Context, req Request) (Decision, error) {
	d := Decision{Policy: "rule", InputHash: req.Hash()}
	if req.Kind == KindPriority {
		d.Label, d.Confidence = PriorityNormal, 0.5
		for _, r := range priorityRules {
			if r.re.MatchString(req.Description) {
				d.Label, d.Confidence = r.label, r.confidence
				break
			}
		}
		return d, nil
	}
	switch {
	case req.Retryable != nil && !*req.Retryable:
		// The worker knows best that retrying cannot help.
		d.Label, d.Confidence = DeadLetter, 1
		return d, nil
	case req.MaxAttempts > 0 && req.Attempt >= req.MaxAttempts:
		d.Label, d.Confidence = DeadLetter, 1
		return d, nil
	}
	text := strings.TrimSpace(req.ErrorType + ": " + req.ErrorMessage)
	for _, r := range triageRules {
		if r.re.MatchString(text) {
			d.Label, d.Confidence = r.label, r.confidence
			return d, nil
		}
	}
	// Unknown errors: retrying with backoff is the cheapest safe default.
	d.Label, d.Confidence = RetryWithBackoff, 0.5
	return d, nil
}

// PriorityValue maps a priority label to a task priority; "normal" keeps
// the priority the step inherited.
func PriorityValue(label string, inherited int) int {
	switch label {
	case PriorityLow:
		return 20
	case PriorityHigh:
		return 80
	case PriorityCritical:
		return 95
	}
	return inherited
}

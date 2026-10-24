package decision

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func triage(typ, msg string) Request {
	return Request{Kind: KindTriage, ErrorType: typ, ErrorMessage: msg, Attempt: 1, MaxAttempts: 5}
}

func TestRuleTriage(t *testing.T) {
	no, yes := false, true
	tests := []struct {
		req  Request
		want string
	}{
		{Request{Kind: KindTriage, Retryable: &no, Attempt: 1, MaxAttempts: 5, ErrorMessage: "timed out"}, DeadLetter},
		{Request{Kind: KindTriage, Retryable: &yes, Attempt: 5, MaxAttempts: 5}, DeadLetter},
		{triage("ConnectionResetError", "connection reset by peer"), Retry},
		{triage("HTTPError", "502 Bad Gateway"), Retry},
		{triage("TimeoutError", "upstream timed out after 30s"), RetryWithBackoff},
		{triage("HTTPError", "429 Too Many Requests: retry after 30s"), RetryWithBackoff},
		{triage("pq.Error", "deadlock detected (SQLSTATE 40P01)"), RetryWithBackoff},
		{triage("ValidationError", "invalid card number"), DeadLetter},
		{triage("PermissionError", "access denied for bucket invoices-prod"), DeadLetter},
		{triage("KeyError", "'customer_id'"), DeadLetter},
		{triage("panic", "runtime error: invalid memory address or nil pointer dereference"), DeadLetter},
		{triage("OSError", "no space left on device"), Escalate},
		{triage("LedgerError", "balance mismatch after partial post; ledger left inconsistent"), Compensate},
		{triage("Weird", "something odd happened"), RetryWithBackoff},
	}
	for _, tt := range tests {
		d, err := RulePolicy{}.Decide(t.Context(), tt.req)
		if err != nil || d.Label != tt.want || d.Policy != "rule" || d.InputHash != tt.req.Hash() {
			t.Errorf("%s %q: %+v %v, want %s", tt.req.ErrorType, tt.req.ErrorMessage, d, err, tt.want)
		}
	}
}

func TestRulePriority(t *testing.T) {
	for desc, want := range map[string]string{
		"Restore checkout after the payment provider outage": PriorityCritical,
		"Refund a customer who was charged twice":            PriorityHigh,
		"Send the weekly newsletter digest":                  PriorityLow,
		"Rebuild the search index for the docs site":         PriorityLow,
		"Resize product images":                              PriorityNormal,
	} {
		d, _ := RulePolicy{}.Decide(t.Context(), Request{Kind: KindPriority, Description: desc})
		if d.Label != want {
			t.Errorf("%q = %s, want %s", desc, d.Label, want)
		}
	}
	if PriorityValue(PriorityNormal, 63) != 63 || PriorityValue(PriorityCritical, 10) != 95 {
		t.Fatal("PriorityValue")
	}
}

type stub struct {
	d     Decision
	err   error
	delay time.Duration
}

func (s stub) Decide(ctx context.Context, _ Request) (Decision, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return Decision{}, ctx.Err()
		}
	}
	return s.d, s.err
}

func TestFallbackPolicy(t *testing.T) {
	model := Decision{Label: Escalate, Confidence: 0.9, Policy: "laya", ModelRevision: "rev"}
	req := triage("TimeoutError", "timed out")
	tests := []struct {
		name    string
		primary Policy
		reason  string
		label   string
	}{
		{"confident model", stub{d: model}, "", Escalate},
		{"low confidence", stub{d: Decision{Label: Escalate, Confidence: 0.2, Policy: "laya"}}, FallbackLowConfidence, RetryWithBackoff},
		{"unavailable", stub{err: ErrUnavailable}, FallbackUnavailable, RetryWithBackoff},
		{"error", stub{err: errors.New("onnx exploded")}, FallbackError, RetryWithBackoff},
		{"timeout", stub{d: model, delay: time.Second}, FallbackTimeout, RetryWithBackoff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen int
			f := FallbackPolicy{Primary: tt.primary, Fallback: RulePolicy{}, Threshold: 0.5,
				Timeouts: map[Kind]time.Duration{KindTriage: 50 * time.Millisecond},
				Observe:  func(Request, Decision, time.Duration) { seen++ }}
			start := time.Now()
			d, err := f.Decide(t.Context(), req)
			if err != nil || d.Label != tt.label || d.Fallback != tt.reason || seen != 1 {
				t.Fatalf("decision = %+v %v (observed %d)", d, err, seen)
			}
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("fallback waited past the timeout")
			}
			if tt.name == "low confidence" && (d.Primary == nil || d.Primary.Label != Escalate || d.Primary.Policy != "laya") {
				t.Fatalf("primary answer not recorded: %+v", d.Primary)
			}
		})
	}
}

func TestRequestHashIsStableAndSensitive(t *testing.T) {
	id := uuid.New()
	a := Request{Kind: KindTriage, TaskID: id, ErrorMessage: "x", Attempt: 1}
	b := a
	if a.Hash() != b.Hash() {
		t.Fatal("equal requests hash differently")
	}
	b.ErrorMessage = "y"
	if a.Hash() == b.Hash() {
		t.Fatal("different requests hash equal")
	}
	if len(a.Hash()) != 64 {
		t.Fatalf("hash %q is not hex sha256", a.Hash())
	}
}

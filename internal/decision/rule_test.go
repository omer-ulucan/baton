package decision

import (
	"testing"

	"github.com/google/uuid"
)

func TestRulePolicyTriage(t *testing.T) {
	no, yes := false, true
	tests := []struct {
		name string
		req  Request
		want string
	}{
		{"non-retryable hint", Request{Kind: KindTriage, Retryable: &no, Attempt: 1, MaxAttempts: 5}, DeadLetter},
		{"attempts exhausted", Request{Kind: KindTriage, Retryable: &yes, Attempt: 5, MaxAttempts: 5}, DeadLetter},
		{"transient", Request{Kind: KindTriage, Attempt: 1, MaxAttempts: 5}, RetryWithBackoff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := RulePolicy{}.Decide(t.Context(), tt.req)
			if err != nil {
				t.Fatal(err)
			}
			if d.Label != tt.want || d.Policy != "rule" || d.InputHash != tt.req.Hash() {
				t.Fatalf("decision = %+v", d)
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

package main

import (
	"testing"

	"github.com/omer-ulucan/baton/internal/decision"
)

// The brief requires at least 200 realistic, labeled failures; keep the
// classes roughly balanced so accuracy is not dominated by one of them.
func TestDatasetShape(t *testing.T) {
	rows, err := load("../../eval/errors.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 200 {
		t.Fatalf("%d cases, need at least 200", len(rows))
	}
	counts, ids := map[string]int{}, map[string]bool{}
	for _, r := range rows {
		counts[r.Label]++
		if ids[r.ID] || r.Message == "" || r.ErrorType == "" {
			t.Fatalf("bad or duplicate row %+v", r)
		}
		ids[r.ID] = true
	}
	for _, l := range labels {
		if counts[l] < 35 {
			t.Errorf("label %s has only %d cases", l, counts[l])
		}
	}
}

// TestRuleBaseline pins the rule policy's result on the dataset so a rule
// change that silently moves the published numbers fails here first.
func TestRuleBaseline(t *testing.T) {
	rows, _ := load("../../eval/errors.jsonl")
	ps := make([]pred, len(rows))
	for i, r := range rows {
		d, _ := decision.RulePolicy{}.Decide(t.Context(), request(r))
		ps[i] = pred{label: d.Label}
	}
	s := evaluate(rows, ps)
	if s.correct != 97 || s.wrongDLQ != 0 {
		t.Fatalf("rule policy: %d correct, %d wrongful DLQ; docs/EVAL.md reports 97 and 0 (regenerate it if this change is intended)", s.correct, s.wrongDLQ)
	}
}

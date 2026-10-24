package laya

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/laya/model"
)

// TestQuestionsMatchFixtures keeps the Go questions and the fixture
// generator in sync; otherwise the parity evidence would be about other
// questions than the ones Baton asks.
func TestQuestionsMatchFixtures(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range loadFixtures(t) {
		for qid, fq := range c.Questions {
			var want Question
			switch qid {
			case "triage":
				want = TriageQuestion
			case "priority":
				want = PriorityQuestion
			default:
				continue
			}
			if got := fq.question(t); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s question differs from the fixtures:\n got  %+v\n want %+v", qid, got, want)
			}
			seen[qid] = true
		}
	}
	if !seen["triage"] || !seen["priority"] {
		t.Fatal("fixtures lack Baton's questions")
	}
}

func TestTriageStateFormat(t *testing.T) {
	got := TriageState(decision.Request{ErrorType: "KeyError", ErrorMessage: "'customer_id'", Stack: "line 1\nline 2\n", Handler: "payments.charge", Attempt: 1, MaxAttempts: 5})
	want := "error_type: KeyError\nmessage: 'customer_id'\nstack:\nline 1\nline 2\nhandler: payments.charge\nattempt: 1 of 5"
	if got != want {
		t.Fatalf("state = %q", got)
	}
	// The fixtures were built from states in this format.
	found := false
	for _, c := range loadFixtures(t) {
		if strings.HasPrefix(c.State, "error_type: KeyError\nmessage: 'customer_id'\nstack:\n") {
			found = true
		}
	}
	if !found {
		t.Fatal("fixture states no longer follow TriageState's format")
	}
}

func TestPolicyUnavailableUntilLoaded(t *testing.T) {
	p := &Policy{}
	if _, err := p.Decide(t.Context(), decision.Request{Kind: decision.KindTriage}); !errors.Is(err, decision.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestLoaderFailsSafelyOnCorruptModel(t *testing.T) {
	dir := t.TempDir()
	p := &Policy{}
	l := NewLoader(LoaderConfig{Session: SessionConfig{ModelDir: dir, Variant: model.FP32, LibPath: "/nonexistent.so"}}, p)
	l.Run(t.Context())
	if l.Loaded() || !strings.HasPrefix(l.State(), "failed:") {
		t.Fatalf("state = %s", l.State())
	}
	f := decision.FallbackPolicy{Primary: p, Fallback: decision.RulePolicy{}, Threshold: 0.5}
	d, err := f.Decide(t.Context(), decision.Request{Kind: decision.KindTriage, ErrorMessage: "timed out", Attempt: 1, MaxAttempts: 3})
	if err != nil || d.Policy != "rule" || d.Fallback != decision.FallbackUnavailable {
		t.Fatalf("decision = %+v %v", d, err)
	}
}

func TestPolicyAnswersWithModel(t *testing.T) {
	m := loadModel(t)
	p := &Policy{}
	p.model.Store(m)
	d, err := p.Decide(t.Context(), decision.Request{
		Kind: decision.KindTriage, ErrorType: "TimeoutError", ErrorMessage: "upstream timed out after 30s",
		Handler: "inventory.reserve", Attempt: 2, MaxAttempts: 5,
	})
	if err != nil || d.Policy != "laya" || d.ModelRevision != model.RevisionLabel(model.FP32) || d.Label != decision.RetryWithBackoff {
		t.Fatalf("decision = %+v %v", d, err)
	}
	// The same input as fixture case 1 gives the same confidence.
	if want := maxProb(loadFixtures(t)[1].Questions["triage"].Probabilities); abs(d.Confidence-want) > 1e-4 {
		t.Fatalf("confidence %v, fixture %v", d.Confidence, want)
	}
	pd, err := p.Decide(t.Context(), decision.Request{Kind: decision.KindPriority, Description: "Restore checkout after the payment provider outage"})
	if err != nil || pd.Policy != "laya" {
		t.Fatalf("priority = %+v %v", pd, err)
	}
	// A deadline shorter than inference falls back with reason timeout.
	f := decision.FallbackPolicy{Primary: p, Fallback: decision.RulePolicy{}, Threshold: 0,
		Timeouts: map[decision.Kind]time.Duration{decision.KindTriage: 5 * time.Millisecond}}
	fd, _ := f.Decide(t.Context(), decision.Request{Kind: decision.KindTriage, ErrorMessage: "x", Attempt: 1, MaxAttempts: 2})
	if fd.Fallback != decision.FallbackTimeout || fd.Policy != "rule" {
		t.Fatalf("short deadline: %+v", fd)
	}
}

func maxProb(p []float64) float64 {
	m := p[0]
	for _, v := range p {
		m = max(m, v)
	}
	return m
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

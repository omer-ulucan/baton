package telemetry

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/store"
)

func TestMetricsRecordEngineAndDecisionEvents(t *testing.T) {
	m := NewMetrics()
	m.Claimed(3 * time.Millisecond)
	m.TaskFinished("payments", "succeeded", time.Second)
	m.TaskFinished("payments", "failed", time.Second)
	m.RetryScheduled("payments")
	m.LeaseExpired("payments")
	m.WorkflowFinished("compensated")
	m.Fenced()
	m.Decision(decision.Request{Kind: decision.KindTriage}, decision.Decision{Policy: "rule", Label: "retry", Fallback: "timeout"}, time.Millisecond)
	m.Decision(decision.Request{Kind: decision.KindTriage}, decision.Decision{Policy: "laya", Label: "retry"}, time.Millisecond)

	for name, want := range map[string]float64{
		"baton_task_retries_total":       1,
		"baton_lease_expirations_total":  1,
		"baton_workflows_finished_total": 1,
		"baton_fencing_rejections_total": 1,
		"baton_decision_fallback_total":  1,
		"baton_decisions_total":          2,
	} {
		if got := sum(t, m, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if n := testutil.CollectAndCount(m.taskDuration); n != 2 {
		t.Errorf("task duration series = %d, want succeeded and failed", n)
	}
}

func TestStateGaugesAreReadAtScrape(t *testing.T) {
	m := NewMetrics()
	leader := false
	m.Observe(Sources{
		QueueStats: func(context.Context) ([]store.QueueStat, error) {
			return []store.QueueStat{{Queue: "q", Ready: 4, Running: 2}}, nil
		},
		OutboxPending: func(context.Context) (int, error) { return 0, errors.New("db down") },
		IsLeader:      func() bool { return leader },
		LeaderChanges: func() int { return 3 },
		ModelLoaded:   func() bool { return true },
	})
	leader = true
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`baton_queue_depth{queue="q",status="ready"} 4`,
		`baton_queue_depth{queue="q",status="running"} 2`,
		`baton_is_leader 1`,
		`baton_leader_changes_total 3`,
		`baton_decision_model_loaded 1`,
		`baton_state_scrape_errors 1`, // the outbox read failed; the scrape still succeeds
		`go_goroutines`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

func sum(t *testing.T, m *Metrics, name string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, mt := range f.GetMetric() {
			total += mt.GetCounter().GetValue()
		}
	}
	return total
}

func TestTraceParentRoundTrip(t *testing.T) {
	if TraceParent(t.Context()) != "" {
		t.Fatal("no span, no traceparent")
	}
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if got := TraceParent(WithTraceParent(t.Context(), tp)); got != tp {
		t.Fatalf("round trip = %q", got)
	}
}

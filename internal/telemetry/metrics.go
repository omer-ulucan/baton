// Package telemetry is Baton's metrics (Prometheus) and tracing
// (OpenTelemetry). Everything hangs off explicit registries and providers
// passed to the components; nothing is registered globally.
package telemetry

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/store"
)

// Metrics are the orchestrator's Prometheus metrics (SPEC 11).
type Metrics struct {
	reg *prometheus.Registry

	claimLatency      prometheus.Histogram
	taskDuration      *prometheus.HistogramVec
	retries           *prometheus.CounterVec
	leaseExpirations  *prometheus.CounterVec
	workflowsFinished *prometheus.CounterVec
	fencingRejections prometheus.Counter
	decisionLatency   *prometheus.HistogramVec
	decisions         *prometheus.CounterVec
	decisionFallbacks *prometheus.CounterVec
}

// NewMetrics creates the metrics on a fresh registry, with Go runtime and
// process collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	f := func(c prometheus.Collector) prometheus.Collector { reg.MustRegister(c); return c }
	m := &Metrics{reg: reg}
	f(collectors.NewGoCollector())
	f(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.claimLatency = f(prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "baton_claim_latency_seconds", Help: "Time to grant a claim that returned tasks (the claim transaction, not the long-poll wait).",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})).(prometheus.Histogram)
	m.taskDuration = f(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "baton_task_duration_seconds", Help: "Attempt duration from claim to its end, by queue and outcome (succeeded, failed, expired, released).",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	}, []string{"queue", "outcome"})).(*prometheus.HistogramVec)
	m.retries = f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "baton_task_retries_total", Help: "Retries scheduled, by queue.",
	}, []string{"queue"})).(*prometheus.CounterVec)
	m.leaseExpirations = f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "baton_lease_expirations_total", Help: "Attempts whose lease expired and were reaped, by queue.",
	}, []string{"queue"})).(*prometheus.CounterVec)
	m.workflowsFinished = f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "baton_workflows_finished_total", Help: "Workflows reaching a terminal state, by status.",
	}, []string{"status"})).(*prometheus.CounterVec)
	m.fencingRejections = f(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "baton_fencing_rejections_total", Help: "Writes refused because this replica's fencing token was stale.",
	})).(prometheus.Counter)
	m.decisionLatency = f(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "baton_decision_latency_seconds", Help: "Decision latency including fallback, by kind and the policy that answered.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, []string{"kind", "policy"})).(*prometheus.HistogramVec)
	m.decisions = f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "baton_decisions_total", Help: "Decisions, by kind, answering policy and label.",
	}, []string{"kind", "policy", "label"})).(*prometheus.CounterVec)
	m.decisionFallbacks = f(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "baton_decision_fallback_total", Help: "Decisions answered by the fallback policy, by kind and reason.",
	}, []string{"kind", "reason"})).(*prometheus.CounterVec)
	return m
}

// Registry is the metrics registry (tests read it).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// The engine.Observer methods.

func (m *Metrics) Claimed(latency time.Duration) { m.claimLatency.Observe(latency.Seconds()) }
func (m *Metrics) TaskFinished(queue, outcome string, d time.Duration) {
	m.taskDuration.WithLabelValues(queue, outcome).Observe(d.Seconds())
}
func (m *Metrics) RetryScheduled(queue string)    { m.retries.WithLabelValues(queue).Inc() }
func (m *Metrics) LeaseExpired(queue string)      { m.leaseExpirations.WithLabelValues(queue).Inc() }
func (m *Metrics) WorkflowFinished(status string) { m.workflowsFinished.WithLabelValues(status).Inc() }
func (m *Metrics) Fenced()                        { m.fencingRejections.Inc() }

// Decision records a decision (decision.FallbackPolicy.Observe).
func (m *Metrics) Decision(req decision.Request, d decision.Decision, latency time.Duration) {
	kind := string(req.Kind)
	m.decisionLatency.WithLabelValues(kind, d.Policy).Observe(latency.Seconds())
	m.decisions.WithLabelValues(kind, d.Policy, d.Label).Inc()
	if d.Fallback != "" {
		m.decisionFallbacks.WithLabelValues(kind, d.Fallback).Inc()
	}
}

// Sources are read at scrape time.
type Sources struct {
	QueueStats    func(context.Context) ([]store.QueueStat, error)
	OutboxPending func(context.Context) (int, error)
	IsLeader      func() bool
	LeaderChanges func() int
	ModelLoaded   func() bool // nil when the rule policy is configured
}

// Observe registers gauges that read state at scrape time.
func (m *Metrics) Observe(s Sources) {
	m.reg.MustRegister(&stateCollector{s: s,
		depth:   prometheus.NewDesc("baton_queue_depth", "Tasks per queue and status.", []string{"queue", "status"}, nil),
		outbox:  prometheus.NewDesc("baton_outbox_pending", "Outbox messages not yet delivered.", nil, nil),
		scrapeE: prometheus.NewDesc("baton_state_scrape_errors", "1 if reading state for this scrape failed.", nil, nil),
	})
	if s.IsLeader != nil {
		m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "baton_is_leader", Help: "1 while this replica holds the leader lease.",
		}, func() float64 { return b2f(s.IsLeader()) }))
	}
	if s.LeaderChanges != nil {
		m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "baton_leader_changes_total", Help: "Leadership transitions of this replica (gained or lost).",
		}, func() float64 { return float64(s.LeaderChanges()) }))
	}
	if s.ModelLoaded != nil {
		m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "baton_decision_model_loaded", Help: "1 once the Laya model answers decisions.",
		}, func() float64 { return b2f(s.ModelLoaded()) }))
	}
}

type stateCollector struct {
	s                      Sources
	depth, outbox, scrapeE *prometheus.Desc
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.outbox
	ch <- c.scrapeE
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	failed := false
	if c.s.QueueStats != nil {
		stats, err := c.s.QueueStats(ctx)
		failed = failed || err != nil
		for _, st := range stats {
			for status, n := range map[string]int{"pending": st.Pending, "ready": st.Ready, "running": st.Running,
				"retry_wait": st.RetryWait, "dead_lettered": st.DeadLettered} {
				ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(n), st.Queue, status)
			}
		}
	}
	if c.s.OutboxPending != nil {
		n, err := c.s.OutboxPending(ctx)
		failed = failed || err != nil
		if err == nil {
			ch <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(n))
		}
	}
	ch <- prometheus.MustNewConstMetric(c.scrapeE, prometheus.GaugeValue, b2f(failed))
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

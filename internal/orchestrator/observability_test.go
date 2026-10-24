package orchestrator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/omer-ulucan/baton/internal/api"
	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/orchestrator"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/telemetry"
	"github.com/omer-ulucan/baton/internal/worker"
	"github.com/omer-ulucan/baton/internal/worker/builtin"
)

// TestTraceSpansSubmissionToExecution runs a replica and a worker with an
// in-memory span recorder and checks that submission, the worker's
// execution and the completion report form one trace, and that /metrics
// reports the run.
func TestTraceSpansSubmissionToExecution(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	_, dbURL := pg.NewDB(t)
	pool, err := pgxpool.New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ln, _ := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	url := "http://" + ln.Addr().String()
	rep := orchestrator.New(orchestrator.Options{
		Config: config.Orchestrator{
			ReplicaID: "r1", AdvertiseAddr: url, LeaderTTL: leaderTTL, LeaseTTL: 30 * time.Second,
			HeartbeatInterval: 10 * time.Second, ShutdownTimeout: time.Second, DecisionThreshold: 0.5,
			TriageTimeout: time.Second, PriorityTimeout: time.Second,
		},
		Pool: pool, Listener: ln, Log: testLog(), Tracer: tp,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rep.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	waitFor(t, "leadership", 3*leaderTTL, rep.IsLeader)

	hc := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport,
		otelhttp.WithTracerProvider(tp), otelhttp.WithPropagators(telemetry.Propagator))}
	wctx, stopWorker := context.WithCancel(context.Background())
	r := worker.NewRunner(worker.NewClient([]string{url}, hc), builtin.Handlers(), worker.Config{
		ID: "w1", Queues: []string{"default"}, ClaimWait: 200 * time.Millisecond, DrainTimeout: time.Second, Tracer: tp,
	}, nil)
	wdone := make(chan error, 1)
	go func() { wdone <- r.Run(wctx) }()
	defer func() {
		stopWorker()
		if err := <-wdone; err != nil {
			t.Error(err)
		}
	}()

	body, _ := json.Marshal(api.SubmitRequest{Definition: "name: t\nversion: 1\nsteps: [{name: a, handler: echo, description: \"Send the weekly digest\"}]"})
	req, _ := http.NewRequestWithContext(t.Context(), "POST", url+"/v1/workflows", bytes.NewReader(body))
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var sub api.SubmitResponse
	_ = json.NewDecoder(resp.Body).Decode(&sub)
	resp.Body.Close()
	waitFor(t, "the workflow to succeed", 15*time.Second, func() bool {
		snap, err := rep.Store().Snapshot(context.Background(), sub.WorkflowID)
		return err == nil && snap.Workflow != nil && snap.Workflow.Status == state.WfSucceeded
	})
	time.Sleep(200 * time.Millisecond) // let the worker's last spans end

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		byName[s.Name()] = s
	}
	submit, okS := byName["workflow.submit"]
	exec, okE := byName["task.execute echo"]
	complete, okC := byName["task.complete"]
	if !okS || !okE || !okC {
		names := []string{}
		for n := range byName {
			names = append(names, n)
		}
		t.Fatalf("missing spans; have %v", names)
	}
	trace := submit.SpanContext().TraceID()
	if exec.SpanContext().TraceID() != trace || complete.SpanContext().TraceID() != trace {
		t.Fatalf("spans are in different traces: submit %s, execute %s, complete %s",
			trace, exec.SpanContext().TraceID(), complete.SpanContext().TraceID())
	}
	if exec.Parent().SpanID() != submit.SpanContext().SpanID() {
		t.Fatal("execution is not a child of the submission")
	}
	if _, ok := byName["POST /v1/workflows"]; !ok {
		t.Error("HTTP server spans are not named after their route")
	}

	mreq, _ := http.NewRequestWithContext(t.Context(), "GET", url+"/metrics", nil)
	mresp, err := http.DefaultClient.Do(mreq)
	if err != nil {
		t.Fatal(err)
	}
	metrics, _ := io.ReadAll(mresp.Body)
	mresp.Body.Close()
	for _, want := range []string{
		`baton_task_duration_seconds_count{outcome="succeeded",queue="default"} 1`,
		`baton_workflows_finished_total{status="succeeded"} 1`,
		`baton_decisions_total{kind="priority",label="low",policy="rule"} 1`,
		`baton_is_leader 1`,
		`baton_queue_depth{queue="default",status="ready"} 0`,
		`baton_claim_latency_seconds_count`,
	} {
		if !strings.Contains(string(metrics), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

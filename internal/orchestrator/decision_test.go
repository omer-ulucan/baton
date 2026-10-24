package orchestrator_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/orchestrator"
	"github.com/omer-ulucan/baton/internal/state"
)

// TestLayaWithoutAModelFallsBack runs a replica configured for Laya whose
// model is missing (download disabled): it must start, triage failures
// with the rule policy and record why, never failing a request.
func TestLayaWithoutAModelFallsBack(t *testing.T) {
	_, dbURL := pg.NewDB(t)
	pool, err := pgxpool.New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ln, _ := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	rep := orchestrator.New(orchestrator.Options{
		Config: config.Orchestrator{
			ReplicaID: "r1", AdvertiseAddr: "http://" + ln.Addr().String(), LeaderTTL: leaderTTL,
			LeaseTTL: 30 * time.Second, HeartbeatInterval: 10 * time.Second, ShutdownTimeout: time.Second,
			DecisionPolicy: "laya", DecisionThreshold: 0.5, ModelDir: t.TempDir(), ModelDownload: false,
			LayaVariant: "fp32", ONNXRuntimeLib: "/nonexistent/libonnxruntime.so",
			TriageTimeout: 200 * time.Millisecond, PriorityTimeout: 200 * time.Millisecond,
		},
		Pool: pool, Listener: ln, Log: testLog(),
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

	eng := rep.Engine()
	out, err := eng.Submit(t.Context(), engine.Submission{Definition: []byte(`
name: fb
version: 1
steps: [{name: a, handler: h, description: "Restore checkout after the outage"}]`)})
	if err != nil {
		t.Fatal(err)
	}
	_ = eng.Register(t.Context(), engine.Worker{ID: "w", Queues: []string{"default"}, Handlers: []string{"h"}})
	got, err := eng.Claim(t.Context(), engine.ClaimRequest{WorkerID: "w"})
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	next, err := eng.Fail(t.Context(), engine.Result{WorkerID: "w", TaskID: got[0].TaskID, AttemptID: got[0].AttemptID},
		engine.Failure{Message: "upstream timed out", Type: "TimeoutError"})
	if err != nil || next != engine.NextRetry {
		t.Fatalf("fail = %v %v", next, err)
	}
	events, _ := rep.Store().Events(t.Context(), out.WorkflowID)
	kinds := map[string]state.DecisionMade{}
	for _, ev := range events {
		if d, ok := ev.Payload.(state.DecisionMade); ok {
			kinds[d.Kind] = d
		}
	}
	for _, kind := range []string{"priority", "triage"} {
		d, ok := kinds[kind]
		if !ok || d.Policy != "rule" || d.Fallback != "unavailable" {
			t.Errorf("%s decision = %+v, want rule with fallback unavailable", kind, d)
		}
	}
	if kinds["triage"].Label != "retry_with_backoff" || kinds["priority"].Label != "critical" {
		t.Errorf("labels: %+v", kinds)
	}
}

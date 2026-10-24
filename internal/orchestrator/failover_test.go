package orchestrator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/api"
	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/orchestrator"
	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/retry"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/worker"
	"github.com/omer-ulucan/baton/internal/worker/builtin"
)

var pg *pgtest.Server

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		ctx := context.Background()
		var err error
		if pg, err = pgtest.Start(ctx, store.Migrate); err != nil {
			fmt.Fprintln(os.Stderr, "pgtest:", err)
			os.Exit(1)
		}
		code := m.Run()
		_ = pg.Close(ctx)
		os.Exit(code)
	}
	os.Exit(m.Run())
}

const leaderTTL = time.Second

// testLog shows replica logs under -v only.
func testLog() *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.DiscardHandler)
}

type node struct {
	rep  *orchestrator.Replica
	url  string
	done chan error
	stop context.CancelFunc
}

func startNode(t *testing.T, dbURL, id string) *node {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String()
	rep := orchestrator.New(orchestrator.Options{
		Config: config.Orchestrator{
			ReplicaID: id, AdvertiseAddr: url, LeaderTTL: leaderTTL,
			LeaseTTL: 2 * time.Second, HeartbeatInterval: 300 * time.Millisecond, ShutdownTimeout: 2 * time.Second,
		},
		Pool: pool, Listener: ln, Log: testLog(),
		Engine: func(o *engine.Options) {
			o.Config.ClaimPoll = 20 * time.Millisecond
			o.Config.ReapInterval = 100 * time.Millisecond
			o.Config.RetryInterval = 50 * time.Millisecond
			o.RetryPolicy = func(*state.Task) retry.Policy {
				return retry.Policy{MaxAttempts: 10, InitialBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, Multiplier: 2, Jitter: retry.JitterFull}
			}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{rep: rep, url: url, done: make(chan error, 1), stop: cancel}
	go func() { n.done <- rep.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-n.done; err != nil {
			t.Errorf("replica %s: %v", id, err)
		}
		pool.Close()
	})
	return n
}

func waitFor(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

const flow = `
name: failover
version: 1
steps:
  - {name: a, handler: sleep, input: {ms: 40}}
  - {name: b, handler: sleep, depends_on: [a], input: {ms: 40}}
  - {name: c, handler: echo, depends_on: [a]}
  - {name: d, handler: sleep, depends_on: [b, c], input: {ms: 40}}
`

// submit posts a workflow to url, retrying through a leadership change.
func submit(t *testing.T, url string, i int) uuid.UUID {
	t.Helper()
	body, _ := json.Marshal(api.SubmitRequest{Definition: flow, Input: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))})
	for attempt := 0; ; attempt++ {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/v1/workflows", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", fmt.Sprintf("failover-%d", i))
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			var out api.SubmitResponse
			_ = json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return out.WorkflowID
			}
		}
		if attempt > 100 {
			t.Fatalf("submission %d never succeeded: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestLeaderCrashMidRun kills the leader while workflows run through a
// follower. The follower must take over, workers must carry on through it,
// no workflow may be lost, history must replay to the projections, and the
// dead leader's token must be refused.
func TestLeaderCrashMidRun(t *testing.T) {
	_, dbURL := pg.NewDB(t)
	r1 := startNode(t, dbURL, "r1")
	waitFor(t, "r1 to lead", 3*leaderTTL, r1.rep.IsLeader)
	r2 := startNode(t, dbURL, "r2")

	// Workers know both replicas and talk to the follower first, so every
	// worker call is forwarded to the leader until it dies.
	wctx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	var workersDone []chan error
	for i := range 3 {
		r := worker.NewRunner(worker.NewClient([]string{r2.url, r1.url}, &http.Client{Timeout: 10 * time.Second}),
			builtin.Handlers(), worker.Config{
				ID: fmt.Sprintf("w%d", i), Queues: []string{"default"}, Concurrency: 4,
				DrainTimeout: 2 * time.Second, ClaimWait: 300 * time.Millisecond,
			}, nil)
		done := make(chan error, 1)
		go func() { done <- r.Run(wctx) }()
		workersDone = append(workersDone, done)
	}

	var ids []uuid.UUID
	for i := range 40 {
		ids = append(ids, submit(t, r2.url, i))
	}
	s := r2.rep.Store()
	succeeded := func() int {
		n := 0
		for _, id := range ids {
			if snap, err := s.Snapshot(context.Background(), id); err == nil && snap.Workflow != nil && snap.Workflow.Status == state.WfSucceeded {
				n++
			}
		}
		return n
	}
	waitFor(t, "some workflows to finish before the crash", 30*time.Second, func() bool { return succeeded() >= 5 })

	before, err := s.Leader(context.Background())
	if err != nil || before.Holder != "r1" {
		t.Fatalf("leader before crash = %+v %v", before, err)
	}
	crashed := time.Now()
	r1.rep.Kill()
	waitFor(t, "r2 to take over", 3*leaderTTL, r2.rep.IsLeader)
	t.Logf("failover after %s with %d/40 done", time.Since(crashed), succeeded())

	for i := 40; i < 50; i++ {
		ids = append(ids, submit(t, r2.url, i))
	}
	waitFor(t, "all 50 workflows to succeed", 60*time.Second, func() bool { return succeeded() == len(ids) })

	after, _ := s.Leader(context.Background())
	if after.Holder != "r2" || after.Token <= before.Token {
		t.Fatalf("leader after crash = %+v (before %+v)", after, before)
	}
	// The dead leader's token is refused, as a write arriving late after a
	// partition would be.
	wf := uuid.New()
	_, _, err = s.Update(context.Background(), store.Fence{Token: before.Token}, wf, func(*state.Snapshot, time.Time) ([]state.Change, error) {
		return []state.Change{{Payload: state.WorkflowSubmitted{}}}, nil
	})
	if !errors.Is(err, store.ErrFenced) {
		t.Fatalf("write with the dead leader's token: %v", err)
	}

	// Under load the new leader must keep its lease: renewals do not wait
	// for write traffic, so it is promoted once and never steps down.
	if n := r2.rep.LeaderChanges(); n != 1 {
		t.Fatalf("r2 changed leadership %d times; renewals are being starved", n)
	}
	stopWorkers()
	for _, d := range workersDone {
		if err := <-d; err != nil {
			t.Error(err)
		}
	}
	mm, err := s.VerifyReplay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range mm {
		t.Error(m)
	}
}

func TestGracefulLeaderShutdownHandsOver(t *testing.T) {
	_, dbURL := pg.NewDB(t)
	r1 := startNode(t, dbURL, "r1")
	waitFor(t, "r1 to lead", 3*leaderTTL, r1.rep.IsLeader)
	r2 := startNode(t, dbURL, "r2")
	stopped := time.Now()
	r1.stop()
	if err := <-r1.done; err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	r1.done <- nil // for cleanup
	waitFor(t, "r2 to take over", leaderTTL, r2.rep.IsLeader)
	if took := time.Since(stopped); took >= leaderTTL {
		t.Fatalf("handover took %s; a released lease should not wait out the TTL", took)
	}
}

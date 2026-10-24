package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/retry"
	"github.com/omer-ulucan/baton/internal/server"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/worker"
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

var defID = uuid.MustParse("00000000-0000-7000-8000-0000000000bb")

type stack struct {
	fence store.Fence
	t     *testing.T
	pool  *pgxpool.Pool
	store *store.Store
	url   string
}

func newStack(t *testing.T) *stack {
	t.Helper()
	pool, _ := pg.NewDB(t)
	if _, err := pool.Exec(t.Context(), `INSERT INTO workflow_definitions (id, name, version, sha256, spec, created_at)
		VALUES ($1, 'test', 1, '\x02', '{}', now())`, defID); err != nil {
		t.Fatal(err)
	}
	s := store.New(pool)
	fence := pgtest.Lead(t, pool)
	eng := engine.New(engine.Options{
		Store: s,
		Fence: func() store.Fence { return fence },
		Config: engine.Config{
			LeaseTTL: 3 * time.Second, HeartbeatInterval: 100 * time.Millisecond,
			ClaimPoll: 20 * time.Millisecond,
		},
		RetryPolicy: func(*state.Task) retry.Policy {
			return retry.Policy{MaxAttempts: 5, InitialBackoff: time.Hour, MaxBackoff: time.Hour, Multiplier: 1, Jitter: retry.JitterNone}
		},
	})
	srv := httptest.NewServer(server.New(server.Deps{DB: pool, Worker: eng}))
	t.Cleanup(srv.Close)
	return &stack{fence: fence, t: t, pool: pool, store: s, url: srv.URL}
}

func (s *stack) seed(n int, handler string) uuid.UUID {
	s.t.Helper()
	wf := uuid.New()
	changes := []state.Change{{Payload: state.WorkflowSubmitted{DefinitionID: defID, Priority: 50}}}
	var ids []uuid.UUID
	for i := range n {
		id := uuid.New()
		ids = append(ids, id)
		step := fmt.Sprintf("s%03d", i)
		changes = append(changes, state.Change{TaskID: id, Payload: state.TaskCreated{
			Step: step, Kind: state.KindStep, Handler: handler, Queue: "default", Priority: 50,
			MaxAttempts: 5, IdempotencyKey: wf.String() + "/" + step + "/step",
		}})
	}
	for _, id := range ids {
		changes = append(changes, state.Change{TaskID: id, Payload: state.TaskReady{}})
	}
	if _, _, err := s.store.Update(s.t.Context(), s.fence, wf, func(*state.Snapshot, time.Time) ([]state.Change, error) {
		return changes, nil
	}); err != nil {
		s.t.Fatal(err)
	}
	return wf
}

func (s *stack) runner(handlers map[string]worker.Handler, drain time.Duration) (cancel func(), done <-chan error) {
	ctx, stop := context.WithCancel(s.t.Context())
	r := worker.NewRunner(worker.NewClient(s.url, nil), handlers, worker.Config{
		ID: "w1", Queues: []string{"default"}, Concurrency: 4, DrainTimeout: drain, ClaimWait: 200 * time.Millisecond,
	}, nil)
	ch := make(chan error, 1)
	go func() { ch <- r.Run(ctx) }()
	return stop, ch
}

// waitFor polls cond until it holds or 15s pass.
func (s *stack) waitFor(what string, cond func(*state.Snapshot) bool, wf uuid.UUID) *state.Snapshot {
	s.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		snap, err := s.store.Snapshot(s.t.Context(), wf)
		if err != nil {
			s.t.Fatal(err)
		}
		if cond(snap) {
			return snap
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func all(status state.TaskStatus) func(*state.Snapshot) bool {
	return func(s *state.Snapshot) bool {
		for _, t := range s.Tasks {
			if t.Status != status {
				return false
			}
		}
		return true
	}
}

func one(status state.TaskStatus) func(*state.Snapshot) bool {
	return func(s *state.Snapshot) bool {
		for _, t := range s.Tasks {
			if t.Status == status {
				return true
			}
		}
		return false
	}
}

func onlyTask(s *state.Snapshot) *state.Task {
	for _, t := range s.Tasks {
		return t
	}
	return nil
}

func TestRunnerProcessesTasks(t *testing.T) {
	s := newStack(t)
	wf := s.seed(40, "echo")
	var seen sync.Map
	var dupKeys atomic.Int64
	stop, done := s.runner(map[string]worker.Handler{
		"echo": func(_ context.Context, task worker.Task) (json.RawMessage, error) {
			if _, loaded := seen.LoadOrStore(task.IdempotencyKey, true); loaded {
				dupKeys.Add(1)
			}
			return json.RawMessage(`{}`), nil
		},
	}, time.Second)
	s.waitFor("all tasks to succeed", all(state.TsSucceeded), wf)
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if dupKeys.Load() != 0 {
		t.Fatalf("%d tasks ran twice", dupKeys.Load())
	}
}

func TestRunnerReportsErrorsAndPanics(t *testing.T) {
	s := newStack(t)
	wf := s.seed(3, "mixed")
	stop, done := s.runner(map[string]worker.Handler{
		"mixed": func(_ context.Context, task worker.Task) (json.RawMessage, error) {
			switch task.Step {
			case "s000":
				return nil, errors.New("transient glitch")
			case "s001":
				return nil, worker.Permanent(errors.New("card declined"))
			default:
				panic("handler bug")
			}
		},
	}, time.Second)
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	snap := s.waitFor("all tasks to be dispositioned", func(snap *state.Snapshot) bool {
		for _, t := range snap.Tasks {
			if t.Status == state.TsRunning || t.Status == state.TsReady {
				return false
			}
		}
		return true
	}, wf)
	for _, tk := range snap.Tasks {
		switch tk.StepName {
		case "s000":
			if tk.Status != state.TsRetryWait || *tk.LastError != "transient glitch" {
				t.Errorf("transient: %s %v", tk.Status, *tk.LastError)
			}
		case "s001":
			if tk.Status != state.TsDeadLettered {
				t.Errorf("permanent error should dead-letter, got %s", tk.Status)
			}
		case "s002":
			if tk.Status != state.TsRetryWait || *tk.LastError != "panic: handler bug" {
				t.Errorf("panic: %s %v", tk.Status, *tk.LastError)
			}
		}
	}
}

func TestRunnerDrainFinishesInFlightWork(t *testing.T) {
	s := newStack(t)
	wf := s.seed(1, "slow")
	started := make(chan struct{})
	stop, done := s.runner(map[string]worker.Handler{
		"slow": func(context.Context, worker.Task) (json.RawMessage, error) {
			close(started)
			time.Sleep(300 * time.Millisecond)
			return nil, nil
		},
	}, 5*time.Second)
	<-started
	stop() // SIGTERM while the attempt runs
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snap, _ := s.store.Snapshot(t.Context(), wf)
	if tk := onlyTask(snap); tk.Status != state.TsSucceeded {
		t.Fatalf("drained task is %s, want succeeded", tk.Status)
	}
}

func TestRunnerDrainReleasesUnfinishedWork(t *testing.T) {
	s := newStack(t)
	wf := s.seed(1, "stuck")
	started := make(chan struct{})
	stop, done := s.runner(map[string]worker.Handler{
		"stuck": func(ctx context.Context, _ worker.Task) (json.RawMessage, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}, 200*time.Millisecond)
	<-started
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snap, _ := s.store.Snapshot(t.Context(), wf)
	tk := onlyTask(snap)
	if tk.Status != state.TsReady || tk.Attempt != 0 {
		t.Fatalf("released task: status %s attempt %d, want ready and 0", tk.Status, tk.Attempt)
	}
}

func TestRevokedLeaseStopsHandler(t *testing.T) {
	s := newStack(t)
	wf := s.seed(1, "stuck")
	stopped := make(chan struct{})
	stop, done := s.runner(map[string]worker.Handler{
		"stuck": func(ctx context.Context, _ worker.Task) (json.RawMessage, error) {
			<-ctx.Done()
			close(stopped)
			return nil, ctx.Err()
		},
	}, time.Second)
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	s.waitFor("task to run", one(state.TsRunning), wf)
	if _, err := s.pool.Exec(t.Context(), `UPDATE task_leases SET revoked = true`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("handler not stopped after its lease was revoked")
	}
	time.Sleep(200 * time.Millisecond)
	snap, _ := s.store.Snapshot(t.Context(), wf)
	if tk := onlyTask(snap); tk.Status != state.TsRunning {
		t.Fatalf("abandoned attempt was reported: task is %s", tk.Status)
	}
}

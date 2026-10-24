package engine_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
)

func ip(v int) *int         { return &v }
func fp(v float64) *float64 { return &v }
func (e *env) setQueue(c store.QueueConfig) {
	e.t.Helper()
	if err := e.eng.SetQueue(e.t.Context(), c); err != nil {
		e.t.Fatal(err)
	}
}

func TestConcurrencyLimitHoldsUnderRacingWorkers(t *testing.T) {
	e := newEnv(t, nil)
	e.setQueue(store.QueueConfig{Queue: "q", MaxConcurrency: ip(3)})
	e.seed(30, "q", "h", 3)
	e.seed(5, "other", "h", 3) // an unlimited queue is not held back
	var claimed []engine.Result
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := range 10 {
		id := fmt.Sprintf("w%d", w)
		e.register(id, []string{"q"}, []string{"h"})
		wg.Go(func() {
			got, err := e.eng.Claim(t.Context(), engine.ClaimRequest{WorkerID: id, Max: 4})
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			for _, a := range got {
				claimed = append(claimed, res(a, id))
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	if len(claimed) != 3 {
		t.Fatalf("%d tasks running on a queue limited to 3", len(claimed))
	}
	// A finished task frees exactly one slot.
	e.register("x", []string{"q", "other"}, []string{"h"})
	if err := e.eng.Complete(t.Context(), claimed[0], nil); err != nil {
		t.Fatal(err)
	}
	got := e.claim("x", 10)
	q, other := 0, 0
	for _, a := range got {
		if a.Queue == "q" {
			q++
		} else {
			other++
		}
	}
	if q != 1 || other != 5 {
		t.Fatalf("after one completion claimed %d from q and %d from other", q, other)
	}
}

func TestRateLimit(t *testing.T) {
	e := newEnv(t, nil)
	e.setQueue(store.QueueConfig{Queue: "q", RatePerSec: fp(5), Burst: ip(2)})
	e.seed(100, "q", "h", 3)
	e.register("w", []string{"q"}, []string{"h"})
	var n atomic.Int64
	start := time.Now()
	for time.Since(start) < time.Second {
		n.Add(int64(len(e.claim("w", 10))))
		time.Sleep(20 * time.Millisecond)
	}
	// Burst 2 at once, then 5 per second.
	if got := n.Load(); got < 5 || got > 8 {
		t.Fatalf("claimed %d in 1s at 5/s with burst 2", got)
	}
}

func TestAgingLetsOldLowPriorityWorkThrough(t *testing.T) {
	e := newEnv(t, func(o *engine.Options) { o.Config.AgingSeconds = 30 })
	_, low := e.seed(1, "q", "h", 3)
	_, high := e.seed(1, "q", "h", 3)
	e.exec(`UPDATE tasks SET priority = 10 WHERE id = $1`, low[0])
	e.exec(`UPDATE tasks SET priority = 100 WHERE id = $1`, high[0])
	e.register("w", []string{"q"}, []string{"h"})
	// Fresh: the high-priority task goes first.
	first := e.claim("w", 1)[0]
	if first.TaskID != high[0] {
		t.Fatal("without aging, priority must win")
	}
	if err := e.eng.Release(t.Context(), "w", []uuid.UUID{first.AttemptID}); err != nil {
		t.Fatal(err)
	}
	// The low-priority task has waited 50 minutes: 10 + 3000/30 = 110 > 100.
	e.exec(`UPDATE tasks SET ready_at = now() - interval '3000 seconds' WHERE id = $1`, low[0])
	if got := e.claim("w", 1)[0]; got.TaskID != low[0] {
		t.Fatal("an aged low-priority task must outrank a fresh high-priority one")
	}
}

const twoStep = `
name: adm
version: 1
queue: q
steps:
  - {name: a, handler: h}
  - {name: b, handler: h, depends_on: [a]}
`

func TestAdmissionRejectsWhenQueueIsFull(t *testing.T) {
	e := wfEnv(t, nil)
	e.setQueue(store.QueueConfig{Queue: "q", MaxDepth: ip(3)})
	e.submit(twoStep, "")
	_, err := e.eng.Submit(t.Context(), engine.Submission{Definition: []byte(twoStep)})
	var full *engine.ErrQueueFull
	if !errors.As(err, &full) || full.Queue != "q" || full.Depth != 2 || full.RetryAfter <= 0 {
		t.Fatalf("err = %v, want ErrQueueFull on q at depth 2", err)
	}
	var n int
	_ = e.pool.QueryRow(t.Context(), `SELECT count(*) FROM workflows`).Scan(&n)
	if n != 1 {
		t.Fatalf("a rejected submission left %d workflows", n)
	}
}

func TestAdmissionDefersAndAdmitsLater(t *testing.T) {
	e := wfEnv(t, nil)
	e.setQueue(store.QueueConfig{Queue: "q", MaxDepth: ip(3), AdmissionMode: "defer"})
	e.register("w", []string{"q"}, []string{"h"})
	first := e.submit(twoStep, "")
	out, err := e.eng.Submit(t.Context(), engine.Submission{Definition: []byte(twoStep)})
	if err != nil || out.Status != state.WfDeferred {
		t.Fatalf("submit = %+v %v, want deferred", out, err)
	}
	if n, _ := e.eng.AdmitDeferred(t.Context()); n != 0 {
		t.Fatal("admitted while the queue is still full")
	}
	for range 2 { // finish the first workflow
		e.complete(e.claim("w", 1)[0])
	}
	if s := e.workflow(first).Workflow.Status; s != state.WfSucceeded {
		t.Fatalf("first workflow %s", s)
	}
	if n, err := e.eng.AdmitDeferred(t.Context()); n != 1 || err != nil {
		t.Fatalf("admitted %d, %v", n, err)
	}
	if s := e.workflow(out.WorkflowID).Workflow.Status; s != state.WfRunning {
		t.Fatalf("deferred workflow is %s after admission", s)
	}
	if got := e.claim("w", 10); len(got) != 1 || got[0].WorkflowID != out.WorkflowID {
		t.Fatalf("admitted workflow's root not ready: %+v", got)
	}
	e.verifyReplay()
}

func TestSetQueueValidation(t *testing.T) {
	e := newEnv(t, nil)
	for _, c := range []store.QueueConfig{
		{Queue: "Bad Name"}, {Queue: "q", MaxConcurrency: ip(0)}, {Queue: "q", RatePerSec: fp(-1)},
		{Queue: "q", AdmissionMode: "maybe"},
	} {
		if err := e.eng.SetQueue(t.Context(), c); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%+v accepted: %v", c, err)
		}
	}
}

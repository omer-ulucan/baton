package engine_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/state"
)

// TestConcurrentClaimsNeverOverlap races many workers on one queue while
// some of them crash mid-task (abandon attempts without reporting) and the
// reaper and retry timer run concurrently. It asserts that no task is ever
// held by two attempts at once, judged from the event history: between a
// TaskClaimed and the event that ends that attempt, no other TaskClaimed
// may appear for the task.
func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	if testing.Short() {
		t.Skip("integration")
	}
	const (
		tasks     = 300
		workers   = 16
		crashRate = 0.1
	)
	e := newEnv(t, func(o *engine.Options) {
		o.Config.LeaseTTL = 300 * time.Millisecond
		o.Config.ClaimPoll = 10 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)

	var wfs []uuid.UUID
	for range tasks / 50 {
		wf, _ := e.seed(50, "q", "h", 20)
		wfs = append(wfs, wf)
	}

	// Leader loops, running as fast as the test allows.
	loops, stopLoops := context.WithCancel(ctx)
	var loopsWG sync.WaitGroup
	loopsWG.Go(func() {
		for loops.Err() == nil {
			_, _ = e.eng.ReapExpired(loops)
			_, _ = e.eng.PromoteRetries(loops)
			time.Sleep(20 * time.Millisecond)
		}
	})

	var succeeded, crashed, rejected atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		id := fmt.Sprintf("w%02d", w)
		e.register(id, []string{"q"}, []string{"h"})
		r := rand.New(rand.NewPCG(seed, uint64(w)))
		wg.Go(func() {
			for ctx.Err() == nil && succeeded.Load() < tasks {
				got, err := e.eng.Claim(ctx, engine.ClaimRequest{WorkerID: id, Max: 1 + r.IntN(4), Wait: 50 * time.Millisecond})
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("claim: %v", err)
					}
					return
				}
				for _, a := range got {
					if r.Float64() < crashRate {
						crashed.Add(1) // walk away; the reaper must recover it
						continue
					}
					time.Sleep(time.Duration(r.IntN(3)) * time.Millisecond)
					if err := e.eng.Complete(ctx, res(a, id), nil); err != nil {
						rejected.Add(1) // lease expired under us; fine, it is retried
						continue
					}
					succeeded.Add(1)
				}
			}
		})
	}
	wg.Wait()
	stopLoops()
	loopsWG.Wait()
	if ctx.Err() != nil {
		t.Fatalf("timed out with %d/%d tasks succeeded", succeeded.Load(), tasks)
	}
	t.Logf("succeeded %d, crashed %d, rejected late results %d", succeeded.Load(), crashed.Load(), rejected.Load())
	if crashed.Load() == 0 {
		t.Fatal("no crashes were simulated; the test did not exercise recovery")
	}

	for _, wf := range wfs {
		events, err := e.store.Events(t.Context(), wf)
		if err != nil {
			t.Fatal(err)
		}
		open := map[uuid.UUID]uuid.UUID{} // task -> attempt in flight
		successes := map[uuid.UUID]int{}
		for _, ev := range events {
			switch p := ev.Payload.(type) {
			case state.TaskClaimed:
				if prev, busy := open[ev.TaskID]; busy {
					t.Fatalf("task %s claimed by attempt %s while attempt %s was in flight (seq %d)", ev.TaskID, p.AttemptID, prev, ev.Seq)
				}
				open[ev.TaskID] = p.AttemptID
			case state.TaskSucceeded:
				successes[ev.TaskID]++
				delete(open, ev.TaskID)
			case state.TaskFailed, state.TaskLeaseExpired, state.TaskReleased, state.TaskCancelled:
				delete(open, ev.TaskID)
			}
		}
		for task, n := range successes {
			if n != 1 {
				t.Errorf("task %s succeeded %d times", task, n)
			}
		}
		if len(successes) != 50 {
			t.Errorf("workflow %s: %d of 50 tasks succeeded", wf, len(successes))
		}
	}
	e.verifyReplay()
}

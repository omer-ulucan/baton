package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/omer-ulucan/baton/internal/worker"
)

func TestEcho(t *testing.T) {
	out, err := Echo(t.Context(), worker.Task{Input: json.RawMessage(`{"a":1}`)})
	if err != nil || string(out) != `{"a":1}` {
		t.Fatalf("echo = %s %v", out, err)
	}
}

func TestSleepHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := Sleep(ctx, worker.Task{Input: json.RawMessage(`{"ms":5000}`)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestFlakyFailAttempts(t *testing.T) {
	in := json.RawMessage(`{"fail_attempts":2}`)
	for attempt, wantFail := range map[int]bool{1: true, 2: true, 3: false} {
		_, err := Flaky(t.Context(), worker.Task{Input: in, Attempt: attempt})
		if (err != nil) != wantFail {
			t.Errorf("attempt %d: err = %v", attempt, err)
		}
	}
}

func TestFlakyRateIsDeterministic(t *testing.T) {
	in := json.RawMessage(`{"fail_rate":0.5}`)
	fails := 0
	for i := range 1000 {
		task := worker.Task{Input: in, Attempt: 1, IdempotencyKey: "k" + string(rune('a'+i%26)) + string(rune(i))}
		_, e1 := Flaky(t.Context(), task)
		_, e2 := Flaky(t.Context(), task)
		if (e1 == nil) != (e2 == nil) {
			t.Fatal("same key and attempt gave different outcomes")
		}
		if e1 != nil {
			fails++
		}
	}
	if fails < 400 || fails > 600 {
		t.Fatalf("%d/1000 failed at rate 0.5", fails)
	}
}

func TestBadInputIsPermanent(t *testing.T) {
	_, err := Flaky(t.Context(), worker.Task{Input: json.RawMessage(`[`)})
	if err == nil || err.Error() == "" {
		t.Fatal("expected error")
	}
	var target interface{ Unwrap() error }
	if !errors.As(err, &target) {
		t.Fatal("bad input should be a permanent (wrapped) error")
	}
}

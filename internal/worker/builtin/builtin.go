// Package builtin provides demo and test handlers for baton-worker: they
// exercise success, latency, failure and cancellation without any
// external system.
package builtin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/omer-ulucan/baton/internal/worker"
)

// Handlers returns every built-in handler by name.
func Handlers() map[string]worker.Handler {
	return map[string]worker.Handler{
		"echo":  Echo,
		"sleep": Sleep,
		"flaky": Flaky,
	}
}

// Echo returns its input.
func Echo(_ context.Context, t worker.Task) (json.RawMessage, error) {
	if len(t.Input) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return t.Input, nil
}

// SleepInput configures Sleep.
type SleepInput struct {
	MS int `json:"ms"`
}

// Sleep waits ms milliseconds, or until cancelled.
func Sleep(ctx context.Context, t worker.Task) (json.RawMessage, error) {
	var in SleepInput
	if err := decode(t.Input, &in); err != nil {
		return nil, err
	}
	select {
	case <-time.After(time.Duration(in.MS) * time.Millisecond):
		return json.RawMessage(`{"slept_ms":` + fmt.Sprint(in.MS) + `}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// FlakyInput configures Flaky.
type FlakyInput struct {
	// FailAttempts makes the first N attempts fail.
	FailAttempts int `json:"fail_attempts"`
	// FailRate fails a deterministic fraction of attempts in [0,1],
	// keyed by idempotency key and attempt so retries can succeed.
	FailRate float64 `json:"fail_rate"`
	Message  string  `json:"message"`
	// Permanent marks the failures as not retryable.
	Permanent bool `json:"permanent"`
}

// Flaky fails according to its input and otherwise succeeds.
func Flaky(_ context.Context, t worker.Task) (json.RawMessage, error) {
	var in FlakyInput
	if err := decode(t.Input, &in); err != nil {
		return nil, err
	}
	fail := t.Attempt <= in.FailAttempts
	if !fail && in.FailRate > 0 {
		h := fnv.New64a()
		fmt.Fprintf(h, "%s#%d", t.IdempotencyKey, t.Attempt)
		fail = float64(h.Sum64()%10_000)/10_000 < in.FailRate
	}
	if !fail {
		return json.RawMessage(`{"ok":true}`), nil
	}
	msg := in.Message
	if msg == "" {
		msg = fmt.Sprintf("flaky failure on attempt %d", t.Attempt)
	}
	err := errors.New(msg)
	if in.Permanent {
		return nil, worker.Permanent(err)
	}
	return nil, err
}

func decode(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return worker.Permanent(fmt.Errorf("invalid input: %w", err))
	}
	return nil
}

package retry

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestBaseGrowsAndCaps(t *testing.T) {
	p := Policy{MaxAttempts: 10, InitialBackoff: time.Second, MaxBackoff: 10 * time.Second, Multiplier: 2, Jitter: JitterNone}
	want := []time.Duration{1, 2, 4, 8, 10, 10}
	for i, w := range want {
		if got := p.Base(i + 1); got != w*time.Second {
			t.Errorf("Base(%d) = %s, want %s", i+1, got, w*time.Second)
		}
	}
	if p.Base(0) != time.Second {
		t.Error("Base(0) should clamp to attempt 1")
	}
	if p.Base(10_000) != 10*time.Second {
		t.Error("huge attempt numbers must cap, not overflow")
	}
}

func TestMultiplierOneIsConstant(t *testing.T) {
	p := Policy{MaxAttempts: 3, InitialBackoff: 3 * time.Second, MaxBackoff: time.Minute, Multiplier: 1, Jitter: JitterNone}
	if p.Base(1) != p.Base(7) {
		t.Fatal("multiplier 1 should not grow")
	}
}

func TestJitterBounds(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	base := Policy{MaxAttempts: 5, InitialBackoff: time.Second, MaxBackoff: time.Minute, Multiplier: 2}
	for _, tc := range []struct {
		j        Jitter
		min, max time.Duration
	}{
		{JitterFull, 0, 8 * time.Second},
		{JitterEqual, 4 * time.Second, 8 * time.Second},
		{JitterNone, 8 * time.Second, 8 * time.Second},
	} {
		p := base
		p.Jitter = tc.j
		var lo, hi time.Duration = time.Hour, 0
		for range 10_000 {
			d := p.Backoff(4, r.Float64)
			if d < tc.min || d > tc.max {
				t.Fatalf("%s: delay %s outside [%s, %s]", tc.j, d, tc.min, tc.max)
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		if tc.j != JitterNone && hi-lo < (tc.max-tc.min)*9/10 {
			t.Errorf("%s: delays span only [%s, %s]", tc.j, lo, hi)
		}
	}
}

func TestBackoffUsesInjectedRandom(t *testing.T) {
	p := Policy{MaxAttempts: 2, InitialBackoff: 10 * time.Second, MaxBackoff: time.Minute, Multiplier: 2, Jitter: JitterFull}
	if got := p.Backoff(1, func() float64 { return 0.5 }); got != 5*time.Second {
		t.Fatalf("Backoff = %s, want 5s", got)
	}
}

func TestValidate(t *testing.T) {
	if err := DefaultPolicy().Validate(); err != nil {
		t.Fatalf("Default invalid: %v", err)
	}
	bad := []Policy{
		{MaxAttempts: 0, InitialBackoff: 1, MaxBackoff: 1, Multiplier: 1, Jitter: JitterNone},
		{MaxAttempts: 101, InitialBackoff: 1, MaxBackoff: 1, Multiplier: 1, Jitter: JitterNone},
		{MaxAttempts: 1, InitialBackoff: 0, MaxBackoff: 1, Multiplier: 1, Jitter: JitterNone},
		{MaxAttempts: 1, InitialBackoff: 2, MaxBackoff: 1, Multiplier: 1, Jitter: JitterNone},
		{MaxAttempts: 1, InitialBackoff: 1, MaxBackoff: 1, Multiplier: 0.5, Jitter: JitterNone},
		{MaxAttempts: 1, InitialBackoff: 1, MaxBackoff: 1, Multiplier: 1, Jitter: "wild"},
	}
	for i, p := range bad {
		if p.Validate() == nil {
			t.Errorf("case %d: %+v accepted", i, p)
		}
	}
}

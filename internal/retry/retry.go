// Package retry computes retry delays (SPEC 2.2).
package retry

import (
	"fmt"
	"math"
	"time"
)

// Jitter selects how a delay is randomized.
type Jitter string

const (
	JitterFull  Jitter = "full"  // uniform in [0, d]
	JitterEqual Jitter = "equal" // d/2 + uniform in [0, d/2]
	JitterNone  Jitter = "none"  // d
)

// Policy is a step's retry policy.
type Policy struct {
	MaxAttempts    int           `json:"max_attempts"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
	Multiplier     float64       `json:"multiplier"`
	Jitter         Jitter        `json:"jitter"`
}

// Default is the policy of a step that declares none.
var Default = Policy{
	MaxAttempts:    5,
	InitialBackoff: time.Second,
	MaxBackoff:     5 * time.Minute,
	Multiplier:     2,
	Jitter:         JitterFull,
}

// Validate reports a policy outside the allowed ranges.
func (p Policy) Validate() error {
	switch {
	case p.MaxAttempts < 1 || p.MaxAttempts > 100:
		return fmt.Errorf("max_attempts must be in [1,100], got %d", p.MaxAttempts)
	case p.InitialBackoff <= 0:
		return fmt.Errorf("initial_backoff must be positive, got %s", p.InitialBackoff)
	case p.MaxBackoff < p.InitialBackoff:
		return fmt.Errorf("max_backoff (%s) must be >= initial_backoff (%s)", p.MaxBackoff, p.InitialBackoff)
	case p.Multiplier < 1 || p.Multiplier > 10:
		return fmt.Errorf("multiplier must be in [1,10], got %v", p.Multiplier)
	}
	switch p.Jitter {
	case JitterFull, JitterEqual, JitterNone:
		return nil
	}
	return fmt.Errorf("jitter must be full, equal or none, got %q", p.Jitter)
}

// Base is the un-jittered delay after failed attempt n (1-based):
// min(max_backoff, initial_backoff * multiplier^(n-1)).
func (p Policy) Base(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(n-1))
	if d >= float64(p.MaxBackoff) || math.IsInf(d, 0) || math.IsNaN(d) {
		return p.MaxBackoff
	}
	return time.Duration(d)
}

// Backoff is the jittered delay after failed attempt n. rand returns a
// uniform value in [0,1); it is injected so callers control the source
// and the drawn delay can be recorded in history.
func (p Policy) Backoff(n int, rand func() float64) time.Duration {
	d := p.Base(n)
	switch p.Jitter {
	case JitterFull:
		return time.Duration(rand() * float64(d))
	case JitterEqual:
		return d/2 + time.Duration(rand()*float64(d/2))
	default:
		return d
	}
}

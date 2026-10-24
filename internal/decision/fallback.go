package decision

import (
	"context"
	"errors"
	"time"
)

// ErrUnavailable means a policy cannot answer at all right now (model not
// loaded, failed to load, overloaded).
var ErrUnavailable = errors.New("decision policy unavailable")

// Fallback reasons recorded in Decision.Fallback (SPEC 10.1).
const (
	FallbackLowConfidence = "low_confidence"
	FallbackTimeout       = "timeout"
	FallbackError         = "error"
	FallbackUnavailable   = "unavailable"
)

// FallbackPolicy asks Primary under a per-kind deadline and answers with
// Fallback instead when Primary errors, times out, is unavailable, or is
// less confident than Threshold. It never returns an error when Fallback
// does not, so the decision layer cannot block or fail a request.
type FallbackPolicy struct {
	Primary   Policy
	Fallback  Policy
	Threshold float64
	Timeouts  map[Kind]time.Duration // missing kinds get 1500ms
	// Observe, when set, sees every decision with its latency (metrics).
	Observe func(req Request, d Decision, latency time.Duration)
}

// Decide implements Policy.
func (f FallbackPolicy) Decide(ctx context.Context, req Request) (Decision, error) {
	start := time.Now()
	timeout, ok := f.Timeouts[req.Kind]
	if !ok {
		timeout = 1500 * time.Millisecond
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	d, err := f.Primary.Decide(pctx, req)
	timedOut := errors.Is(pctx.Err(), context.DeadlineExceeded) // before cancel overwrites it
	cancel()

	reason := ""
	switch {
	case err == nil && d.Confidence >= f.Threshold:
		f.observe(req, d, start)
		return d, nil
	case err == nil:
		reason = FallbackLowConfidence
	case errors.Is(err, ErrUnavailable):
		reason = FallbackUnavailable
	case timedOut && ctx.Err() == nil:
		reason = FallbackTimeout
	default:
		reason = FallbackError
	}
	fb, ferr := f.Fallback.Decide(ctx, req)
	if ferr != nil {
		return Decision{}, ferr
	}
	fb.Fallback = reason
	if err == nil {
		primary := d
		fb.Primary = &primary
	}
	f.observe(req, fb, start)
	return fb, nil
}

func (f FallbackPolicy) observe(req Request, d Decision, start time.Time) {
	if f.Observe != nil {
		f.Observe(req, d, time.Since(start))
	}
}

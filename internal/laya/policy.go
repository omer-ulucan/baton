package laya

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/laya/model"
)

// Baton's two questions. tools/parity/laya_fixtures.py asks exactly these,
// which TestQuestionsMatchFixtures checks.
var (
	TriageQuestion = Question{
		Type:         Choice,
		Instructions: "A task in a workflow failed with the error below. How should the orchestrator handle this failure?",
		Options: []Option{
			{decision.Retry, "a transient glitch (network blip, connection reset) that should succeed if run again right away"},
			{decision.RetryWithBackoff, "temporary overload or unavailability (timeouts, rate limits, 429/503, locks, deadlocks) that needs time before retrying"},
			{decision.DeadLetter, "a permanent error that retrying cannot fix (invalid input, validation, not found, permission denied, declined)"},
			{decision.Compensate, "a failure that leaves partial side effects or corrupt state, so completed steps must be undone"},
			{decision.Escalate, "needs a human: data loss, security, disk full, misconfiguration, or anything unclear and risky"},
		},
	}
	PriorityQuestion = Question{
		Type:         Score,
		Instructions: "How urgent is this task?",
		Options: []Option{
			{Label: "low: can wait hours or days"}, {Label: "normal: routine work"},
			{Label: "high: user-facing or time-sensitive"}, {Label: "critical: outage, revenue or data at risk"},
		},
	}
	priorityLabels = []string{decision.PriorityLow, decision.PriorityNormal, decision.PriorityHigh, decision.PriorityCritical}
)

// TriageState renders a failure as the question's state text.
func TriageState(r decision.Request) string {
	var b strings.Builder
	fmt.Fprintf(&b, "error_type: %s\nmessage: %s\n", r.ErrorType, r.ErrorMessage)
	if r.Stack != "" {
		fmt.Fprintf(&b, "stack:\n%s\n", strings.TrimRight(r.Stack, "\n"))
	}
	fmt.Fprintf(&b, "handler: %s\nattempt: %d of %d", r.Handler, r.Attempt, r.MaxAttempts)
	return b.String()
}

// Policy answers decisions with the Laya model (decision.Policy). Until the
// model is loaded, or if loading failed, it reports decision.ErrUnavailable
// and the fallback policy answers.
type Policy struct {
	model atomic.Pointer[Model]
}

// Decide implements decision.Policy.
func (p *Policy) Decide(ctx context.Context, req decision.Request) (decision.Decision, error) {
	m := p.model.Load()
	if m == nil {
		return decision.Decision{}, decision.ErrUnavailable
	}
	q, state, labels := TriageQuestion, TriageState(req), optionLabels(TriageQuestion)
	if req.Kind == decision.KindPriority {
		q, state, labels = PriorityQuestion, req.Description, priorityLabels
	}
	seq, err := m.Build(state, q)
	if err != nil {
		return decision.Decision{}, err
	}
	logits, err := m.Logits(ctx, seq)
	if errors.Is(err, ErrOverloaded) {
		return decision.Decision{}, fmt.Errorf("%w: %w", decision.ErrUnavailable, err)
	}
	if err != nil {
		return decision.Decision{}, err
	}
	probs := m.Config().Calibrate(seq.QType, logits)
	best := 0
	for i, v := range probs {
		if v > probs[best] {
			best = i
		}
	}
	return decision.Decision{
		Label: labels[best], Confidence: probs[best], Policy: "laya",
		ModelRevision: m.Revision(), InputHash: req.Hash(),
	}, nil
}

func optionLabels(q Question) []string {
	out := make([]string, len(q.Options))
	for i, o := range q.Options {
		out[i] = o.Label
	}
	return out
}

// LoaderConfig configures background model loading.
type LoaderConfig struct {
	Session  SessionConfig
	Download bool // fetch missing or corrupt files from the pinned revision
	HTTP     *http.Client
	Log      *slog.Logger
}

// Loader loads the model in the background so the orchestrator starts at
// once on the rule policy (SPEC 10.3).
type Loader struct {
	cfg    LoaderConfig
	policy *Policy
	mu     sync.Mutex
	state  string // "loading" | "loaded" | "failed: ..."
	model  *Model
}

// NewLoader returns a loader feeding policy.
func NewLoader(cfg LoaderConfig, policy *Policy) *Loader {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	return &Loader{cfg: cfg, policy: policy, state: "loading"}
}

// Run fetches (if allowed) and loads the model, then returns; the policy
// starts answering when it succeeds.
func (l *Loader) Run(ctx context.Context) {
	sc := l.cfg.Session
	files, err := model.Files(sc.Variant, false)
	if err == nil {
		if verr := model.Verify(sc.ModelDir, files); verr != nil && l.cfg.Download {
			l.cfg.Log.InfoContext(ctx, "fetching laya model", "dir", sc.ModelDir, "revision", model.Revision, "reason", verr.Error())
			fe := &model.Fetcher{HTTP: l.cfg.HTTP, Log: func(f string, a ...any) { l.cfg.Log.InfoContext(ctx, fmt.Sprintf(f, a...)) }}
			err = fe.Fetch(ctx, sc.ModelDir, files)
		}
	}
	var m *Model
	if err == nil {
		m, err = Load(sc) // verifies again: a fetch never bypasses the check
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.state = "failed: " + err.Error()
		l.cfg.Log.ErrorContext(ctx, "laya model unavailable; decisions use the rule policy", "err", err)
		return
	}
	l.state, l.model = "loaded", m
	l.policy.model.Store(m)
	l.cfg.Log.InfoContext(ctx, "laya model loaded", "revision", m.Revision())
}

// State reports "loading", "loaded" or "failed: <reason>".
func (l *Loader) State() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Loaded reports whether the policy is answering.
func (l *Loader) Loaded() bool { return l.policy.model.Load() != nil }

// Close releases the model.
func (l *Loader) Close() error {
	l.mu.Lock()
	m := l.model
	l.mu.Unlock()
	if m == nil {
		return nil
	}
	l.policy.model.Store(nil)
	return m.Close()
}

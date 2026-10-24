// Package workflow parses and validates workflow definitions (SPEC 2):
// YAML DAGs of steps with dependencies, timeouts, retry policies,
// priorities, queues and optional compensation steps.
package workflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/omer-ulucan/baton/internal/retry"
)

// Limits from SPEC 2.1.
const (
	MaxSteps    = 1000
	MaxInputLen = 256 << 10
)

// Definition is a validated definition with all defaults applied. It is
// what is stored (as JSON) in workflow_definitions.spec.
type Definition struct {
	Name        string `json:"name"`
	Version     int    `json:"version"`
	Description string `json:"description,omitempty"`
	Queue       string `json:"queue"`
	Priority    int    `json:"priority"`
	Steps       []Step `json:"steps"`
	// Order is the canonical topological order of step names (Kahn's
	// algorithm, ties by name). Scheduling ties and compensation use it.
	Order []string `json:"order"`
}

// Step is a resolved step.
type Step struct {
	Name     string `json:"name"`
	Handler  string `json:"handler"`
	Queue    string `json:"queue"`
	Priority int    `json:"priority"`
	// PriorityExplicit is false when the step inherited the definition's
	// priority; only such steps may be classified by the decision layer.
	PriorityExplicit bool            `json:"priority_explicit"`
	Description      string          `json:"description,omitempty"`
	Timeout          time.Duration   `json:"timeout"`
	Retry            retry.Policy    `json:"retry"`
	Input            json.RawMessage `json:"input,omitempty"`
	DependsOn        []string        `json:"depends_on,omitempty"`
	Compensate       *Compensation   `json:"compensate,omitempty"`
}

// Compensation is a step's resolved undo action.
type Compensation struct {
	Handler string        `json:"handler"`
	Timeout time.Duration `json:"timeout"`
	Retry   retry.Policy  `json:"retry"`
}

// Step returns the named step.
func (d *Definition) Step(name string) (*Step, bool) {
	for i := range d.Steps {
		if d.Steps[i].Name == name {
			return &d.Steps[i], true
		}
	}
	return nil, false
}

// ValidationError lists every problem found in a definition.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid workflow definition: " + strings.Join(e.Problems, "; ")
}

// Parsed is the result of Parse: the definition and the SHA-256 of its
// canonical form, which identifies it in workflow_definitions.
type Parsed struct {
	Definition Definition
	SHA256     [32]byte
	Spec       []byte // canonical JSON of Definition
}

var namePattern = regexp.MustCompile(`^[a-z0-9-]{1,63}$`)

// raw* mirror the YAML with optional fields as pointers so that "absent"
// and "zero" can be told apart when applying defaults.
type rawDefinition struct {
	Name        string       `yaml:"name"`
	Version     int          `yaml:"version"`
	Queue       *string      `yaml:"queue"`
	Priority    *int         `yaml:"priority"`
	Description string       `yaml:"description"`
	Defaults    *rawDefaults `yaml:"defaults"`
	Steps       []rawStep    `yaml:"steps"`
}

type rawDefaults struct {
	Timeout *string   `yaml:"timeout"`
	Retry   *rawRetry `yaml:"retry"`
}

type rawRetry struct {
	MaxAttempts    *int     `yaml:"max_attempts"`
	InitialBackoff *string  `yaml:"initial_backoff"`
	MaxBackoff     *string  `yaml:"max_backoff"`
	Multiplier     *float64 `yaml:"multiplier"`
	Jitter         *string  `yaml:"jitter"`
}

type rawStep struct {
	Name        string         `yaml:"name"`
	Handler     string         `yaml:"handler"`
	Queue       *string        `yaml:"queue"`
	Priority    *int           `yaml:"priority"`
	Description string         `yaml:"description"`
	Timeout     *string        `yaml:"timeout"`
	Retry       *rawRetry      `yaml:"retry"`
	Input       map[string]any `yaml:"input"`
	DependsOn   []string       `yaml:"depends_on"`
	Compensate  *rawCompensate `yaml:"compensate"`
}

type rawCompensate struct {
	Handler string    `yaml:"handler"`
	Timeout *string   `yaml:"timeout"`
	Retry   *rawRetry `yaml:"retry"`
}

// Parse decodes YAML strictly (unknown keys are errors), applies defaults
// and validates. On invalid input it returns a *ValidationError listing
// every problem, not just the first.
func Parse(src []byte) (*Parsed, error) {
	var raw rawDefinition
	dec := yaml.NewDecoder(bytes.NewReader(src))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil {
		return nil, &ValidationError{Problems: []string{"yaml: " + err.Error()}}
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return nil, &ValidationError{Problems: []string{"yaml: exactly one document expected"}}
	}

	v := &validator{}
	d := v.resolve(raw)
	if len(v.problems) == 0 {
		d.Order = v.order(d)
	}
	if len(v.problems) > 0 {
		return nil, &ValidationError{Problems: v.problems}
	}
	spec, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &Parsed{Definition: d, SHA256: sha256.Sum256(spec), Spec: spec}, nil
}

// Decode reads a stored definition (workflow_definitions.spec).
func Decode(spec []byte) (*Definition, error) {
	var d Definition
	if err := json.Unmarshal(spec, &d); err != nil {
		return nil, fmt.Errorf("decode definition: %w", err)
	}
	return &d, nil
}

type validator struct {
	problems []string
}

func (v *validator) addf(format string, args ...any) {
	v.problems = append(v.problems, fmt.Sprintf(format, args...))
}

func (v *validator) resolve(raw rawDefinition) Definition {
	d := Definition{Name: raw.Name, Version: raw.Version, Description: raw.Description, Queue: "default", Priority: 50}
	if !namePattern.MatchString(raw.Name) {
		v.addf("name %q must match [a-z0-9-]{1,63}", raw.Name)
	}
	if raw.Version < 1 {
		v.addf("version must be a positive integer")
	}
	if raw.Queue != nil {
		d.Queue = *raw.Queue
	}
	if !namePattern.MatchString(d.Queue) {
		v.addf("queue %q must match [a-z0-9-]{1,63}", d.Queue)
	}
	if raw.Priority != nil {
		d.Priority = *raw.Priority
	}
	if d.Priority < 0 || d.Priority > 100 {
		v.addf("priority must be in [0,100], got %d", d.Priority)
	}

	timeout := 30 * time.Second
	policy := retry.DefaultPolicy()
	if raw.Defaults != nil {
		timeout = v.duration("defaults.timeout", raw.Defaults.Timeout, timeout)
		policy = v.retry("defaults.retry", raw.Defaults.Retry, policy)
	}

	switch n := len(raw.Steps); {
	case n == 0:
		v.addf("a workflow needs at least one step")
	case n > MaxSteps:
		v.addf("a workflow may have at most %d steps, got %d", MaxSteps, n)
	}
	seen := map[string]bool{}
	for i, rs := range raw.Steps {
		where := fmt.Sprintf("steps[%d]", i)
		if rs.Name != "" {
			where = fmt.Sprintf("step %q", rs.Name)
		}
		if !namePattern.MatchString(rs.Name) {
			v.addf("%s: name %q must match [a-z0-9-]{1,63}", where, rs.Name)
		}
		if seen[rs.Name] {
			v.addf("%s: duplicate step name", where)
		}
		seen[rs.Name] = true
		if rs.Handler == "" {
			v.addf("%s: handler is required", where)
		}
		s := Step{
			Name: rs.Name, Handler: rs.Handler, Queue: d.Queue, Priority: d.Priority, Description: rs.Description,
			Timeout: v.duration(where+".timeout", rs.Timeout, timeout), Retry: v.retry(where+".retry", rs.Retry, policy),
			DependsOn: rs.DependsOn,
		}
		if rs.Queue != nil {
			s.Queue = *rs.Queue
			if !namePattern.MatchString(s.Queue) {
				v.addf("%s: queue %q must match [a-z0-9-]{1,63}", where, s.Queue)
			}
		}
		if rs.Priority != nil {
			s.Priority, s.PriorityExplicit = *rs.Priority, true
			if s.Priority < 0 || s.Priority > 100 {
				v.addf("%s: priority must be in [0,100], got %d", where, s.Priority)
			}
		}
		if rs.Input != nil {
			b, err := json.Marshal(rs.Input)
			switch {
			case err != nil:
				v.addf("%s: input is not representable as JSON: %v", where, err)
			case len(b) > MaxInputLen:
				v.addf("%s: input is %d bytes, limit %d", where, len(b), MaxInputLen)
			default:
				s.Input = b
			}
		}
		if rs.Compensate != nil {
			if rs.Compensate.Handler == "" {
				v.addf("%s.compensate: handler is required", where)
			}
			s.Compensate = &Compensation{
				Handler: rs.Compensate.Handler,
				Timeout: v.duration(where+".compensate.timeout", rs.Compensate.Timeout, s.Timeout),
				Retry:   v.retry(where+".compensate.retry", rs.Compensate.Retry, s.Retry),
			}
		}
		d.Steps = append(d.Steps, s)
	}
	for _, s := range d.Steps {
		deps := map[string]bool{}
		for _, dep := range s.DependsOn {
			switch {
			case dep == s.Name:
				v.addf("step %q depends on itself", s.Name)
			case !seen[dep]:
				v.addf("step %q depends on unknown step %q", s.Name, dep)
			case deps[dep]:
				v.addf("step %q lists dependency %q twice", s.Name, dep)
			}
			deps[dep] = true
		}
	}
	return d
}

func (v *validator) duration(field string, s *string, def time.Duration) time.Duration {
	if s == nil {
		return def
	}
	d, err := time.ParseDuration(*s)
	if err != nil {
		v.addf("%s: %v", field, err)
		return def
	}
	if d <= 0 {
		v.addf("%s must be positive, got %s", field, d)
	}
	return d
}

func (v *validator) retry(field string, r *rawRetry, base retry.Policy) retry.Policy {
	if r == nil {
		return base
	}
	p := base
	if r.MaxAttempts != nil {
		p.MaxAttempts = *r.MaxAttempts
	}
	p.InitialBackoff = v.duration(field+".initial_backoff", r.InitialBackoff, p.InitialBackoff)
	p.MaxBackoff = v.duration(field+".max_backoff", r.MaxBackoff, p.MaxBackoff)
	if r.Multiplier != nil {
		p.Multiplier = *r.Multiplier
	}
	if r.Jitter != nil {
		p.Jitter = retry.Jitter(*r.Jitter)
	}
	if err := p.Validate(); err != nil {
		v.addf("%s: %v", field, err)
	}
	return p
}

// order checks for cycles and returns the canonical topological order.
func (v *validator) order(d Definition) []string {
	if cycle := findCycle(d.Steps); cycle != nil {
		v.addf("cycle: %s", strings.Join(cycle, " -> "))
		return nil
	}
	return TopoOrder(d.Steps)
}

// findCycle runs a white/grey/black DFS from each step in name order and
// returns the first cycle found as a closed path, or nil. Visiting in name
// order makes the reported cycle deterministic.
func findCycle(steps []Step) []string {
	deps := map[string][]string{}
	names := make([]string, 0, len(steps))
	for _, s := range steps {
		ds := slices.Clone(s.DependsOn)
		slices.Sort(ds)
		deps[s.Name] = ds
		names = append(names, s.Name)
	}
	slices.Sort(names)
	const (
		white = iota
		grey
		black
	)
	color := map[string]int{}
	var path []string
	var visit func(string) []string
	visit = func(n string) []string {
		color[n] = grey
		path = append(path, n)
		for _, d := range deps[n] {
			switch color[d] {
			case grey:
				i := slices.Index(path, d)
				return append(slices.Clone(path[i:]), d)
			case white:
				if c := visit(d); c != nil {
					return c
				}
			}
		}
		path = path[:len(path)-1]
		color[n] = black
		return nil
	}
	for _, n := range names {
		if color[n] == white {
			if c := visit(n); c != nil {
				return c
			}
		}
	}
	return nil
}

// TopoOrder is Kahn's algorithm with ties broken by step name ascending,
// so the order is a pure function of the definition (SPEC 2.1). Steps must
// form a DAG.
func TopoOrder(steps []Step) []string {
	indeg := map[string]int{}
	dependents := map[string][]string{}
	for _, s := range steps {
		indeg[s.Name] += 0
		for _, d := range s.DependsOn {
			indeg[s.Name]++
			dependents[d] = append(dependents[d], s.Name)
		}
	}
	var ready []string
	for n, k := range indeg {
		if k == 0 {
			ready = append(ready, n)
		}
	}
	order := make([]string, 0, len(steps))
	for len(ready) > 0 {
		slices.Sort(ready)
		n := ready[0]
		ready = ready[1:]
		order = append(order, n)
		for _, m := range dependents[n] {
			if indeg[m]--; indeg[m] == 0 {
				ready = append(ready, m)
			}
		}
	}
	return order
}

// ErrNotObject is returned by MergeInput when an input is not an object.
var ErrNotObject = errors.New("input must be a JSON object")

// MergeInput builds a handler's input: the workflow input with the step's
// input merged over it (shallow; step keys win). Either may be empty.
func MergeInput(workflowInput, stepInput json.RawMessage) (json.RawMessage, error) {
	if len(workflowInput) == 0 {
		return stepInput, nil
	}
	if len(stepInput) == 0 {
		return workflowInput, nil
	}
	var w, s map[string]json.RawMessage
	if err := json.Unmarshal(workflowInput, &w); err != nil || w == nil {
		return nil, ErrNotObject
	}
	if err := json.Unmarshal(stepInput, &s); err != nil || s == nil {
		return nil, ErrNotObject
	}
	for k, v := range s {
		w[k] = v
	}
	return json.Marshal(w)
}

package workflow

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/omer-ulucan/baton/internal/retry"
)

const orderYAML = `
name: order-fulfilment
version: 3
queue: default
priority: 50
defaults:
  timeout: 30s
  retry:
    max_attempts: 5
    initial_backoff: 1s
    max_backoff: 5m
    multiplier: 2.0
    jitter: full
steps:
  - name: charge-card
    handler: payments.charge
    queue: payments
    priority: 80
    timeout: 10s
    retry: { max_attempts: 3 }
    input: { amount_cents: 1299 }
    compensate:
      handler: payments.refund
      timeout: 20s
      retry: { max_attempts: 10 }
  - name: reserve-stock
    handler: inventory.reserve
    description: "Reserve stock for the order"
    depends_on: [charge-card]
  - name: ship
    handler: shipping.create
    depends_on: [charge-card, reserve-stock]
`

func TestParseSpecExample(t *testing.T) {
	p, err := Parse([]byte(orderYAML))
	if err != nil {
		t.Fatal(err)
	}
	d := p.Definition
	if d.Name != "order-fulfilment" || d.Version != 3 || len(d.Steps) != 3 {
		t.Fatalf("definition = %+v", d)
	}
	charge, _ := d.Step("charge-card")
	if charge.Queue != "payments" || charge.Priority != 80 || !charge.PriorityExplicit || charge.Timeout != 10*time.Second {
		t.Errorf("charge-card = %+v", charge)
	}
	// Partial retry blocks merge over the defaults.
	if charge.Retry.MaxAttempts != 3 || charge.Retry.MaxBackoff != 5*time.Minute || charge.Retry.Jitter != retry.JitterFull {
		t.Errorf("charge-card retry = %+v", charge.Retry)
	}
	if string(charge.Input) != `{"amount_cents":1299}` {
		t.Errorf("input = %s", charge.Input)
	}
	if c := charge.Compensate; c == nil || c.Handler != "payments.refund" || c.Timeout != 20*time.Second || c.Retry.MaxAttempts != 10 {
		t.Errorf("compensate = %+v", c)
	}
	reserve, _ := d.Step("reserve-stock")
	if reserve.Queue != "default" || reserve.Priority != 50 || reserve.PriorityExplicit || reserve.Timeout != 30*time.Second {
		t.Errorf("reserve-stock = %+v", reserve)
	}
	if got := strings.Join(d.Order, ","); got != "charge-card,reserve-stock,ship" {
		t.Errorf("order = %s", got)
	}
}

func TestParseIsDeterministicAndRoundTrips(t *testing.T) {
	a, _ := Parse([]byte(orderYAML))
	b, _ := Parse([]byte(orderYAML + "\n# a comment changes nothing\n"))
	if a.SHA256 != b.SHA256 {
		t.Fatal("equivalent YAML hashed differently")
	}
	d, err := Decode(a.Spec)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := json.Marshal(d)
	if string(again) != string(a.Spec) {
		t.Fatal("stored spec does not round-trip")
	}
}

func problems(t *testing.T, src string) []string {
	t.Helper()
	_, err := Parse([]byte(src))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *ValidationError", err)
	}
	return ve.Problems
}

func TestValidationReportsEveryProblem(t *testing.T) {
	got := problems(t, `
name: Bad_Name
version: 0
priority: 101
steps:
  - name: a
    handler: h
    timeout: soon
    retry: { max_attempts: 0, jitter: wild }
  - name: a
    depends_on: [ghost, a]
`)
	want := []string{
		`name "Bad_Name"`, "version must be", "priority must be in [0,100]",
		`step "a".timeout`, "max_attempts must be in [1,100]", "duplicate step name",
		"handler is required", `unknown step "ghost"`, "depends on itself",
	}
	joined := strings.Join(got, "\n")
	for _, w := range want {
		if !strings.Contains(joined, w) {
			t.Errorf("missing problem %q in:\n%s", w, joined)
		}
	}
}

func TestStrictDecoding(t *testing.T) {
	for name, src := range map[string]string{
		"unknown key":    "name: x\nversion: 1\nsteps: [{name: a, handler: h, retries: 3}]",
		"two documents":  "name: x\nversion: 1\nsteps: [{name: a, handler: h}]\n---\nname: y\n",
		"not yaml":       "name: [",
		"zero steps":     "name: x\nversion: 1\nsteps: []",
		"bad step input": "name: x\nversion: 1\nsteps: [{name: a, handler: h, input: [1,2]}]",
	} {
		t.Run(name, func(t *testing.T) {
			if len(problems(t, src)) == 0 {
				t.Fatal("accepted")
			}
		})
	}
}

func TestCycleIsReportedDeterministically(t *testing.T) {
	src := `
name: x
version: 1
steps:
  - {name: d, handler: h}
  - {name: c, handler: h, depends_on: [b]}
  - {name: b, handler: h, depends_on: [a]}
  - {name: a, handler: h, depends_on: [c, d]}
`
	for range 20 {
		got := problems(t, src)
		if len(got) != 1 || got[0] != "cycle: a -> c -> b -> a" {
			t.Fatalf("problems = %v", got)
		}
	}
}

func TestTopoOrderBreaksTiesByName(t *testing.T) {
	steps := []Step{
		{Name: "z"}, {Name: "m", DependsOn: []string{"z"}}, {Name: "a"},
		{Name: "b", DependsOn: []string{"a"}}, {Name: "y", DependsOn: []string{"m", "b"}},
	}
	if got := strings.Join(TopoOrder(steps), ","); got != "a,b,z,m,y" {
		t.Fatalf("order = %s", got)
	}
}

func TestStepLimit(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: big\nversion: 1\nsteps:\n")
	for i := range MaxSteps + 1 {
		b.WriteString("  - {name: s" + strconv.Itoa(i) + ", handler: h}\n")
	}
	got := problems(t, b.String())
	if !strings.Contains(strings.Join(got, ";"), "at most 1000 steps") {
		t.Fatalf("problems = %v", got)
	}
}

func TestMergeInput(t *testing.T) {
	got, err := MergeInput(json.RawMessage(`{"a":1,"b":2}`), json.RawMessage(`{"b":3,"c":4}`))
	if err != nil || string(got) != `{"a":1,"b":3,"c":4}` {
		t.Fatalf("merge = %s %v", got, err)
	}
	if got, _ := MergeInput(nil, json.RawMessage(`{"x":1}`)); string(got) != `{"x":1}` {
		t.Fatalf("step only = %s", got)
	}
	if _, err := MergeInput(json.RawMessage(`[1]`), json.RawMessage(`{"x":1}`)); !errors.Is(err, ErrNotObject) {
		t.Fatalf("array input: %v", err)
	}
}

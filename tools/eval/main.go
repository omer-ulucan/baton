// Command eval compares the rule policy and the Laya policy on the labeled
// failure dataset (eval/errors.jsonl) and writes docs/EVAL.md.
//
//	go run ./tools/eval                 # needs the verified model and libonnxruntime
//	go run ./tools/eval -laya=false     # rules only
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/laya"
	"github.com/omer-ulucan/baton/internal/laya/model"
)

type row struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	ErrorType   string `json:"error_type"`
	Message     string `json:"message"`
	Handler     string `json:"handler"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"max_attempts"`
}

var labels = []string{decision.Retry, decision.RetryWithBackoff, decision.DeadLetter, decision.Compensate, decision.Escalate}

type pred struct {
	label      string
	confidence float64
	latency    time.Duration
}

func main() {
	data := flag.String("data", "eval/errors.jsonl", "labeled dataset")
	out := flag.String("out", "docs/EVAL.md", "report")
	useLaya := flag.Bool("laya", true, "also evaluate the Laya policy")
	flag.Parse()
	if err := run(*data, *out, *useLaya); err != nil {
		fmt.Fprintln(os.Stderr, "eval:", err)
		os.Exit(1)
	}
}

func run(dataPath, outPath string, useLaya bool) error {
	rows, err := load(dataPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	rule := make([]pred, len(rows))
	for i, r := range rows {
		start := time.Now()
		d, _ := decision.RulePolicy{}.Decide(ctx, request(r))
		rule[i] = pred{d.Label, d.Confidence, time.Since(start)}
	}
	var model *laya.Model
	var lp []pred
	if useLaya {
		if model, err = loadModel(); err != nil {
			return fmt.Errorf("laya: %w (run with -laya=false to evaluate the rules alone)", err)
		}
		defer model.Close()
		p := laya.NewPolicyForModel(model)
		for i, r := range rows {
			start := time.Now()
			d, err := p.Decide(ctx, request(r))
			if err != nil {
				return fmt.Errorf("%s: %w", r.ID, err)
			}
			lp = append(lp, pred{d.Label, d.Confidence, time.Since(start)})
			if (i+1)%25 == 0 {
				fmt.Fprintf(os.Stderr, "laya: %d/%d\n", i+1, len(rows))
			}
		}
	}
	report := render(rows, rule, lp, model)
	// The interpretation is written by hand and kept beside the dataset.
	if analysis, err := os.ReadFile(filepath.Join(filepath.Dir(dataPath), "analysis.md")); err == nil {
		report += string(analysis)
	}
	if err := os.WriteFile(outPath, []byte(report), 0o644); err != nil {
		return err
	}
	fmt.Println("wrote", outPath)
	return nil
}

func load(path string) ([]row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var rows []row
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var r row
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, err
		}
		if !slices.Contains(labels, r.Label) {
			return nil, fmt.Errorf("%s: unknown label %q", r.ID, r.Label)
		}
		rows = append(rows, r)
	}
	return rows, sc.Err()
}

func request(r row) decision.Request {
	return decision.Request{
		Kind: decision.KindTriage, TaskID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(r.ID)),
		Handler: r.Handler, ErrorType: r.ErrorType, ErrorMessage: r.Message,
		Attempt: r.Attempt, MaxAttempts: r.MaxAttempts,
	}
}

func loadModel() (*laya.Model, error) {
	dir := os.Getenv("BATON_MODEL_DIR")
	if dir == "" {
		var err error
		if dir, err = model.DefaultDir(); err != nil {
			return nil, err
		}
	}
	lib := os.Getenv("ONNXRUNTIME_LIB_PATH")
	if lib == "" {
		home, _ := os.UserHomeDir()
		lib = filepath.Join(home, ".cache", "baton", "onnxruntime", "1.29.0", "lib", "libonnxruntime.so")
	}
	return laya.Load(laya.SessionConfig{LibPath: lib, ModelDir: dir, Variant: model.FP32})
}

// combine is the production policy at a threshold: the model's answer when
// it is at least that confident, the rules' otherwise.
func combine(lp, rule []pred, threshold float64) ([]pred, int) {
	out := make([]pred, len(lp))
	used := 0
	for i := range lp {
		if lp[i].confidence >= threshold {
			out[i] = lp[i]
			used++
		} else {
			out[i] = rule[i]
		}
	}
	return out, used
}

type score struct {
	n, correct, coarse   int
	wrongRetry, wrongDLQ int
	confusion            map[string]map[string]int
}

func retryFamily(l string) bool { return l == decision.Retry || l == decision.RetryWithBackoff }

func evaluate(rows []row, ps []pred) score {
	s := score{n: len(rows), confusion: map[string]map[string]int{}}
	for _, l := range labels {
		s.confusion[l] = map[string]int{}
	}
	for i, r := range rows {
		p := ps[i].label
		s.confusion[r.Label][p]++
		if p == r.Label {
			s.correct++
		}
		if p == r.Label || retryFamily(p) && retryFamily(r.Label) {
			s.coarse++
		}
		if retryFamily(p) && !retryFamily(r.Label) {
			s.wrongRetry++ // retrying what retrying cannot fix
		}
		if p == decision.DeadLetter && retryFamily(r.Label) {
			s.wrongDLQ++ // giving up on what would have recovered
		}
	}
	return s
}

func pct(a, b int) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(a)/float64(b))
}

func render(rows []row, rule, lp []pred, m *laya.Model) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.Label]++
	}
	w("# Decision eval: failure triage\n\n")
	w("Generated by `go run ./tools/eval` on %s (%s/%s, %d CPUs). Dataset: `eval/errors.jsonl`, %d labeled failures",
		time.Now().UTC().Format("2006-01-02"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), len(rows))
	parts := []string{}
	for _, l := range labels {
		parts = append(parts, fmt.Sprintf("%s %d", l, counts[l]))
	}
	w(" (%s).", strings.Join(parts, ", "))
	if m != nil {
		w(" Model: `%s`.", m.Revision())
	}
	w("\n\n")
	w("Every case is attempt 1 of 5 with no retryable hint from the worker, so the\n")
	w("policies judge from the error text alone (with a hint or exhausted attempts,\n")
	w("both dead-letter deterministically).\n\n")

	type entry struct {
		name string
		ps   []pred
		note string
	}
	entries := []entry{{"RulePolicy", rule, "deterministic"}}
	if lp != nil {
		entries = append(entries, entry{"LayaPolicy (raw)", lp, "argmax, no fallback"})
		for _, t := range []float64{0.3, 0.4, 0.5, 0.6} {
			c, used := combine(lp, rule, t)
			entries = append(entries, entry{fmt.Sprintf("Laya + rules, threshold %.1f", t), c, fmt.Sprintf("model answers %s", pct(used, len(rows)))})
		}
	}
	w("## Summary\n\n")
	w("| Policy | Accuracy | Coarse accuracy | Wrongful retries | Wrongful DLQ | Notes |\n|---|---|---|---|---|---|\n")
	for _, e := range entries {
		s := evaluate(rows, e.ps)
		nonRetry := counts[decision.DeadLetter] + counts[decision.Compensate] + counts[decision.Escalate]
		retryable := counts[decision.Retry] + counts[decision.RetryWithBackoff]
		w("| %s | %s | %s | %d (%s) | %d (%s) | %s |\n", e.name, pct(s.correct, s.n), pct(s.coarse, s.n),
			s.wrongRetry, pct(s.wrongRetry, nonRetry), s.wrongDLQ, pct(s.wrongDLQ, retryable), e.note)
	}
	w("\n- *Accuracy*: exact label. *Coarse accuracy*: `retry` and `retry_with_backoff` count as one,\n")
	w("  since confusing them only changes the delay.\n")
	w("- *Wrongful retries*: a retry label for a failure labeled dead_letter, compensate or escalate\n")
	w("  (percent of those %d cases). Each costs up to four more doomed attempts and delays\n", counts[decision.DeadLetter]+counts[decision.Compensate]+counts[decision.Escalate])
	w("  compensation or a human.\n")
	w("- *Wrongful DLQ*: dead_letter for a failure that would have recovered (percent of the %d\n", counts[decision.Retry]+counts[decision.RetryWithBackoff])
	w("  retryable cases). Each strands a workflow until someone retries it by hand.\n\n")

	for _, e := range entries[:min(2, len(entries))] {
		s := evaluate(rows, e.ps)
		w("## Confusion matrix: %s\n\nRows are the gold label, columns the prediction.\n\n| gold \\ predicted |", e.name)
		for _, l := range labels {
			w(" %s |", l)
		}
		w(" recall |\n|---|")
		for range labels {
			w("---|")
		}
		w("---|\n")
		for _, g := range labels {
			w("| %s |", g)
			for _, p := range labels {
				w(" %d |", s.confusion[g][p])
			}
			w(" %s |\n", pct(s.confusion[g][g], counts[g]))
		}
		w("| precision |")
		for _, p := range labels {
			col := 0
			for _, g := range labels {
				col += s.confusion[g][p]
			}
			w(" %s |", pct(s.confusion[p][p], col))
		}
		w(" |\n\n")
	}

	if lp != nil {
		w("## Laya confidence and calibration\n\n")
		w("| Confidence | Cases | Accuracy in bin |\n|---|---|---|\n")
		bins := []float64{0, 0.2, 0.3, 0.4, 0.5, 0.6, 1.01}
		for i := 0; i+1 < len(bins); i++ {
			n, ok := 0, 0
			for j, p := range lp {
				if p.confidence >= bins[i] && p.confidence < bins[i+1] {
					n++
					if p.label == rows[j].Label {
						ok++
					}
				}
			}
			w("| %.1f-%.1f | %d | %s |\n", bins[i], min(bins[i+1], 1.0), n, pct(ok, n))
		}
		var lat []time.Duration
		for _, p := range lp {
			lat = append(lat, p.latency)
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		w("\nLatency per decision (sequential, fp32 on CPU): p50 %s, p99 %s, max %s.\n\n",
			lat[len(lat)/2].Round(time.Millisecond), lat[len(lat)*99/100].Round(time.Millisecond), lat[len(lat)-1].Round(time.Millisecond))
	}
	return b.String()
}

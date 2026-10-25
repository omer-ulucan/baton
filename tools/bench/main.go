// Command bench measures claim throughput and latency at increasing worker
// counts, and decision latency, and writes docs/BENCHMARKS.md.
//
//	go run ./tools/bench                   # needs Docker; Laya part needs the model
//	go run ./tools/bench -laya=false -tasks 1000
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/laya"
	"github.com/omer-ulucan/baton/internal/laya/model"
	"github.com/omer-ulucan/baton/internal/orchestrator"
	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/protocol"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/worker"
)

type claimResult struct {
	workers    int
	tasks      int
	elapsed    time.Duration
	p50, p99   time.Duration
	emptyPolls int64
}

type layoutResult struct {
	inflight, threads, batch int
	perSec                   float64
}

type decisionResult struct {
	name        string
	concurrency int
	n           int
	elapsed     time.Duration
	p50, p99    time.Duration
}

func main() {
	tasks := flag.Int("tasks", 2000, "tasks per claim run")
	workerCounts := flag.String("workers", "1,2,4,8,16,32,64", "worker counts")
	useLaya := flag.Bool("laya", true, "also measure Laya decision latency")
	out := flag.String("out", "docs/BENCHMARKS.md", "report")
	layout := flag.String("layout", "", "internal: measure one session layout (inflight,threads,batch) and exit")
	flag.Parse()
	if *layout != "" {
		if err := measureLayout(context.Background(), *layout); err != nil {
			fmt.Fprintln(os.Stderr, "layout:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*tasks, *workerCounts, *useLaya, *out); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(tasks int, workerCounts string, useLaya bool, out string) error {
	ctx := context.Background()
	pg, err := pgtest.Start(ctx, store.Migrate)
	if err != nil {
		return err
	}
	defer func() { _ = pg.Close(ctx) }()

	var claims []claimResult
	for _, w := range strings.Split(workerCounts, ",") {
		var n int
		if _, err := fmt.Sscan(w, &n); err != nil {
			return err
		}
		r, err := benchClaims(ctx, pg, n, tasks)
		if err != nil {
			return fmt.Errorf("%d workers: %w", n, err)
		}
		fmt.Fprintf(os.Stderr, "%2d workers: %6.0f tasks/s, claim p50 %s p99 %s\n", n, float64(r.tasks)/r.elapsed.Seconds(), r.p50, r.p99)
		claims = append(claims, r)
	}
	decisions := []decisionResult{benchRule()}
	var layouts []layoutResult
	if useLaya {
		ds, ls, err := benchLaya(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "laya skipped:", err)
		}
		decisions, layouts = append(decisions, ds...), ls
	}
	return os.WriteFile(out, []byte(render(tasks, claims, decisions, layouts)), 0o644)
}

// benchClaims runs a replica over HTTP, preloads tasks, then lets n workers
// race to claim and complete them, timing every claim that returned work.
func benchClaims(ctx context.Context, pg *pgtest.Server, workers, tasks int) (claimResult, error) {
	dbURL, err := pg.NewDatabase(ctx)
	if err != nil {
		return claimResult{}, err
	}
	pcfg, _ := pgxpool.ParseConfig(dbURL)
	pcfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return claimResult{}, err
	}
	defer pool.Close()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return claimResult{}, err
	}
	url := "http://" + ln.Addr().String()
	rep := orchestrator.New(orchestrator.Options{
		Config: config.Orchestrator{
			ReplicaID: "bench", AdvertiseAddr: url, LeaderTTL: 10 * time.Second, LeaseTTL: 30 * time.Second,
			HeartbeatInterval: 10 * time.Second, ShutdownTimeout: 2 * time.Second, DecisionThreshold: 0.5,
			TriageTimeout: time.Second, PriorityTimeout: time.Second,
		},
		Pool: pool, Listener: ln, Log: slog.New(slog.DiscardHandler),
		Engine: func(o *engine.Options) { o.Config.ClaimPoll = 5 * time.Millisecond },
	})
	rctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rep.Run(rctx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			fmt.Fprintln(os.Stderr, "replica:", err)
		}
	}()
	for !rep.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}

	// Preload: one single-step workflow per task, submitted concurrently.
	eng := rep.Engine()
	def := []byte("name: bench\nversion: 1\nsteps: [{name: a, handler: echo}]")
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	var subErr atomic.Value
	for range tasks {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if _, err := eng.Submit(ctx, engine.Submission{Definition: def}); err != nil {
				subErr.Store(err)
			}
		})
	}
	wg.Wait()
	if err, ok := subErr.Load().(error); ok {
		return claimResult{}, err
	}

	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: workers * 2}, Timeout: 30 * time.Second}
	var (
		mu       sync.Mutex
		lat      []time.Duration
		finished atomic.Int64
		empty    atomic.Int64
	)
	start := time.Now()
	for w := range workers {
		id := fmt.Sprintf("bench-%d", w)
		c := worker.NewClient([]string{url}, hc)
		if _, err := c.Register(ctx, protocol.RegisterRequest{WorkerID: id, Queues: []string{"default"}, Handlers: []string{"echo"}}); err != nil {
			return claimResult{}, err
		}
		wg.Go(func() {
			for finished.Load() < int64(tasks) {
				t0 := time.Now()
				got, err := c.Claim(ctx, protocol.ClaimRequest{WorkerID: id, Max: 1, WaitMS: 100})
				d := time.Since(t0)
				if err != nil || len(got) == 0 {
					empty.Add(1)
					continue
				}
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
				for _, tk := range got {
					if err := c.Complete(ctx, protocol.CompleteRequest{WorkerID: id, TaskID: tk.TaskID, AttemptID: tk.AttemptID}); err == nil {
						finished.Add(1)
					}
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	slices.Sort(lat)
	return claimResult{workers: workers, tasks: tasks, elapsed: elapsed, p50: q(lat, 0.5), p99: q(lat, 0.99), emptyPolls: empty.Load()}, nil
}

func q(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	return xs[min(len(xs)-1, int(float64(len(xs))*p))]
}

var benchRequest = decision.Request{
	Kind: decision.KindTriage, TaskID: uuid.New(), Handler: "inventory.reserve",
	ErrorType: "TimeoutError", ErrorMessage: "upstream timed out after 30s", Attempt: 2, MaxAttempts: 5,
}

func benchRule() decisionResult {
	const n = 100_000
	lat := make([]time.Duration, n)
	start := time.Now()
	for i := range n {
		t0 := time.Now()
		_, _ = decision.RulePolicy{}.Decide(context.Background(), benchRequest)
		lat[i] = time.Since(t0)
	}
	elapsed := time.Since(start)
	slices.Sort(lat)
	return decisionResult{name: "RulePolicy", concurrency: 1, n: n, elapsed: elapsed, p50: q(lat, 0.5), p99: q(lat, 0.99)}
}

func benchLaya(ctx context.Context) ([]decisionResult, []layoutResult, error) {
	dir, err := model.DefaultDir()
	if err != nil {
		return nil, nil, err
	}
	m, err := laya.Load(laya.SessionConfig{LibPath: libPath(), ModelDir: dir, Variant: model.FP32})
	if err != nil {
		return nil, nil, err
	}
	p := laya.NewPolicyForModel(m)
	_, _ = p.Decide(ctx, benchRequest) // warm up
	var out []decisionResult
	for _, conc := range []int{1, 4, 8} {
		const perWorker = 8
		var mu sync.Mutex
		var lat []time.Duration
		var wg sync.WaitGroup
		start := time.Now()
		for range conc {
			wg.Go(func() {
				for range perWorker {
					t0 := time.Now()
					if _, err := p.Decide(ctx, benchRequest); err == nil {
						d := time.Since(t0)
						mu.Lock()
						lat = append(lat, d)
						mu.Unlock()
					}
				}
			})
		}
		wg.Wait()
		elapsed := time.Since(start)
		slices.Sort(lat)
		out = append(out, decisionResult{name: "LayaPolicy (fp32)", concurrency: conc, n: len(lat), elapsed: elapsed, p50: q(lat, 0.5), p99: q(lat, 0.99)})
		fmt.Fprintf(os.Stderr, "laya concurrency %d: p50 %s p99 %s, %.2f decisions/s\n", conc, q(lat, 0.5), q(lat, 0.99), float64(len(lat))/elapsed.Seconds())
	}
	_ = m.Close()

	// Session layouts under 8 concurrent callers: how the defaults were
	// chosen. Each runs in a child process: a closed session's memory is
	// not returned to the OS, and four 1.7GB loads in one process run out.
	half := max(1, runtime.NumCPU()/2)
	var layouts []layoutResult
	self, err := os.Executable()
	if err != nil {
		return out, nil, err
	}
	for _, l := range []layoutResult{{2, half, 8, 0}, {1, runtime.NumCPU(), 8, 0}, {1, runtime.NumCPU(), 1, 0}, {2, half, 1, 0}} {
		cmd := exec.CommandContext(ctx, self, "-layout", fmt.Sprintf("%d,%d,%d", l.inflight, l.threads, l.batch))
		cmd.Stderr = os.Stderr
		raw, err := cmd.Output()
		if err != nil {
			return out, layouts, fmt.Errorf("layout %+v: %w", l, err)
		}
		if _, err := fmt.Sscan(strings.TrimSpace(string(raw)), &l.perSec); err != nil {
			return out, layouts, err
		}
		fmt.Fprintf(os.Stderr, "layout inflight=%d threads=%d batch=%d: %.2f decisions/s\n", l.inflight, l.threads, l.batch, l.perSec)
		layouts = append(layouts, l)
	}
	return out, layouts, nil
}

// measureLayout (child process) prints decisions per second for one
// session layout under 8 concurrent callers.
func measureLayout(ctx context.Context, spec string) error {
	var l layoutResult
	if _, err := fmt.Sscanf(spec, "%d,%d,%d", &l.inflight, &l.threads, &l.batch); err != nil {
		return err
	}
	dir, err := model.DefaultDir()
	if err != nil {
		return err
	}
	lm, err := laya.Load(laya.SessionConfig{LibPath: libPath(), ModelDir: dir, Variant: model.FP32, SkipVerify: true,
		MaxInflight: l.inflight, IntraOpThreads: l.threads, MaxBatch: l.batch})
	if err != nil {
		return err
	}
	defer lm.Close()
	lp := laya.NewPolicyForModel(lm)
	_, _ = lp.Decide(ctx, benchRequest)
	var ok atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for range 8 {
		wg.Go(func() {
			for range 4 {
				if _, err := lp.Decide(ctx, benchRequest); err == nil {
					ok.Add(1)
				}
			}
		})
	}
	wg.Wait()
	fmt.Println(float64(ok.Load()) / time.Since(start).Seconds())
	return nil
}

func libPath() string {
	if lib := os.Getenv("ONNXRUNTIME_LIB_PATH"); lib != "" {
		return lib
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cache", "baton", "onnxruntime", "1.29.0", "lib", "libonnxruntime.so")
}

func cpuModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown CPU"
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "model name") {
			if i := strings.Index(line, ":"); i >= 0 {
				return strings.TrimSpace(line[i+1:])
			}
		}
	}
	return "unknown CPU"
}

func render(tasks int, claims []claimResult, decisions []decisionResult, layouts []layoutResult) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	w("# Benchmarks\n\n")
	w("Generated by `go run ./tools/bench` on %s: %s, %d logical CPUs, %s, Go %s. Postgres 16 in a\n",
		time.Now().UTC().Format("2006-01-02"), cpuModel(), runtime.NumCPU(), runtime.GOOS+"/"+runtime.GOARCH, strings.TrimPrefix(runtime.Version(), "go"))
	w("local Docker container (testcontainers), one orchestrator replica in process, workers as\n")
	w("goroutines speaking the real HTTP worker protocol. Everything shares one machine, so the\n")
	w("numbers are relative, not capacity planning; no race detector.\n\n")
	w("## Claim throughput and latency\n\n")
	w("Each run preloads %d ready single-step tasks, then N workers loop: claim one task\n", tasks)
	w("(long-poll 100ms), complete it, repeat until all are done. Claim latency is the client-side\n")
	w("time of a claim request that returned a task (transaction, lease, HTTP); empty polls at the\n")
	w("end of a run are excluded. Throughput is completed tasks per second of the whole run.\n\n")
	w("| Workers | Throughput (tasks/s) | Claim p50 | Claim p99 |\n|---|---|---|---|\n")
	for _, c := range claims {
		w("| %d | %.0f | %s | %s |\n", c.workers, float64(c.tasks)/c.elapsed.Seconds(), c.p50.Round(10*time.Microsecond), c.p99.Round(10*time.Microsecond))
	}
	w("\n## Decision latency\n\n")
	w("Triage of one timeout error. RulePolicy is called in a tight loop; LayaPolicy (fp32, CPU)\n")
	w("is called by 1, 4 and 8 concurrent callers, %d calls each, with the default session\n", 8)
	w("layout: two runs in flight, half the CPU threads each, no batching.\n\n")
	w("| Policy | Concurrent callers | p50 | p99 | Decisions/s |\n|---|---|---|---|---|\n")
	for _, d := range decisions {
		w("| %s | %d | %s | %s | %.1f |\n", d.name, d.concurrency, d.p50.Round(time.Microsecond), d.p99.Round(time.Microsecond), float64(d.n)/d.elapsed.Seconds())
	}
	if len(layouts) > 0 {
		w("\n### Session layout\n\n")
		w("Throughput with 8 concurrent callers (32 decisions) for different ONNX Runtime layouts;\n")
		w("this is why batching is off by default.\n\n")
		w("| Runs in flight | Threads per run | Max batch | Decisions/s |\n|---|---|---|---|\n")
		for _, l := range layouts {
			w("| %d | %d | %d | %.2f |\n", l.inflight, l.threads, l.batch, l.perSec)
		}
	}
	w("\n")
	w("Notes:\n\n")
	w("- Claim throughput levels off around 16 workers: every write goes through the one leader\n")
	w("  (ADR 0003) and each claim is a transaction of about ten statements plus six per task,\n")
	w("  all on the database clock and fenced. More workers then mostly add queueing (p99).\n")
	w("- The same statement count makes the system latency-sensitive: in the chaos suite, with\n")
	w("  60±30ms added to every database round trip, a claim takes about two seconds.\n")
	w("- Laya inference is CPU-bound at roughly 0.5s of CPU per decision on this machine, so\n")
	w("  extra concurrency raises latency, not throughput. Padded batching did not help on CPU,\n")
	w("  which is why it is off by default (the table above).\n")
	w("- A closed ONNX Runtime session's memory is not returned to the OS: loading the 1.7GB model\n")
	w("  repeatedly in one process grows RSS by roughly that much per load. Baton loads it once\n")
	w("  per process; the layout sweep runs each layout in a child process for this reason.\n")
	return b.String()
}

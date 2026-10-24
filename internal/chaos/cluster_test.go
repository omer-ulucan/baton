//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/omer-ulucan/baton/internal/api"
	"github.com/omer-ulucan/baton/internal/store"
)

// env is the shared infrastructure of the suite: binaries, Postgres, and
// Toxiproxy with one proxy per orchestrator slot so a replica can be
// partitioned or slowed on its own.
type env struct {
	bin     string
	pgAdmin *url.URL // direct, for setup and invariant checks
	toxAPI  string
	proxies []string // "host:port" per orchestrator slot
}

var (
	suite   *env
	dbCount int
	seed    uint64
)

const maxOrchestrators = 2

func TestMain(m *testing.M) {
	flag.Parse()
	seed = uint64(time.Now().UnixNano())
	if s := os.Getenv("BATON_CHAOS_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	fmt.Fprintf(os.Stderr, "chaos seed %d (BATON_CHAOS_SEED to reproduce)\n", seed)
	ctx := context.Background()
	e, cleanup, err := setup(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos setup:", err)
		os.Exit(1)
	}
	suite = e
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func setup(ctx context.Context) (*env, func(), error) {
	dir, err := os.MkdirTemp("", "baton-chaos-bin-")
	if err != nil {
		return nil, nil, err
	}
	root, err := moduleRoot(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, pkg := range []string{"./cmd/baton", "./cmd/baton-worker"} {
		cmd := exec.CommandContext(ctx, "go", "build", "-o", dir+"/", pkg)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, nil, fmt.Errorf("build %s: %w\n%s", pkg, err, out)
		}
	}
	nw, err := network.New(ctx)
	if err != nil {
		return nil, nil, err
	}
	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("postgres"), tcpostgres.WithUsername("baton"), tcpostgres.WithPassword("baton"),
		network.WithNetwork([]string{"pg"}, nw),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		return nil, nil, err
	}
	var opts []testcontainers.ContainerCustomizer
	for i := range maxOrchestrators {
		opts = append(opts, toxiproxy.WithProxy(fmt.Sprintf("pg-%d", i), "pg:5432"))
	}
	opts = append(opts, network.WithNetwork([]string{"toxiproxy"}, nw))
	tox, err := toxiproxy.Run(ctx, "ghcr.io/shopify/toxiproxy:2.12.0", opts...)
	if err != nil {
		return nil, nil, err
	}
	raw, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, err
	}
	admin, _ := url.Parse(raw)
	toxURI, err := tox.URI(ctx)
	if err != nil {
		return nil, nil, err
	}
	e := &env{bin: dir, pgAdmin: admin, toxAPI: toxURI}
	for i := range maxOrchestrators {
		host, port, err := tox.ProxiedEndpoint(8666 + i)
		if err != nil {
			return nil, nil, err
		}
		e.proxies = append(e.proxies, net.JoinHostPort(host, port))
	}
	if err := e.exec(ctx, "CREATE DATABASE baton_template"); err != nil {
		return nil, nil, err
	}
	if err := store.Migrate(e.dbURL("baton_template")); err != nil {
		return nil, nil, err
	}
	done := context.WithoutCancel(ctx)
	cleanup := func() {
		_ = tox.Terminate(done)
		_ = pg.Terminate(done)
		_ = nw.Remove(done)
		_ = os.RemoveAll(dir)
	}
	return e, cleanup, nil
}

func moduleRoot(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "go", "env", "GOMOD").Output()
	if err != nil {
		return "", err
	}
	return filepath.Dir(strings.TrimSpace(string(out))), nil
}

func (e *env) dbURL(db string) string {
	u := *e.pgAdmin
	u.Path = "/" + db
	return u.String()
}

func (e *env) exec(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, e.dbURL("postgres"))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, sql)
	return err
}

// toxiproxy control (REST API).

func (e *env) tox(t *testing.T, method, path string, body any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(t.Context(), method, e.toxAPI+path, rd)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("toxiproxy %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("toxiproxy %s %s: %d %s", method, path, resp.StatusCode, b)
	}
}

func (e *env) setProxyEnabled(t *testing.T, slot int, enabled bool) {
	e.tox(t, "POST", fmt.Sprintf("/proxies/pg-%d", slot), map[string]any{"enabled": enabled})
}

func (e *env) addLatency(t *testing.T, slot int, ms, jitter int) {
	for _, stream := range []string{"upstream", "downstream"} {
		e.tox(t, "POST", fmt.Sprintf("/proxies/pg-%d/toxics", slot), map[string]any{
			"name": "latency-" + stream, "type": "latency", "stream": stream,
			"attributes": map[string]int{"latency": ms, "jitter": jitter},
		})
	}
}

func (e *env) reset(t *testing.T) { e.tox(t, "POST", "/reset", nil) }

// cluster is one scenario's processes on a fresh database.
type cluster struct {
	t       *testing.T
	env     *env
	db      string
	pool    *pgxpool.Pool // direct, not through toxiproxy
	orchs   []*proc
	workers []*proc
	rng     *rand.Rand
}

type proc struct {
	name string
	bin  string
	env  []string
	url  string // orchestrators only
	cmd  *exec.Cmd
	log  string
	mu   sync.Mutex
}

type clusterOpts struct {
	orchestrators int
	workers       int
	orchEnv       []string // extra env for every orchestrator
}

func newCluster(t *testing.T, o clusterOpts) *cluster {
	t.Helper()
	suite.reset(t)
	dbCount++
	db := fmt.Sprintf("chaos%d", dbCount)
	if err := suite.exec(t.Context(), fmt.Sprintf("CREATE DATABASE %s TEMPLATE baton_template", db)); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), suite.dbURL(db))
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, env: suite, db: db, pool: pool, rng: rand.New(rand.NewPCG(seed, uint64(dbCount)))}
	logs := t.TempDir()
	var addrs []string
	for i := range o.orchestrators {
		port := freePort(t)
		u := fmt.Sprintf("http://127.0.0.1:%d", port)
		addrs = append(addrs, u)
		dbu := fmt.Sprintf("postgres://baton:baton@%s/%s?sslmode=disable", suite.proxies[i], db)
		c.orchs = append(c.orchs, &proc{
			name: fmt.Sprintf("baton-%d", i+1), bin: filepath.Join(suite.bin, "baton"), url: u,
			log: filepath.Join(logs, fmt.Sprintf("baton-%d.log", i+1)),
			env: append([]string{
				"BATON_DATABASE_URL=" + dbu, "BATON_HTTP_ADDR=127.0.0.1:" + strconv.Itoa(port), "BATON_ADVERTISE_ADDR=" + u,
				fmt.Sprintf("BATON_REPLICA_ID=baton-%d", i+1), "BATON_AUTO_MIGRATE=false",
				"BATON_LEADER_TTL=2s", "BATON_LEASE_TTL=3s", "BATON_HEARTBEAT_INTERVAL=1s", "BATON_SHUTDOWN_TIMEOUT=3s",
			}, o.orchEnv...),
		})
	}
	for i := range o.workers {
		c.workers = append(c.workers, &proc{
			name: fmt.Sprintf("worker-%d", i+1), bin: filepath.Join(suite.bin, "baton-worker"),
			log: filepath.Join(logs, fmt.Sprintf("worker-%d.log", i+1)),
			env: []string{"BATON_ADDR=" + strings.Join(addrs, ","), fmt.Sprintf("BATON_WORKER_ID=worker-%d", i+1),
				"BATON_WORKER_QUEUES=default", "BATON_WORKER_CONCURRENCY=4", "BATON_WORKER_DRAIN_TIMEOUT=2s"},
		})
	}
	for _, p := range c.orchs {
		p.start(t.Context(), t)
	}
	c.waitLeader(10 * time.Second)
	for _, p := range c.workers {
		p.start(t.Context(), t)
	}
	t.Cleanup(func() {
		for _, p := range append(append([]*proc{}, c.workers...), c.orchs...) {
			p.stop()
		}
		pool.Close()
		if t.Failed() {
			for _, p := range append(append([]*proc{}, c.orchs...), c.workers...) {
				t.Logf("---- %s log tail ----\n%s", p.name, tail(p.log, 30))
			}
		}
	})
	return c
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func (p *proc) start(ctx context.Context, t *testing.T) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, err := os.OpenFile(p.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Not tied to the test's context: processes are stopped (or killed)
	// explicitly, and must outlive the context's cancellation at test end
	// long enough for a graceful stop.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), p.bin)
	cmd.Env = append(os.Environ(), p.env...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", p.name, err)
	}
	p.cmd = cmd
	go func() { _ = cmd.Wait(); _ = f.Close() }()
}

// kill9 is a crash: no drain, no release, no deregistration.
func (p *proc) kill9() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		_, _ = p.cmd.Process.Wait()
		p.cmd = nil
	}
}

func (p *proc) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _, _ = p.cmd.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
	}
	p.cmd = nil
}

func tail(path string, n int) string {
	b, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// role asks an orchestrator whether it leads ("" if unreachable).
func (p *proc) role(ctx context.Context) string {
	req, _ := http.NewRequestWithContext(ctx, "GET", p.url+"/readyz", nil)
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var body struct {
		Role string `json:"role"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return body.Role
}

func (c *cluster) leader() *proc {
	for _, p := range c.orchs {
		if p.role(c.t.Context()) == "leader" {
			return p
		}
	}
	return nil
}

func (c *cluster) waitLeader(within time.Duration) *proc {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != nil {
			return l
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("no leader within %s", within)
	return nil
}

// post sends a write to any reachable orchestrator, retrying through
// failover. It returns 0 if no leader answered.
func (c *cluster) post(path string, body any, out any) int {
	b, _ := json.Marshal(body)
	for attempt := 0; attempt < 200; attempt++ {
		p := c.orchs[attempt%len(c.orchs)]
		req, _ := http.NewRequestWithContext(c.t.Context(), "POST", p.url+path, bytes.NewReader(b))
		if path == "/v1/workflows" {
			sum := sha256.Sum256(b) // retries through failover must not double-submit
			req.Header.Set("Idempotency-Key", hex.EncodeToString(sum[:]))
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			code := resp.StatusCode
			if out != nil {
				_ = json.NewDecoder(resp.Body).Decode(out)
			}
			resp.Body.Close()
			if code != http.StatusServiceUnavailable {
				return code
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0 // never reached a leader; callers decide (no t.Fatal off the test goroutine)
}

// The workload: fast retry policies so every workflow terminates, a mix
// of successes, transient failures, compensations, and long runs to
// cancel.
var definitions = map[string]string{
	"diamond": `
name: chaos-diamond
version: 1
defaults: {timeout: 20s, retry: {max_attempts: 10, initial_backoff: 100ms, max_backoff: 1s}}
steps:
  - {name: a, handler: sleep, input: {ms: 150}}
  - {name: b, handler: flaky, depends_on: [a], input: {fail_rate: 0.3}}
  - {name: c, handler: sleep, depends_on: [a], input: {ms: 250}}
  - {name: d, handler: flaky, depends_on: [b, c], input: {fail_rate: 0.3}}`,
	"saga": `
name: chaos-saga
version: 1
defaults: {timeout: 20s, retry: {max_attempts: 10, initial_backoff: 100ms, max_backoff: 1s}}
steps:
  - {name: charge, handler: echo, compensate: {handler: echo}}
  - {name: reserve, handler: sleep, depends_on: [charge], input: {ms: 200}, compensate: {handler: echo}}
  - {name: ship, handler: flaky, depends_on: [reserve], input: {fail_attempts: 99, permanent: true, message: "address rejected"}}`,
	"long": `
name: chaos-long
version: 1
defaults: {timeout: 20s, retry: {max_attempts: 10, initial_backoff: 100ms, max_backoff: 1s}}
steps:
  - {name: one, handler: sleep, input: {ms: 1500}, compensate: {handler: echo}}
  - {name: two, handler: sleep, depends_on: [one], input: {ms: 1500}}`,
}

// workload submits n workflows and cancels some long ones mid-flight, half
// of them with compensation.
func (c *cluster) workload(n int) []uuid.UUID {
	c.t.Helper()
	kinds := []string{"diamond", "diamond", "saga", "long"}
	var ids []uuid.UUID
	var long []uuid.UUID
	for i := range n {
		kind := kinds[c.rng.IntN(len(kinds))]
		var out api.SubmitResponse
		code := c.post("/v1/workflows", api.SubmitRequest{Definition: definitions[kind], Input: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))}, &out)
		if code/100 != 2 {
			c.t.Fatalf("submit %d: %d", i, code)
		}
		ids = append(ids, out.WorkflowID)
		if kind == "long" {
			long = append(long, out.WorkflowID)
		}
	}
	go func() {
		for _, id := range long {
			if c.rng.IntN(3) != 0 {
				continue
			}
			time.Sleep(time.Duration(200+c.rng.IntN(1500)) * time.Millisecond)
			c.post("/v1/workflows/"+id.String()+"/cancel", api.CancelRequest{Compensate: c.rng.IntN(2) == 0, Reason: "chaos"}, nil)
		}
	}()
	return ids
}

// waitTerminal waits until every workflow is terminal.
func (c *cluster) waitTerminal(ids []uuid.UUID, within time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for {
		var open int
		err := c.pool.QueryRow(c.t.Context(), `SELECT count(*) FROM workflows
			WHERE status NOT IN ('succeeded','failed','compensated','compensation_failed','cancelled')`).Scan(&open)
		var total int
		_ = c.pool.QueryRow(c.t.Context(), `SELECT count(*) FROM workflows`).Scan(&total)
		if err == nil && open == 0 && total == len(ids) {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%d of %d workflows still running after %s (%d stored)", open, len(ids), within, total)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// checkInvariants runs SPEC 13 over the scenario's database.
func (c *cluster) checkInvariants() {
	c.t.Helper()
	vs, err := Check(c.t.Context(), c.pool)
	if err != nil {
		c.t.Fatal(err)
	}
	for _, v := range vs {
		c.t.Error(v)
	}
	var byStatus []string
	rows, _ := c.pool.Query(c.t.Context(), `SELECT status, count(*) FROM workflows GROUP BY status ORDER BY status`)
	for rows.Next() {
		var s string
		var n int
		_ = rows.Scan(&s, &n)
		byStatus = append(byStatus, fmt.Sprintf("%s=%d", s, n))
	}
	rows.Close()
	c.t.Logf("outcome: %s; %d invariant violations", strings.Join(byStatus, " "), len(vs))
}

// metric reads one sample from an orchestrator's /metrics.
func (p *proc) metric(ctx context.Context, prefix string) (float64, bool) {
	req, _ := http.NewRequestWithContext(ctx, "GET", p.url+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, prefix+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
			return v, err == nil
		}
	}
	return 0, false
}

//go:build chaos

package chaos

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omer-ulucan/baton/internal/laya/model"
)

const runTimeout = 4 * time.Minute

// faultLoop runs fault every period until ctx ends.
func faultLoop(ctx context.Context, fault func()) {
	for ctx.Err() == nil {
		fault()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// TestKillWorkers SIGKILLs random workers mid-task and restarts them. Their
// leases expire, the reaper retries the work, and late results from the
// dead attempts never land.
func TestKillWorkers(t *testing.T) {
	c := newCluster(t, clusterOpts{orchestrators: 2, workers: 3})
	ids := c.workload(60)
	ctx, stop := context.WithCancel(t.Context())
	kills := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		faultLoop(ctx, func() {
			if !sleepCtx(ctx, time.Duration(400+c.rng.IntN(1200))*time.Millisecond) {
				return
			}
			w := c.workers[c.rng.IntN(len(c.workers))]
			w.kill9()
			kills++
			if sleepCtx(ctx, 300*time.Millisecond) {
				w.start(ctx, t)
			}
		})
	}()
	c.waitTerminal(ids, runTimeout)
	stop()
	<-done
	var expired int
	_ = c.pool.QueryRow(t.Context(), `SELECT count(*) FROM workflow_events WHERE type = 'TaskLeaseExpired'`).Scan(&expired)
	t.Logf("%d worker kills, %d leases reaped", kills, expired)
	if kills == 0 || expired == 0 {
		t.Fatal("the scenario never killed a worker mid-task")
	}
	c.checkInvariants()
}

// TestKillLeader SIGKILLs the leader twice mid-run. The follower takes over
// once the lease expires and no workflow is lost.
func TestKillLeader(t *testing.T) {
	c := newCluster(t, clusterOpts{orchestrators: 2, workers: 3})
	ids := c.workload(50)
	for round := range 2 {
		time.Sleep(2 * time.Second)
		old := c.waitLeader(10 * time.Second)
		old.kill9()
		killed := time.Now()
		var next *proc
		for time.Since(killed) < 10*time.Second {
			if l := c.leader(); l != nil && l != old {
				next = l
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if next == nil {
			t.Fatalf("round %d: no failover within 10s", round)
		}
		t.Logf("round %d: %s killed, %s leads after %s", round, old.name, next.name, time.Since(killed).Round(10*time.Millisecond))
		old.start(t.Context(), t)
	}
	c.waitTerminal(ids, runTimeout)
	c.checkInvariants()
}

// TestPartitionedLeaderIsFenced cuts the leader off from Postgres. It must
// step down on its own, the follower must take over, and when the
// partition heals nothing the old leader still had in flight may commit:
// tokens in history never go backwards.
func TestPartitionedLeaderIsFenced(t *testing.T) {
	c := newCluster(t, clusterOpts{orchestrators: 2, workers: 3})
	ids := c.workload(40)
	time.Sleep(2 * time.Second)
	old := c.waitLeader(10 * time.Second)
	slot := 0
	if old == c.orchs[1] {
		slot = 1
	}
	c.env.setProxyEnabled(t, slot, false)
	start := time.Now()
	var next *proc
	for time.Since(start) < 15*time.Second {
		if l := c.leader(); l != nil && l != old {
			next = l
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if next == nil {
		t.Fatal("follower did not take over from the partitioned leader")
	}
	t.Logf("%s partitioned; %s leads after %s", old.name, next.name, time.Since(start).Round(10*time.Millisecond))
	time.Sleep(3 * time.Second)
	if r := old.role(t.Context()); r == "leader" {
		t.Fatal("the partitioned replica still claims to lead")
	}
	c.env.setProxyEnabled(t, slot, true)
	c.waitTerminal(ids, runTimeout)
	if r := old.role(t.Context()); r != "follower" {
		t.Fatalf("healed replica is %q, want follower", r)
	}
	if v, ok := old.metric(t.Context(), "baton_fencing_rejections_total"); ok {
		t.Logf("writes refused by fencing on the old leader: %v", v)
	}
	c.checkInvariants()
}

// TestPostgresLatency slows every orchestrator's database connection down
// for the whole run.
func TestPostgresLatency(t *testing.T) {
	c := newCluster(t, clusterOpts{orchestrators: 2, workers: 3})
	for slot := range c.orchs {
		c.env.addLatency(t, slot, 60, 30)
	}
	// Every statement now costs ~120ms of round trip and a claim runs about
	// ten plus six per task, so this run is slow by design: the point is
	// that it stays correct and the leader keeps its lease.
	ids := c.workload(20)
	c.waitTerminal(ids, 6*time.Minute)
	if l := c.leader(); l == nil {
		t.Fatal("no leader under latency")
	}
	for _, o := range c.orchs {
		if v, ok := o.metric(t.Context(), "baton_leader_changes_total"); ok && v > 1 {
			t.Errorf("%s changed leadership %v times under latency", o.name, v)
		}
	}
	c.checkInvariants()
}

// TestDecisionFaults runs with the Laya policy and a model that cannot
// load: every decision must fall back to the rules and every workflow
// must still finish.
func TestDecisionFaultsCorruptModel(t *testing.T) {
	c := newCluster(t, clusterOpts{orchestrators: 2, workers: 3, orchEnv: []string{
		"BATON_DECISION_POLICY=laya", "BATON_MODEL_DIR=" + t.TempDir(), "BATON_MODEL_DOWNLOAD=false",
	}})
	ids := c.workload(30)
	c.waitTerminal(ids, runTimeout)
	c.assertDecisions(t, "unavailable")
	c.checkInvariants()
}

// TestDecisionFaultsInferenceTimeout runs one orchestrator with the real
// model and a decision deadline shorter than inference: decisions must time
// out into the rules without slowing anything else. Needs the verified
// model and libonnxruntime locally; skipped otherwise.
func TestDecisionFaultsInferenceTimeout(t *testing.T) {
	dir, err := model.DefaultDir()
	files, _ := model.Files(model.FP32, false)
	if err != nil || model.Verify(dir, files[2:3]) != nil { // cheap presence check of the graph
		t.Skip("verified model not in the cache; run `baton model fetch`")
	}
	home, _ := os.UserHomeDir()
	lib := os.Getenv("ONNXRUNTIME_LIB_PATH")
	if lib == "" {
		lib = filepath.Join(home, ".cache", "baton", "onnxruntime", "1.29.0", "lib", "libonnxruntime.so")
	}
	if _, err := os.Stat(lib); err != nil {
		t.Skip("libonnxruntime not found")
	}
	c := newCluster(t, clusterOpts{orchestrators: 1, workers: 3, orchEnv: []string{
		"BATON_DECISION_POLICY=laya", "BATON_MODEL_DIR=" + dir, "BATON_MODEL_DOWNLOAD=false",
		"ONNXRUNTIME_LIB_PATH=" + lib, "BATON_TRIAGE_TIMEOUT=20ms", "BATON_PRIORITY_TIMEOUT=20ms",
	}})
	// Wait for the model so failures meet a loaded model, not "unavailable".
	deadline := time.Now().Add(90 * time.Second)
	for {
		if v, ok := c.orchs[0].metric(t.Context(), "baton_decision_model_loaded"); ok && v == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("model did not load")
		}
		time.Sleep(time.Second)
	}
	ids := c.workload(20)
	c.waitTerminal(ids, runTimeout)
	c.assertDecisions(t, "timeout")
	c.checkInvariants()
}

// assertDecisions checks that every triage decision was answered by the
// rule policy with the given fallback reason.
func (c *cluster) assertDecisions(t *testing.T, reason string) {
	t.Helper()
	var total, matching int
	err := c.pool.QueryRow(t.Context(), `SELECT count(*), count(*) FILTER (WHERE payload->>'policy' = 'rule' AND payload->>'fallback' = $1)
		FROM workflow_events WHERE type = 'DecisionMade' AND payload->>'kind' = 'triage'`, reason).Scan(&total, &matching)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d of %d triage decisions fell back with reason %q", matching, total, reason)
	if total == 0 || matching != total {
		t.Fatalf("%d of %d triage decisions fell back with %q", matching, total, reason)
	}
}

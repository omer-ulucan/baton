// Package config loads process configuration from environment variables.
//
// Loaders take a getenv function instead of reading os.Getenv directly, so
// callers own the environment and tests need no process-level state.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Orchestrator is the configuration of a baton replica.
type Orchestrator struct {
	DatabaseURL       string
	AutoMigrate       bool // apply pending migrations at startup
	HTTPAddr          string
	AdvertiseAddr     string // address other replicas use to reach this one
	ReplicaID         string
	LeaderTTL         time.Duration
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
	AgingSeconds      int
	DecisionPolicy    string // "rule" | "laya"
	DecisionThreshold float64
	EscalationWebhook string // receives escalation messages; empty disables them
}

// Worker is the configuration of a baton-worker process.
type Worker struct {
	// OrchestratorAddrs lists replicas; the client fails over between them.
	OrchestratorAddrs []string
	WorkerID          string
	Queues            []string
	Concurrency       int
	DrainTimeout      time.Duration
}

// LoadOrchestrator reads orchestrator settings. hostname is the default
// replica id. All problems are reported together.
func LoadOrchestrator(getenv func(string) string, hostname string) (Orchestrator, error) {
	r := reader{getenv: getenv}
	c := Orchestrator{
		DatabaseURL:       r.str("BATON_DATABASE_URL", ""),
		AutoMigrate:       r.boolean("BATON_AUTO_MIGRATE", true),
		HTTPAddr:          r.str("BATON_HTTP_ADDR", ":8080"),
		ReplicaID:         r.str("BATON_REPLICA_ID", hostname),
		LeaderTTL:         r.duration("BATON_LEADER_TTL", 10*time.Second),
		LeaseTTL:          r.duration("BATON_LEASE_TTL", 30*time.Second),
		HeartbeatInterval: r.duration("BATON_HEARTBEAT_INTERVAL", 10*time.Second),
		ShutdownTimeout:   r.duration("BATON_SHUTDOWN_TIMEOUT", 20*time.Second),
		AgingSeconds:      r.positiveInt("BATON_AGING_SECONDS", 30),
		DecisionPolicy:    r.str("BATON_DECISION_POLICY", "rule"),
		DecisionThreshold: r.float("BATON_DECISION_THRESHOLD", 0.5),
		EscalationWebhook: r.str("BATON_ESCALATION_WEBHOOK", ""),
	}
	c.AdvertiseAddr = r.str("BATON_ADVERTISE_ADDR", defaultAdvertise(c.ReplicaID, c.HTTPAddr))

	if c.DatabaseURL == "" {
		r.fail("BATON_DATABASE_URL is required")
	}
	if c.ReplicaID == "" {
		r.fail("BATON_REPLICA_ID is required when the hostname is unknown")
	}
	if c.LeaseTTL > 0 && c.HeartbeatInterval > 0 && c.LeaseTTL < 3*c.HeartbeatInterval {
		r.fail("BATON_LEASE_TTL (%s) must be at least 3x BATON_HEARTBEAT_INTERVAL (%s)", c.LeaseTTL, c.HeartbeatInterval)
	}
	if c.DecisionPolicy != "rule" && c.DecisionPolicy != "laya" {
		r.fail("BATON_DECISION_POLICY must be rule or laya, got %q", c.DecisionPolicy)
	}
	if c.DecisionThreshold < 0 || c.DecisionThreshold > 1 {
		r.fail("BATON_DECISION_THRESHOLD must be in [0,1], got %v", c.DecisionThreshold)
	}
	return c, r.err()
}

// LoadWorker reads worker settings. hostname is the default worker id.
func LoadWorker(getenv func(string) string, hostname string) (Worker, error) {
	r := reader{getenv: getenv}
	c := Worker{
		OrchestratorAddrs: splitList(r.str("BATON_ADDR", "http://localhost:8080")),
		WorkerID:          r.str("BATON_WORKER_ID", hostname),
		Queues:            splitList(r.str("BATON_WORKER_QUEUES", "default")),
		Concurrency:       r.positiveInt("BATON_WORKER_CONCURRENCY", 4),
		DrainTimeout:      r.duration("BATON_WORKER_DRAIN_TIMEOUT", 25*time.Second),
	}
	if len(c.OrchestratorAddrs) == 0 {
		r.fail("BATON_ADDR must name at least one orchestrator")
	}
	if c.WorkerID == "" {
		r.fail("BATON_WORKER_ID is required when the hostname is unknown")
	}
	if len(c.Queues) == 0 {
		r.fail("BATON_WORKER_QUEUES must name at least one queue")
	}
	return c, r.err()
}

func defaultAdvertise(replicaID, httpAddr string) string {
	port := httpAddr
	if i := strings.LastIndex(httpAddr, ":"); i >= 0 {
		port = httpAddr[i+1:]
	}
	return "http://" + replicaID + ":" + port
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// reader accumulates parse errors so a misconfigured process reports
// every problem at once instead of one per restart.
type reader struct {
	getenv func(string) string
	errs   []error
}

func (r *reader) fail(format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf(format, args...))
}

func (r *reader) err() error { return errors.Join(r.errs...) }

func (r *reader) str(key, def string) string {
	if v := strings.TrimSpace(r.getenv(key)); v != "" {
		return v
	}
	return def
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail("%s: %v", key, err)
		return def
	}
	if d <= 0 {
		r.fail("%s must be positive, got %s", key, d)
	}
	return d
}

func (r *reader) boolean(key string, def bool) bool {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.fail("%s: %v", key, err)
		return def
	}
	return b
}

func (r *reader) positiveInt(key string, def int) int {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail("%s: %v", key, err)
		return def
	}
	if n <= 0 {
		r.fail("%s must be positive, got %d", key, n)
	}
	return n
}

func (r *reader) float(key string, def float64) float64 {
	v := strings.TrimSpace(r.getenv(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		r.fail("%s: %v", key, err)
		return def
	}
	return f
}

package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestOrchestratorDefaults(t *testing.T) {
	c, err := LoadOrchestrator(env(map[string]string{
		"BATON_DATABASE_URL": "postgres://localhost/baton",
	}), "host-1")
	if err != nil {
		t.Fatalf("LoadOrchestrator: %v", err)
	}
	if c.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", c.HTTPAddr)
	}
	if c.ReplicaID != "host-1" {
		t.Errorf("ReplicaID = %q, want host-1", c.ReplicaID)
	}
	if c.AdvertiseAddr != "http://host-1:8080" {
		t.Errorf("AdvertiseAddr = %q, want http://host-1:8080", c.AdvertiseAddr)
	}
	if c.LeaderTTL != 10*time.Second || c.LeaseTTL != 30*time.Second || c.HeartbeatInterval != 10*time.Second {
		t.Errorf("unexpected durations: %+v", c)
	}
	if c.AgingSeconds != 30 {
		t.Errorf("AgingSeconds = %d, want 30", c.AgingSeconds)
	}
	if c.DecisionPolicy != "rule" || c.DecisionThreshold != 0.5 {
		t.Errorf("decision defaults = %q %v", c.DecisionPolicy, c.DecisionThreshold)
	}
}

func TestOrchestratorOverrides(t *testing.T) {
	c, err := LoadOrchestrator(env(map[string]string{
		"BATON_DATABASE_URL":       "postgres://db/baton",
		"BATON_HTTP_ADDR":          ":9000",
		"BATON_ADVERTISE_ADDR":     "http://baton-1:9000",
		"BATON_REPLICA_ID":         "r1",
		"BATON_LEASE_TTL":          "45s",
		"BATON_HEARTBEAT_INTERVAL": "15s",
		"BATON_DECISION_POLICY":    "laya",
		"BATON_DECISION_THRESHOLD": "0.7",
	}), "ignored")
	if err != nil {
		t.Fatalf("LoadOrchestrator: %v", err)
	}
	if c.HTTPAddr != ":9000" || c.ReplicaID != "r1" || c.AdvertiseAddr != "http://baton-1:9000" {
		t.Errorf("overrides not applied: %+v", c)
	}
	if c.LeaseTTL != 45*time.Second || c.HeartbeatInterval != 15*time.Second {
		t.Errorf("durations not applied: %+v", c)
	}
	if c.DecisionPolicy != "laya" || c.DecisionThreshold != 0.7 {
		t.Errorf("decision not applied: %+v", c)
	}
}

func TestOrchestratorValidation(t *testing.T) {
	base := map[string]string{"BATON_DATABASE_URL": "postgres://db/baton"}
	tests := []struct {
		name    string
		set     map[string]string
		wantErr string
	}{
		{"missing database url", map[string]string{"BATON_DATABASE_URL": ""}, "BATON_DATABASE_URL is required"},
		{"bad duration", map[string]string{"BATON_LEASE_TTL": "soon"}, "BATON_LEASE_TTL"},
		{"non-positive duration", map[string]string{"BATON_LEADER_TTL": "0s"}, "BATON_LEADER_TTL must be positive"},
		{"lease too short for heartbeat", map[string]string{"BATON_LEASE_TTL": "20s", "BATON_HEARTBEAT_INTERVAL": "10s"}, "at least 3x"},
		{"unknown policy", map[string]string{"BATON_DECISION_POLICY": "magic"}, "BATON_DECISION_POLICY"},
		{"threshold out of range", map[string]string{"BATON_DECISION_THRESHOLD": "1.5"}, "BATON_DECISION_THRESHOLD"},
		{"bad int", map[string]string{"BATON_AGING_SECONDS": "x"}, "BATON_AGING_SECONDS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := map[string]string{}
			for k, v := range base {
				m[k] = v
			}
			for k, v := range tt.set {
				m[k] = v
			}
			_, err := LoadOrchestrator(env(m), "h")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestOrchestratorReportsAllErrors(t *testing.T) {
	_, err := LoadOrchestrator(env(map[string]string{
		"BATON_LEASE_TTL":     "bad",
		"BATON_AGING_SECONDS": "bad",
	}), "h")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"BATON_DATABASE_URL", "BATON_LEASE_TTL", "BATON_AGING_SECONDS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestWorkerDefaultsAndQueues(t *testing.T) {
	c, err := LoadWorker(env(map[string]string{
		"BATON_WORKER_QUEUES": " default, payments ,,",
	}), "w-host")
	if err != nil {
		t.Fatalf("LoadWorker: %v", err)
	}
	if c.OrchestratorAddr != "http://localhost:8080" || c.WorkerID != "w-host" {
		t.Errorf("defaults: %+v", c)
	}
	if strings.Join(c.Queues, "|") != "default|payments" {
		t.Errorf("Queues = %q", c.Queues)
	}
	if c.Concurrency != 4 || c.DrainTimeout != 25*time.Second {
		t.Errorf("defaults: %+v", c)
	}
}

func TestWorkerValidation(t *testing.T) {
	_, err := LoadWorker(env(map[string]string{
		"BATON_WORKER_QUEUES":      ",",
		"BATON_WORKER_CONCURRENCY": "0",
	}), "w")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"BATON_WORKER_QUEUES", "BATON_WORKER_CONCURRENCY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

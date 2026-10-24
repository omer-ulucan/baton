package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type call struct {
	method, path, query string
	header              http.Header
	body                map[string]any
}

// fakeAPI answers canned responses by "METHOD /path" and records calls.
type fakeAPI struct {
	mu    sync.Mutex
	calls []call
	resp  map[string]struct {
		code int
		body string
	}
}

func newFake(t *testing.T) (*fakeAPI, string) {
	f := &fakeAPI{resp: map[string]struct {
		code int
		body string
	}{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func (f *fakeAPI) on(route string, code int, body string) {
	f.resp[route] = struct {
		code int
		body string
	}{code, body}
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	f.mu.Lock()
	f.calls = append(f.calls, call{r.Method, r.URL.Path, r.URL.RawQuery, r.Header, body})
	f.mu.Unlock()
	route := r.Method + " " + r.URL.Path
	resp, ok := f.resp[route]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.code)
	_, _ = w.Write([]byte(resp.body))
}

func (f *fakeAPI) last() call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

func runCLI(t *testing.T, server string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	env := map[string]string{"BATON_ADDR": server}
	code := run(t.Context(), args, &out, &errOut, func(k string) string { return env[k] })
	return code, out.String(), errOut.String()
}

const wfID = "01a16fb4-6d80-751a-902e-5fd250918152"

func TestUsage(t *testing.T) {
	for _, args := range [][]string{{}, {"frobnicate"}, {"status"}, {"submit"}, {"dlq"}, {"dlq", "nope"}, {"--output", "xml", "list"}} {
		code, _, stderr := runCLI(t, "http://unused", args...)
		if code != 2 || !strings.Contains(stderr, "usage: batonctl") {
			t.Errorf("%v: code=%d stderr=%q", args, code, stderr)
		}
	}
}

func TestHealth(t *testing.T) {
	f, url := newFake(t)
	f.on("GET /readyz", 200, `{"status":"ready"}`)
	if code, out, _ := runCLI(t, url, "health"); code != 0 || strings.TrimSpace(out) != "ready" {
		t.Fatalf("code=%d out=%q", code, out)
	}
	f.on("GET /readyz", 503, `{"status":"unavailable"}`)
	if code, _, stderr := runCLI(t, url, "health"); code != 1 || !strings.Contains(stderr, "not ready (503)") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestSubmit(t *testing.T) {
	f, url := newFake(t)
	f.on("POST /v1/workflows", 201, `{"workflow_id":"`+wfID+`","status":"running"}`)
	file := filepath.Join(t.TempDir(), "wf.yaml")
	if err := os.WriteFile(file, []byte("name: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, stderr := runCLI(t, url, "submit", "-f", file, "--input", `{"a":1}`, "--priority", "70", "--idempotency-key", "k1")
	if code != 0 || !strings.Contains(out, wfID) {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, stderr)
	}
	c := f.last()
	if c.body["definition"] != "name: x\n" || c.body["priority"] != float64(70) || c.header.Get("Idempotency-Key") != "k1" {
		t.Fatalf("request = %+v", c)
	}
	if code, _, _ := runCLI(t, url, "submit", "-f", file, "--input", "{nope"); code != 2 {
		t.Fatal("invalid --input accepted")
	}
}

func TestSubmitShowsValidationProblems(t *testing.T) {
	f, url := newFake(t)
	f.on("POST /v1/workflows", 422, `{"error":{"code":"invalid_definition","message":"invalid workflow submission","details":{"problems":["cycle: a -> a","version must be a positive integer"]}}}`)
	file := filepath.Join(t.TempDir(), "wf.yaml")
	_ = os.WriteFile(file, []byte("x"), 0o600)
	code, _, stderr := runCLI(t, url, "submit", "-f", file)
	if code != 1 || !strings.Contains(stderr, "- cycle: a -> a") || !strings.Contains(stderr, "- version must be") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestStatusTableAndJSON(t *testing.T) {
	f, url := newFake(t)
	f.on("GET /v1/workflows/"+wfID, 200, `{"id":"`+wfID+`","status":"running","priority":50,"created_at":"2026-01-01T00:00:00Z",
		"tasks":[{"step":"charge","kind":"step","status":"retry_wait","attempt":2,"max_attempts":5,"queue":"payments","last_error":"connection reset"}]}`)
	f.on("GET /v1/workflows/"+wfID+"/events", 200, `{"events":[{"seq":1,"type":"WorkflowSubmitted","occurred_at":"2026-01-01T00:00:00Z","payload":{}}]}`)
	code, out, _ := runCLI(t, url, "status", wfID, "--events")
	for _, want := range []string{"retry_wait", "2/5", "connection reset", "WorkflowSubmitted"} {
		if code != 0 || !strings.Contains(out, want) {
			t.Errorf("table output lacks %q:\n%s", want, out)
		}
	}
	code, out, _ = runCLI(t, url, "--output", "json", "status", wfID)
	var got map[string]any
	if code != 0 || json.Unmarshal([]byte(out), &got) != nil || got["status"] != "running" {
		t.Fatalf("json output = %q", out)
	}
}

func TestLifecycleCommands(t *testing.T) {
	f, url := newFake(t)
	for _, op := range []string{"cancel", "pause", "resume"} {
		f.on("POST /v1/workflows/"+wfID+"/"+op, 200, `{"status":"done"}`)
	}
	if code, _, _ := runCLI(t, url, "cancel", wfID, "--reason", "oops"); code != 0 || f.last().body["reason"] != "oops" {
		t.Fatalf("cancel request = %+v", f.last())
	}
	for _, op := range []string{"pause", "resume"} {
		if code, _, _ := runCLI(t, url, op, wfID); code != 0 || f.last().path != "/v1/workflows/"+wfID+"/"+op {
			t.Fatalf("%s request = %+v", op, f.last())
		}
	}
	f.on("POST /v1/workflows/"+wfID+"/pause", 409, `{"error":{"code":"invalid_transition","message":"workflow is cancelled"}}`)
	if code, _, stderr := runCLI(t, url, "pause", wfID); code != 1 || !strings.Contains(stderr, "invalid_transition") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestDLQCommands(t *testing.T) {
	f, url := newFake(t)
	f.on("GET /v1/dlq", 200, `{"entries":[{"task":{"id":"`+wfID+`","step":"charge","queue":"payments","attempt":3,"last_error":"card declined"},
		"workflow_id":"`+wfID+`","last_decision":{"label":"dead_letter","policy":"rule","confidence":1}}]}`)
	code, out, _ := runCLI(t, url, "dlq", "list", "--queue", "payments")
	if code != 0 || !strings.Contains(out, "card declined") || !strings.Contains(out, "dead_letter (rule 1.00)") || f.last().query != "limit=50&queue=payments" {
		t.Fatalf("code=%d out=%q query=%q", code, out, f.last().query)
	}
	f.on("POST /v1/dlq/"+wfID+"/retry", 200, `{"status":"requeued"}`)
	if code, out, _ := runCLI(t, url, "dlq", "retry", wfID); code != 0 || strings.TrimSpace(out) != "requeued" {
		t.Fatalf("code=%d out=%q", code, out)
	}
}

func TestQueuesCommands(t *testing.T) {
	f, url := newFake(t)
	f.on("GET /v1/queues", 200, `{"queues":[{"queue":"payments","ready":3,"running":2,"config":{"max_concurrency":2,"rate_per_sec":5,"burst":2,"admission_mode":"defer"}}]}`)
	code, out, _ := runCLI(t, url, "queues")
	if code != 0 || !strings.Contains(out, "payments") || !strings.Contains(out, "5/s (burst 2)") || !strings.Contains(out, "defer") {
		t.Fatalf("code=%d out=%q", code, out)
	}
	f.on("PUT /v1/queues/payments", 200, `{"status":"updated"}`)
	code, _, _ = runCLI(t, url, "queues", "set", "payments", "--max-concurrency", "3", "--admission", "reject")
	if c := f.last(); code != 0 || c.body["max_concurrency"] != float64(3) || c.body["admission_mode"] != "reject" || c.body["max_depth"] != nil {
		t.Fatalf("code=%d request=%+v", code, c)
	}
}

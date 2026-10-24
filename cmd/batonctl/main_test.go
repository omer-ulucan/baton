package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func runCLI(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(t.Context(), args, &out, &errOut, func(k string) string { return env[k] })
	return code, out.String(), errOut.String()
}

func readyServer(t *testing.T, code int, status string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"status":"` + status + `"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestNoArgsPrintsUsage(t *testing.T) {
	code, _, stderr := runCLI(t, nil)
	if code != 2 || !strings.Contains(stderr, "usage: batonctl") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestUnknownCommand(t *testing.T) {
	code, _, stderr := runCLI(t, nil, "frobnicate")
	if code != 2 || !strings.Contains(stderr, `unknown command "frobnicate"`) {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestHealthReady(t *testing.T) {
	srv := readyServer(t, http.StatusOK, "ready")
	code, stdout, stderr := runCLI(t, nil, "--server", srv.URL+"/", "health")
	if code != 0 || strings.TrimSpace(stdout) != "ready" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestHealthUsesBatonAddr(t *testing.T) {
	srv := readyServer(t, http.StatusOK, "ready")
	code, _, stderr := runCLI(t, map[string]string{"BATON_ADDR": srv.URL}, "health")
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

func TestHealthUnavailable(t *testing.T) {
	srv := readyServer(t, http.StatusServiceUnavailable, "unavailable")
	code, _, stderr := runCLI(t, nil, "--server", srv.URL, "health")
	if code != 1 || !strings.Contains(stderr, "not ready: unavailable") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

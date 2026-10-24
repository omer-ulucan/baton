package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type pingFunc func(context.Context) error

func (f pingFunc) Ping(ctx context.Context) error { return f(ctx) }

func newTestServer(t *testing.T, db Pinger) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(Deps{DB: db}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.StatusCode, body
}

func TestHealthzIgnoresDatabase(t *testing.T) {
	srv := newTestServer(t, pingFunc(func(context.Context) error { return errors.New("down") }))
	code, body := get(t, srv.URL+"/healthz")
	if code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz = %d %v", code, body)
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name     string
		pingErr  error
		wantCode int
		wantStat string
	}{
		{"database reachable", nil, http.StatusOK, "ready"},
		{"database unreachable", errors.New("connection refused"), http.StatusServiceUnavailable, "unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, pingFunc(func(context.Context) error { return tt.pingErr }))
			code, body := get(t, srv.URL+"/readyz")
			if code != tt.wantCode || body["status"] != tt.wantStat {
				t.Fatalf("readyz = %d %v", code, body)
			}
		})
	}
}

func TestReadyzDoesNotLeakErrorDetails(t *testing.T) {
	srv := newTestServer(t, pingFunc(func(context.Context) error {
		return errors.New("password authentication failed for user baton")
	}))
	_, body := get(t, srv.URL+"/readyz")
	if _, ok := body["error"]; ok {
		t.Fatalf("readyz exposed error: %v", body)
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	srv := newTestServer(t, pingFunc(func(context.Context) error { return nil }))
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/nope", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

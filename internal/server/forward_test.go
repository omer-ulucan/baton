package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/omer-ulucan/baton/internal/protocol"
)

type fakeLead struct {
	leader bool
	addr   string
}

func (f fakeLead) IsLeader() bool { return f.leader }
func (f fakeLead) LeaderAddr() (string, bool) {
	if f.leader {
		return "self", true
	}
	return f.addr, f.addr != ""
}

func TestFollowerForwardsWritesToLeader(t *testing.T) {
	var forwardedBy atomic.Value
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwardedBy.Store(r.Header.Get(forwardedHeader))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"from":"leader"}`))
	}))
	defer leaderSrv.Close()

	local := &fakeWP{}
	h := New(Deps{
		DB: pingFunc(func(context.Context) error { return nil }), Worker: local,
		Leadership: fakeLead{addr: leaderSrv.URL}, ReplicaID: "follower-1",
	})
	code, body := post(t, h, protocol.PathComplete, `{"worker_id":"w"}`)
	if code != 200 || body["from"] != "leader" || forwardedBy.Load() != "follower-1" {
		t.Fatalf("forwarded = %d %v by %v", code, body, forwardedBy.Load())
	}
	if local.got != nil {
		t.Fatal("follower executed the write locally")
	}

	// Reads stay local.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
	if !strings.Contains(rec.Body.String(), `"role":"follower"`) {
		t.Fatalf("readyz = %s", rec.Body.String())
	}
}

func TestNoLeaderOrDoubleForwardIs503(t *testing.T) {
	h := New(Deps{DB: pingFunc(func(context.Context) error { return nil }), Worker: &fakeWP{}, Leadership: fakeLead{}})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, protocol.PathClaim, strings.NewReader(`{}`)))
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("no leader = %d %v", rec.Code, rec.Header())
	}

	h = New(Deps{DB: pingFunc(func(context.Context) error { return nil }), Worker: &fakeWP{}, Leadership: fakeLead{addr: "http://127.0.0.1:1"}})
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, protocol.PathClaim, strings.NewReader(`{}`))
	req.Header.Set(forwardedHeader, "other")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Fatalf("double forward = %d", rec.Code)
	}

	// Leader unreachable: 503, not a hang or a 502.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, protocol.PathClaim, strings.NewReader(`{}`)))
	if rec.Code != 503 {
		t.Fatalf("unreachable leader = %d", rec.Code)
	}
}

func TestLeaderRunsWritesLocally(t *testing.T) {
	local := &fakeWP{}
	h := New(Deps{DB: pingFunc(func(context.Context) error { return nil }), Worker: local, Leadership: fakeLead{leader: true}})
	if code, _ := post(t, h, protocol.PathRegister, `{"worker_id":"w","queues":["q"],"handlers":["h"]}`); code != 200 || local.got == nil {
		t.Fatalf("leader write = %d, executed %v", code, local.got != nil)
	}
}

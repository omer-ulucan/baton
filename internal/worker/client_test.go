package worker

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/omer-ulucan/baton/internal/protocol"
)

func replica(t *testing.T, status int, hits *atomic.Int64) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write([]byte(`{"heartbeat_interval_ms":1000,"lease_ttl_ms":3000}`))
			return
		}
		_, _ = w.Write([]byte(`{"error":{"code":"x","message":"y"}}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestClientFailsOverOn503AndConnectionErrors(t *testing.T) {
	var noLeader, ok atomic.Int64
	// A closed server refuses connections.
	srv := httptest.NewServer(http.NotFoundHandler())
	refused := srv.URL
	srv.Close()

	c := NewClient([]string{refused, replica(t, http.StatusServiceUnavailable, &noLeader), replica(t, http.StatusOK, &ok)}, nil)
	resp, err := c.Register(t.Context(), protocol.RegisterRequest{WorkerID: "w"})
	if err != nil || resp.LeaseTTLMS != 3000 {
		t.Fatalf("register = %+v %v", resp, err)
	}
	if noLeader.Load() != 1 || ok.Load() != 1 {
		t.Fatalf("hits: 503 replica %d, healthy %d", noLeader.Load(), ok.Load())
	}
	// The client stays on the replica that worked.
	if _, err := c.Register(t.Context(), protocol.RegisterRequest{WorkerID: "w"}); err != nil || ok.Load() != 2 || noLeader.Load() != 1 {
		t.Fatalf("second call did not stick to the healthy replica: %v", err)
	}
}

func TestClientDoesNotFailOverOnClientErrors(t *testing.T) {
	var conflict, other atomic.Int64
	c := NewClient([]string{replica(t, http.StatusConflict, &conflict), replica(t, http.StatusOK, &other)}, nil)
	err := c.Complete(t.Context(), protocol.CompleteRequest{WorkerID: "w"})
	if !IsCode(err, "x") || conflict.Load() != 1 || other.Load() != 0 {
		t.Fatalf("err = %v, other replica hit %d times", err, other.Load())
	}
}

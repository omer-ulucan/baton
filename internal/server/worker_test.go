package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/protocol"
	"github.com/omer-ulucan/baton/internal/state"
)

type fakeWP struct {
	err  error
	next engine.Next
	got  any
}

func (f *fakeWP) Register(_ context.Context, w engine.Worker) error { f.got = w; return f.err }
func (f *fakeWP) Claim(_ context.Context, r engine.ClaimRequest) ([]engine.Assignment, error) {
	f.got = r
	return []engine.Assignment{{TaskID: uuid.New(), Handler: "h"}}, f.err
}
func (f *fakeWP) Heartbeat(context.Context, string, []uuid.UUID) (engine.HeartbeatResult, error) {
	return engine.HeartbeatResult{Revoked: map[uuid.UUID]string{uuid.New(): engine.RevokedCancelled}}, f.err
}
func (f *fakeWP) Complete(context.Context, engine.Result, json.RawMessage) error { return f.err }
func (f *fakeWP) Fail(_ context.Context, _ engine.Result, fl engine.Failure) (engine.Next, error) {
	f.got = fl
	return f.next, f.err
}
func (f *fakeWP) Release(context.Context, string, []uuid.UUID) error { return f.err }
func (f *fakeWP) Config() engine.Config                              { return engine.Config{} }

func post(t *testing.T, h http.Handler, path, body string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body)))
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func errCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	s, _ := e["code"].(string)
	return s
}

func TestWorkerErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{engine.ErrInvalid, 422, protocol.CodeInvalidRequest},
		{engine.ErrUnknownWorker, 404, protocol.CodeUnknownWorker},
		{engine.ErrNotFound, 404, protocol.CodeNotFound},
		{fmt.Errorf("wrapped: %w", engine.ErrAttemptSuperseded), 409, protocol.CodeAttemptSuperseded},
		{engine.ErrCancelled, 409, protocol.CodeCancelled},
		{engine.ErrLeaseExpired, 409, protocol.CodeLeaseExpired},
		{state.ErrInvalidTransition, 409, protocol.CodeInvalidTransition},
		{fmt.Errorf("db exploded: password=hunter2"), 500, protocol.CodeInternal},
	}
	for _, tt := range tests {
		h := New(Deps{DB: pingFunc(func(context.Context) error { return nil }), Worker: &fakeWP{err: tt.err}})
		code, body := post(t, h, protocol.PathComplete, `{"worker_id":"w"}`)
		if code != tt.status || errCode(body) != tt.code {
			t.Errorf("%v: got %d %v", tt.err, code, body)
		}
		if tt.status == 500 && strings.Contains(fmt.Sprint(body), "hunter2") {
			t.Error("internal error details leaked")
		}
	}
}

func TestWorkerRejectsBadBodies(t *testing.T) {
	h := New(Deps{DB: pingFunc(func(context.Context) error { return nil }), Worker: &fakeWP{}})
	if code, _ := post(t, h, protocol.PathClaim, `{"worker_id":`); code != 400 {
		t.Errorf("malformed JSON: %d", code)
	}
	if code, _ := post(t, h, protocol.PathClaim, `{"worker_id":"w","typo":1}`); code != 400 {
		t.Errorf("unknown field: %d", code)
	}
	if code, _ := post(t, h, protocol.PathClaim, `{"worker_id":"w","max":99}`); code != 422 {
		t.Errorf("max out of range: %d", code)
	}
	big := `{"worker_id":"` + strings.Repeat("x", maxBody) + `"}`
	if code, _ := post(t, h, protocol.PathRegister, big); code != 413 {
		t.Errorf("oversized body: %d", code)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, protocol.PathClaim, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET claim: %d", rec.Code)
	}
}

func TestWorkerFailRouteAndPayloads(t *testing.T) {
	wp := &fakeWP{next: engine.NextDeadLetter}
	h := New(Deps{DB: pingFunc(func(context.Context) error { return nil }), Worker: wp})
	var b bytes.Buffer
	_ = json.NewEncoder(&b).Encode(protocol.FailRequest{WorkerID: "w", TaskID: uuid.New(), AttemptID: uuid.New(),
		Error: protocol.TaskError{Message: "boom", Type: "E"}})
	code, body := post(t, h, protocol.PathFail, b.String())
	if code != 200 || body["next"] != "dead_letter" {
		t.Fatalf("fail = %d %v", code, body)
	}
	if f := wp.got.(engine.Failure); f.Message != "boom" || f.Type != "E" {
		t.Fatalf("failure passed as %+v", f)
	}

	code, body = post(t, h, protocol.PathHeartbeat, `{"worker_id":"w","attempts":[]}`)
	revoked, _ := body["revoked"].([]any)
	if code != 200 || len(revoked) != 1 {
		t.Fatalf("heartbeat = %d %v", code, body)
	}
	code, body = post(t, h, protocol.PathClaim, `{"worker_id":"w","max":2,"wait_ms":100}`)
	tasks, _ := body["tasks"].([]any)
	if code != 200 || len(tasks) != 1 {
		t.Fatalf("claim = %d %v", code, body)
	}
}

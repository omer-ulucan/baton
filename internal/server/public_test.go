package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/api"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/server"
	"github.com/omer-ulucan/baton/internal/store"
)

var pg *pgtest.Server

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		ctx := context.Background()
		var err error
		if pg, err = pgtest.Start(ctx, store.Migrate); err != nil {
			fmt.Fprintln(os.Stderr, "pgtest:", err)
			os.Exit(1)
		}
		code := m.Run()
		_ = pg.Close(ctx)
		os.Exit(code)
	}
	os.Exit(m.Run())
}

type client struct {
	t   *testing.T
	url string
	eng *engine.Engine
}

func newAPI(t *testing.T) *client {
	t.Helper()
	pool, _ := pg.NewDB(t)
	st := store.New(pool)
	eng := engine.New(engine.Options{Store: st})
	srv := httptest.NewServer(server.New(server.Deps{DB: pool, Worker: eng, Commands: eng, Reader: st}))
	t.Cleanup(srv.Close)
	return &client{t: t, url: srv.URL, eng: eng}
}

func (c *client) do(method, path string, body any, hdr map[string]string, out any) int {
	c.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(c.t.Context(), method, c.url+path, rd)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			c.t.Fatalf("%s %s: decode: %v", method, path, err)
		}
	}
	return resp.StatusCode
}

const twoSteps = `
name: api-test
version: 1
steps:
  - {name: a, handler: h}
  - {name: b, handler: h, depends_on: [a]}
`

func TestSubmitGetAndList(t *testing.T) {
	c := newAPI(t)
	var sub api.SubmitResponse
	if code := c.do("POST", "/v1/workflows", api.SubmitRequest{Definition: twoSteps, Input: json.RawMessage(`{"x":1}`)},
		map[string]string{"Idempotency-Key": "k1"}, &sub); code != 201 || sub.Status != "running" {
		t.Fatalf("submit = %d %+v", code, sub)
	}
	var again api.SubmitResponse
	if code := c.do("POST", "/v1/workflows", api.SubmitRequest{Definition: twoSteps},
		map[string]string{"Idempotency-Key": "k1"}, &again); code != 200 || again.WorkflowID != sub.WorkflowID || !again.Existing {
		t.Fatalf("resubmit = %d %+v", code, again)
	}

	var wf api.Workflow
	if code := c.do("GET", "/v1/workflows/"+sub.WorkflowID.String(), nil, nil, &wf); code != 200 {
		t.Fatalf("get = %d", code)
	}
	if len(wf.Tasks) != 2 || wf.Tasks[0].Step != "a" || wf.Tasks[0].Status != "ready" || wf.Tasks[1].Status != "pending" {
		t.Fatalf("workflow = %+v", wf)
	}

	for range 3 {
		c.do("POST", "/v1/workflows", api.SubmitRequest{Definition: twoSteps}, nil, nil)
	}
	var page1, page2 api.WorkflowList
	c.do("GET", "/v1/workflows?limit=3", nil, nil, &page1)
	c.do("GET", "/v1/workflows?limit=3&cursor="+page1.NextCursor, nil, nil, &page2)
	if len(page1.Workflows) != 3 || page1.NextCursor == "" || len(page2.Workflows) != 1 || page2.NextCursor != "" {
		t.Fatalf("pages: %d (next %q), %d (next %q)", len(page1.Workflows), page1.NextCursor, len(page2.Workflows), page2.NextCursor)
	}
	var filtered api.WorkflowList
	c.do("GET", "/v1/workflows?status=succeeded", nil, nil, &filtered)
	if len(filtered.Workflows) != 0 {
		t.Fatal("status filter ignored")
	}

	var events api.EventList
	c.do("GET", "/v1/workflows/"+sub.WorkflowID.String()+"/events?after_seq=1", nil, nil, &events)
	if len(events.Events) != 3 || events.Events[0].Type != "TaskCreated" || events.Events[0].Seq != 2 {
		t.Fatalf("events = %+v", events)
	}
}

func TestSubmitValidationErrorListsProblems(t *testing.T) {
	c := newAPI(t)
	var eb api.ErrorBody
	code := c.do("POST", "/v1/workflows", api.SubmitRequest{Definition: "name: Bad\nversion: 0\nsteps: []"}, nil, &eb)
	problems, _ := eb.Error.Details["problems"].([]any)
	if code != 422 || eb.Error.Code != api.CodeInvalidDefinition || len(problems) != 3 {
		t.Fatalf("invalid submit = %d %+v", code, eb)
	}
}

func TestLifecycleEndpoints(t *testing.T) {
	c := newAPI(t)
	var sub api.SubmitResponse
	c.do("POST", "/v1/workflows", api.SubmitRequest{Definition: twoSteps}, nil, &sub)
	id := sub.WorkflowID.String()

	var st api.StatusResponse
	if code := c.do("POST", "/v1/workflows/"+id+"/pause", nil, nil, &st); code != 200 || st.Status != "paused" {
		t.Fatalf("pause = %d %+v", code, st)
	}
	var eb api.ErrorBody
	if code := c.do("POST", "/v1/workflows/"+id+"/pause", nil, nil, &eb); code != 409 || eb.Error.Code != api.CodeInvalidTransition {
		t.Fatalf("double pause = %d %+v", code, eb)
	}
	if code := c.do("POST", "/v1/workflows/"+id+"/resume", nil, nil, &st); code != 200 || st.Status != "running" {
		t.Fatalf("resume = %d %+v", code, st)
	}
	if code := c.do("POST", "/v1/workflows/"+id+"/cancel", api.CancelRequest{Reason: "test"}, nil, &st); code != 200 || st.Status != "cancelled" {
		t.Fatalf("cancel = %d %+v", code, st)
	}
	if code := c.do("POST", "/v1/workflows/"+uuid.NewString()+"/cancel", nil, nil, &eb); code != 404 {
		t.Fatalf("cancel unknown = %d", code)
	}
	if code := c.do("GET", "/v1/workflows/not-a-uuid", nil, nil, &eb); code != 400 {
		t.Fatalf("bad id = %d", code)
	}
	if code := c.do("GET", "/v1/workflows/"+uuid.NewString(), nil, nil, &eb); code != 404 {
		t.Fatalf("get unknown = %d", code)
	}
}

func TestDLQEndpoints(t *testing.T) {
	c := newAPI(t)
	var sub api.SubmitResponse
	c.do("POST", "/v1/workflows", api.SubmitRequest{Definition: twoSteps}, nil, &sub)
	ctx := t.Context()
	if err := c.eng.Register(ctx, engine.Worker{ID: "w", Queues: []string{"default"}, Handlers: []string{"h"}}); err != nil {
		t.Fatal(err)
	}
	got, err := c.eng.Claim(ctx, engine.ClaimRequest{WorkerID: "w"})
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	no := false
	if _, err := c.eng.Fail(ctx, engine.Result{WorkerID: "w", TaskID: got[0].TaskID, AttemptID: got[0].AttemptID},
		engine.Failure{Message: "declined", Retryable: &no}); err != nil {
		t.Fatal(err)
	}

	var dlq api.DLQList
	if code := c.do("GET", "/v1/dlq?queue=default", nil, nil, &dlq); code != 200 || len(dlq.Entries) != 1 {
		t.Fatalf("dlq = %d %+v", code, dlq)
	}
	e := dlq.Entries[0]
	if e.LastDecision == nil || e.LastDecision.Label != "dead_letter" || *e.Task.LastError != "declined" {
		t.Fatalf("entry = %+v", e)
	}
	var st api.StatusResponse
	if code := c.do("POST", "/v1/dlq/"+e.Task.ID.String()+"/retry", nil, nil, &st); code != 200 {
		t.Fatalf("retry = %d", code)
	}
	var eb api.ErrorBody
	if code := c.do("POST", "/v1/dlq/"+e.Task.ID.String()+"/retry", nil, nil, &eb); code != 409 {
		t.Fatalf("second retry = %d", code)
	}
	var wf api.Workflow
	c.do("GET", "/v1/workflows/"+sub.WorkflowID.String(), nil, nil, &wf)
	if wf.Status != "running" || wf.Tasks[0].Status != "ready" {
		t.Fatalf("after retry: %+v", wf)
	}
}

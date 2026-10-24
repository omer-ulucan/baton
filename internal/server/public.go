package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/api"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/workflow"
)

// Commands are the public API's write operations.
type Commands interface {
	Submit(ctx context.Context, sub engine.Submission) (engine.Submitted, error)
	Cancel(ctx context.Context, wfID uuid.UUID, compensate bool, reason string) error
	Pause(ctx context.Context, wfID uuid.UUID) error
	Resume(ctx context.Context, wfID uuid.UUID) error
	RetryDLQ(ctx context.Context, taskID uuid.UUID, by string) error
	SetQueue(ctx context.Context, c store.QueueConfig) error
}

// Reader is the public API's read side; any replica can serve it.
type Reader interface {
	Snapshot(ctx context.Context, wfID uuid.UUID) (*state.Snapshot, error)
	Events(ctx context.Context, wfID uuid.UUID) ([]state.Event, error)
	ListWorkflows(ctx context.Context, status string, after uuid.UUID, limit int) ([]state.Workflow, error)
	ListDLQ(ctx context.Context, queue string, after uuid.UUID, limit int) ([]store.DLQEntry, error)
	QueueStats(ctx context.Context) ([]store.QueueStat, error)
}

const maxDefinition = 1 << 20

func registerPublicRoutes(mux *http.ServeMux, cmd Commands, rd Reader, fw *forwarder, log *slog.Logger) {
	h := publicHandlers{cmd: cmd, rd: rd, log: log}
	if cmd != nil {
		mux.Handle("POST /v1/workflows", fw.write(h.submit))
		mux.Handle("POST /v1/workflows/{id}/cancel", fw.write(h.cancel))
		mux.Handle("POST /v1/workflows/{id}/pause", fw.write(h.simple(cmd.Pause)))
		mux.Handle("POST /v1/workflows/{id}/resume", fw.write(h.simple(cmd.Resume)))
		mux.Handle("POST /v1/dlq/{task_id}/retry", fw.write(h.retryDLQ))
		mux.Handle("PUT /v1/queues/{queue}", fw.write(h.setQueue))
	}
	if rd != nil {
		mux.HandleFunc("GET /v1/workflows", h.list)
		mux.HandleFunc("GET /v1/workflows/{id}", h.get)
		mux.HandleFunc("GET /v1/workflows/{id}/events", h.events)
		mux.HandleFunc("GET /v1/dlq", h.dlq)
		mux.HandleFunc("GET /v1/queues", h.queues)
	}
}

type publicHandlers struct {
	cmd Commands
	rd  Reader
	log *slog.Logger
}

func (h publicHandlers) submit(w http.ResponseWriter, r *http.Request) {
	var req api.SubmitRequest
	if !decodeLimit(w, r, &req, maxDefinition+workflow.MaxInputLen) {
		return
	}
	out, err := h.cmd.Submit(r.Context(), engine.Submission{
		Definition: []byte(req.Definition), Input: req.Input, Priority: req.Priority,
		Key: r.Header.Get("Idempotency-Key"), CallbackURL: req.CallbackURL,
	})
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	status := http.StatusCreated
	switch {
	case out.Existing:
		status = http.StatusOK
	case out.Status == state.WfDeferred:
		status = http.StatusAccepted
	}
	writeJSON(w, status, api.SubmitResponse{WorkflowID: out.WorkflowID, Status: string(out.Status), Existing: out.Existing})
}

func (h publicHandlers) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var req api.CancelRequest
	if r.ContentLength != 0 && !decode(w, r, &req) {
		return
	}
	if err := h.cmd.Cancel(r.Context(), id, req.Compensate, req.Reason); err != nil {
		h.respondErr(w, r, err)
		return
	}
	h.writeStatus(w, r, id)
}

func (h publicHandlers) simple(op func(context.Context, uuid.UUID) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathUUID(w, r, "id")
		if !ok {
			return
		}
		if err := op(r.Context(), id); err != nil {
			h.respondErr(w, r, err)
			return
		}
		h.writeStatus(w, r, id)
	}
}

func (h publicHandlers) retryDLQ(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "task_id")
	if !ok {
		return
	}
	if err := h.cmd.RetryDLQ(r.Context(), id, r.Header.Get("Baton-Actor")); err != nil {
		h.respondErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, api.StatusResponse{Status: "requeued"})
}

func (h publicHandlers) writeStatus(w http.ResponseWriter, r *http.Request, id uuid.UUID) {
	snap, err := h.rd.Snapshot(r.Context(), id)
	if err != nil || snap.Workflow == nil {
		writeJSON(w, http.StatusOK, api.StatusResponse{})
		return
	}
	writeJSON(w, http.StatusOK, api.StatusResponse{Status: string(snap.Workflow.Status)})
}

func (h publicHandlers) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	snap, err := h.rd.Snapshot(r.Context(), id)
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	if snap.Workflow == nil {
		writePublicError(w, http.StatusNotFound, api.CodeNotFound, "workflow not found", nil)
		return
	}
	view := workflowView(snap.Workflow)
	for _, t := range sortedTasks(snap) {
		view.Tasks = append(view.Tasks, taskView(t))
	}
	writeJSON(w, http.StatusOK, view)
}

func (h publicHandlers) list(w http.ResponseWriter, r *http.Request) {
	after, limit, ok := page(w, r)
	if !ok {
		return
	}
	wfs, err := h.rd.ListWorkflows(r.Context(), r.URL.Query().Get("status"), after, limit+1)
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	resp := api.WorkflowList{Workflows: []api.Workflow{}}
	for i := range wfs {
		if i == limit {
			resp.NextCursor = wfs[i-1].ID.String()
			break
		}
		resp.Workflows = append(resp.Workflows, workflowView(&wfs[i]))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h publicHandlers) events(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	after := 0
	if v := r.URL.Query().Get("after_seq"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writePublicError(w, http.StatusUnprocessableEntity, api.CodeInvalidRequest, "after_seq must be a non-negative integer", nil)
			return
		}
		after = n
	}
	events, err := h.rd.Events(r.Context(), id)
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	if len(events) == 0 {
		writePublicError(w, http.StatusNotFound, api.CodeNotFound, "workflow not found", nil)
		return
	}
	resp := api.EventList{Events: []api.Event{}}
	for _, ev := range events {
		if ev.Seq <= after {
			continue
		}
		payload, err := state.EncodePayload(ev.Payload)
		if err != nil {
			h.respondErr(w, r, err)
			return
		}
		out := api.Event{Seq: ev.Seq, Type: string(ev.Type()), OccurredAt: ev.OccurredAt, Payload: payload}
		if ev.TaskID != uuid.Nil {
			out.TaskID = &ev.TaskID
		}
		resp.Events = append(resp.Events, out)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h publicHandlers) dlq(w http.ResponseWriter, r *http.Request) {
	after, limit, ok := page(w, r)
	if !ok {
		return
	}
	entries, err := h.rd.ListDLQ(r.Context(), r.URL.Query().Get("queue"), after, limit+1)
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	resp := api.DLQList{Entries: []api.DLQEntry{}}
	for i, e := range entries {
		if i == limit {
			resp.NextCursor = entries[i-1].Task.ID.String()
			break
		}
		out := api.DLQEntry{Task: taskView(&e.Task), WorkflowID: e.Task.WorkflowID, DeadLettered: e.DeadLettered}
		if d := e.Decision; d != nil {
			out.LastDecision = &api.Decision{Label: d.Label, Confidence: d.Confidence, Policy: d.Policy, Fallback: d.Fallback}
		}
		resp.Entries = append(resp.Entries, out)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h publicHandlers) queues(w http.ResponseWriter, r *http.Request) {
	stats, err := h.rd.QueueStats(r.Context())
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	resp := api.QueueList{Queues: []api.Queue{}}
	for _, st := range stats {
		c := st.Config
		resp.Queues = append(resp.Queues, api.Queue{
			Queue: st.Queue, Pending: st.Pending, Ready: st.Ready, Running: st.Running, RetryWait: st.RetryWait,
			DeadLettered: st.DeadLettered, Config: api.QueueConfig{MaxConcurrency: c.MaxConcurrency,
				RatePerSec: c.RatePerSec, Burst: c.Burst, MaxDepth: c.MaxDepth, AdmissionMode: c.AdmissionMode},
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h publicHandlers) setQueue(w http.ResponseWriter, r *http.Request) {
	var req api.QueueConfig
	if !decode(w, r, &req) {
		return
	}
	c := store.QueueConfig{Queue: r.PathValue("queue"), MaxConcurrency: req.MaxConcurrency, RatePerSec: req.RatePerSec,
		Burst: req.Burst, MaxDepth: req.MaxDepth, AdmissionMode: req.AdmissionMode}
	if err := h.cmd.SetQueue(r.Context(), c); err != nil {
		if errors.Is(err, engine.ErrInvalid) {
			writePublicError(w, http.StatusUnprocessableEntity, api.CodeInvalidRequest, err.Error(), nil)
			return
		}
		h.respondErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, api.StatusResponse{Status: "updated"})
}

func (h publicHandlers) respondErr(w http.ResponseWriter, r *http.Request, err error) {
	var ve *workflow.ValidationError
	var full *engine.ErrQueueFull
	switch {
	case errors.As(err, &full):
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(full.RetryAfter.Seconds()))))
		writePublicError(w, http.StatusTooManyRequests, api.CodeQueueFull, err.Error(),
			map[string]any{"queue": full.Queue, "depth": full.Depth, "max_depth": full.MaxDepth})
	case errors.Is(err, store.ErrFenced):
		notLeader(w)
	case errors.As(err, &ve):
		writePublicError(w, http.StatusUnprocessableEntity, api.CodeInvalidDefinition, "invalid workflow submission",
			map[string]any{"problems": ve.Problems})
	case errors.Is(err, engine.ErrWorkflowNotFound), errors.Is(err, engine.ErrNotFound):
		writePublicError(w, http.StatusNotFound, api.CodeNotFound, err.Error(), nil)
	case errors.Is(err, engine.ErrInvalid):
		writePublicError(w, http.StatusConflict, api.CodeInvalidTransition, err.Error(), nil)
	case errors.Is(err, state.ErrInvalidTransition):
		writePublicError(w, http.StatusConflict, api.CodeInvalidTransition, err.Error(), nil)
	default:
		h.log.ErrorContext(r.Context(), "public api", "path", r.URL.Path, "err", err)
		writePublicError(w, http.StatusInternalServerError, api.CodeInternal, "internal error", nil)
	}
}

func writePublicError(w http.ResponseWriter, status int, code, msg string, details map[string]any) {
	writeJSON(w, status, api.ErrorBody{Error: api.ErrorDetail{Code: code, Message: msg, Details: details}})
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writePublicError(w, http.StatusBadRequest, api.CodeInvalidRequest, name+" is not a UUID", nil)
		return uuid.Nil, false
	}
	return id, true
}

func page(w http.ResponseWriter, r *http.Request) (uuid.UUID, int, bool) {
	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writePublicError(w, http.StatusUnprocessableEntity, api.CodeInvalidRequest, "limit must be in [1,500]", nil)
			return uuid.Nil, 0, false
		}
		limit = n
	}
	var after uuid.UUID
	if v := q.Get("cursor"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			writePublicError(w, http.StatusUnprocessableEntity, api.CodeInvalidRequest, "bad cursor", nil)
			return uuid.Nil, 0, false
		}
		after = id
	}
	return after, limit, true
}

func workflowView(w *state.Workflow) api.Workflow {
	return api.Workflow{
		ID: w.ID, DefinitionID: w.DefinitionID, Status: string(w.Status), Priority: w.Priority,
		CancelRequested: w.CancelRequested, LastSeq: w.LastSeq, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt,
		FinishedAt: w.FinishedAt,
	}
}

func taskView(t *state.Task) api.Task {
	return api.Task{
		ID: t.ID, Step: t.StepName, Kind: string(t.Kind), Handler: t.Handler, Queue: t.Queue, Priority: t.Priority,
		Status: string(t.Status), Attempt: t.Attempt, MaxAttempts: t.MaxAttempts, WorkerID: t.WorkerID,
		RunAt: t.RunAt, LastError: t.LastError, UpdatedAt: t.UpdatedAt,
	}
}

func sortedTasks(s *state.Snapshot) []*state.Task {
	ts := make([]*state.Task, 0, len(s.Tasks))
	for _, t := range s.Tasks {
		ts = append(ts, t)
	}
	slices.SortFunc(ts, func(a, b *state.Task) int {
		if c := strings.Compare(string(b.Kind), string(a.Kind)); c != 0 {
			return c // "step" before "compensation"
		}
		return strings.Compare(a.StepName, b.StepName)
	})
	return ts
}

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/protocol"
	"github.com/omer-ulucan/baton/internal/state"
)

// WorkerProtocol is the engine as seen by the worker endpoints.
type WorkerProtocol interface {
	Register(ctx context.Context, w engine.Worker) error
	Claim(ctx context.Context, req engine.ClaimRequest) ([]engine.Assignment, error)
	Heartbeat(ctx context.Context, workerID string, attempts []uuid.UUID) (engine.HeartbeatResult, error)
	Complete(ctx context.Context, r engine.Result, output json.RawMessage) error
	Fail(ctx context.Context, r engine.Result, f engine.Failure) (engine.Next, error)
	Release(ctx context.Context, workerID string, attempts []uuid.UUID) error
	Config() engine.Config
}

const maxBody = 1 << 20

func registerWorkerRoutes(mux *http.ServeMux, wp WorkerProtocol, log *slog.Logger) {
	h := workerHandlers{wp: wp, log: log}
	mux.HandleFunc("POST "+protocol.PathRegister, h.register)
	mux.HandleFunc("POST "+protocol.PathClaim, h.claim)
	mux.HandleFunc("POST "+protocol.PathHeartbeat, h.heartbeat)
	mux.HandleFunc("POST "+protocol.PathComplete, h.complete)
	mux.HandleFunc("POST "+protocol.PathFail, h.failTask)
	mux.HandleFunc("POST "+protocol.PathRelease, h.release)
}

type workerHandlers struct {
	wp  WorkerProtocol
	log *slog.Logger
}

func (h workerHandlers) register(w http.ResponseWriter, r *http.Request) {
	var req protocol.RegisterRequest
	if !decode(w, r, &req) {
		return
	}
	if err := h.wp.Register(r.Context(), engine.Worker{ID: req.WorkerID, Queues: req.Queues, Handlers: req.Handlers, Version: req.Version}); err != nil {
		h.respondErr(w, r, err)
		return
	}
	cfg := h.wp.Config()
	writeJSON(w, http.StatusOK, protocol.RegisterResponse{
		HeartbeatIntervalMS: cfg.HeartbeatInterval.Milliseconds(),
		LeaseTTLMS:          cfg.LeaseTTL.Milliseconds(),
	})
}

func (h workerHandlers) claim(w http.ResponseWriter, r *http.Request) {
	var req protocol.ClaimRequest
	if !decode(w, r, &req) {
		return
	}
	if req.WorkerID == "" || req.Max < 0 || req.Max > 32 || req.WaitMS < 0 || req.WaitMS > 30_000 {
		writeError(w, http.StatusUnprocessableEntity, protocol.CodeInvalidRequest, "worker_id required, max in [0,32], wait_ms in [0,30000]")
		return
	}
	got, err := h.wp.Claim(r.Context(), engine.ClaimRequest{
		WorkerID: req.WorkerID, Queues: req.Queues, Max: req.Max, Wait: time.Duration(req.WaitMS) * time.Millisecond,
	})
	if err != nil {
		if r.Context().Err() != nil {
			return // client went away during the long poll
		}
		h.respondErr(w, r, err)
		return
	}
	resp := protocol.ClaimResponse{Tasks: make([]protocol.Task, len(got))}
	for i, a := range got {
		resp.Tasks[i] = protocol.Task{
			TaskID: a.TaskID, AttemptID: a.AttemptID, WorkflowID: a.WorkflowID, Step: a.Step, Kind: string(a.Kind),
			Handler: a.Handler, Queue: a.Queue, Input: a.Input, IdempotencyKey: a.IdempotencyKey,
			Attempt: a.Attempt, MaxAttempts: a.MaxAttempts, Deadline: a.Deadline, LeaseUntil: a.LeaseUntil,
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h workerHandlers) heartbeat(w http.ResponseWriter, r *http.Request) {
	var req protocol.HeartbeatRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := h.wp.Heartbeat(r.Context(), req.WorkerID, req.Attempts)
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	resp := protocol.HeartbeatResponse{Extended: []protocol.Extended{}, Revoked: []protocol.Revoked{}}
	for id, until := range res.Extended {
		resp.Extended = append(resp.Extended, protocol.Extended{AttemptID: id, LeaseUntil: until})
	}
	for id, reason := range res.Revoked {
		resp.Revoked = append(resp.Revoked, protocol.Revoked{AttemptID: id, Reason: reason})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h workerHandlers) complete(w http.ResponseWriter, r *http.Request) {
	var req protocol.CompleteRequest
	if !decode(w, r, &req) {
		return
	}
	if err := h.wp.Complete(r.Context(), engine.Result{WorkerID: req.WorkerID, TaskID: req.TaskID, AttemptID: req.AttemptID}, req.Output); err != nil {
		h.respondErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (h workerHandlers) respondErr(w http.ResponseWriter, r *http.Request, err error) {
	code, status := errorCode(err)
	if status == http.StatusInternalServerError {
		h.log.ErrorContext(r.Context(), "worker protocol", "path", r.URL.Path, "err", err)
		writeError(w, status, code, "internal error")
		return
	}
	writeError(w, status, code, err.Error())
}

func (h workerHandlers) failTask(w http.ResponseWriter, r *http.Request) {
	var req protocol.FailRequest
	if !decode(w, r, &req) {
		return
	}
	next, err := h.wp.Fail(r.Context(), engine.Result{WorkerID: req.WorkerID, TaskID: req.TaskID, AttemptID: req.AttemptID}, engine.Failure{
		Message: req.Error.Message, Type: req.Error.Type, Stack: req.Error.Stack, Retryable: req.Error.Retryable,
	})
	if err != nil {
		h.respondErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, protocol.FailResponse{Next: string(next)})
}

func (h workerHandlers) release(w http.ResponseWriter, r *http.Request) {
	var req protocol.ReleaseRequest
	if !decode(w, r, &req) {
		return
	}
	if err := h.wp.Release(r.Context(), req.WorkerID, req.Attempts); err != nil {
		h.respondErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func errorCode(err error) (string, int) {
	switch {
	case errors.Is(err, engine.ErrInvalid):
		return protocol.CodeInvalidRequest, http.StatusUnprocessableEntity
	case errors.Is(err, engine.ErrUnknownWorker):
		return protocol.CodeUnknownWorker, http.StatusNotFound
	case errors.Is(err, engine.ErrNotFound):
		return protocol.CodeNotFound, http.StatusNotFound
	case errors.Is(err, engine.ErrAttemptSuperseded):
		return protocol.CodeAttemptSuperseded, http.StatusConflict
	case errors.Is(err, engine.ErrCancelled):
		return protocol.CodeCancelled, http.StatusConflict
	case errors.Is(err, engine.ErrLeaseExpired):
		return protocol.CodeLeaseExpired, http.StatusConflict
	case errors.Is(err, state.ErrInvalidTransition):
		return protocol.CodeInvalidTransition, http.StatusConflict
	}
	return protocol.CodeInternal, http.StatusInternalServerError
}

// decode reads a JSON body strictly. On failure it writes a 4xx and
// returns false.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, protocol.CodeInvalidRequest, fmt.Sprintf("body exceeds %d bytes", maxBody))
			return false
		}
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, "malformed JSON: "+err.Error())
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var body protocol.ErrorBody
	body.Error.Code, body.Error.Message = code, msg
	writeJSON(w, status, body)
}

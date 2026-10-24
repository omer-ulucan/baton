// Package protocol holds the JSON wire types of the worker protocol
// (SPEC 8.2), shared by the orchestrator's handlers and the worker client.
package protocol

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Paths of the worker protocol endpoints.
const (
	PathRegister  = "/v1/worker/register"
	PathClaim     = "/v1/worker/claim"
	PathHeartbeat = "/v1/worker/heartbeat"
	PathComplete  = "/v1/worker/complete"
	PathFail      = "/v1/worker/fail"
	PathRelease   = "/v1/worker/release"
)

// Error codes returned in ErrorBody.
const (
	CodeInvalidRequest    = "invalid_request"
	CodeUnknownWorker     = "unknown_worker"
	CodeNotFound          = "not_found"
	CodeAttemptSuperseded = "attempt_superseded"
	CodeCancelled         = "cancelled"
	CodeLeaseExpired      = "lease_expired"
	CodeInvalidTransition = "invalid_transition"
	CodeInternal          = "internal"
)

// ErrorBody is the error envelope of every endpoint.
type ErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type RegisterRequest struct {
	WorkerID string   `json:"worker_id"`
	Queues   []string `json:"queues"`
	Handlers []string `json:"handlers"`
	Version  string   `json:"version,omitempty"`
}

type RegisterResponse struct {
	HeartbeatIntervalMS int64 `json:"heartbeat_interval_ms"`
	LeaseTTLMS          int64 `json:"lease_ttl_ms"`
}

type ClaimRequest struct {
	WorkerID string   `json:"worker_id"`
	Queues   []string `json:"queues,omitempty"`
	Max      int      `json:"max"`
	WaitMS   int64    `json:"wait_ms"`
}

type Task struct {
	TaskID         uuid.UUID       `json:"task_id"`
	AttemptID      uuid.UUID       `json:"attempt_id"`
	WorkflowID     uuid.UUID       `json:"workflow_id"`
	Step           string          `json:"step"`
	Kind           string          `json:"kind"`
	Handler        string          `json:"handler"`
	Queue          string          `json:"queue"`
	Input          json.RawMessage `json:"input,omitempty"`
	IdempotencyKey string          `json:"idempotency_key"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	Deadline       time.Time       `json:"deadline"`
	LeaseUntil     time.Time       `json:"lease_until"`
	// TraceParent is the workflow's W3C trace context; the worker's
	// execution span joins that trace.
	TraceParent string `json:"traceparent,omitempty"`
}

type ClaimResponse struct {
	Tasks []Task `json:"tasks"`
}

type HeartbeatRequest struct {
	WorkerID string      `json:"worker_id"`
	Attempts []uuid.UUID `json:"attempts"`
}

type Extended struct {
	AttemptID  uuid.UUID `json:"attempt_id"`
	LeaseUntil time.Time `json:"lease_until"`
}

type Revoked struct {
	AttemptID uuid.UUID `json:"attempt_id"`
	Reason    string    `json:"reason"` // cancelled | superseded | expired
}

type HeartbeatResponse struct {
	Extended []Extended `json:"extended"`
	Revoked  []Revoked  `json:"revoked"`
}

type CompleteRequest struct {
	WorkerID  string          `json:"worker_id"`
	TaskID    uuid.UUID       `json:"task_id"`
	AttemptID uuid.UUID       `json:"attempt_id"`
	Output    json.RawMessage `json:"output,omitempty"`
}

type TaskError struct {
	Message   string `json:"message"`
	Type      string `json:"type,omitempty"`
	Stack     string `json:"stack,omitempty"`
	Retryable *bool  `json:"retryable,omitempty"`
}

type FailRequest struct {
	WorkerID  string    `json:"worker_id"`
	TaskID    uuid.UUID `json:"task_id"`
	AttemptID uuid.UUID `json:"attempt_id"`
	Error     TaskError `json:"error"`
}

type FailResponse struct {
	Next string `json:"next"` // retry | dead_letter
}

type ReleaseRequest struct {
	WorkerID string      `json:"worker_id"`
	Attempts []uuid.UUID `json:"attempt_ids"`
}

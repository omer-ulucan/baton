// Package api holds the JSON wire types of the public REST API (SPEC 8.1),
// shared by the server and batonctl. api/openapi.yaml is the contract.
package api

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Error codes of the public API.
const (
	CodeInvalidRequest    = "invalid_request"
	CodeInvalidDefinition = "invalid_definition"
	CodeNotFound          = "not_found"
	CodeInvalidTransition = "invalid_transition"
	CodeQueueFull         = "queue_full"
	CodeUnavailable       = "unavailable"
	CodeInternal          = "internal"
)

// ErrorBody is the error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type SubmitRequest struct {
	Definition  string          `json:"definition"`
	Input       json.RawMessage `json:"input,omitempty"`
	Priority    *int            `json:"priority,omitempty"`
	CallbackURL string          `json:"callback_url,omitempty"`
}

type SubmitResponse struct {
	WorkflowID uuid.UUID `json:"workflow_id"`
	Status     string    `json:"status"`
	Existing   bool      `json:"existing,omitempty"`
}

type Workflow struct {
	ID              uuid.UUID  `json:"id"`
	DefinitionID    uuid.UUID  `json:"definition_id"`
	Status          string     `json:"status"`
	Priority        int        `json:"priority"`
	CancelRequested bool       `json:"cancel_requested,omitempty"`
	LastSeq         int        `json:"last_seq"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	Tasks           []Task     `json:"tasks,omitempty"`
}

type Task struct {
	ID          uuid.UUID  `json:"id"`
	Step        string     `json:"step"`
	Kind        string     `json:"kind"`
	Handler     string     `json:"handler"`
	Queue       string     `json:"queue"`
	Priority    int        `json:"priority"`
	Status      string     `json:"status"`
	Attempt     int        `json:"attempt"`
	MaxAttempts int        `json:"max_attempts"`
	WorkerID    *string    `json:"worker_id,omitempty"`
	RunAt       *time.Time `json:"run_at,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type WorkflowList struct {
	Workflows  []Workflow `json:"workflows"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

type Event struct {
	Seq        int             `json:"seq"`
	Type       string          `json:"type"`
	TaskID     *uuid.UUID      `json:"task_id,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

type EventList struct {
	Events []Event `json:"events"`
}

type CancelRequest struct {
	Compensate bool   `json:"compensate"`
	Reason     string `json:"reason,omitempty"`
}

type StatusResponse struct {
	Status string `json:"status"`
}

type Decision struct {
	Label      string  `json:"label"`
	Confidence float64 `json:"confidence"`
	Policy     string  `json:"policy"`
	Fallback   string  `json:"fallback,omitempty"`
}

type DLQEntry struct {
	Task         Task      `json:"task"`
	WorkflowID   uuid.UUID `json:"workflow_id"`
	DeadLettered time.Time `json:"dead_lettered_at"`
	LastDecision *Decision `json:"last_decision,omitempty"`
}

type DLQList struct {
	Entries    []DLQEntry `json:"entries"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

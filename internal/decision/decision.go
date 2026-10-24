// Package decision is the decision layer contract (SPEC 10.1): failure
// triage and priority classification behind one small interface.
package decision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"
)

// Kind is a decision point.
type Kind string

const (
	KindTriage   Kind = "triage"
	KindPriority Kind = "priority"
)

// Triage labels.
const (
	Retry            = "retry"
	RetryWithBackoff = "retry_with_backoff"
	DeadLetter       = "dead_letter"
	Compensate       = "compensate"
	Escalate         = "escalate"
)

// Priority labels.
const (
	PriorityLow      = "low"
	PriorityNormal   = "normal"
	PriorityHigh     = "high"
	PriorityCritical = "critical"
)

// Request is the input of one decision.
type Request struct {
	Kind   Kind      `json:"kind"`
	TaskID uuid.UUID `json:"task_id"`
	// Triage.
	Handler      string `json:"handler,omitempty"`
	ErrorType    string `json:"error_type,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	Stack        string `json:"stack,omitempty"`
	Attempt      int    `json:"attempt,omitempty"`
	MaxAttempts  int    `json:"max_attempts,omitempty"`
	Retryable    *bool  `json:"retryable,omitempty"`
	// Priority.
	Description string `json:"description,omitempty"`
}

// Hash is the hex SHA-256 of the request's canonical JSON encoding,
// recorded so a decision can be tied to its exact input.
func (r Request) Hash() string {
	b, _ := json.Marshal(r) // struct of plain fields: cannot fail
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Decision is a policy's answer.
type Decision struct {
	Label         string
	Confidence    float64 // [0,1]
	Policy        string  // "rule" | "laya"
	ModelRevision string
	InputHash     string
	// Fallback is why a fallback policy answered instead of the primary:
	// "" | "low_confidence" | "timeout" | "error" | "unavailable".
	Fallback string
	// Primary is the primary policy's answer when Fallback is set.
	Primary *Decision
}

// Policy decides (the DecisionPolicy of SPEC 10.1). Implementations must honour ctx and must be safe
// for concurrent use.
type Policy interface {
	Decide(ctx context.Context, req Request) (Decision, error)
}

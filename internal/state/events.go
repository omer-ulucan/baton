package state

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// EventType names an event; it is the workflow_events.type column.
type EventType string

const (
	EvWorkflowSubmitted       EventType = "WorkflowSubmitted"
	EvWorkflowDeferred        EventType = "WorkflowDeferred"
	EvWorkflowAdmitted        EventType = "WorkflowAdmitted"
	EvWorkflowPaused          EventType = "WorkflowPaused"
	EvWorkflowResumed         EventType = "WorkflowResumed"
	EvWorkflowCancelRequested EventType = "WorkflowCancelRequested"
	EvWorkflowCancelled       EventType = "WorkflowCancelled"
	EvWorkflowSucceeded       EventType = "WorkflowSucceeded"
	EvWorkflowFailed          EventType = "WorkflowFailed"
	EvWorkflowRetried         EventType = "WorkflowRetried"
	EvCompensationStarted     EventType = "CompensationStarted"
	EvCompensationCompleted   EventType = "CompensationCompleted"
	EvCompensationFailed      EventType = "CompensationFailed"

	EvTaskCreated        EventType = "TaskCreated"
	EvTaskReady          EventType = "TaskReady"
	EvTaskClaimed        EventType = "TaskClaimed"
	EvTaskSucceeded      EventType = "TaskSucceeded"
	EvTaskFailed         EventType = "TaskFailed"
	EvTaskLeaseExpired   EventType = "TaskLeaseExpired"
	EvTaskReleased       EventType = "TaskReleased"
	EvTaskRetryScheduled EventType = "TaskRetryScheduled"
	EvTaskDeadLettered   EventType = "TaskDeadLettered"
	EvTaskRequeued       EventType = "TaskRequeued"
	EvTaskCancelled      EventType = "TaskCancelled"
	EvTaskSkipped        EventType = "TaskSkipped"

	EvDecisionMade EventType = "DecisionMade"
)

// Event is one entry of a workflow's history. Seq and OccurredAt are
// assigned by the writer when the event is appended.
type Event struct {
	WorkflowID uuid.UUID
	Seq        int
	TaskID     uuid.UUID // uuid.Nil for workflow-level events
	OccurredAt time.Time
	Payload    Payload
}

// Type is the event's type, taken from its payload.
func (e Event) Type() EventType { return e.Payload.Type() }

// Payload is the type-specific body of an event.
type Payload interface {
	Type() EventType
}

type (
	WorkflowSubmitted struct {
		DefinitionID uuid.UUID       `json:"definition_id"`
		Priority     int             `json:"priority"`
		Input        json.RawMessage `json:"input,omitempty"`
		SubmitKey    *string         `json:"submit_key,omitempty"`
	}
	WorkflowDeferred        struct{}
	WorkflowAdmitted        struct{}
	WorkflowPaused          struct{}
	WorkflowResumed         struct{}
	WorkflowCancelRequested struct {
		Compensate bool   `json:"compensate"`
		Reason     string `json:"reason,omitempty"`
	}
	WorkflowCancelled struct{}
	WorkflowSucceeded struct{}
	WorkflowFailed    struct {
		CauseTaskID uuid.UUID `json:"cause_task_id"`
	}
	WorkflowRetried     struct{}
	CompensationStarted struct {
		Order []string `json:"order"`
	}
	CompensationCompleted struct{}
	CompensationFailed    struct{}

	TaskCreated struct {
		Step           string   `json:"step"`
		Kind           TaskKind `json:"kind"`
		Handler        string   `json:"handler"`
		Queue          string   `json:"queue"`
		Priority       int      `json:"priority"`
		MaxAttempts    int      `json:"max_attempts"`
		IdempotencyKey string   `json:"idempotency_key"`
	}
	TaskReady   struct{}
	TaskClaimed struct {
		AttemptID uuid.UUID `json:"attempt_id"`
		WorkerID  string    `json:"worker_id"`
		Attempt   int       `json:"attempt"`
	}
	TaskSucceeded struct {
		AttemptID    uuid.UUID `json:"attempt_id"`
		OutputDigest string    `json:"output_digest,omitempty"`
	}
	TaskFailed struct {
		AttemptID   uuid.UUID `json:"attempt_id"`
		ErrorClass  string    `json:"error_class,omitempty"`
		Message     string    `json:"message"`
		StackDigest string    `json:"stack_digest,omitempty"`
	}
	TaskLeaseExpired struct {
		AttemptID uuid.UUID `json:"attempt_id"`
	}
	TaskReleased struct {
		AttemptID uuid.UUID `json:"attempt_id"`
	}
	TaskRetryScheduled struct {
		RunAt   time.Time `json:"run_at"`
		DelayMS int64     `json:"delay_ms"`
	}
	TaskDeadLettered struct {
		Reason string `json:"reason"`
	}
	TaskRequeued struct {
		By string `json:"by,omitempty"`
	}
	TaskCancelled struct{}
	TaskSkipped   struct{}

	// DecisionMade records a decision-layer result (SPEC 10.5). Replay
	// reads it; it never re-runs a policy.
	DecisionMade struct {
		Kind          string           `json:"kind"`
		AttemptID     *uuid.UUID       `json:"attempt_id,omitempty"`
		InputHash     string           `json:"input_hash"`
		Label         string           `json:"label"`
		Confidence    float64          `json:"confidence"`
		Policy        string           `json:"policy"`
		ModelRevision string           `json:"model_revision,omitempty"`
		Fallback      string           `json:"fallback,omitempty"`
		Primary       *PrimaryDecision `json:"primary,omitempty"`
		LatencyMS     int64            `json:"latency_ms"`
	}
	// PrimaryDecision is what the primary policy answered when a fallback
	// policy's answer was used instead.
	PrimaryDecision struct {
		Policy     string  `json:"policy"`
		Label      string  `json:"label,omitempty"`
		Confidence float64 `json:"confidence"`
	}
)

func (WorkflowSubmitted) Type() EventType       { return EvWorkflowSubmitted }
func (WorkflowDeferred) Type() EventType        { return EvWorkflowDeferred }
func (WorkflowAdmitted) Type() EventType        { return EvWorkflowAdmitted }
func (WorkflowPaused) Type() EventType          { return EvWorkflowPaused }
func (WorkflowResumed) Type() EventType         { return EvWorkflowResumed }
func (WorkflowCancelRequested) Type() EventType { return EvWorkflowCancelRequested }
func (WorkflowCancelled) Type() EventType       { return EvWorkflowCancelled }
func (WorkflowSucceeded) Type() EventType       { return EvWorkflowSucceeded }
func (WorkflowFailed) Type() EventType          { return EvWorkflowFailed }
func (WorkflowRetried) Type() EventType         { return EvWorkflowRetried }
func (CompensationStarted) Type() EventType     { return EvCompensationStarted }
func (CompensationCompleted) Type() EventType   { return EvCompensationCompleted }
func (CompensationFailed) Type() EventType      { return EvCompensationFailed }
func (TaskCreated) Type() EventType             { return EvTaskCreated }
func (TaskReady) Type() EventType               { return EvTaskReady }
func (TaskClaimed) Type() EventType             { return EvTaskClaimed }
func (TaskSucceeded) Type() EventType           { return EvTaskSucceeded }
func (TaskFailed) Type() EventType              { return EvTaskFailed }
func (TaskLeaseExpired) Type() EventType        { return EvTaskLeaseExpired }
func (TaskReleased) Type() EventType            { return EvTaskReleased }
func (TaskRetryScheduled) Type() EventType      { return EvTaskRetryScheduled }
func (TaskDeadLettered) Type() EventType        { return EvTaskDeadLettered }
func (TaskRequeued) Type() EventType            { return EvTaskRequeued }
func (TaskCancelled) Type() EventType           { return EvTaskCancelled }
func (TaskSkipped) Type() EventType             { return EvTaskSkipped }
func (DecisionMade) Type() EventType            { return EvDecisionMade }

// PayloadVersion is the current payload schema version (SPEC 5.2).
const PayloadVersion = 1

func newPayload(t EventType) (Payload, bool) {
	switch t {
	case EvWorkflowSubmitted:
		return &WorkflowSubmitted{}, true
	case EvWorkflowDeferred:
		return &WorkflowDeferred{}, true
	case EvWorkflowAdmitted:
		return &WorkflowAdmitted{}, true
	case EvWorkflowPaused:
		return &WorkflowPaused{}, true
	case EvWorkflowResumed:
		return &WorkflowResumed{}, true
	case EvWorkflowCancelRequested:
		return &WorkflowCancelRequested{}, true
	case EvWorkflowCancelled:
		return &WorkflowCancelled{}, true
	case EvWorkflowSucceeded:
		return &WorkflowSucceeded{}, true
	case EvWorkflowFailed:
		return &WorkflowFailed{}, true
	case EvWorkflowRetried:
		return &WorkflowRetried{}, true
	case EvCompensationStarted:
		return &CompensationStarted{}, true
	case EvCompensationCompleted:
		return &CompensationCompleted{}, true
	case EvCompensationFailed:
		return &CompensationFailed{}, true
	case EvTaskCreated:
		return &TaskCreated{}, true
	case EvTaskReady:
		return &TaskReady{}, true
	case EvTaskClaimed:
		return &TaskClaimed{}, true
	case EvTaskSucceeded:
		return &TaskSucceeded{}, true
	case EvTaskFailed:
		return &TaskFailed{}, true
	case EvTaskLeaseExpired:
		return &TaskLeaseExpired{}, true
	case EvTaskReleased:
		return &TaskReleased{}, true
	case EvTaskRetryScheduled:
		return &TaskRetryScheduled{}, true
	case EvTaskDeadLettered:
		return &TaskDeadLettered{}, true
	case EvTaskRequeued:
		return &TaskRequeued{}, true
	case EvTaskCancelled:
		return &TaskCancelled{}, true
	case EvTaskSkipped:
		return &TaskSkipped{}, true
	case EvDecisionMade:
		return &DecisionMade{}, true
	}
	return nil, false
}

// EncodePayload returns the stored JSON form of p: its fields plus "v".
func EncodePayload(p Payload) ([]byte, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", p.Type(), err)
	}
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("encode %s: %w", p.Type(), err)
	}
	fields["v"] = json.RawMessage(fmt.Sprint(PayloadVersion))
	return json.Marshal(fields)
}

// DecodePayload parses a stored payload of type t. Unknown types and
// versions are errors: a projector must never guess.
func DecodePayload(t EventType, data []byte) (Payload, error) {
	p, ok := newPayload(t)
	if !ok {
		return nil, fmt.Errorf("decode: unknown event type %q", t)
	}
	var v struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("decode %s: %w", t, err)
	}
	if v.V != PayloadVersion {
		return nil, fmt.Errorf("decode %s: unsupported payload version %d", t, v.V)
	}
	if err := json.Unmarshal(data, p); err != nil {
		return nil, fmt.Errorf("decode %s: %w", t, err)
	}
	return deref(p), nil
}

// deref turns the *T used for decoding into the T used everywhere else,
// so a type switch on Payload never has to consider both forms.
func deref(p Payload) Payload {
	return reflect.ValueOf(p).Elem().Interface().(Payload)
}

// Change is an event as proposed by the engine, before the writer assigns
// its seq and occurred_at.
type Change struct {
	TaskID  uuid.UUID
	Payload Payload
}

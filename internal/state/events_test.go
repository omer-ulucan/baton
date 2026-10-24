package state

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func allPayloads() []Payload {
	key := "k"
	att := uuid.New()
	return []Payload{
		WorkflowSubmitted{DefinitionID: uuid.New(), Priority: 7, Input: json.RawMessage(`{"a":1}`), SubmitKey: &key},
		WorkflowDeferred{}, WorkflowAdmitted{}, WorkflowPaused{}, WorkflowResumed{},
		WorkflowCancelRequested{Compensate: true, Reason: "r"}, WorkflowCancelled{},
		WorkflowSucceeded{}, WorkflowFailed{CauseTaskID: uuid.New()}, WorkflowRetried{},
		CompensationStarted{Order: []string{"b", "a"}}, CompensationCompleted{}, CompensationFailed{},
		TaskCreated{Step: "s", Kind: KindStep, Handler: "h", Queue: "q", Priority: 3, MaxAttempts: 4, IdempotencyKey: "i"},
		TaskReady{}, TaskClaimed{AttemptID: att, WorkerID: "w", Attempt: 1},
		TaskSucceeded{AttemptID: att, OutputDigest: "d"},
		TaskFailed{AttemptID: att, ErrorClass: "E", Message: "m", StackDigest: "s"},
		TaskLeaseExpired{AttemptID: att}, TaskReleased{AttemptID: att},
		TaskRetryScheduled{RunAt: time.Date(2026, 5, 6, 7, 8, 9, 123456000, time.UTC), DelayMS: 1500},
		TaskDeadLettered{Reason: "r"}, TaskRequeued{By: "ops"}, TaskCancelled{}, TaskSkipped{},
		DecisionMade{Kind: "triage", AttemptID: &att, InputHash: "h", Label: "retry", Confidence: 0.25,
			Policy: "rule", Fallback: "low_confidence", Primary: &PrimaryDecision{Policy: "laya", Label: "escalate", Confidence: 0.2}, LatencyMS: 9},
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	seen := map[EventType]bool{}
	for _, p := range allPayloads() {
		seen[p.Type()] = true
		data, err := EncodePayload(p)
		if err != nil {
			t.Fatalf("%s: encode: %v", p.Type(), err)
		}
		if !strings.Contains(string(data), `"v":1`) {
			t.Errorf("%s: payload %s lacks version", p.Type(), data)
		}
		got, err := DecodePayload(p.Type(), data)
		if err != nil {
			t.Fatalf("%s: decode: %v", p.Type(), err)
		}
		if !reflect.DeepEqual(got, p) {
			t.Errorf("%s: round trip\n got %#v\nwant %#v", p.Type(), got, p)
		}
	}
	// Every registered type is covered by this test.
	for _, et := range []EventType{EvWorkflowSubmitted, EvDecisionMade, EvTaskSkipped} {
		if !seen[et] {
			t.Errorf("%s not covered", et)
		}
	}
	if len(seen) != 26 {
		t.Errorf("covered %d event types, want 26", len(seen))
	}
}

func TestDecodeRejectsUnknown(t *testing.T) {
	if _, err := DecodePayload("Nope", []byte(`{"v":1}`)); err == nil {
		t.Error("unknown type accepted")
	}
	if _, err := DecodePayload(EvTaskReady, []byte(`{"v":2}`)); err == nil {
		t.Error("future version accepted")
	}
	if _, err := DecodePayload(EvTaskReady, []byte(`{}`)); err == nil {
		t.Error("missing version accepted")
	}
}

func TestEveryPayloadIsAppliable(t *testing.T) {
	// Guards against adding an event type without handling it in Apply:
	// an unhandled payload yields "unhandled ... event", never a panic.
	for _, p := range allPayloads() {
		s := NewSnapshot()
		_ = Apply(s, Event{WorkflowID: uuid.New(), Seq: 1, OccurredAt: t0, Payload: p})
		f := newFixture(t)
		f.submit()
		a := f.step("a", 1)
		err := Apply(f.s, f.event(a, p))
		if err != nil && strings.Contains(err.Error(), "unhandled") {
			t.Errorf("%s is not handled by Apply: %v", p.Type(), err)
		}
	}
}

package chaos

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/state"
)

func ev(seq int, task uuid.UUID, p state.Payload) state.Event {
	return state.Event{Seq: seq, TaskID: task, OccurredAt: time.Unix(int64(seq), 0), Payload: p}
}

func violations(events []state.Event) []string {
	var out []string
	checkStream(uuid.New(), events, func(inv, _ string, _ ...any) { out = append(out, inv) })
	return out
}

func TestStreamCheckerCatchesViolations(t *testing.T) {
	step, comp := uuid.New(), uuid.New()
	created := []state.Event{
		ev(1, uuid.Nil, state.WorkflowSubmitted{}),
		ev(2, step, state.TaskCreated{Step: "a", Kind: state.KindStep}),
		ev(3, comp, state.TaskCreated{Step: "a", Kind: state.KindCompensation}),
	}
	tests := []struct {
		name string
		tail []state.Event
		want string
	}{
		{"clean", []state.Event{ev(4, step, state.TaskClaimed{AttemptID: uuid.New()}), ev(5, step, state.TaskSucceeded{})}, ""},
		{"double claim", []state.Event{ev(4, step, state.TaskClaimed{AttemptID: uuid.New()}), ev(5, step, state.TaskClaimed{AttemptID: uuid.New()})}, "no double claim"},
		{"claim after expiry is fine", []state.Event{ev(4, step, state.TaskClaimed{AttemptID: uuid.New()}), ev(5, step, state.TaskLeaseExpired{}), ev(6, step, state.TaskClaimed{AttemptID: uuid.New()})}, ""},
		{"step after cancel request", []state.Event{ev(4, uuid.Nil, state.WorkflowCancelRequested{}), ev(5, step, state.TaskClaimed{AttemptID: uuid.New()})}, "no run after cancel"},
		{"compensation after cancel request is fine", []state.Event{ev(4, uuid.Nil, state.WorkflowCancelRequested{Compensate: true}), ev(5, comp, state.TaskClaimed{AttemptID: uuid.New()})}, ""},
		{"result after cancel", []state.Event{ev(4, step, state.TaskClaimed{AttemptID: uuid.New()}), ev(5, uuid.Nil, state.WorkflowCancelled{}), ev(6, step, state.TaskSucceeded{})}, "no run after cancel"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := strings.Join(violations(append(append([]state.Event{}, created...), tt.tail...)), ",")
			if (tt.want == "" && got != "") || (tt.want != "" && !strings.Contains(got, tt.want)) {
				t.Fatalf("violations = %q, want %q", got, tt.want)
			}
		})
	}
}

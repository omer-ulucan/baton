package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
)

// Outbox delivery policy (SPEC 7).
const (
	outboxBatch       = 10
	outboxGiveUpAfter = 24 * time.Hour
	outboxMaxBackoff  = time.Hour
)

// EscalationMessage is the body of an escalation webhook.
type EscalationMessage struct {
	WorkflowID uuid.UUID `json:"workflow_id"`
	TaskID     uuid.UUID `json:"task_id"`
	Step       string    `json:"step"`
	Handler    string    `json:"handler"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error"`
	ErrorType  string    `json:"error_type,omitempty"`
	Policy     string    `json:"policy"`
	Confidence float64   `json:"confidence"`
}

func (e *Engine) queueEscalation(ctx context.Context, tx *store.Tx, t *state.Task, f Failure, d decision.Decision) error {
	if e.escalateURL == "" {
		e.log.WarnContext(ctx, "escalation requested but BATON_ESCALATION_WEBHOOK is not set", "task", t.ID)
		return nil
	}
	body, err := json.Marshal(EscalationMessage{
		WorkflowID: t.WorkflowID, TaskID: t.ID, Step: t.StepName, Handler: t.Handler, Attempt: t.Attempt,
		Error: f.Message, ErrorType: f.Type, Policy: d.Policy, Confidence: d.Confidence,
	})
	if err != nil {
		return err
	}
	return tx.InsertOutbox(ctx, "escalation", fmt.Sprintf("%s:%d", t.ID, t.Attempt), e.escalateURL, body)
}

// RelayOutbox delivers due outbox messages: at least once, with
// exponential backoff, giving up after 24h (SPEC 7). Each message is
// locked while it is sent, so concurrent relays never send it twice at the
// same time. It returns the number delivered.
func (e *Engine) RelayOutbox(ctx context.Context) (int, error) {
	sent := 0
	err := e.store.InTx(ctx, e.fence(), func(tx *store.Tx) error {
		sent = 0
		msgs, err := tx.DueOutbox(ctx, outboxBatch)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if err := e.deliver(ctx, m); err != nil {
				backoff := min(time.Duration(1<<min(m.Attempts, 20))*time.Second, outboxMaxBackoff)
				giveUp := tx.Now().Sub(m.CreatedAt) > outboxGiveUpAfter
				if giveUp {
					e.log.ErrorContext(ctx, "outbox message undeliverable; giving up", "id", m.ID, "topic", m.Topic, "err", err)
				}
				if err := tx.MarkRetry(ctx, m.ID, tx.Now().Add(backoff), clean(err.Error()), giveUp); err != nil {
					return err
				}
				continue
			}
			if err := tx.MarkPublished(ctx, m.ID); err != nil {
				return err
			}
			sent++
		}
		return nil
	})
	return sent, err
}

func (e *Engine) deliver(ctx context.Context, m store.OutboxMessage) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.URL, bytes.NewReader(m.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Baton-Message-Key", m.Key)
	req.Header.Set("Baton-Topic", m.Topic)
	resp, err := e.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s answered %d", m.URL, resp.StatusCode)
	}
	return nil
}

package store

import (
	"context"
	"fmt"
	"time"
)

// OutboxMessage is a pending outbound message.
type OutboxMessage struct {
	ID        int64
	Topic     string
	Key       string
	URL       string
	Payload   []byte
	CreatedAt time.Time
	Attempts  int
}

// InsertOutbox queues a message in the current transaction, so it is sent
// if and only if the state change commits (ADR 0006).
func (t *Tx) InsertOutbox(ctx context.Context, topic, key, url string, payload []byte) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO outbox (topic, key, url, payload, created_at, next_try_at)
		VALUES ($1, $2, $3, $4, $5, $5)`, topic, key, url, payload, t.now)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// DueOutbox locks up to limit messages due for delivery. Rows stay locked
// for the transaction, so concurrent relays never send the same message
// at the same time.
func (t *Tx) DueOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT id, topic, key, url, payload, created_at, attempts FROM outbox
		 WHERE published_at IS NULL AND failed_at IS NULL AND next_try_at <= $1
		 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED`, t.now, limit)
	if err != nil {
		return nil, fmt.Errorf("due outbox: %w", err)
	}
	defer rows.Close()
	var out []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.Topic, &m.Key, &m.URL, &m.Payload, &m.CreatedAt, &m.Attempts); err != nil {
			return nil, err
		}
		m.CreatedAt = m.CreatedAt.UTC()
		out = append(out, m)
	}
	return out, rows.Err()
}

// MarkPublished records a successful delivery.
func (t *Tx) MarkPublished(ctx context.Context, id int64) error {
	_, err := t.tx.Exec(ctx, `UPDATE outbox SET published_at = $2, attempts = attempts + 1, last_error = NULL WHERE id = $1`, id, t.now)
	return err
}

// MarkRetry records a failed delivery and when to try again; with
// giveUp it marks the message failed for good.
func (t *Tx) MarkRetry(ctx context.Context, id int64, next time.Time, lastErr string, giveUp bool) error {
	var failedAt *time.Time
	if giveUp {
		failedAt = &t.now
	}
	_, err := t.tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1, next_try_at = $2, last_error = $3, failed_at = $4
		WHERE id = $1`, id, next, lastErr, failedAt)
	return err
}

// OutboxPending counts messages not yet delivered or given up on.
func (s *Store) OutboxPending(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND failed_at IS NULL`).Scan(&n)
	return n, err
}

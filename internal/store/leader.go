package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrFenced means the writer's fencing token is not the current leader's,
// or its lease has expired: it must stop writing and step down.
var ErrFenced = errors.New("fenced: not the current leader")

// sqlStateFenced is raised by the workflow_events fencing trigger.
const sqlStateFenced = "BT001"

// LeaderInfo is the leader lease as stored.
type LeaderInfo struct {
	Holder    string
	Addr      string
	Token     int64
	ExpiresAt time.Time
	Live      bool // expires_at is in the future by the database clock
}

// AcquireLeadership takes or renews the leader lease for holder (SPEC 6.1).
// The token increments whenever the lease changes hands or is re-taken
// after expiring; a renewal of a live lease keeps it and touches only
// leader_lease, so it never waits for in-flight writes. ok is false when
// another replica holds a live lease.
func (s *Store) AcquireLeadership(ctx context.Context, holder, addr string, ttl time.Duration) (token int64, ok bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var renewal bool
		err := tx.QueryRow(ctx, `
			WITH prev AS (
				SELECT holder_id = $1 AND expires_at > clock_timestamp() AS renewal
				  FROM leader_lease
				 WHERE name = 'leader' AND (holder_id = $1 OR expires_at <= clock_timestamp())
				   FOR UPDATE
			)
			UPDATE leader_lease l
			   SET holder_id = $1, holder_addr = $2, expires_at = clock_timestamp() + $3 * interval '1 microsecond'
			  FROM prev
			 WHERE l.name = 'leader'
			RETURNING prev.renewal`, holder, addr, ttl.Microseconds()).Scan(&renewal)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		if renewal {
			return tx.QueryRow(ctx, `SELECT fencing_token FROM leader_fence WHERE name = 'leader'`).Scan(&token)
		}
		// A new term: bumping the token waits for the previous leader's
		// in-flight transactions (they hold the fence FOR SHARE) and
		// fences everything after them.
		return tx.QueryRow(ctx, `UPDATE leader_fence SET fencing_token = fencing_token + 1
			WHERE name = 'leader' RETURNING fencing_token`).Scan(&token)
	})
	if err != nil {
		return 0, false, fmt.Errorf("acquire leadership: %w", err)
	}
	return token, ok, nil
}

// ReleaseLeadership gives up the lease if holder still has it in the
// given term, so a follower can take over at once instead of waiting out
// the TTL.
func (s *Store) ReleaseLeadership(ctx context.Context, holder string, token int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE leader_lease SET expires_at = '-infinity'
		WHERE name = 'leader' AND holder_id = $1
		  AND (SELECT fencing_token FROM leader_fence WHERE name = 'leader') = $2`, holder, token)
	if err != nil {
		return fmt.Errorf("release leadership: %w", err)
	}
	return nil
}

// Leader returns the current lease.
func (s *Store) Leader(ctx context.Context) (LeaderInfo, error) {
	var li LeaderInfo
	var holder, addr *string
	err := s.pool.QueryRow(ctx, `SELECT l.holder_id, l.holder_addr, f.fencing_token, l.expires_at, l.expires_at > clock_timestamp()
		FROM leader_lease l, leader_fence f WHERE l.name = 'leader' AND f.name = 'leader'`).
		Scan(&holder, &addr, &li.Token, &li.ExpiresAt, &li.Live)
	if err != nil {
		return LeaderInfo{}, fmt.Errorf("read leader: %w", err)
	}
	if holder != nil {
		li.Holder = *holder
	}
	if addr != nil {
		li.Addr = *addr
	}
	li.ExpiresAt = li.ExpiresAt.UTC()
	return li, nil
}

// checkFence locks the fence row FOR SHARE for the rest of the
// transaction and verifies the writer's token and the lease's liveness.
// Holding the share lock until commit means no takeover can commit while
// this transaction runs, so the check covers every statement in it,
// including writes that append no event.
func (t *Tx) checkFence(ctx context.Context) error {
	var token int64
	var live bool
	err := t.tx.QueryRow(ctx, `SELECT f.fencing_token, l.expires_at > clock_timestamp()
		FROM leader_fence f, leader_lease l WHERE f.name = 'leader' AND l.name = 'leader'
		FOR SHARE OF f`).Scan(&token, &live)
	if err != nil {
		return fmt.Errorf("check fence: %w", err)
	}
	if token != t.fence.Token || !live {
		return fmt.Errorf("%w (token %d, current %d, live %t)", ErrFenced, t.fence.Token, token, live)
	}
	return nil
}

// fencedErr maps the trigger's rejection to ErrFenced.
func fencedErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlStateFenced {
		return fmt.Errorf("%w: %s", ErrFenced, pgErr.Message)
	}
	return err
}

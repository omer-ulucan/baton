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
// after expiring; a renewal of a live lease keeps it. ok is false when
// another replica holds a live lease.
func (s *Store) AcquireLeadership(ctx context.Context, holder, addr string, ttl time.Duration) (token int64, ok bool, err error) {
	err = s.pool.QueryRow(ctx, `
		UPDATE leader_lease
		   SET fencing_token = CASE WHEN holder_id = $1 AND expires_at > clock_timestamp()
		                            THEN fencing_token ELSE fencing_token + 1 END,
		       holder_id   = $1,
		       holder_addr = $2,
		       expires_at  = clock_timestamp() + $3 * interval '1 microsecond'
		 WHERE name = 'leader'
		   AND (holder_id = $1 OR expires_at <= clock_timestamp())
		RETURNING fencing_token`, holder, addr, ttl.Microseconds()).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("acquire leadership: %w", err)
	}
	return token, true, nil
}

// ReleaseLeadership gives up the lease if holder still has it, so a
// follower can take over at once instead of waiting out the TTL.
func (s *Store) ReleaseLeadership(ctx context.Context, holder string, token int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE leader_lease SET expires_at = '-infinity'
		WHERE name = 'leader' AND holder_id = $1 AND fencing_token = $2`, holder, token)
	if err != nil {
		return fmt.Errorf("release leadership: %w", err)
	}
	return nil
}

// Leader returns the current lease.
func (s *Store) Leader(ctx context.Context) (LeaderInfo, error) {
	var li LeaderInfo
	var holder, addr *string
	err := s.pool.QueryRow(ctx, `SELECT holder_id, holder_addr, fencing_token, expires_at, expires_at > clock_timestamp()
		FROM leader_lease WHERE name = 'leader'`).Scan(&holder, &addr, &li.Token, &li.ExpiresAt, &li.Live)
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

// checkFence locks the lease row FOR SHARE for the rest of the transaction
// and verifies the writer's token. Holding the share lock until commit
// means no takeover can commit while this transaction runs, so the check
// covers every statement in it, including writes that append no event.
func (t *Tx) checkFence(ctx context.Context) error {
	var token int64
	var live bool
	err := t.tx.QueryRow(ctx, `SELECT fencing_token, expires_at > clock_timestamp()
		FROM leader_lease WHERE name = 'leader' FOR SHARE`).Scan(&token, &live)
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

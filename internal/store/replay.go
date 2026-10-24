package store

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/omer-ulucan/baton/internal/state"
)

// Replay rebuilds a workflow's state from its events alone (SPEC 5.3).
// It never runs a decision policy: DecisionMade events are applied as
// recorded.
func (s *Store) Replay(ctx context.Context, wfID uuid.UUID) (*state.Snapshot, error) {
	events, err := s.Events(ctx, wfID)
	if err != nil {
		return nil, err
	}
	snap := state.NewSnapshot()
	for _, ev := range events {
		if err := state.Apply(snap, ev); err != nil {
			return nil, fmt.Errorf("replay %s: %w", wfID, err)
		}
	}
	snap.ResetTouched()
	return snap, nil
}

// Mismatch is a difference between a workflow's replayed state and its
// materialized projection.
type Mismatch struct {
	WorkflowID uuid.UUID
	Diffs      []string
}

func (m Mismatch) String() string {
	return fmt.Sprintf("workflow %s:\n  %s", m.WorkflowID, strings.Join(m.Diffs, "\n  "))
}

// VerifyReplay replays every workflow that has events and compares the
// result with the projection, column by column. It is the replay
// invariant checked by tests and after every chaos run (SPEC 13).
func (s *Store) VerifyReplay(ctx context.Context) ([]Mismatch, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT workflow_id FROM workflow_events GROUP BY workflow_id
		UNION SELECT id FROM workflows ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	ids, err := collectUUIDs(rows)
	if err != nil {
		return nil, err
	}
	var out []Mismatch
	for _, id := range ids {
		replayed, err := s.Replay(ctx, id)
		if err != nil {
			out = append(out, Mismatch{WorkflowID: id, Diffs: []string{err.Error()}})
			continue
		}
		stored, err := s.Snapshot(ctx, id)
		if err != nil {
			return nil, err
		}
		if d := DiffSnapshots(replayed, stored); len(d) > 0 {
			out = append(out, Mismatch{WorkflowID: id, Diffs: d})
		}
	}
	return out, nil
}

func collectUUIDs(rows pgx.Rows) ([]uuid.UUID, error) {
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// DiffSnapshots lists every field where replayed and stored differ, as
// "path: replayed != stored". An empty result means they are identical.
func DiffSnapshots(replayed, stored *state.Snapshot) []string {
	var d []string
	switch {
	case replayed.Workflow == nil && stored.Workflow == nil:
	case replayed.Workflow == nil || stored.Workflow == nil:
		d = append(d, fmt.Sprintf("workflow: replayed present=%t, stored present=%t", replayed.Workflow != nil, stored.Workflow != nil))
	default:
		d = append(d, diffStruct("workflow", *replayed.Workflow, *stored.Workflow)...)
	}
	ids := make([]uuid.UUID, 0, len(replayed.Tasks)+len(stored.Tasks))
	for id := range replayed.Tasks {
		ids = append(ids, id)
	}
	for id := range stored.Tasks {
		if _, ok := replayed.Tasks[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
	for _, id := range ids {
		r, rok := replayed.Tasks[id]
		st, sok := stored.Tasks[id]
		if !rok || !sok {
			d = append(d, fmt.Sprintf("task %s: replayed present=%t, stored present=%t", id, rok, sok))
			continue
		}
		d = append(d, diffStruct("task "+r.StepName+"/"+string(r.Kind), *r, *st)...)
	}
	return d
}

func diffStruct[T any](prefix string, a, b T) []string {
	var d []string
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	for i := range va.NumField() {
		fa, fb := va.Field(i), vb.Field(i)
		if !fieldEqual(fa, fb) {
			d = append(d, fmt.Sprintf("%s.%s: %s != %s", prefix, va.Type().Field(i).Name, show(fa), show(fb)))
		}
	}
	return d
}

// fieldEqual compares by value, treating times by instant and pointers by
// what they point to.
func fieldEqual(a, b reflect.Value) bool {
	if a.Kind() == reflect.Pointer {
		if a.IsNil() || b.IsNil() {
			return a.IsNil() && b.IsNil()
		}
		return fieldEqual(a.Elem(), b.Elem())
	}
	if t, ok := a.Interface().(interface{ Equal(time.Time) bool }); ok {
		return t.Equal(b.Interface().(time.Time))
	}
	return reflect.DeepEqual(a.Interface(), b.Interface())
}

func show(v reflect.Value) string {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "nil"
		}
		v = v.Elem()
	}
	return fmt.Sprintf("%v", v.Interface())
}

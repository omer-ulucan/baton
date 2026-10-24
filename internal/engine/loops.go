package engine

import (
	"context"

	"github.com/omer-ulucan/baton/internal/decision"
	"github.com/omer-ulucan/baton/internal/state"
	"github.com/omer-ulucan/baton/internal/store"
)

// ReapExpired returns attempts whose lease ran out to retry or the DLQ
// (SPEC 6.4). An expired lease consumes the attempt, so a task that keeps
// killing its workers ends in the DLQ instead of looping. The reaper never
// consults a decision policy (SPEC 10.2). It returns the number reaped.
func (e *Engine) ReapExpired(ctx context.Context) (int, error) {
	n := 0
	err := e.inTx(ctx, func(tx *store.Tx) error {
		n = 0
		if err := tx.PurgeRevokedLeases(ctx); err != nil {
			return err
		}
		leases, err := tx.ExpiredLeases(ctx, e.cfg.LoopBatch)
		if err != nil {
			return err
		}
		for _, l := range leases {
			snap, err := tx.Load(ctx, l.WorkflowID)
			if err != nil {
				return err
			}
			t := snap.Tasks[l.TaskID]
			if t.Status != state.TsRunning || t.CurrentAttempt == nil || *t.CurrentAttempt != l.AttemptID {
				// The attempt already ended; the lease row is a leftover.
				if err := tx.DeleteLease(ctx, l.TaskID); err != nil {
					return err
				}
				continue
			}
			def, err := e.defs.get(ctx, snap.Workflow.DefinitionID)
			if err != nil {
				return err
			}
			disposition, _ := e.dispose(def, t, decision.RetryWithBackoff, tx.Now())
			if err := e.commit(ctx, tx, snap, l.WorkflowID, []state.Change{
				{TaskID: t.ID, Payload: state.TaskLeaseExpired{AttemptID: l.AttemptID}},
				disposition,
			}); err != nil {
				return err
			}
			if err := tx.DeleteLease(ctx, l.TaskID); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if n > 0 {
		e.log.InfoContext(ctx, "reaped expired leases", "count", n)
	}
	return n, nil
}

// PromoteRetries moves tasks whose retry time has come back to ready.
// Tasks of paused workflows stay in retry_wait until resumed.
func (e *Engine) PromoteRetries(ctx context.Context) (int, error) {
	n := 0
	err := e.inTx(ctx, func(tx *store.Tx) error {
		n = 0
		refs, err := tx.DueRetries(ctx, e.cfg.LoopBatch)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			snap, err := tx.Load(ctx, ref.WorkflowID)
			if err != nil {
				return err
			}
			if err := e.commit(ctx, tx, snap, ref.WorkflowID, []state.Change{{TaskID: ref.TaskID, Payload: state.TaskReady{}}}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

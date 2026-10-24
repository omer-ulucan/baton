# Effectively-once handlers

Baton executes tasks **at least once** (SPEC 7, ADR 0005). A worker can
finish the work and crash before reporting it, a network partition can make
a live worker look dead, and a lease can expire under a slow handler. In
each case the task is retried and the handler runs again. No orchestrator
can avoid this without the cooperation of the system the handler changes.

What Baton does guarantee:

- **One attempt at a time.** A task is never held by two live leases. The
  concurrency test (`internal/engine/concurrency_test.go`) checks this
  against the event history under crashes and lease expiry.
- **One recorded result per attempt.** A result is accepted only from the
  worker holding the current attempt's live, unrevoked lease. Late results
  from superseded attempts get `409 attempt_superseded`; repeating an
  accepted result is a no-op.
- **A stable idempotency key.** Every task carries
  `idempotency_key = "<workflow_id>/<step>/<kind>"`. It is the same for every
  attempt of the task, and different for the step and its compensation.
  Handlers receive it as `worker.Task.IdempotencyKey`.

Effectively-once behaviour is built on that key, using one of two patterns.

## Pattern 1: pass the key downstream

When the system you call accepts idempotency keys (most payment providers,
many internal APIs), send Baton's key. A retried attempt then replays the
original response instead of repeating the side effect.

```go
func Charge(ctx context.Context, t worker.Task) (json.RawMessage, error) {
	var in struct {
		Customer string `json:"customer"`
		Cents    int64  `json:"cents"`
	}
	if err := json.Unmarshal(t.Input, &in); err != nil {
		return nil, worker.Permanent(err) // bad input will not fix itself
	}
	charge, err := payments.Charge(ctx, payments.ChargeRequest{
		Customer:       in.Customer,
		AmountCents:    in.Cents,
		IdempotencyKey: t.IdempotencyKey, // same on every attempt
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]string{"charge_id": charge.ID})
}
```

Check how long the downstream service remembers keys. It must be longer
than the task's whole retry window, i.e. the sum of all backoffs plus
attempt timeouts.

## Pattern 2: record the key with the side effect

When the side effect is a write to a database you own, record the key in
the same transaction as the write, under a unique constraint. A conflict
means an earlier attempt already committed, and is treated as success.

```sql
CREATE TABLE processed_tasks (
  idempotency_key text PRIMARY KEY,
  result          jsonb NOT NULL,
  processed_at    timestamptz NOT NULL DEFAULT now()
);
```

```go
func ReserveStock(ctx context.Context, t worker.Task) (json.RawMessage, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Already done by an earlier attempt? Return its result.
	var prev json.RawMessage
	err = tx.QueryRow(ctx, `SELECT result FROM processed_tasks WHERE idempotency_key = $1`,
		t.IdempotencyKey).Scan(&prev)
	if err == nil {
		return prev, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `UPDATE stock SET reserved = reserved + 1 WHERE sku = $1`, sku); err != nil {
		return nil, err
	}
	result := json.RawMessage(`{"reserved":1}`)
	// The unique key makes a concurrent duplicate fail here, and roll back
	// its stock update with it.
	if _, err := tx.Exec(ctx, `INSERT INTO processed_tasks (idempotency_key, result) VALUES ($1, $2)`,
		t.IdempotencyKey, result); err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}
```

Both checks matter. The `SELECT` makes retries after a commit cheap, and
the unique constraint makes two attempts that overlap (only possible if a
handler outlives its lease, see below) safe.

## Things that break effectively-once

- **Keys built from the attempt.** `AttemptID` changes on every attempt. Never
  use it as an idempotency key.
- **Side effects outside the key's protection.** Sending an email, then
  writing the dedupe row in a separate step, can still send twice. Put the
  side effect behind the key, or make it idempotent itself.
- **Ignoring cancellation.** When a lease is revoked (workflow cancelled) or
  the deadline passes, the handler's `ctx` is cancelled. A handler that
  keeps going may overlap a retry on another worker. Pass `ctx` to every
  call and return promptly when it ends.
- **Compensations.** A compensation can also run more than once, and may run
  after a forward step that only partly applied. Compensations use their
  own key (`.../compensation`) and must tolerate "nothing to undo".

## Marking errors

Return `worker.Permanent(err)` when retrying cannot help: invalid input, a
declined card, a missing record. The worker then reports
`retryable: false`, and the rule policy dead-letters the task instead of
burning its retries. Any other error is reported as possibly transient. In
both cases the decision layer makes the final call, and that call is recorded
in the workflow history as a `DecisionMade` event.

package store_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/omer-ulucan/baton/internal/store"
)

func TestMigrateIsIdempotent(t *testing.T) {
	_, url := pg.NewDB(t)
	if err := store.Migrate(url); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestMigrateDownAndUpAgain(t *testing.T) {
	pool, url := pg.NewDB(t)
	if err := store.MigrateDown(url); err != nil {
		t.Fatalf("MigrateDown: %v", err)
	}
	var n int
	err := pool.QueryRow(t.Context(),
		`SELECT count(*) FROM information_schema.tables
		  WHERE table_schema = 'public' AND table_name <> 'schema_migrations'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d tables left after down migration", n)
	}
	if err := store.Migrate(url); err != nil {
		t.Fatalf("Migrate after down: %v", err)
	}
}

func TestEventsAreAppendOnly(t *testing.T) {
	pool, _ := pg.NewDB(t)
	ctx := t.Context()
	_, err := pool.Exec(ctx, `INSERT INTO workflow_events
		(workflow_id, seq, type, payload, occurred_at, fencing_token)
		VALUES (gen_random_uuid(), 1, 'WorkflowSubmitted', '{}', now(), 1)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE workflow_events SET type = 'X'`,
		`DELETE FROM workflow_events`,
		`TRUNCATE workflow_events`,
	} {
		_, err := pool.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "BT002" {
			t.Errorf("%s: err = %v, want BT002", sql, err)
		}
	}
}

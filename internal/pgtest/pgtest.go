// Package pgtest runs integration tests against a real PostgreSQL 16.
//
// One container is started per test binary. The schema is migrated once
// into a template database, and every test gets its own database cloned
// from that template, so tests are isolated and cheap.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	image    = "postgres:16-alpine"
	template = "baton_template"
)

// Server is a running Postgres container with a migrated template database.
type Server struct {
	container *tcpostgres.PostgresContainer
	baseURL   *url.URL
	n         atomic.Int64
}

// Start launches Postgres and runs migrate against the template database.
func Start(ctx context.Context, migrate func(databaseURL string) error) (*Server, error) {
	c, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("baton"),
		tcpostgres.WithPassword("baton"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	s := &Server{container: c}
	raw, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = s.Close(ctx)
		return nil, err
	}
	if s.baseURL, err = url.Parse(raw); err != nil {
		_ = s.Close(ctx)
		return nil, err
	}
	if err := s.exec(ctx, "CREATE DATABASE "+template); err != nil {
		_ = s.Close(ctx)
		return nil, err
	}
	if err := migrate(s.url(template)); err != nil {
		_ = s.Close(ctx)
		return nil, fmt.Errorf("migrate template: %w", err)
	}
	return s, nil
}

// Close terminates the container.
func (s *Server) Close(ctx context.Context) error {
	return s.container.Terminate(ctx)
}

// NewDB creates a fresh, migrated database for t and returns a pool to it.
// The test is skipped when s is nil (Postgres not started, e.g. -short).
func (s *Server) NewDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	if s == nil {
		t.Skip("integration test: Postgres not available (-short)")
	}
	ctx := t.Context()
	name := fmt.Sprintf("t%d", s.n.Add(1))
	if err := s.exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, template)); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	dbURL := s.url(name)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, dbURL
}

func (s *Server) url(db string) string {
	u := *s.baseURL
	u.Path = "/" + db
	return u.String()
}

func (s *Server) exec(ctx context.Context, sql string) error {
	conn, err := pgx.Connect(ctx, s.url("postgres"))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, sql)
	return err
}

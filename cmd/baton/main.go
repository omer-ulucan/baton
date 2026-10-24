// Command baton is the orchestrator.
//
// Usage:
//
//	baton              run the orchestrator
//	baton migrate      apply pending database migrations and exit
//	baton healthcheck  probe the local /healthz (for container health checks)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/engine"
	"github.com/omer-ulucan/baton/internal/server"
	"github.com/omer-ulucan/baton/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(os.Getenv("BATON_HTTP_ADDR")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	var err error
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		err = migrate()
	} else {
		err = run(ctx, log)
	}
	stop()
	if err != nil {
		log.Error("baton exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	hostname, _ := os.Hostname()
	cfg, err := config.LoadOrchestrator(os.Getenv, hostname)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log = log.With("replica", cfg.ReplicaID)

	if cfg.AutoMigrate {
		if err := store.Migrate(cfg.DatabaseURL); err != nil {
			return err
		}
		log.Info("database migrated")
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()

	eng := engine.New(engine.Options{
		Store: store.New(pool),
		Log:   log,
		Config: engine.Config{
			LeaseTTL:          cfg.LeaseTTL,
			HeartbeatInterval: cfg.HeartbeatInterval,
		},
	})
	// Until leader election (phase 5) every replica runs the timer loops;
	// that is safe because they lock with SKIP LOCKED and Apply rejects
	// stale transitions, but it is not the intended single-runner design.
	loopsDone := make(chan struct{})
	go func() {
		defer close(loopsDone)
		eng.Run(ctx)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server.New(server.Deps{DB: pool, Worker: eng, Log: log}),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-loopsDone
	return nil
}

func migrate() error {
	hostname, _ := os.Hostname()
	cfg, err := config.LoadOrchestrator(os.Getenv, hostname)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return store.Migrate(cfg.DatabaseURL)
}

func healthcheck(addr string) error {
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("BATON_HTTP_ADDR: %w", err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz returned %d", resp.StatusCode)
	}
	return nil
}

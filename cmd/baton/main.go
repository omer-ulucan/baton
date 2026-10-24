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
	"github.com/omer-ulucan/baton/internal/orchestrator"
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

func loadConfig() (config.Orchestrator, error) {
	hostname, _ := os.Hostname()
	cfg, err := config.LoadOrchestrator(os.Getenv, hostname)
	if err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.AutoMigrate {
		if err := store.Migrate(cfg.DatabaseURL); err != nil {
			return err
		}
		log.Info("database migrated", "replica", cfg.ReplicaID)
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	return orchestrator.New(orchestrator.Options{Config: cfg, Pool: pool, Listener: ln, Log: log}).Run(ctx)
}

func migrate() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
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

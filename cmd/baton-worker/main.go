// Command baton-worker runs task handlers against an orchestrator. It talks
// to the orchestrator's HTTP API only and never connects to the database.
//
// This binary ships the built-in handlers (echo, sleep, flaky). Real
// deployments build their own worker binary around worker.Runner with
// their handlers.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/worker"
	"github.com/omer-ulucan/baton/internal/worker/builtin"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, log)
	stop()
	if err != nil {
		log.Error("baton-worker exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	hostname, _ := os.Hostname()
	cfg, err := config.LoadWorker(os.Getenv, hostname)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	// Claims long-poll for up to 20s; the client timeout must exceed it.
	client := worker.NewClient(cfg.OrchestratorAddrs, &http.Client{Timeout: 45 * time.Second})
	r := worker.NewRunner(client, builtin.Handlers(), worker.Config{
		ID:           cfg.WorkerID,
		Queues:       cfg.Queues,
		Concurrency:  cfg.Concurrency,
		DrainTimeout: cfg.DrainTimeout,
		Version:      version(),
	}, log)
	return r.Run(ctx)
}

func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		return bi.Main.Version
	}
	return ""
}

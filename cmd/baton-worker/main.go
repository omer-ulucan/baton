// Command baton-worker runs task handlers against an orchestrator.
//
// The worker protocol (register, claim, heartbeat, complete, fail) lands in
// phase 3. Until then the worker loads its configuration and waits for a
// shutdown signal, so the compose stack and process supervision can be
// exercised end to end.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/omer-ulucan/baton/internal/config"
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
	log.Info("worker started", "worker", cfg.WorkerID, "orchestrator", cfg.OrchestratorAddr, "queues", cfg.Queues)
	<-ctx.Done()
	log.Info("worker stopping", "worker", cfg.WorkerID)
	return nil
}

// Command baton is the orchestrator.
//
// Usage:
//
//	baton              run the orchestrator
//	baton migrate      apply pending database migrations and exit
//	baton model fetch  download and verify the pinned Laya model
//	                   [--small] [--variant fp32|fp16] [--dir DIR]
//	baton healthcheck  probe the local /healthz (for container health checks)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omer-ulucan/baton/internal/config"
	"github.com/omer-ulucan/baton/internal/laya/model"
	"github.com/omer-ulucan/baton/internal/orchestrator"
	"github.com/omer-ulucan/baton/internal/store"
	"github.com/omer-ulucan/baton/internal/telemetry"
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
	switch {
	case len(os.Args) > 1 && os.Args[1] == "migrate":
		err = migrate()
	case len(os.Args) > 2 && os.Args[1] == "model" && os.Args[2] == "fetch":
		err = fetchModel(ctx, os.Args[3:], log)
	default:
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
	tracing, err := telemetry.NewTracing(ctx, "baton", os.Getenv)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() { _ = tracing.Shutdown(context.WithoutCancel(ctx)) }()
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	pcfg.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithTracerProvider(tracing.Provider))
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	return orchestrator.New(orchestrator.Options{
		Config: cfg, Pool: pool, Listener: ln, Log: log, Tracer: tracing.Provider,
	}).Run(ctx)
}

// fetchModel downloads the pinned model files, verifying every one. It is
// how the image bakes the model and how CI gets the small files.
func fetchModel(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("model fetch", flag.ContinueOnError)
	small := fs.Bool("small", false, "only tokenizer.json and config.json")
	variant := fs.String("variant", "fp32", "fp32 or fp16")
	dir := fs.String("dir", os.Getenv("BATON_MODEL_DIR"), "model dir (default: the user cache)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		var err error
		if *dir, err = model.DefaultDir(); err != nil {
			return err
		}
	}
	files, err := model.Files(model.Variant(*variant), *small)
	if err != nil {
		return err
	}
	fe := &model.Fetcher{Log: func(f string, a ...any) { log.Info(fmt.Sprintf(f, a...)) }}
	if err := fe.Fetch(ctx, *dir, files); err != nil {
		return err
	}
	log.Info("model verified", "dir", *dir, "revision", model.Revision, "files", len(files))
	return nil
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

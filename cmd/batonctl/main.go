// Command batonctl is the Baton CLI.
//
// Phase 1 ships the command skeleton with `health`; submit, status, cancel,
// retry and DLQ commands arrive with the APIs they call.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
)

const usage = `usage: batonctl [--server URL] <command>

commands:
  health    check that the orchestrator is ready
  version   print the batonctl version

The server defaults to $BATON_ADDR, then http://localhost:8080.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("batonctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	defaultServer := getenv("BATON_ADDR")
	if defaultServer == "" {
		defaultServer = "http://localhost:8080"
	}
	serverURL := fs.String("server", defaultServer, "orchestrator base URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}

	switch cmd := fs.Arg(0); cmd {
	case "version":
		fmt.Fprintln(stdout, version())
		return 0
	case "health":
		if err := health(ctx, strings.TrimRight(*serverURL, "/")); err != nil {
			fmt.Fprintln(stderr, "not ready:", err)
			return 1
		}
		fmt.Fprintln(stdout, "ready")
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", cmd)
		fs.Usage()
		return 2
	}
}

func health(ctx context.Context, base string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(body.Status)
	}
	return nil
}

func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "unknown"
}

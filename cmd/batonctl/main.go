// Command batonctl is the Baton CLI, a thin client of the public REST API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
)

const usage = `usage: batonctl [--server URL] [--output table|json] <command> [args]

commands:
  submit -f FILE [--input JSON] [--priority N] [--idempotency-key K] [--callback URL]
  status ID [--events]       workflow status with tasks (and its history)
  list [--status S] [--limit N]
  cancel ID [--compensate] [--reason TEXT]
  pause ID
  resume ID
  dlq list [--queue Q] [--limit N]
  dlq retry TASK_ID
  queues                     load and limits per queue
  queues set QUEUE [--max-concurrency N] [--rate R] [--burst B]
                   [--max-depth D] [--admission reject|defer]
  health                     check that the orchestrator is ready
  version                    print the batonctl version

The server defaults to $BATON_ADDR, then http://localhost:8080.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

// cli is one invocation's context.
type cli struct {
	ctx    context.Context
	api    *client
	out    io.Writer
	json   bool
	readFn func(string) ([]byte, error)
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
	output := fs.String("output", "table", "table or json")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 || (*output != "table" && *output != "json") {
		fs.Usage()
		return 2
	}
	c := &cli{ctx: ctx, api: newClient(*serverURL), out: stdout, json: *output == "json", readFn: os.ReadFile}

	cmd, rest := fs.Arg(0), fs.Args()[1:]
	var err error
	switch cmd {
	case "version":
		fmt.Fprintln(stdout, version())
	case "health":
		err = c.health()
	case "submit":
		err = c.submit(rest)
	case "status":
		err = c.status(rest)
	case "list":
		err = c.list(rest)
	case "cancel":
		err = c.cancel(rest)
	case "pause", "resume":
		err = c.lifecycle(cmd, rest)
	case "dlq":
		err = c.dlq(rest)
	case "queues":
		err = c.queues(rest)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", cmd)
		fs.Usage()
		return 2
	}
	var ue usageError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ue):
		fmt.Fprintf(stderr, "%s\n\n", ue)
		fs.Usage()
		return 2
	default:
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
}

type usageError string

func (u usageError) Error() string { return string(u) }

// parse parses a subcommand's flags, allowing flags after positional
// arguments ("status ID --events"), and returns the positionals.
func parse(name string, args []string, define func(*flag.FlagSet)) ([]string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	define(fs)
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, usageError(fmt.Sprintf("%s: %v", name, err))
		}
		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}
	return pos, nil
}

func one(name string, pos []string, what string) (string, error) {
	if len(pos) != 1 || strings.TrimSpace(pos[0]) == "" {
		return "", usageError(fmt.Sprintf("%s needs exactly one %s", name, what))
	}
	return pos[0], nil
}

func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "unknown"
}

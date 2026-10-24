// Package server is the orchestrator's HTTP surface.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/omer-ulucan/baton/internal/telemetry"
)

// Pinger reports whether the database is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

const readyTimeout = 2 * time.Second

// Deps are the server's collaborators.
type Deps struct {
	DB       Pinger
	Worker   WorkerProtocol // nil disables the worker protocol endpoints
	Commands Commands       // nil disables the public write endpoints
	Reader   Reader         // nil disables the public read endpoints
	// Leadership, when set, makes followers forward writes to the leader
	// and /readyz report the replica's role. Without it every write runs
	// locally (single-replica setups and tests).
	Leadership Leadership
	ReplicaID  string
	Log        *slog.Logger
	// Metrics serves /metrics when set.
	Metrics http.Handler
	// Tracer, when set, traces every request, continuing the caller's
	// trace context.
	Tracer trace.TracerProvider
}

// New returns the orchestrator's HTTP handler.
func New(d Deps) http.Handler {
	db, log := d.DB, d.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	var fw *forwarder
	if d.Leadership != nil {
		fw = newForwarder(d.Leadership, d.ReplicaID, log)
	}
	if d.Worker != nil {
		registerWorkerRoutes(mux, d.Worker, fw, log)
	}
	registerPublicRoutes(mux, d.Commands, d.Reader, fw, log)
	if d.Metrics != nil {
		mux.Handle("GET /metrics", d.Metrics)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			// Details go to the log only; probes may be exposed publicly.
			log.WarnContext(ctx, "readiness check failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		body := map[string]string{"status": "ready"}
		if d.Leadership != nil {
			body["role"] = "follower"
			if d.Leadership.IsLeader() {
				body["role"] = "leader"
			}
		}
		writeJSON(w, http.StatusOK, body)
	})
	if d.Tracer == nil {
		return mux
	}
	return otelhttp.NewHandler(mux, "baton",
		otelhttp.WithTracerProvider(d.Tracer), otelhttp.WithPropagators(telemetry.Propagator),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			// The route pattern, not the path: ids would explode the names.
			if _, pattern := mux.Handler(r); pattern != "" {
				return pattern
			}
			return r.Method
		}),
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && r.URL.Path != "/metrics"
		}))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Package server is the orchestrator's HTTP surface.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Pinger reports whether the database is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

const readyTimeout = 2 * time.Second

// New returns the orchestrator's HTTP handler.
func New(db Pinger, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
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
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

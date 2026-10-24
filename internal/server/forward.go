package server

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
)

// Leadership tells the server whether this replica may execute writes and,
// if not, where the leader is (SPEC 6.3).
type Leadership interface {
	IsLeader() bool
	LeaderAddr() (string, bool)
}

// forwardedHeader marks a request a follower forwarded, so it is never
// forwarded twice (which could loop during a leadership change).
const forwardedHeader = "Baton-Forwarded-By"

// forwarder runs write requests locally on the leader and reverse-proxies
// them to the leader on followers. Reads never pass through it.
type forwarder struct {
	lead    Leadership
	self    string
	log     *slog.Logger
	mu      sync.Mutex
	proxies map[string]*httputil.ReverseProxy
}

func newForwarder(lead Leadership, self string, log *slog.Logger) *forwarder {
	return &forwarder{lead: lead, self: self, log: log, proxies: map[string]*httputil.ReverseProxy{}}
}

// write wraps a write handler.
func (f *forwarder) write(h http.HandlerFunc) http.Handler {
	if f == nil || f.lead == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.lead.IsLeader() {
			h(w, r)
			return
		}
		addr, ok := f.lead.LeaderAddr()
		if !ok || r.Header.Get(forwardedHeader) != "" {
			notLeader(w)
			return
		}
		f.proxy(addr).ServeHTTP(w, r)
	})
}

func (f *forwarder) proxy(addr string) *httputil.ReverseProxy {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.proxies[addr]; ok {
		return p
	}
	target, err := url.Parse(addr)
	if err != nil {
		f.log.Error("bad leader address", "addr", addr, "err", err)
		target = &url.URL{Scheme: "http", Host: "invalid"}
	}
	p := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
			pr.Out.Header.Set(forwardedHeader, f.self)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			f.log.WarnContext(r.Context(), "forward to leader failed", "leader", addr, "err", err)
			notLeader(w)
		},
	}
	f.proxies[addr] = p
	return p
}

// notLeader answers 503 so clients and workers retry shortly, by which
// time a leader has been elected or discovered.
func notLeader(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, "not_leader", "no reachable leader; retry shortly")
}

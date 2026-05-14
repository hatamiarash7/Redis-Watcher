package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/hatamiarash7/redis-watcher/internal/sentryx"
)

// Server wraps the Prometheus HTTP exposition endpoint and the health
// probes. Listening is started by Run; tests can drive the handler
// directly via Handler.
type Server struct {
	addr    string
	path    string
	srv     *http.Server
	handler http.Handler
	log     *slog.Logger
}

// NewServer builds a Server. It does not start listening.
func NewServer(addr, path string, reg *Registry, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(reg.r, promhttp.HandlerOpts{Registry: reg.r}))
	mux.HandleFunc("/healthz", healthzHandler)
	mux.HandleFunc("/readyz", readyzHandler(reg))

	// sentryx.WrapHTTP attaches a per-request hub and (when tracing is
	// enabled) opens an http.server transaction per request, so panics
	// in any of the handlers above are captured. No-op when Sentry is
	// disabled.
	handler := sentryx.WrapHTTP(mux)
	return &Server{
		addr:    addr,
		path:    path,
		handler: handler,
		log:     log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

// Handler exposes the composite mux. Useful for end-to-end tests that
// drive /metrics, /healthz, /readyz without binding a real socket.
func (s *Server) Handler() http.Handler { return s.handler }

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("metrics server listening", "addr", s.addr, "path", s.path)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

// healthzHandler is a liveness probe: it returns OK as long as the
// process is running and the HTTP server is accepting connections. It
// deliberately does NOT consult any internal state — Kubernetes treats a
// failed liveness probe as a restart trigger, and we never want to ask
// for a restart on transient downstream outages.
func healthzHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// readyzHandler is a readiness probe: it returns 503 when the watcher is
// not currently doing useful work. Kubernetes will remove the pod from
// service endpoints (it has none here) but, more importantly, scrapers
// like Argo Rollouts / blue-green deployers gate promotion on /readyz.
//
// Failure conditions:
//   - the MONITOR session is down (no events flowing),
//   - the role checker has never produced a determinate role,
//   - any output is currently in a failing state,
//   - any alert channel is currently in a failing state.
//
// The body is JSON for easy diagnosis.
func readyzHandler(reg *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		state := reg.Readiness()
		ready := state.MonitorConnected && state.RoleKnown &&
			len(state.FailingOutputs) == 0 &&
			len(state.FailingAlertChannels) == 0
		w.Header().Set("Content-Type", "application/json")
		if ready {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		body := map[string]any{
			"ready":                  ready,
			"monitor_connected":      state.MonitorConnected,
			"role_known":             state.RoleKnown,
			"failing_outputs":        state.FailingOutputs,
			"failing_alert_channels": state.FailingAlertChannels,
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			// Best-effort: response is already half-sent. Errors writing
			// the trailer cannot be surfaced to the client at this point.
			_, _ = fmt.Fprintln(w, err.Error())
		}
	}
}

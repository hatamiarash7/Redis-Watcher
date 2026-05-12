// Package metrics defines and serves Prometheus metrics for Redis Watcher.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// Registry bundles every metric exported by the application.
type Registry struct {
	r *prometheus.Registry

	ignored       map[string]struct{}
	trackSourceIP bool

	CommandsTotal      *prometheus.CounterVec
	CommandsByIPTotal  *prometheus.CounterVec
	CommandsByDBTotal  *prometheus.CounterVec
	SuspiciousTotal    *prometheus.CounterVec
	ParseErrorsTotal   prometheus.Counter
	ReconnectsTotal    prometheus.Counter
	DroppedEventsTotal *prometheus.CounterVec
	IgnoredEventsTotal *prometheus.CounterVec
	AlertsSentTotal    *prometheus.CounterVec
	AlertSendErrors    *prometheus.CounterVec
	BuildInfo          *prometheus.GaugeVec
	EventsProcessed    prometheus.Counter
	RedisIsMaster      prometheus.Gauge
	RedisRoleInfo      *prometheus.GaugeVec
	RoleTransitions    *prometheus.CounterVec
}

// New builds a Registry. The returned object owns its own *prometheus.Registry
// so we don't pollute the global default registry.
func New(ignoredCommands []string, trackSourceIP bool, version, commit string) *Registry {
	r := &Registry{
		r:             prometheus.NewRegistry(),
		ignored:       make(map[string]struct{}, len(ignoredCommands)),
		trackSourceIP: trackSourceIP,
	}
	for _, c := range ignoredCommands {
		r.ignored[c] = struct{}{}
	}

	r.CommandsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "commands_total",
		Help:      "Total Redis commands observed via MONITOR, labeled by command name and database.",
	}, []string{"command", "db"})

	r.CommandsByIPTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "commands_by_ip_total",
		Help:      "Total Redis commands observed, broken down by source IP.",
	}, []string{"command", "source_ip"})

	r.CommandsByDBTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "commands_by_db_total",
		Help:      "Total Redis commands observed, broken down by database number.",
	}, []string{"db"})

	r.SuspiciousTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "suspicious_commands_total",
		Help:      "Total suspicious commands seen (matched by the alert engine).",
	}, []string{"command", "source_ip"})

	r.ParseErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "parse_errors_total",
		Help:      "Total MONITOR lines that failed to parse.",
	})

	r.ReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "monitor_reconnects_total",
		Help:      "Total reconnect attempts against the upstream Redis.",
	})

	r.DroppedEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "dropped_events_total",
		Help:      "Events dropped because a downstream consumer was saturated.",
	}, []string{"consumer"})

	r.IgnoredEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "ignored_events_total",
		Help:      "Events filtered out by filter.ignored_commands before dispatch.",
	}, []string{"command"})

	r.AlertsSentTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alerts_sent_total",
		Help:      "Total alerts dispatched, labeled by channel and command.",
	}, []string{"channel", "command"})

	r.AlertSendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alert_send_errors_total",
		Help:      "Total alert send failures, labeled by channel.",
	}, []string{"channel"})

	r.EventsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "events_processed_total",
		Help:      "Total parsed events processed by the dispatcher.",
	})

	r.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "build_info",
		Help:      "Build information.",
	}, []string{"version", "commit"})

	r.RedisIsMaster = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "redis_is_master",
		Help:      "1 when the upstream Redis is the primary, 0 otherwise.",
	})

	r.RedisRoleInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "redis_role_info",
		Help:      "Current upstream Redis replication role. Value is always 1; the role is encoded in the `role` label.",
	}, []string{"role"})

	r.RoleTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "redis_role_transitions_total",
		Help:      "Total observed transitions in the upstream Redis role.",
	}, []string{"from", "to"})

	r.r.MustRegister(
		r.CommandsTotal,
		r.CommandsByIPTotal,
		r.CommandsByDBTotal,
		r.SuspiciousTotal,
		r.ParseErrorsTotal,
		r.ReconnectsTotal,
		r.DroppedEventsTotal,
		r.IgnoredEventsTotal,
		r.AlertsSentTotal,
		r.AlertSendErrors,
		r.EventsProcessed,
		r.BuildInfo,
		r.RedisIsMaster,
		r.RedisRoleInfo,
		r.RoleTransitions,
	)
	r.BuildInfo.WithLabelValues(version, commit).Set(1)
	return r
}

// Record updates the per-event counters.
func (r *Registry) Record(ev *event.Event) {
	r.EventsProcessed.Inc()
	if _, skip := r.ignored[ev.Command]; !skip {
		db := strconv.Itoa(ev.DB)
		r.CommandsTotal.WithLabelValues(ev.Command, db).Inc()
		r.CommandsByDBTotal.WithLabelValues(db).Inc()
		if r.trackSourceIP && ev.Source.IP != "" {
			r.CommandsByIPTotal.WithLabelValues(ev.Command, ev.Source.IP).Inc()
		}
	}
}

// RecordSuspicious increments the suspicious-command counter.
func (r *Registry) RecordSuspicious(command, sourceIP string) {
	r.SuspiciousTotal.WithLabelValues(command, sourceIP).Inc()
}

// SetRedisRole publishes the current upstream role for the
// `redis_watcher_redis_is_master` gauge and the `redis_role_info` label.
// Older label values are reset to 0 so a single role is always "1".
func (r *Registry) SetRedisRole(roleName string) {
	r.RedisRoleInfo.Reset()
	r.RedisRoleInfo.WithLabelValues(roleName).Set(1)
	if roleName == "master" {
		r.RedisIsMaster.Set(1)
	} else {
		r.RedisIsMaster.Set(0)
	}
}

// RecordRoleTransition bumps the role-transitions counter.
func (r *Registry) RecordRoleTransition(from, to string) {
	r.RoleTransitions.WithLabelValues(from, to).Inc()
}

// Gatherer exposes the underlying gatherer for Pushgateway integration.
func (r *Registry) Gatherer() prometheus.Gatherer { return r.r }

// Registerer exposes the underlying registerer (used for tests / extensions).
func (r *Registry) Registerer() prometheus.Registerer { return r.r }

// Server wraps the Prometheus HTTP exposition endpoint.
type Server struct {
	addr string
	path string
	srv  *http.Server
	log  *slog.Logger
}

// NewServer builds a Server. It does not start listening.
func NewServer(addr, path string, reg *Registry, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(reg.r, promhttp.HandlerOpts{Registry: reg.r}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return &Server{
		addr: addr,
		path: path,
		log:  log,
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

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

// Package metrics defines and serves Prometheus metrics for Redis Watcher.
//
// The Registry owns its own *prometheus.Registry so it never pollutes the
// global default registry. It also wires in the Go runtime and process
// collectors so a single /metrics scrape gives operators a complete view
// of the binary's health (CPU, memory, GC, FDs, goroutines) alongside the
// project-specific series.
//
// The exported series fall into four buckets:
//
//   - Health gauges (monitor_connected, last_event_timestamp_seconds,
//     output_failing, alert_channel_failing, redis_is_master). These are
//     the primary alerting surface — a single PromQL on each gives a
//     yes/no answer to "is the watcher doing its job".
//   - Throughput counters (events_processed_total, alerts_sent_total,
//     output_written_total, …). Use rate() over a window.
//   - Error counters (parse_errors_total, alert_send_errors_total,
//     output_errors_total, role_probe_failures_total). Use rate() and
//     compare to throughput for "error rate" alerts.
//   - Latency histograms (alert_send_duration_seconds,
//     output_write_duration_seconds, role_probe_duration_seconds,
//     monitor_session_duration_seconds). Use histogram_quantile() for SLO
//     tracking.
//
// Every method that increments a vector takes raw strings; cardinality
// is bounded by configuration (number of channels / outputs / commands)
// and the optional metrics.track_source_ip flag.
package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// Registry bundles every metric exported by the application.
//
// Fields named in TitleCase remain part of the package's surface; many
// of them are still accessed directly by tests or, in the case of
// AlertsSentTotal/AlertSendErrors, by the metricsReporter shim in
// internal/app. New code should prefer the Registry's typed methods
// (SetMonitorConnected, ObserveAlertSend, …) which centralise the
// label conventions and protect against typos.
type Registry struct {
	r *prometheus.Registry

	ignored       map[string]struct{}
	trackSourceIP bool

	// Throughput / volume.
	CommandsTotal     *prometheus.CounterVec
	CommandsByIPTotal *prometheus.CounterVec
	CommandsByDBTotal *prometheus.CounterVec
	EventsProcessed   prometheus.Counter

	// Suspicious / filtered.
	SuspiciousTotal    *prometheus.CounterVec
	IgnoredEventsTotal *prometheus.CounterVec

	// Errors / drops.
	ParseErrorsTotal       prometheus.Counter
	ReconnectsTotal        prometheus.Counter
	MonitorDroppedTotal    prometheus.Counter
	DroppedEventsTotal     *prometheus.CounterVec
	OutputErrorsTotal      *prometheus.CounterVec
	AlertSendErrors        *prometheus.CounterVec
	AlertRetryAttempts     *prometheus.CounterVec
	AlertDroppedTotal      prometheus.Counter
	AlertRateLimitedTotal  *prometheus.CounterVec
	RoleProbeSuccessTotal  prometheus.Counter
	RoleProbeFailuresTotal prometheus.Counter

	// Outputs.
	OutputWrittenTotal *prometheus.CounterVec
	OutputWriteSeconds *prometheus.HistogramVec
	OutputFailing      *prometheus.GaugeVec

	// Alerts.
	AlertsSentTotal       *prometheus.CounterVec
	AlertSendSeconds      *prometheus.HistogramVec
	AlertChannelFailing   *prometheus.GaugeVec
	AlertEngineQueueDepth prometheus.GaugeFunc

	// Connectivity / freshness.
	MonitorConnected           prometheus.Gauge
	LastEventTimestampSeconds  prometheus.Gauge
	LastRoleProbeTimestampSecs prometheus.Gauge
	MonitorSessionSeconds      prometheus.Histogram
	RoleProbeDurationSeconds   prometheus.Histogram
	RedisIsMaster              prometheus.Gauge
	RedisRoleInfo              *prometheus.GaugeVec
	RoleTransitions            *prometheus.CounterVec

	// Process / build.
	BuildInfo        *prometheus.GaugeVec
	StartTimeSeconds prometheus.Gauge

	// Health state used by /readyz. Set by the components above.
	healthMu             sync.RWMutex
	monitorConnected     bool
	roleKnown            bool
	failingOutputs       map[string]struct{}
	failingAlertChannels map[string]struct{}
}

// New builds a Registry. version/commit are stamped onto the
// redis_watcher_build_info gauge.
func New(ignoredCommands []string, trackSourceIP bool, version, commit string) *Registry {
	r := &Registry{
		r:                    prometheus.NewRegistry(),
		ignored:              make(map[string]struct{}, len(ignoredCommands)),
		trackSourceIP:        trackSourceIP,
		failingOutputs:       map[string]struct{}{},
		failingAlertChannels: map[string]struct{}{},
	}
	for _, c := range ignoredCommands {
		r.ignored[c] = struct{}{}
	}

	r.registerCommandMetrics()
	r.registerErrorMetrics()
	r.registerOutputMetrics()
	r.registerAlertMetrics()
	r.registerRoleMetrics()
	r.registerConnectivityMetrics()
	r.registerBuildMetrics(version, commit)
	r.registerCollectors()

	return r
}

func (r *Registry) registerCommandMetrics() {
	r.CommandsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "commands_total",
		Help:      "Total Redis commands observed via MONITOR, labeled by command name and database.",
	}, []string{"command", "db"})
	r.CommandsByIPTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "commands_by_ip_total",
		Help:      "Total Redis commands observed, broken down by source IP. Only populated when metrics.track_source_ip is enabled.",
	}, []string{"command", "source_ip"})
	r.CommandsByDBTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "commands_by_db_total",
		Help:      "Total Redis commands observed, broken down by database number.",
	}, []string{"db"})
	r.EventsProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "events_processed_total",
		Help:      "Total parsed events processed by the dispatcher (post-filter).",
	})
	r.SuspiciousTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "suspicious_commands_total",
		Help:      "Total suspicious commands matched by the alert engine.",
	}, []string{"command", "source_ip"})
	r.IgnoredEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "ignored_events_total",
		Help:      "Events dropped by filter.ignored_commands before dispatch.",
	}, []string{"command"})

	r.r.MustRegister(
		r.CommandsTotal, r.CommandsByIPTotal, r.CommandsByDBTotal,
		r.EventsProcessed, r.SuspiciousTotal, r.IgnoredEventsTotal,
	)
}

func (r *Registry) registerErrorMetrics() {
	r.ParseErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "parse_errors_total",
		Help:      "Total MONITOR lines that failed to parse.",
	})
	r.ReconnectsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "monitor_reconnects_total",
		Help:      "Total reconnect attempts against the upstream Redis MONITOR connection.",
	})
	r.MonitorDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "monitor_dropped_events_total",
		Help:      "Events dropped at the MONITOR/dispatcher boundary because the dispatcher channel was full.",
	})
	r.DroppedEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "dropped_events_total",
		Help:      "Events dropped because a downstream output consumer was saturated.",
	}, []string{"consumer"})
	r.OutputErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "output_errors_total",
		Help:      "Total output write errors, labeled by output name.",
	}, []string{"output"})
	r.AlertSendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alert_send_errors_total",
		Help:      "Total alert-send failures (one per failed attempt, including retries).",
	}, []string{"channel"})
	r.AlertRetryAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alert_retry_attempts_total",
		Help:      "Total retry attempts performed by the alert engine, labeled by channel.",
	}, []string{"channel"})
	r.AlertDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alert_dropped_total",
		Help:      "Alerts dropped because the alert engine's input queue was full.",
	})
	r.AlertRateLimitedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alert_rate_limited_total",
		Help:      "Alerts suppressed by the per-(command, source_ip) rate limiter.",
	}, []string{"command"})
	r.RoleProbeSuccessTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "role_probe_successes_total",
		Help:      "Successful INFO replication probes performed by the role checker.",
	})
	r.RoleProbeFailuresTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "role_probe_failures_total",
		Help:      "Failed INFO replication probes performed by the role checker.",
	})
	r.r.MustRegister(
		r.ParseErrorsTotal, r.ReconnectsTotal, r.MonitorDroppedTotal,
		r.DroppedEventsTotal, r.OutputErrorsTotal,
		r.AlertSendErrors, r.AlertRetryAttempts, r.AlertDroppedTotal,
		r.AlertRateLimitedTotal,
		r.RoleProbeSuccessTotal, r.RoleProbeFailuresTotal,
	)
}

func (r *Registry) registerOutputMetrics() {
	r.OutputWrittenTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "output_written_total",
		Help:      "Total events successfully written, labeled by output name.",
	}, []string{"output"})
	r.OutputWriteSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "redis_watcher",
		Name:      "output_write_duration_seconds",
		Help:      "Time spent in Sink.Write per output. Stdout/file writes are sub-millisecond; network sinks can be much slower.",
		Buckets: []float64{
			0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5,
		},
	}, []string{"output"})
	r.OutputFailing = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "output_failing",
		Help:      "1 when the output's last write failed and no successful write has happened since; 0 otherwise.",
	}, []string{"output"})

	r.r.MustRegister(r.OutputWrittenTotal, r.OutputWriteSeconds, r.OutputFailing)
}

func (r *Registry) registerAlertMetrics() {
	r.AlertsSentTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "redis_watcher",
		Name:      "alerts_sent_total",
		Help:      "Total alerts successfully dispatched, labeled by channel and command.",
	}, []string{"channel", "command"})
	r.AlertSendSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "redis_watcher",
		Name:      "alert_send_duration_seconds",
		Help:      "End-to-end channel.Send latency (a single attempt — retries are counted separately).",
		Buckets: []float64{
			0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"channel"})
	r.AlertChannelFailing = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "alert_channel_failing",
		Help:      "1 when the last dispatch (after all retries) failed for this channel; 0 otherwise. Use this as the primary alert source for notification breakage.",
	}, []string{"channel"})

	r.r.MustRegister(r.AlertsSentTotal, r.AlertSendSeconds, r.AlertChannelFailing)
}

func (r *Registry) registerRoleMetrics() {
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
	r.RoleProbeDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "redis_watcher",
		Name:      "role_probe_duration_seconds",
		Help:      "Time spent executing one `INFO replication` probe (dial + AUTH + INFO + parse).",
		Buckets: []float64{
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
		},
	})
	r.LastRoleProbeTimestampSecs = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "last_role_probe_timestamp_seconds",
		Help:      "Unix timestamp of the most recent successful role probe. Alert when (time() - this) exceeds 2× the probe interval.",
	})

	r.r.MustRegister(
		r.RedisIsMaster, r.RedisRoleInfo, r.RoleTransitions,
		r.RoleProbeDurationSeconds, r.LastRoleProbeTimestampSecs,
	)
}

func (r *Registry) registerConnectivityMetrics() {
	r.MonitorConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "monitor_connected",
		Help:      "1 while a MONITOR session is active, 0 while reconnecting or paused. Primary alerting surface for ingest health.",
	})
	r.LastEventTimestampSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "last_event_timestamp_seconds",
		Help:      "Unix timestamp of the most recently observed event. Alert when (time() - this) > expected idle window.",
	})
	r.MonitorSessionSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "redis_watcher",
		Name:      "monitor_session_duration_seconds",
		Help:      "Length of each MONITOR session. Sudden short sessions indicate Redis is repeatedly dropping the watcher (e.g. back-pressure).",
		Buckets:   []float64{1, 10, 60, 600, 3600, 21600, 86400},
	})

	r.r.MustRegister(r.MonitorConnected, r.LastEventTimestampSeconds, r.MonitorSessionSeconds)
}

func (r *Registry) registerBuildMetrics(version, commit string) {
	r.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "build_info",
		Help:      "Build information. Value is always 1; the version and commit are encoded in the labels.",
	}, []string{"version", "commit"})
	r.StartTimeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "redis_watcher",
		Name:      "start_time_seconds",
		Help:      "Unix timestamp of process start. Use to compute uptime: time() - this.",
	})
	r.r.MustRegister(r.BuildInfo, r.StartTimeSeconds)
	r.BuildInfo.WithLabelValues(version, commit).Set(1)
	r.StartTimeSeconds.Set(float64(time.Now().Unix()))
}

// registerCollectors wires the Go runtime + process collectors. They make
// the scrape complete enough to alert on memory/CPU/GC/FD pressure without
// needing a separate node exporter sidecar for the watcher process.
func (r *Registry) registerCollectors() {
	r.r.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
		Namespace: "redis_watcher",
	}))
}

// Record updates the per-event command counters.
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

// SetRedisRole publishes the current upstream role.
func (r *Registry) SetRedisRole(roleName string) {
	r.RedisRoleInfo.Reset()
	r.RedisRoleInfo.WithLabelValues(roleName).Set(1)
	if roleName == "master" {
		r.RedisIsMaster.Set(1)
	} else {
		r.RedisIsMaster.Set(0)
	}
	r.healthMu.Lock()
	r.roleKnown = roleName != "" && roleName != "unknown"
	r.healthMu.Unlock()
}

// RecordRoleTransition bumps the role-transitions counter.
func (r *Registry) RecordRoleTransition(from, to string) {
	r.RoleTransitions.WithLabelValues(from, to).Inc()
}

// --- monitor.MetricsSink -----------------------------------------------------

// SetMonitorConnected publishes the MONITOR session state.
func (r *Registry) SetMonitorConnected(connected bool) {
	v := 0.0
	if connected {
		v = 1
	}
	r.MonitorConnected.Set(v)
	r.healthMu.Lock()
	r.monitorConnected = connected
	r.healthMu.Unlock()
}

// ObserveLastEvent stamps the freshness gauge with the supplied time. Use
// the parsed timestamp from the MONITOR line (Redis sends second
// precision) rather than time.Now() so a paused clock on the watcher host
// stays visible in the metric.
func (r *Registry) ObserveLastEvent(t time.Time) {
	r.LastEventTimestampSeconds.Set(float64(t.Unix()))
}

// ObserveMonitorSession records the lifetime of one MONITOR connection.
func (r *Registry) ObserveMonitorSession(d time.Duration) {
	r.MonitorSessionSeconds.Observe(d.Seconds())
}

// IncrParseError bumps the parser-error counter.
func (r *Registry) IncrParseError() { r.ParseErrorsTotal.Inc() }

// IncrReconnect bumps the MONITOR reconnect counter.
func (r *Registry) IncrReconnect() { r.ReconnectsTotal.Inc() }

// IncrMonitorDropped bumps the MONITOR→dispatcher drop counter.
func (r *Registry) IncrMonitorDropped() { r.MonitorDroppedTotal.Inc() }

// --- output.MetricsSink ------------------------------------------------------

// IncrOutputWritten bumps the per-output written counter.
func (r *Registry) IncrOutputWritten(output string) {
	r.OutputWrittenTotal.WithLabelValues(output).Inc()
}

// IncrOutputDropped bumps the per-output drop counter.
func (r *Registry) IncrOutputDropped(output string) {
	r.DroppedEventsTotal.WithLabelValues(output).Inc()
}

// IncrOutputError bumps the per-output error counter.
func (r *Registry) IncrOutputError(output string) {
	r.OutputErrorsTotal.WithLabelValues(output).Inc()
}

// SetOutputFailing toggles the per-output health gauge. The Registry also
// remembers the latest state so /readyz can fail-closed when an output is
// stuck in a failing state.
func (r *Registry) SetOutputFailing(output string, failing bool) {
	v := 0.0
	if failing {
		v = 1
	}
	r.OutputFailing.WithLabelValues(output).Set(v)
	r.healthMu.Lock()
	if failing {
		r.failingOutputs[output] = struct{}{}
	} else {
		delete(r.failingOutputs, output)
	}
	r.healthMu.Unlock()
}

// ObserveOutputWrite records the latency of one Sink.Write call.
func (r *Registry) ObserveOutputWrite(output string, d time.Duration) {
	r.OutputWriteSeconds.WithLabelValues(output).Observe(d.Seconds())
}

// --- alert.Reporter additions ------------------------------------------------

// AlertSent is called once per channel/command on successful delivery.
func (r *Registry) AlertSent(channel, command string) {
	r.AlertsSentTotal.WithLabelValues(channel, command).Inc()
}

// AlertError is called once per failed channel.Send attempt (including
// each retry attempt). Pair with AlertSent and AlertRetry to derive the
// retry rate and the eventual-success rate.
func (r *Registry) AlertError(channel string, _ error) {
	r.AlertSendErrors.WithLabelValues(channel).Inc()
}

// SuspiciousObserved is the existing entry point kept for backwards
// compatibility with internal/alert.Reporter.
func (r *Registry) SuspiciousObserved(command, sourceIP string) {
	r.RecordSuspicious(command, sourceIP)
}

// AlertRetry is called every time the engine waits and tries again.
func (r *Registry) AlertRetry(channel string) {
	r.AlertRetryAttempts.WithLabelValues(channel).Inc()
}

// AlertDropped is called when the alert engine's input buffer is full.
func (r *Registry) AlertDropped() { r.AlertDroppedTotal.Inc() }

// AlertRateLimited is called when an alert is suppressed by the
// per-(command, source_ip) rate limiter.
func (r *Registry) AlertRateLimited(command string) {
	r.AlertRateLimitedTotal.WithLabelValues(command).Inc()
}

// ObserveAlertSend records latency of a single channel.Send attempt.
func (r *Registry) ObserveAlertSend(channel string, d time.Duration) {
	r.AlertSendSeconds.WithLabelValues(channel).Observe(d.Seconds())
}

// SetAlertChannelFailing flips the per-channel health gauge after the
// engine has finished retrying. Pair with the AlertRetry counter to
// distinguish "retried but recovered" from "channel is broken".
func (r *Registry) SetAlertChannelFailing(channel string, failing bool) {
	v := 0.0
	if failing {
		v = 1
	}
	r.AlertChannelFailing.WithLabelValues(channel).Set(v)
	r.healthMu.Lock()
	if failing {
		r.failingAlertChannels[channel] = struct{}{}
	} else {
		delete(r.failingAlertChannels, channel)
	}
	r.healthMu.Unlock()
}

// --- role.Checker MetricsSink additions --------------------------------------

// RecordRoleProbe records the outcome and latency of a single role probe.
func (r *Registry) RecordRoleProbe(success bool, d time.Duration) {
	r.RoleProbeDurationSeconds.Observe(d.Seconds())
	if success {
		r.RoleProbeSuccessTotal.Inc()
		r.LastRoleProbeTimestampSecs.Set(float64(time.Now().Unix()))
	} else {
		r.RoleProbeFailuresTotal.Inc()
	}
}

// --- /readyz support ---------------------------------------------------------

// ReadinessState is the snapshot consulted by the /readyz handler. It is
// intentionally a value type so tests and the HTTP handler can compare
// without locking the registry.
type ReadinessState struct {
	MonitorConnected     bool
	RoleKnown            bool
	FailingOutputs       []string
	FailingAlertChannels []string
}

// Readiness returns a snapshot of the components' last-known state.
func (r *Registry) Readiness() ReadinessState {
	r.healthMu.RLock()
	defer r.healthMu.RUnlock()
	out := ReadinessState{
		MonitorConnected: r.monitorConnected,
		RoleKnown:        r.roleKnown,
	}
	for o := range r.failingOutputs {
		out.FailingOutputs = append(out.FailingOutputs, o)
	}
	for c := range r.failingAlertChannels {
		out.FailingAlertChannels = append(out.FailingAlertChannels, c)
	}
	return out
}

// RegisterCollector exposes the underlying registerer so app/ can add
// the pipeline-depth collector (defined in collectors.go) without
// reaching into Registry's private state.
func (r *Registry) RegisterCollector(c prometheus.Collector) error {
	return r.r.Register(c)
}

// Gatherer exposes the underlying gatherer for Pushgateway integration.
func (r *Registry) Gatherer() prometheus.Gatherer { return r.r }

// Registerer exposes the underlying registerer (used for tests / extensions).
func (r *Registry) Registerer() prometheus.Registerer { return r.r }

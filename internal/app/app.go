// Package app wires together the configuration, MONITOR consumer, output
// pipeline, metrics server and alert engine. main.go is intentionally thin;
// almost everything lives here so it can be covered by tests.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/alert"
	"github.com/hatamiarash7/redis-watcher/internal/config"
	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/logging"
	"github.com/hatamiarash7/redis-watcher/internal/metrics"
	"github.com/hatamiarash7/redis-watcher/internal/monitor"
	"github.com/hatamiarash7/redis-watcher/internal/output"
	"github.com/hatamiarash7/redis-watcher/internal/sentryx"
)

// BuildInfo carries compile-time metadata propagated via -ldflags.
type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

// Run is the entry point used by cmd/redis-watcher/main.go. It loads the
// configuration, builds every subsystem and blocks until ctx is cancelled
// (typically by SIGINT/SIGTERM).
func Run(ctx context.Context, configPath string, info BuildInfo) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log, closer, err := logging.Build(cfg.Log)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}
	slog.SetDefault(log)

	flushSentry, err := sentryx.Init(cfg.Sentry, info.Version)
	if err != nil {
		return fmt.Errorf("init sentry: %w", err)
	}
	defer flushSentry()

	log.Info("redis-watcher starting",
		"version", info.Version, "commit", info.Commit, "date", info.Date,
		"redis_network", cfg.Redis.Network)

	reg := metrics.New(cfg.Metrics.IgnoredCommands, cfg.Metrics.TrackSourceIP, info.Version, info.Commit)

	outMgr, err := buildOutputs(cfg, log)
	if err != nil {
		return fmt.Errorf("build outputs: %w", err)
	}

	alertEngine, err := buildAlertEngine(cfg, reg, log)
	if err != nil {
		return fmt.Errorf("build alerts: %w", err)
	}

	events := make(chan *event.Event, cfg.Pipeline.EventBuffer)
	report := func(err error, kv ...any) {
		sentryx.Report(err, kv...)
	}
	mon := monitor.New(monitor.Options{
		Network:     cfg.Redis.Network,
		Address:     cfg.Redis.Address,
		Username:    cfg.Redis.Username,
		Password:    cfg.Redis.Password,
		DialTimeout: cfg.Redis.DialTimeout,
		ReadTimeout: cfg.Redis.ReadTimeout,
		BackoffMin:  cfg.Redis.BackoffMin,
		BackoffMax:  cfg.Redis.BackoffMax,
	}, events, log, report, cfg.Pipeline.DropOnFull)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	installSignalHandlers(runCtx, cancel, log)

	var wg sync.WaitGroup
	if cfg.Metrics.Enabled {
		srv := metrics.NewServer(cfg.Metrics.Address, cfg.Metrics.Path, reg, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.Run(runCtx); err != nil {
				log.Error("metrics server failed", "err", err)
				report(err, "component", "metrics_server")
			}
		}()
	}

	if outMgr != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := outMgr.Run(runCtx); err != nil {
				log.Error("outputs failed", "err", err)
			}
		}()
	}

	if alertEngine != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := alertEngine.Run(runCtx); err != nil {
				log.Error("alert engine failed", "err", err)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatch(runCtx, log, reg, mon, events, outMgr, alertEngine)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := mon.Run(runCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("monitor exited with error", "err", err)
			report(err, "component", "monitor")
		}
		cancel()
	}()

	<-runCtx.Done()
	log.Info("shutdown initiated")
	shutdownStart := time.Now()
	wg.Wait()
	log.Info("shutdown complete", "elapsed", time.Since(shutdownStart))
	return nil
}

func dispatch(
	ctx context.Context,
	log *slog.Logger,
	reg *metrics.Registry,
	mon *monitor.Client,
	source <-chan *event.Event,
	outMgr *output.Manager,
	alertEngine *alert.Engine,
) {
	statsTicker := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-source:
			if !ok {
				return
			}
			reg.Record(ev)
			if outMgr != nil {
				outMgr.Dispatch(ev)
			}
			if alertEngine != nil {
				alertEngine.Submit(ev)
			}
		case <-statsTicker.C:
			s := mon.Stats()
			log.Debug("monitor stats",
				"reconnects", s.Reconnections,
				"parse_errors", s.ParseErrors,
				"dropped", s.Dropped)
		}
	}
}

func buildOutputs(cfg *config.Config, log *slog.Logger) (*output.Manager, error) {
	mgr := output.NewManager(log)
	added := 0
	for _, oc := range cfg.Outputs {
		if !oc.Enabled {
			continue
		}
		var sink output.Sink
		switch oc.Type {
		case "stdout":
			sink = output.NewStdoutSink(oc.Format)
		case "file":
			sink = output.NewFileSink(output.FileOptions{
				Path:       oc.Path,
				MaxSizeMB:  oc.Rotation.MaxSizeMB,
				MaxBackups: oc.Rotation.MaxBackups,
				MaxAgeDays: oc.Rotation.MaxAgeDays,
				Compress:   oc.Rotation.Compress,
				LocalTime:  oc.Rotation.LocalTime,
				Format:     oc.Format,
			})
		case "udp", "tcp":
			s, err := output.NewNetSink(output.NetOptions{
				Network: oc.Type, Address: oc.Address,
				Timeout: oc.Timeout, Keepalive: oc.Keepalive,
				Format: oc.Format,
			})
			if err != nil {
				return nil, err
			}
			sink = s
		default:
			return nil, fmt.Errorf("unsupported output type %q", oc.Type)
		}
		mgr.Add(output.NewConsumer(sink, cfg.Pipeline.ConsumerBuffer, cfg.Pipeline.DropOnFull, log))
		added++
	}
	if added == 0 {
		log.Warn("no audit outputs enabled")
		return nil, nil
	}
	return mgr, nil
}

type metricsReporter struct {
	reg *metrics.Registry
}

func (m metricsReporter) AlertSent(channel, command string) {
	m.reg.AlertsSentTotal.WithLabelValues(channel, command).Inc()
}
func (m metricsReporter) AlertError(channel string, _ error) {
	m.reg.AlertSendErrors.WithLabelValues(channel).Inc()
}
func (m metricsReporter) SuspiciousObserved(command, sourceIP string) {
	m.reg.RecordSuspicious(command, sourceIP)
}

func buildAlertEngine(cfg *config.Config, reg *metrics.Registry, log *slog.Logger) (*alert.Engine, error) {
	if !cfg.Alerts.Enabled {
		return nil, nil
	}
	var channels []alert.Channel
	if cfg.Alerts.Telegram.Enabled {
		channels = append(channels, alert.NewTelegramChannel(
			cfg.Alerts.Telegram.BotToken,
			cfg.Alerts.Telegram.ChatID,
			cfg.Alerts.Telegram.Timeout))
	}
	if cfg.Alerts.Webhook.Enabled {
		channels = append(channels, alert.NewWebhookChannel(
			cfg.Alerts.Webhook.URL,
			cfg.Alerts.Webhook.Method,
			cfg.Alerts.Webhook.Headers,
			cfg.Alerts.Webhook.Timeout))
	}
	if cfg.Alerts.Pushgateway.Enabled {
		channels = append(channels, alert.NewPushgatewayChannel(
			cfg.Alerts.Pushgateway.URL,
			cfg.Alerts.Pushgateway.Job,
			cfg.Alerts.Pushgateway.Labels,
			cfg.Alerts.Pushgateway.Timeout))
	}
	if len(channels) == 0 {
		log.Warn("alerts.enabled but no channel is configured")
		return nil, nil
	}
	return alert.New(alert.Options{
		Channels:         channels,
		Commands:         cfg.Alerts.SuspiciousCommands,
		Patterns:         cfg.Alerts.Patterns,
		IgnoredSourceIPs: cfg.Alerts.IgnoredSourceIPs,
		RateLimitEnabled: cfg.Alerts.RateLimit.Enabled,
		RateWindow:       cfg.Alerts.RateLimit.Window,
		RateMaxAlerts:    cfg.Alerts.RateLimit.MaxAlerts,
		BufferSize:       cfg.Pipeline.ConsumerBuffer,
		DropOnFull:       cfg.Pipeline.DropOnFull,
		Log:              log,
		Reporter:         metricsReporter{reg: reg},
	})
}

func installSignalHandlers(_ context.Context, cancel context.CancelFunc, log *slog.Logger) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-ch
		log.Info("signal received", "signal", sig.String())
		cancel()
		// A second signal forces immediate exit.
		sig = <-ch
		log.Warn("second signal received, exiting immediately", "signal", sig.String())
		os.Exit(1)
	}()
}

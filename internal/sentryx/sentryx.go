// Package sentryx wraps the Sentry SDK with the project's defaults so the
// rest of the code can stay unaware of Sentry-specific types.
package sentryx

import (
	"errors"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/hatamiarash7/redis-watcher/internal/config"
)

// Init initializes the global Sentry client. It is safe to call when
// cfg.Enabled is false (it becomes a no-op).
func Init(cfg config.SentryConfig, version string) (flush func(), err error) {
	if !cfg.Enabled {
		return func() {}, nil
	}
	if cfg.DSN == "" {
		return nil, errors.New("sentry: DSN required when enabled")
	}
	opts := sentry.ClientOptions{
		Dsn:              cfg.DSN,
		Environment:      cfg.Environment,
		Release:          cfg.Release,
		SampleRate:       cfg.SampleRate,
		TracesSampleRate: cfg.TracesSampleRate,
		AttachStacktrace: cfg.AttachStacktrace,
		ServerName:       "redis-watcher",
	}
	if opts.Release == "" && version != "" {
		opts.Release = "redis-watcher@" + version
	}
	if err := sentry.Init(opts); err != nil {
		return nil, err
	}
	sentry.ConfigureScope(func(s *sentry.Scope) {
		s.SetTag("component", "redis-watcher")
	})
	return func() { sentry.Flush(2 * time.Second) }, nil
}

// Report captures an error with optional structured key/value pairs.
// It is safe to call when Sentry is disabled.
func Report(err error, kv ...any) {
	if err == nil {
		return
	}
	hub := sentry.CurrentHub()
	if hub == nil {
		return
	}
	client := hub.Client()
	if client == nil || client.Options().Dsn == "" {
		return
	}
	hub.WithScope(func(scope *sentry.Scope) {
		ctx := sentry.Context{}
		for i := 0; i+1 < len(kv); i += 2 {
			k, ok := kv[i].(string)
			if !ok {
				continue
			}
			ctx[k] = kv[i+1]
		}
		if len(ctx) > 0 {
			scope.SetContext("details", ctx)
		}
		hub.CaptureException(err)
	})
}

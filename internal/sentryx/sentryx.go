// Package sentryx wraps the Sentry SDK with the project's defaults so the
// rest of the code can stay unaware of Sentry-specific types. Every other
// package MUST go through sentryx — do not import getsentry/sentry-go
// elsewhere.
//
// All functions in this package are safe to call when Sentry is disabled
// (no DSN configured) — they degrade to no-ops without panicking, so
// callers don't need to guard each invocation behind a config check.
package sentryx

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"

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
		// EnableTracing is the master switch in sentry-go v0.46; without it
		// TracesSampleRate is ignored. Tying the two together keeps the
		// operator-facing surface a single knob.
		EnableTracing:    cfg.TracesSampleRate > 0,
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

// Enabled reports whether a Sentry client is configured. Callers should
// not need this for normal capture/tracing — every function in this
// package is already a safe no-op — but it can be useful when building
// expensive context (e.g. truncating a long string) that should be
// skipped entirely when Sentry is off.
func Enabled() bool {
	hub := sentry.CurrentHub()
	if hub == nil {
		return false
	}
	client := hub.Client()
	return client != nil && client.Options().Dsn != ""
}

// Report captures an error with optional structured key/value pairs.
// Equivalent to Capture(context.Background(), err, kv...). Prefer Capture
// when you have a context available so the event lands on the right hub
// and is correlated with the active transaction.
func Report(err error, kv ...any) {
	Capture(context.Background(), err, kv...)
}

// Capture captures an exception, attaching it to the hub bound to ctx
// (typically by an enclosing transaction). Falls back to the global hub
// when ctx is bare. Safe when Sentry is disabled.
func Capture(ctx context.Context, err error, kv ...any) {
	if err == nil {
		return
	}
	hub := sentry.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentry.CurrentHub()
	}
	if hub == nil {
		return
	}
	client := hub.Client()
	if client == nil || client.Options().Dsn == "" {
		return
	}
	hub.WithScope(func(scope *sentry.Scope) {
		details := sentry.Context{}
		for i := 0; i+1 < len(kv); i += 2 {
			k, ok := kv[i].(string)
			if !ok {
				continue
			}
			details[k] = kv[i+1]
		}
		if len(details) > 0 {
			scope.SetContext("details", details)
		}
		hub.CaptureException(err)
	})
}

// FinishSpan is the callback returned by StartSpan. It records the final
// status of the span from the supplied error (nil = ok) and submits it.
// Always call it (typically via defer). Passing nil is the common case.
type FinishSpan func(err error)

// StartSpan starts a tracing span on ctx. If ctx already carries a
// transaction, the new span is a child; otherwise a root transaction is
// created.
//
//	op           identifies the operation kind ("http.client",
//	             "db.redis.info", "task.alert", ...) -- see Sentry's
//	             OpenTelemetry semantic conventions.
//	description  is a short, human-readable label (e.g. "telegram").
//
// The returned context carries the span; passing it down preserves
// causality for any nested StartSpan calls. Always invoke the returned
// FinishSpan to submit the span. Safe when Sentry tracing is disabled —
// in that case the span is sampled out and the callback is a no-op.
func StartSpan(ctx context.Context, op, description string) (context.Context, FinishSpan) {
	var span *sentry.Span
	if sentry.SpanFromContext(ctx) != nil {
		span = sentry.StartSpan(ctx, op)
		span.Description = description
	} else {
		span = sentry.StartTransaction(ctx, description)
		span.Op = op
	}
	finish := func(err error) {
		if err != nil {
			span.Status = sentry.SpanStatusInternalError
			span.SetData("error", err.Error())
		} else {
			span.Status = sentry.SpanStatusOK
		}
		span.Finish()
	}
	return span.Context(), finish
}

// SetSpanData attaches a structured key/value pair to the span attached
// to ctx (if any). Use it for searchable attributes like command names or
// destination URLs that are useful when triaging a transaction in Sentry.
// No-op when ctx carries no span.
func SetSpanData(ctx context.Context, key string, value any) {
	if span := sentry.SpanFromContext(ctx); span != nil {
		span.SetData(key, value)
	}
}

// SetSpanTag attaches a string tag to the span on ctx (if any). Tags are
// indexed and useful for filtering in the Sentry UI; prefer SetSpanData
// for higher-cardinality structured values.
func SetSpanTag(ctx context.Context, key, value string) {
	if span := sentry.SpanFromContext(ctx); span != nil {
		span.SetTag(key, value)
	}
}

// WrapHTTP returns an http.Handler that binds a fresh hub to each
// request, recovers panics into Sentry, and (when tracing is enabled)
// starts an http.server transaction per request. When Sentry is disabled
// the returned handler is the input unchanged.
func WrapHTTP(h http.Handler) http.Handler {
	if !Enabled() {
		return h
	}
	return sentryhttp.New(sentryhttp.Options{Repanic: true}).Handle(h)
}

// Recover captures any panic that may be happening on the calling
// goroutine. Intended as `defer sentryx.Recover()` at the top of
// goroutines we own. It always re-panics so process supervisors still
// see a crash.
func Recover() {
	if r := recover(); r != nil {
		hub := sentry.CurrentHub()
		if hub != nil {
			hub.Recover(r)
			hub.Flush(2 * time.Second)
		}
		panic(r)
	}
}

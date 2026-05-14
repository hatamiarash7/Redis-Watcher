package sentryx

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/hatamiarash7/redis-watcher/internal/config"
)

// recorder is a sentry.Transport that captures events in-memory so tests
// can assert on what would have been sent over the wire.
type recorder struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (r *recorder) Flush(time.Duration) bool              { return true }
func (r *recorder) FlushWithContext(context.Context) bool { return true }
func (r *recorder) Configure(sentry.ClientOptions)        {}
func (r *recorder) Close()                                {}
func (r *recorder) SendEvent(e *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) snapshot() []*sentry.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*sentry.Event, len(r.events))
	copy(out, r.events)
	return out
}

// withRecorder swaps in a recording transport on the global hub and
// returns it along with a teardown that restores the previous client.
//
// We avoid going through Init because we need control over the transport
// and TracesSampleRate values for individual tests.
func withRecorder(t *testing.T, tracesSampleRate float64) *recorder {
	t.Helper()
	prev := sentry.CurrentHub().Client()
	rec := &recorder{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              "https://public@example.com/1",
		Transport:        rec,
		SampleRate:       1.0,
		TracesSampleRate: tracesSampleRate,
		EnableTracing:    tracesSampleRate > 0,
	})
	if err != nil {
		t.Fatalf("new sentry client: %v", err)
	}
	sentry.CurrentHub().BindClient(client)
	t.Cleanup(func() { sentry.CurrentHub().BindClient(prev) })
	return rec
}

func TestCaptureNoopWhenDisabled(t *testing.T) {
	t.Parallel()
	// No client bound: capture should not panic.
	hub := sentry.NewHub(nil, sentry.NewScope())
	ctx := sentry.SetHubOnContext(context.Background(), hub)
	Capture(ctx, errors.New("ignored"), "key", "value")
	if Enabled() {
		t.Fatalf("Enabled() should be false when no DSN is set")
	}
}

func TestCaptureNilError(t *testing.T) {
	t.Parallel()
	// Should not crash when the error is nil.
	Capture(context.Background(), nil, "k", "v")
}

func TestCaptureEmitsEvent(t *testing.T) {
	rec := withRecorder(t, 0)
	Capture(context.Background(), errors.New("boom"),
		"stage", "test",
		"redis_address", "localhost:6379",
	)
	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if len(ev.Exception) == 0 || ev.Exception[0].Value != "boom" {
		t.Fatalf("expected exception 'boom', got %+v", ev.Exception)
	}
	details, ok := ev.Contexts["details"]
	if !ok {
		t.Fatalf("expected 'details' context, got %+v", ev.Contexts)
	}
	if details["stage"] != "test" || details["redis_address"] != "localhost:6379" {
		t.Fatalf("unexpected context payload: %+v", details)
	}
}

func TestStartSpanEmitsTransactionWhenTracingEnabled(t *testing.T) {
	rec := withRecorder(t, 1.0)
	ctx, finish := StartSpan(context.Background(), "task.alert", "alert.dispatch")
	SetSpanTag(ctx, "command", "FLUSHALL")
	SetSpanData(ctx, "source_ip", "10.0.0.5")
	// Nested child span should attach to the transaction.
	_, finishChild := StartSpan(ctx, "http.client", "alert.send.telegram")
	finishChild(nil)
	finish(nil)
	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 transaction, got %d events", len(events))
	}
	tx := events[0]
	if tx.Type != "transaction" {
		t.Fatalf("expected transaction event, got type %q", tx.Type)
	}
	if tx.Transaction != "alert.dispatch" {
		t.Fatalf("expected transaction name 'alert.dispatch', got %q", tx.Transaction)
	}
	// Child span recorded in the Spans slice.
	if len(tx.Spans) != 1 {
		t.Fatalf("expected 1 child span, got %d", len(tx.Spans))
	}
	if tx.Spans[0].Description != "alert.send.telegram" {
		t.Fatalf("expected child description 'alert.send.telegram', got %q", tx.Spans[0].Description)
	}
}

func TestStartSpanSetsStatusFromError(t *testing.T) {
	rec := withRecorder(t, 1.0)
	_, finish := StartSpan(context.Background(), "task.alert", "alert.dispatch")
	finish(errors.New("network down"))
	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 transaction, got %d events", len(events))
	}
	got := events[0].Contexts["trace"]["status"]
	if got != sentry.SpanStatusInternalError {
		t.Fatalf("expected trace.status=internal_error, got %v (%T)", got, got)
	}
	if data, ok := events[0].Contexts["trace"]["data"].(map[string]any); !ok || data["error"] != "network down" {
		t.Fatalf("expected trace.data.error=network down, got %v", events[0].Contexts["trace"]["data"])
	}
}

func TestStartSpanNoopWhenTracingDisabled(t *testing.T) {
	rec := withRecorder(t, 0)
	_, finish := StartSpan(context.Background(), "task.alert", "alert.dispatch")
	finish(nil)
	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("expected no transactions when tracing disabled, got %d", got)
	}
}

func TestWrapHTTPCapturesPanics(t *testing.T) {
	rec := withRecorder(t, 0)
	handler := WrapHTTP(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(errors.New("handler boom"))
	}))
	srv := httptest.NewUnstartedServer(handler)
	// Suppress the expected panic stack trace from net/http's recover.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.Start()
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
	}
	// Allow async send to register.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(rec.snapshot()) == 0 {
		t.Fatalf("expected panic to be captured")
	}
}

func TestInitDisabled(t *testing.T) {
	t.Parallel()
	flush, err := Init(config.SentryConfig{Enabled: false}, "v0")
	if err != nil {
		t.Fatalf("Init disabled returned err: %v", err)
	}
	flush()
}

func TestInitRequiresDSN(t *testing.T) {
	t.Parallel()
	_, err := Init(config.SentryConfig{Enabled: true}, "v0")
	if err == nil {
		t.Fatalf("expected error when DSN missing")
	}
}

package alert

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/sentryx/sentryxtest"
)

type fakeChannel struct {
	mu   sync.Mutex
	name string
	got  []Alert
	err  error
}

func (f *fakeChannel) Name() string { return f.name }
func (f *fakeChannel) Send(_ context.Context, a Alert) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, a)
	return nil
}
func (*fakeChannel) Close() error { return nil }

func newEvent(cmd string, sub string, args []string, ip string) *event.Event {
	full := append([]string{}, args...)
	if sub != "" {
		full = append([]string{sub}, args...)
	}
	return &event.Event{
		Timestamp:  time.Unix(1700000000, 0).UTC(),
		Source:     event.Source{IP: ip, Port: "5555"},
		Command:    cmd,
		Subcommand: sub,
		Args:       full,
	}
}

func TestEngineDispatchesSuspiciousCommand(t *testing.T) {
	ch := &fakeChannel{name: "fake"}
	e, err := New(Options{
		Channels:    []Channel{ch},
		Commands:    []string{"FLUSHALL", "CONFIG"},
		BufferSize:  4,
		DropOnFull:  false,
		SendTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	e.Submit(newEvent("FLUSHALL", "", nil, "1.2.3.4"))
	e.Submit(newEvent("GET", "", []string{"k"}, "1.2.3.4"))
	e.Submit(newEvent("CONFIG", "SET", []string{"maxmemory", "0"}, "1.2.3.4"))

	waitFor(t, func() bool {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		return len(ch.got) == 2
	}, time.Second)
}

func TestEnginePatternMatch(t *testing.T) {
	ch := &fakeChannel{name: "fake"}
	e, err := New(Options{
		Channels:    []Channel{ch},
		Patterns:    []string{`(?i)SCAN .*COUNT 10000`},
		BufferSize:  4,
		SendTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	e.Submit(&event.Event{
		Command: "SCAN", Args: []string{"0", "COUNT", "10000"},
		Source: event.Source{IP: "1.1.1.1", Port: "1"},
	})

	waitFor(t, func() bool {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		return len(ch.got) == 1
	}, time.Second)
}

func TestEngineIgnoresAllowedIP(t *testing.T) {
	ch := &fakeChannel{name: "fake"}
	e, err := New(Options{
		Channels:         []Channel{ch},
		Commands:         []string{"FLUSHALL"},
		IgnoredSourceIPs: []string{"127.0.0.1"},
		BufferSize:       4,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	e.Submit(newEvent("FLUSHALL", "", nil, "127.0.0.1"))
	time.Sleep(100 * time.Millisecond)
	if len(ch.got) != 0 {
		t.Errorf("expected suppression, got %d", len(ch.got))
	}
}

func TestEngineRateLimits(t *testing.T) {
	ch := &fakeChannel{name: "fake"}
	e, err := New(Options{
		Channels:         []Channel{ch},
		Commands:         []string{"FLUSHALL"},
		RateLimitEnabled: true,
		RateWindow:       time.Minute,
		RateMaxAlerts:    2,
		BufferSize:       16,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	for i := 0; i < 5; i++ {
		e.Submit(newEvent("FLUSHALL", "", nil, "8.8.8.8"))
	}
	time.Sleep(200 * time.Millisecond)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.got) != 2 {
		t.Errorf("expected 2 alerts after rate limit, got %d", len(ch.got))
	}
}

func TestEngineReportsErrors(t *testing.T) {
	failing := &fakeChannel{name: "fail", err: errors.New("nope")}
	rep := &countingReporter{}
	e, err := New(Options{
		Channels:    []Channel{failing},
		Commands:    []string{"FLUSHALL"},
		BufferSize:  4,
		SendTimeout: time.Second,
		Reporter:    rep,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()
	e.Submit(newEvent("FLUSHALL", "", nil, "1.1.1.1"))
	waitFor(t, func() bool { return rep.errors.Load() == 1 && rep.suspicious.Load() == 1 }, time.Second)
}

func TestEngineReportsFailedSendToSentry(t *testing.T) {
	// Tracing disabled: only errors should reach Sentry — no transactions
	// to confuse the assertion.
	rec := sentryxtest.Swap(t, false)
	failing := &fakeChannel{name: "telegram", err: errors.New("api unreachable")}
	e, err := New(Options{
		Channels:    []Channel{failing},
		Commands:    []string{"FLUSHALL"},
		BufferSize:  4,
		SendTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()
	e.Submit(newEvent("FLUSHALL", "", nil, "10.0.0.5"))

	if !rec.WaitFor(time.Second, func(ev sentryxtest.CapturedEvent) bool {
		if len(ev.Exceptions) == 0 || !strings.Contains(ev.Exceptions[0], "api unreachable") {
			return false
		}
		return ev.Details["channel"] == "telegram" && ev.Details["source_ip"] == "10.0.0.5"
	}) {
		t.Fatalf("expected sentry event for failed alert; got %+v", rec.Events())
	}
}

func TestEngineEmitsTransactionsForAlertDispatch(t *testing.T) {
	rec := sentryxtest.Swap(t, true)
	ch := &fakeChannel{name: "telegram"}
	e, err := New(Options{
		Channels:    []Channel{ch},
		Commands:    []string{"FLUSHALL"},
		BufferSize:  4,
		SendTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()
	e.Submit(newEvent("FLUSHALL", "", nil, "10.0.0.5"))

	if !rec.WaitFor(time.Second, func(ev sentryxtest.CapturedEvent) bool {
		return ev.Type == "transaction" && ev.Transaction == "alert.dispatch" && ev.Tags["command"] == "FLUSHALL"
	}) {
		t.Fatalf("expected alert.dispatch transaction with command tag; got %+v", rec.Events())
	}
}

type countingReporter struct {
	sent       atomic.Int64
	errors     atomic.Int64
	suspicious atomic.Int64
}

func (c *countingReporter) AlertSent(string, string)          { c.sent.Add(1) }
func (c *countingReporter) AlertError(string, error)          { c.errors.Add(1) }
func (c *countingReporter) SuspiciousObserved(string, string) { c.suspicious.Add(1) }

func TestWebhookChannel(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ch := NewWebhookChannel(srv.URL, "POST", map[string]string{"X-Test": "1"}, time.Second)
	err := ch.Send(context.Background(), Alert{
		Timestamp: time.Unix(1700000000, 0).UTC(),
		Command:   "FLUSHALL", Source: "1.1.1.1:1", DB: 0, Reason: "test",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !strings.Contains(got, `"command":"FLUSHALL"`) {
		t.Errorf("payload: %s", got)
	}
}

func TestWebhookChannelStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	ch := NewWebhookChannel(srv.URL, "POST", nil, time.Second)
	err := ch.Send(context.Background(), Alert{Command: "X"})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error, got %v", err)
	}
}

func waitFor(t *testing.T, fn func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

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

// scriptedChannel returns the i-th error from a script for the i-th Send
// call. A nil entry counts as success. Excess calls return success too.
type scriptedChannel struct {
	mu     sync.Mutex
	name   string
	script []error
	idx    int
	calls  atomic.Int64
}

func (s *scriptedChannel) Name() string { return s.name }
func (s *scriptedChannel) Send(_ context.Context, _ Alert) error {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.script) {
		return nil
	}
	err := s.script[s.idx]
	s.idx++
	return err
}
func (*scriptedChannel) Close() error { return nil }

func TestEngineRetriesUntilSuccess(t *testing.T) {
	ch := &scriptedChannel{
		name:   "telegram",
		script: []error{errors.New("503"), errors.New("503"), nil},
	}
	rep := &countingReporter{}
	e, err := New(Options{
		Channels:            []Channel{ch},
		Commands:            []string{"FLUSHALL"},
		BufferSize:          4,
		SendTimeout:         time.Second,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 5 * time.Millisecond,
		RetryMaxBackoff:     20 * time.Millisecond,
		Reporter:            rep,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	e.Submit(newEvent("FLUSHALL", "", nil, "10.0.0.1"))

	waitFor(t, func() bool { return rep.sent.Load() == 1 }, time.Second)
	if ch.calls.Load() != 3 {
		t.Errorf("expected 3 channel calls, got %d", ch.calls.Load())
	}
	// First two attempts are failed → 2 error increments.
	if rep.errors.Load() != 2 {
		t.Errorf("expected 2 error increments (one per retry), got %d", rep.errors.Load())
	}
}

func TestEngineRetriesExhausted(t *testing.T) {
	ch := &scriptedChannel{
		name:   "telegram",
		script: []error{errors.New("503"), errors.New("503"), errors.New("503")},
	}
	rep := &countingReporter{}
	rec := sentryxtest.Swap(t, false)
	e, err := New(Options{
		Channels:            []Channel{ch},
		Commands:            []string{"FLUSHALL"},
		BufferSize:          4,
		SendTimeout:         time.Second,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 5 * time.Millisecond,
		RetryMaxBackoff:     20 * time.Millisecond,
		Reporter:            rep,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	e.Submit(newEvent("FLUSHALL", "", nil, "10.0.0.2"))

	waitFor(t, func() bool { return rep.errors.Load() == 3 }, time.Second)
	if rep.sent.Load() != 0 {
		t.Errorf("no success expected, sent=%d", rep.sent.Load())
	}
	if ch.calls.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", ch.calls.Load())
	}

	// Sentry should see exactly one captured exception for the channel
	// (not one per attempt) — and it carries the attempts count.
	if !rec.WaitFor(time.Second, func(ev sentryxtest.CapturedEvent) bool {
		if len(ev.Exceptions) == 0 {
			return false
		}
		return ev.Details["channel"] == "telegram" &&
			ev.Details["attempts"] == 3 &&
			ev.Details["max_attempts"] == 3
	}) {
		t.Fatalf("expected one final Sentry capture with attempts=3; got %+v", rec.Events())
	}
}

func TestEngineNoRetryWhenDisabled(t *testing.T) {
	ch := &scriptedChannel{
		name:   "telegram",
		script: []error{errors.New("boom"), nil}, // would succeed on retry
	}
	rep := &countingReporter{}
	e, err := New(Options{
		Channels:         []Channel{ch},
		Commands:         []string{"FLUSHALL"},
		BufferSize:       4,
		SendTimeout:      time.Second,
		RetryMaxAttempts: 1, // explicit "no retries"
		Reporter:         rep,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	e.Submit(newEvent("FLUSHALL", "", nil, "10.0.0.3"))
	waitFor(t, func() bool { return rep.errors.Load() == 1 }, time.Second)
	if ch.calls.Load() != 1 {
		t.Errorf("expected exactly 1 call with retries disabled, got %d", ch.calls.Load())
	}
}

func TestEngineRetryAbortsOnShutdown(t *testing.T) {
	// Long backoff plus a small Send timeout ensures we are sleeping when
	// the context cancels. The retry loop must drop back to the dispatcher
	// promptly instead of waiting out the full backoff.
	ch := &scriptedChannel{
		name:   "telegram",
		script: []error{errors.New("503"), errors.New("503"), errors.New("503")},
	}
	e, err := New(Options{
		Channels:            []Channel{ch},
		Commands:            []string{"FLUSHALL"},
		BufferSize:          4,
		SendTimeout:         50 * time.Millisecond,
		RetryMaxAttempts:    5,
		RetryInitialBackoff: 5 * time.Second,
		RetryMaxBackoff:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = e.Run(ctx) }()

	e.Submit(newEvent("FLUSHALL", "", nil, "10.0.0.4"))
	// Wait for the first attempt to fail, then cancel.
	waitFor(t, func() bool { return ch.calls.Load() >= 1 }, time.Second)
	start := time.Now()
	cancel()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		// After cancellation no further calls should happen.
		time.Sleep(20 * time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("retry loop did not bail promptly on shutdown (elapsed %s)", elapsed)
	}
	if ch.calls.Load() > 2 {
		t.Errorf("expected retry loop to stop after cancel, observed %d calls", ch.calls.Load())
	}
}

func TestNextBackoff(t *testing.T) {
	t.Parallel()
	cases := []struct {
		cur, maxBackoff, want time.Duration
	}{
		{100 * time.Millisecond, time.Second, 200 * time.Millisecond},
		{800 * time.Millisecond, time.Second, time.Second}, // capped
		{2 * time.Second, time.Second, time.Second},        // already over cap
		{100 * time.Millisecond, 0, 200 * time.Millisecond},
		{40 * time.Second, 0, time.Minute}, // safety cap when max=0
	}
	for _, c := range cases {
		got := nextBackoff(c.cur, c.maxBackoff)
		if got != c.want {
			t.Errorf("nextBackoff(%s, %s) = %s, want %s", c.cur, c.maxBackoff, got, c.want)
		}
	}
}

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

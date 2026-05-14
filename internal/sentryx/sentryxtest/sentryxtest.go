// Package sentryxtest provides a recording Sentry transport for tests in
// other packages. It is the only sanctioned place outside sentryx where
// the sentry-go SDK is imported — production code MUST go through
// internal/sentryx instead.
package sentryxtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// CapturedEvent is a minimal projection of a Sentry event surfaced to
// tests, so callers don't have to import sentry-go themselves.
type CapturedEvent struct {
	Type        string
	Message     string
	Transaction string
	Exceptions  []string
	Tags        map[string]string
	Details     map[string]any
}

type recordingTransport struct {
	mu     sync.Mutex
	events []CapturedEvent
}

func (r *recordingTransport) Flush(time.Duration) bool              { return true }
func (r *recordingTransport) FlushWithContext(context.Context) bool { return true }
func (r *recordingTransport) Configure(sentry.ClientOptions)        {}
func (r *recordingTransport) Close()                                {}
func (r *recordingTransport) SendEvent(e *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := CapturedEvent{
		Type:        e.Type,
		Message:     e.Message,
		Transaction: e.Transaction,
		Tags:        cloneStringMap(e.Tags),
	}
	for _, ex := range e.Exception {
		c.Exceptions = append(c.Exceptions, ex.Value)
	}
	if details, ok := e.Contexts["details"]; ok {
		c.Details = map[string]any{}
		for k, v := range details {
			c.Details[k] = v
		}
	}
	r.events = append(r.events, c)
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Recorder is the test handle returned by Swap. Call Events to snapshot
// everything captured since installation; call WaitFor to block on async
// paths.
type Recorder struct {
	rt *recordingTransport
}

// Events returns a snapshot of all events captured so far.
func (r *Recorder) Events() []CapturedEvent {
	r.rt.mu.Lock()
	defer r.rt.mu.Unlock()
	out := make([]CapturedEvent, len(r.rt.events))
	copy(out, r.rt.events)
	return out
}

// WaitFor blocks until pred matches at least one event or the deadline
// passes. Returns true if a match was seen.
func (r *Recorder) WaitFor(timeout time.Duration, pred func(CapturedEvent) bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range r.Events() {
			if pred(ev) {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// Swap installs a recording Sentry client on the global hub for the
// duration of t. Tests can then exercise production code that calls
// sentryx.Report / sentryx.Capture and assert on the recorded events.
func Swap(t *testing.T, tracesEnabled bool) *Recorder {
	t.Helper()
	hub := sentry.CurrentHub()
	prev := hub.Client()
	rt := &recordingTransport{}
	rate := 0.0
	if tracesEnabled {
		rate = 1.0
	}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              "https://public@example.com/1",
		Transport:        rt,
		SampleRate:       1.0,
		TracesSampleRate: rate,
		EnableTracing:    tracesEnabled,
	})
	if err != nil {
		t.Fatalf("sentryxtest: Swap: %v", err)
	}
	hub.BindClient(client)
	t.Cleanup(func() { hub.BindClient(prev) })
	return &Recorder{rt: rt}
}

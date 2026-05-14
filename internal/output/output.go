// Package output implements pluggable audit-event sinks. Each sink runs in
// its own goroutine and consumes events from a buffered channel so a slow
// sink cannot back-pressure the MONITOR connection.
package output

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/sentryx"
)

// MetricsSink is implemented by metrics.Registry. Consumers publish
// per-output written / dropped / errors / write-duration / failing state
// through it. A nil sink is replaced with a no-op at construction so the
// hot path can call into it unconditionally.
type MetricsSink interface {
	IncrOutputWritten(output string)
	IncrOutputDropped(output string)
	IncrOutputError(output string)
	SetOutputFailing(output string, failing bool)
	ObserveOutputWrite(output string, d time.Duration)
}

type noopSink struct{}

func (noopSink) IncrOutputWritten(string)                 {}
func (noopSink) IncrOutputDropped(string)                 {}
func (noopSink) IncrOutputError(string)                   {}
func (noopSink) SetOutputFailing(string, bool)            {}
func (noopSink) ObserveOutputWrite(string, time.Duration) {}

// Sink is the writer interface implemented by every output backend.
type Sink interface {
	// Name returns a stable identifier used for logging and metrics.
	Name() string
	// Write serializes an event and writes it. It MUST be safe to call from a
	// single goroutine. Implementations should treat transient I/O errors as
	// recoverable and reconnect on the next call where applicable.
	Write(ev *event.Event) error
	// Close releases any held resources.
	Close() error
}

// Consumer wraps a Sink with its own buffered channel and goroutine.
type Consumer struct {
	sink    Sink
	in      chan *event.Event
	dropped atomic.Uint64
	written atomic.Uint64
	errors  atomic.Uint64
	// failing tracks whether the last write attempt errored. It is used to
	// throttle Sentry reports: we capture an event only on the
	// success-to-failure transition so sustained outages don't flood the
	// project. A successful write resets the flag.
	failing atomic.Bool
	drop    bool
	log     *slog.Logger
	metrics MetricsSink
}

// NewConsumer builds a Consumer with the given buffer size and drop policy.
func NewConsumer(sink Sink, buffer int, dropOnFull bool, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		sink:    sink,
		in:      make(chan *event.Event, buffer),
		drop:    dropOnFull,
		log:     log.With("output", sink.Name()),
		metrics: noopSink{},
	}
}

// SetMetricsSink installs the metrics sink. Safe to call before Run; a
// nil sink leaves the default no-op in place.
func (c *Consumer) SetMetricsSink(s MetricsSink) {
	if s == nil {
		c.metrics = noopSink{}
		return
	}
	c.metrics = s
}

// QueueDepth returns the instantaneous number of buffered events for the
// metrics package's queue-depth collector.
func (c *Consumer) QueueDepth() int { return len(c.in) }

// QueueCapacity returns the channel's capacity (constant).
func (c *Consumer) QueueCapacity() int { return cap(c.in) }

// Submit enqueues an event. When the consumer is configured to drop on full
// and the queue is saturated, the event is discarded and the dropped counter
// incremented.
func (c *Consumer) Submit(ev *event.Event) {
	if c.drop {
		select {
		case c.in <- ev:
		default:
			c.dropped.Add(1)
			c.metrics.IncrOutputDropped(c.sink.Name())
		}
		return
	}
	c.in <- ev
}

// Counters exposes per-consumer statistics for metrics.
type Counters struct {
	Written uint64
	Dropped uint64
	Errors  uint64
}

// Counters snapshots the consumer's atomic counters.
func (c *Consumer) Counters() Counters {
	return Counters{
		Written: c.written.Load(),
		Dropped: c.dropped.Load(),
		Errors:  c.errors.Load(),
	}
}

// Name returns the underlying sink's name.
func (c *Consumer) Name() string { return c.sink.Name() }

// Run drains the queue until ctx is done, then closes the sink.
func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		if err := c.sink.Close(); err != nil {
			c.log.Warn("close error", "err", err)
			sentryx.Report(err, "stage", "output.close", "output", c.sink.Name())
		}
	}()

	for {
		select {
		case <-ctx.Done():
			c.drainAfterShutdown()
			return nil
		case ev := <-c.in:
			c.writeOne(ev)
		}
	}
}

// writeOne writes one event to the sink, records latency and reports
// failures.
//
// Sentry capture is intentionally throttled: a write error on a busy
// stream can fire on every event during an outage, so we only capture on
// the transition from healthy to failing. Returning to healthy resets the
// state so the next failure is captured again. The `output_failing` gauge
// follows the same transition logic so dashboards can light up red while
// the outage lasts.
func (c *Consumer) writeOne(ev *event.Event) {
	start := time.Now()
	err := c.sink.Write(ev)
	c.metrics.ObserveOutputWrite(c.sink.Name(), time.Since(start))
	if err != nil {
		c.errors.Add(1)
		c.metrics.IncrOutputError(c.sink.Name())
		c.log.Error("write failed", "err", err)
		if !c.failing.Swap(true) {
			c.metrics.SetOutputFailing(c.sink.Name(), true)
			sentryx.Report(err,
				"stage", "output.write",
				"output", c.sink.Name(),
				"command", ev.Command,
			)
		}
		return
	}
	c.written.Add(1)
	c.metrics.IncrOutputWritten(c.sink.Name())
	if c.failing.Swap(false) {
		c.metrics.SetOutputFailing(c.sink.Name(), false)
	}
}

// drainAfterShutdown writes any events buffered in the channel using a short
// best-effort deadline so we don't lose recent activity on SIGTERM.
func (c *Consumer) drainAfterShutdown() {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case ev := <-c.in:
			start := time.Now()
			err := c.sink.Write(ev)
			c.metrics.ObserveOutputWrite(c.sink.Name(), time.Since(start))
			if err != nil {
				c.errors.Add(1)
				c.metrics.IncrOutputError(c.sink.Name())
				if !c.failing.Swap(true) {
					c.metrics.SetOutputFailing(c.sink.Name(), true)
					sentryx.Report(err,
						"stage", "output.write.shutdown",
						"output", c.sink.Name(),
						"command", ev.Command,
					)
				}
				return
			}
			c.written.Add(1)
			c.metrics.IncrOutputWritten(c.sink.Name())
			if c.failing.Swap(false) {
				c.metrics.SetOutputFailing(c.sink.Name(), false)
			}
		case <-deadline.C:
			return
		default:
			return
		}
	}
}

// Manager owns a set of Consumers and starts them all.
type Manager struct {
	consumers []*Consumer
	log       *slog.Logger
}

// NewManager builds an empty Manager.
func NewManager(log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{log: log}
}

// Add registers a consumer.
func (m *Manager) Add(c *Consumer) { m.consumers = append(m.consumers, c) }

// Consumers returns the registered consumers (for metrics inspection).
func (m *Manager) Consumers() []*Consumer { return m.consumers }

// Dispatch fan-outs a single event to every registered consumer.
func (m *Manager) Dispatch(ev *event.Event) {
	for _, c := range m.consumers {
		c.Submit(ev)
	}
}

// Run starts each consumer in its own goroutine and waits for them.
func (m *Manager) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, c := range m.consumers {
		wg.Add(1)
		go func(c *Consumer) {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				m.log.Error("output run terminated", "name", c.Name(), "err", err)
			}
		}(c)
	}
	wg.Wait()
	return nil
}

// encode serializes an event according to format ("json" or "text").
func encode(ev *event.Event, format string) ([]byte, error) {
	switch strings.ToLower(format) {
	case "", "json":
		buf, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		return append(buf, '\n'), nil
	case "text":
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s db=%d src=%s:%s cmd=%s",
			ev.Timestamp.UTC().Format(time.RFC3339Nano),
			ev.DB, ev.Source.IP, ev.Source.Port, ev.Command)
		if ev.Subcommand != "" {
			fmt.Fprintf(&sb, " sub=%s", ev.Subcommand)
		}
		for _, a := range ev.Args {
			sb.WriteByte(' ')
			sb.WriteString(quoteText(a))
		}
		sb.WriteByte('\n')
		return []byte(sb.String()), nil
	default:
		return nil, fmt.Errorf("unknown format %q", format)
	}
}

func quoteText(s string) string {
	if !strings.ContainsAny(s, " \t\n\r\"") {
		return s
	}
	var sb strings.Builder
	sb.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' {
			sb.WriteByte('\\')
		}
		sb.WriteByte(c)
	}
	sb.WriteByte('"')
	return sb.String()
}

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
)

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
	drop    bool
	log     *slog.Logger
}

// NewConsumer builds a Consumer with the given buffer size and drop policy.
func NewConsumer(sink Sink, buffer int, dropOnFull bool, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		sink: sink,
		in:   make(chan *event.Event, buffer),
		drop: dropOnFull,
		log:  log.With("output", sink.Name()),
	}
}

// Submit enqueues an event. When the consumer is configured to drop on full
// and the queue is saturated, the event is discarded and the dropped counter
// incremented.
func (c *Consumer) Submit(ev *event.Event) {
	if c.drop {
		select {
		case c.in <- ev:
		default:
			c.dropped.Add(1)
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
		}
	}()

	for {
		select {
		case <-ctx.Done():
			c.drainAfterShutdown()
			return nil
		case ev := <-c.in:
			if err := c.sink.Write(ev); err != nil {
				c.errors.Add(1)
				c.log.Error("write failed", "err", err)
				continue
			}
			c.written.Add(1)
		}
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
			if err := c.sink.Write(ev); err != nil {
				c.errors.Add(1)
				return
			}
			c.written.Add(1)
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

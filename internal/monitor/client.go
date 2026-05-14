// Package monitor implements a Redis MONITOR consumer. It speaks raw RESP
// over a dedicated connection (TCP or unix socket) because go-redis (and
// most pooled clients) do not expose MONITOR cleanly -- its response is an
// open-ended stream of simple strings that breaks every pooling invariant.
package monitor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/resp"
)

// Options configures a Client.
type Options struct {
	Network     string // "unix" or "tcp"
	Address     string
	Username    string
	Password    string
	DialTimeout time.Duration
	ReadTimeout time.Duration
	BackoffMin  time.Duration
	BackoffMax  time.Duration
}

// ErrorReporter is called whenever a non-fatal error is observed (parse
// failure, dropped event, etc.). Implementations should be cheap because
// they may be invoked on the hot path.
type ErrorReporter func(err error, kv ...any)

// noopReporter is the default ErrorReporter used when the caller passes
// nil. It is intentionally empty so the hot path can call report() without
// a nil-check; suppression of every non-fatal error is correct behaviour
// for the "no telemetry attached" configuration.
func noopReporter(_ error, _ ...any) {
	// intentionally a no-op
}

// Gate gates the MONITOR connection. When IsActive returns false the
// Client will not connect (or will disconnect if already connected). Wait
// blocks until the gate is active again. Subscribe lets the Client tear
// down its connection promptly on transitions.
//
// The role.Checker in internal/role implements this interface.
type Gate interface {
	IsActive() bool
	Wait(ctx context.Context) error
	Subscribe() (<-chan struct{}, func())
}

// MetricsSink is implemented by metrics.Registry. The Client uses it to
// publish connection state, event freshness, dropped/parse/reconnect
// counters and per-session lifetime. The package-internal noopSink keeps
// the Client useful in tests and in barebones builds.
type MetricsSink interface {
	SetMonitorConnected(connected bool)
	ObserveLastEvent(t time.Time)
	ObserveMonitorSession(d time.Duration)
	IncrParseError()
	IncrReconnect()
	IncrMonitorDropped()
}

type noopSink struct{}

func (noopSink) SetMonitorConnected(bool)            {}
func (noopSink) ObserveLastEvent(time.Time)          {}
func (noopSink) ObserveMonitorSession(time.Duration) {}
func (noopSink) IncrParseError()                     {}
func (noopSink) IncrReconnect()                      {}
func (noopSink) IncrMonitorDropped()                 {}

// Client repeatedly connects to Redis, issues MONITOR, parses the resulting
// stream and emits Events.
type Client struct {
	opts    Options
	log     *slog.Logger
	out     chan<- *event.Event
	report  ErrorReporter
	metrics MetricsSink
	drop    bool
	gate    Gate

	dropped atomic.Uint64
	parseEr atomic.Uint64
	recon   atomic.Uint64
}

// New constructs a new Client. The caller owns the output channel.
func New(opts Options, out chan<- *event.Event, log *slog.Logger, report ErrorReporter, dropOnFull bool) *Client {
	if log == nil {
		log = slog.Default()
	}
	if report == nil {
		report = noopReporter
	}
	if opts.BackoffMin <= 0 {
		opts.BackoffMin = time.Second
	}
	if opts.BackoffMax < opts.BackoffMin {
		opts.BackoffMax = 30 * time.Second
	}
	return &Client{opts: opts, log: log, out: out, report: report, metrics: noopSink{}, drop: dropOnFull}
}

// SetGate installs a Gate. When set, Run blocks while the gate is inactive
// (e.g. when the upstream Redis is a Sentinel replica) and tears down the
// MONITOR connection on every transition so the next iteration re-evaluates
// the role. Pass nil to remove the gate.
func (c *Client) SetGate(g Gate) { c.gate = g }

// SetMetricsSink installs the metrics sink. Safe to call before Run; a
// nil sink leaves the default no-op in place.
func (c *Client) SetMetricsSink(s MetricsSink) {
	if s == nil {
		c.metrics = noopSink{}
		return
	}
	c.metrics = s
}

// QueueDepth reports the current number of buffered events awaiting
// dispatch. Used by the metrics package's queue-depth collector. Returns
// the channel cap as the second value so the collector can also report
// utilization.
func (c *Client) QueueDepth() int { return len(c.out) }

// QueueCapacity returns the channel capacity (constant).
func (c *Client) QueueCapacity() int { return cap(c.out) }

// Stats reports counters exposed for metrics or logging.
type Stats struct {
	Dropped       uint64
	ParseErrors   uint64
	Reconnections uint64
}

// Stats returns a snapshot of internal counters.
func (c *Client) Stats() Stats {
	return Stats{
		Dropped:       c.dropped.Load(),
		ParseErrors:   c.parseEr.Load(),
		Reconnections: c.recon.Load(),
	}
}

// Run blocks until ctx is cancelled, repeatedly establishing the MONITOR
// connection with exponential backoff on failure. If a Gate is installed,
// Run waits for the gate to be active before each (re)connect.
func (c *Client) Run(ctx context.Context) error {
	backoff := c.opts.BackoffMin
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.gate != nil && !c.gate.IsActive() {
			c.log.Info("MONITOR paused: upstream is not master")
			if err := c.gate.Wait(ctx); err != nil {
				return err
			}
			c.log.Info("MONITOR resuming: upstream is now master")
			backoff = c.opts.BackoffMin
		}
		if !first {
			c.recon.Add(1)
			c.metrics.IncrReconnect()
		}
		first = false

		err := c.runOnce(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		// If the gate just deactivated, treat the disconnect as expected and
		// skip the backoff so we can re-enter Wait immediately.
		if c.gate != nil && !c.gate.IsActive() {
			c.log.Info("MONITOR connection closed due to role transition")
			continue
		}
		c.log.Error("MONITOR connection failed", "err", err, "backoff", backoff)
		c.report(err, "stage", "monitor_connection")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.opts.BackoffMax {
			backoff = c.opts.BackoffMax
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	dialer := &net.Dialer{Timeout: c.opts.DialTimeout}
	conn, err := dialer.DialContext(ctx, c.opts.Network, c.opts.Address)
	if err != nil {
		return fmt.Errorf("dial %s/%s: %w", c.opts.Network, c.opts.Address, err)
	}

	closed := make(chan struct{})
	defer func() {
		close(closed)
		_ = conn.Close()
	}()
	// Close the conn when the context is cancelled so blocking reads unwind.
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-closed:
		}
	}()

	// If a gate is installed, watch for transitions and drop the connection
	// the moment we are no longer the master.
	if c.gate != nil {
		notify, unsub := c.gate.Subscribe()
		go func() {
			defer unsub()
			for {
				select {
				case <-closed:
					return
				case <-ctx.Done():
					return
				case <-notify:
					if !c.gate.IsActive() {
						c.log.Info("role transitioned to replica, closing MONITOR")
						_ = conn.Close()
						return
					}
				}
			}
		}()
	}

	r := bufio.NewReaderSize(conn, 64*1024)
	w := bufio.NewWriter(conn)

	if c.opts.Password != "" {
		if err := resp.WriteCommand(w, resp.AuthArgs(c.opts.Username, c.opts.Password)...); err != nil {
			return fmt.Errorf("send AUTH: %w", err)
		}
		if err := resp.ReadSimpleString(r); err != nil {
			return fmt.Errorf("AUTH failed: %w", err)
		}
	}

	if err := resp.WriteCommand(w, "MONITOR"); err != nil {
		return fmt.Errorf("send MONITOR: %w", err)
	}
	if err := resp.ReadSimpleString(r); err != nil {
		return fmt.Errorf("MONITOR rejected: %w", err)
	}

	c.log.Info("MONITOR started",
		"network", c.opts.Network,
		"address", redact(c.opts.Address))

	c.metrics.SetMonitorConnected(true)
	sessionStart := time.Now()
	defer func() {
		c.metrics.SetMonitorConnected(false)
		c.metrics.ObserveMonitorSession(time.Since(sessionStart))
	}()

	return c.readLoop(ctx, conn, r)
}

func (c *Client) readLoop(ctx context.Context, conn net.Conn, r *bufio.Reader) error {
	for {
		if c.opts.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
		}
		line, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("server closed connection")
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read: %w", err)
		}

		// Redis only emits "+...\r\n" simple-string frames while MONITORing,
		// but we tolerate stray empty lines defensively.
		if len(line) == 0 || line[0] != '+' {
			if strings.HasPrefix(line, "-") {
				return fmt.Errorf("server error: %s", strings.TrimSpace(line[1:]))
			}
			continue
		}

		ev, perr := Parse(line)
		if perr != nil {
			c.parseEr.Add(1)
			c.metrics.IncrParseError()
			c.log.Debug("monitor parse error", "err", perr, "line", strings.TrimSpace(line))
			c.report(perr, "stage", "monitor_parse", "line", strings.TrimSpace(line))
			continue
		}

		c.metrics.ObserveLastEvent(ev.Timestamp)

		if c.drop {
			select {
			case c.out <- ev:
			default:
				c.dropped.Add(1)
				c.metrics.IncrMonitorDropped()
			}
		} else {
			select {
			case c.out <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// redact masks the host part of a "host:port" string so it can be logged
// safely. Unix sockets are returned as-is.
func redact(addr string) string {
	if !strings.Contains(addr, ":") || strings.HasPrefix(addr, "/") {
		return addr
	}
	idx := strings.LastIndex(addr, ":")
	if idx <= 0 {
		return addr
	}
	return addr[:idx] + ":***"
}

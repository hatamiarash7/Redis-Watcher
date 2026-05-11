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

// Client repeatedly connects to Redis, issues MONITOR, parses the resulting
// stream and emits Events.
type Client struct {
	opts   Options
	log    *slog.Logger
	out    chan<- *event.Event
	report ErrorReporter
	drop   bool

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
		report = func(_ error, _ ...any) {}
	}
	if opts.BackoffMin <= 0 {
		opts.BackoffMin = time.Second
	}
	if opts.BackoffMax < opts.BackoffMin {
		opts.BackoffMax = 30 * time.Second
	}
	return &Client{opts: opts, log: log, out: out, report: report, drop: dropOnFull}
}

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
// connection with exponential backoff on failure.
func (c *Client) Run(ctx context.Context) error {
	backoff := c.opts.BackoffMin
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !first {
			c.recon.Add(1)
		}
		first = false

		err := c.runOnce(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
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

	r := bufio.NewReaderSize(conn, 64*1024)
	w := bufio.NewWriter(conn)

	if c.opts.Password != "" {
		if err := writeCmd(w, authArgs(c.opts.Username, c.opts.Password)...); err != nil {
			return fmt.Errorf("send AUTH: %w", err)
		}
		if err := readSimpleString(r); err != nil {
			return fmt.Errorf("AUTH failed: %w", err)
		}
	}

	if err := writeCmd(w, "MONITOR"); err != nil {
		return fmt.Errorf("send MONITOR: %w", err)
	}
	if err := readSimpleString(r); err != nil {
		return fmt.Errorf("MONITOR rejected: %w", err)
	}

	c.log.Info("MONITOR started",
		"network", c.opts.Network,
		"address", redact(c.opts.Address))

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
			c.log.Debug("monitor parse error", "err", perr, "line", strings.TrimSpace(line))
			c.report(perr, "stage", "monitor_parse", "line", strings.TrimSpace(line))
			continue
		}

		if c.drop {
			select {
			case c.out <- ev:
			default:
				c.dropped.Add(1)
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

func writeCmd(w *bufio.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return w.Flush()
}

func readSimpleString(r *bufio.Reader) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return errors.New("empty response")
	}
	switch line[0] {
	case '+':
		return nil
	case '-':
		return errors.New(line[1:])
	default:
		return fmt.Errorf("unexpected response: %s", line)
	}
}

func authArgs(user, pass string) []string {
	if user != "" {
		return []string{"AUTH", user, pass}
	}
	return []string{"AUTH", pass}
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

// Package alert detects suspicious commands and dispatches notifications to
// one or more channels (Telegram, generic webhook, Prometheus Pushgateway).
package alert

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/sentryx"
)

// Channel is a notification backend.
type Channel interface {
	// Name returns a stable identifier used for logging and metrics.
	Name() string
	// Send delivers an alert. The context carries a per-alert deadline.
	Send(ctx context.Context, a Alert) error
	// Close releases held resources.
	Close() error
}

// Alert is a normalized notification payload.
type Alert struct {
	Timestamp time.Time
	Command   string
	Source    string // ip:port
	DB        int
	Reason    string
	Event     *event.Event
}

// Renderer formats an alert for human consumption.
func (a Alert) Renderer() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[Redis Watcher] %s\n", a.Reason)
	fmt.Fprintf(&sb, "time: %s\n", a.Timestamp.UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "command: %s\n", a.Command)
	fmt.Fprintf(&sb, "source: %s (db %d)\n", a.Source, a.DB)
	if a.Event != nil {
		line := a.Event.CommandLine()
		if len(line) > 512 {
			line = line[:512] + "…"
		}
		fmt.Fprintf(&sb, "raw: %s", line)
	}
	return sb.String()
}

// Reporter is invoked for per-channel send errors (e.g. to bump a counter).
type Reporter interface {
	AlertSent(channel, command string)
	AlertError(channel string, err error)
	SuspiciousObserved(command, sourceIP string)
}

// Engine is the central rule evaluator that fan-outs matching events to all
// configured channels.
type Engine struct {
	channels []Channel

	commands      map[string]struct{}
	patterns      []*regexp.Regexp
	ignoredIPs    map[string]struct{}
	rateLimit     bool
	rateWindow    time.Duration
	rateMaxAlerts int

	retryMax            int
	retryInitialBackoff time.Duration
	retryMaxBackoff     time.Duration

	mu    sync.Mutex
	rates map[string]*bucket

	in      chan *event.Event
	log     *slog.Logger
	rep     Reporter
	timeout time.Duration
	drop    bool
	dropped uint64
}

type bucket struct {
	windowStart time.Time
	count       int
}

// Options configures the Engine.
type Options struct {
	Channels         []Channel
	Commands         []string
	Patterns         []string
	IgnoredSourceIPs []string

	RateLimitEnabled bool
	RateWindow       time.Duration
	RateMaxAlerts    int

	// RetryMaxAttempts is the total number of times each channel will be
	// invoked before a single alert is declared failed. 1 disables retries.
	RetryMaxAttempts int
	// RetryInitialBackoff is the sleep between the first failure and the
	// second attempt. Each subsequent failure doubles this delay until
	// RetryMaxBackoff is reached. Zero disables sleeping between attempts.
	RetryInitialBackoff time.Duration
	// RetryMaxBackoff caps the exponential backoff.
	RetryMaxBackoff time.Duration

	BufferSize  int
	DropOnFull  bool
	SendTimeout time.Duration

	Log      *slog.Logger
	Reporter Reporter
}

// New compiles the Engine. Pattern compilation errors are returned eagerly.
func New(o Options) (*Engine, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.BufferSize <= 0 {
		o.BufferSize = 1024
	}
	if o.SendTimeout <= 0 {
		o.SendTimeout = 10 * time.Second
	}
	if o.RetryMaxAttempts <= 0 {
		o.RetryMaxAttempts = 1
	}
	if o.RetryInitialBackoff < 0 {
		o.RetryInitialBackoff = 0
	}
	if o.RetryMaxBackoff > 0 && o.RetryInitialBackoff > o.RetryMaxBackoff {
		o.RetryInitialBackoff = o.RetryMaxBackoff
	}

	cmds := make(map[string]struct{}, len(o.Commands))
	for _, c := range o.Commands {
		cmds[strings.ToUpper(strings.TrimSpace(c))] = struct{}{}
	}
	ips := make(map[string]struct{}, len(o.IgnoredSourceIPs))
	for _, ip := range o.IgnoredSourceIPs {
		ips[ip] = struct{}{}
	}
	regexps := make([]*regexp.Regexp, 0, len(o.Patterns))
	for _, p := range o.Patterns {
		re, err := regexp.Compile("(?i)" + p)
		if err != nil {
			return nil, fmt.Errorf("compile pattern %q: %w", p, err)
		}
		regexps = append(regexps, re)
	}

	return &Engine{
		channels:            o.Channels,
		commands:            cmds,
		patterns:            regexps,
		ignoredIPs:          ips,
		rateLimit:           o.RateLimitEnabled,
		rateWindow:          o.RateWindow,
		rateMaxAlerts:       o.RateMaxAlerts,
		retryMax:            o.RetryMaxAttempts,
		retryInitialBackoff: o.RetryInitialBackoff,
		retryMaxBackoff:     o.RetryMaxBackoff,
		rates:               make(map[string]*bucket),
		in:                  make(chan *event.Event, o.BufferSize),
		log:                 o.Log,
		rep:                 o.Reporter,
		timeout:             o.SendTimeout,
		drop:                o.DropOnFull,
	}, nil
}

// Submit pushes an event into the engine's queue.
func (e *Engine) Submit(ev *event.Event) {
	if e.drop {
		select {
		case e.in <- ev:
		default:
			e.dropped++
		}
		return
	}
	e.in <- ev
}

// Run drains the queue until ctx is done.
func (e *Engine) Run(ctx context.Context) error {
	defer e.closeAll()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-e.in:
			e.handle(ctx, ev)
		}
	}
}

func (e *Engine) handle(ctx context.Context, ev *event.Event) {
	reason, ok := e.match(ev)
	if !ok {
		return
	}
	if _, ignored := e.ignoredIPs[ev.Source.IP]; ignored {
		return
	}
	if e.rep != nil {
		e.rep.SuspiciousObserved(ev.Command, ev.Source.IP)
	}
	if !e.rateLimitAllow(ev) {
		e.log.Debug("alert rate limited", "cmd", ev.Command, "ip", ev.Source.IP)
		return
	}

	a := Alert{
		Timestamp: ev.Timestamp,
		Command:   ev.FullCommand(),
		Source:    ev.Source.IP + ":" + ev.Source.Port,
		DB:        ev.DB,
		Reason:    reason,
		Event:     ev,
	}

	// One transaction per fired alert. Per-channel spans nest under this
	// so each notification's latency and outcome is visible end-to-end in
	// Sentry's performance UI.
	txCtx, finishTx := sentryx.StartSpan(ctx, "task.alert", "alert.dispatch")
	sentryx.SetSpanTag(txCtx, "command", ev.Command)
	sentryx.SetSpanData(txCtx, "source_ip", ev.Source.IP)
	sentryx.SetSpanData(txCtx, "db", ev.DB)

	var dispatchErr error
	for _, ch := range e.channels {
		if err := e.sendOne(txCtx, ch, a, ev); err != nil && dispatchErr == nil {
			dispatchErr = err
		}
	}
	finishTx(dispatchErr)
}

// sendOne dispatches a single alert to ch with bounded retries and
// exponential backoff. Each attempt is bounded by the per-attempt send
// timeout; the entire retry chain is bounded by ctx.
//
// Metrics semantics:
//   - AlertError fires per failed attempt (so dashboards see retry noise).
//   - AlertSent fires once on eventual success.
//
// Sentry semantics:
//   - The whole channel dispatch is one child span under alert.dispatch;
//     span data carries the total attempt count and final outcome.
//   - sentryx.Capture is invoked only after all retries are exhausted, so
//     a single transient failure that recovers does not page anyone.
//
// The returned error is the final attempt's error (nil on eventual
// success). It is used by the caller to set the parent transaction's
// status.
func (e *Engine) sendOne(ctx context.Context, ch Channel, a Alert, ev *event.Event) error {
	spanCtx, finish := sentryx.StartSpan(ctx, "http.client", "alert.send."+ch.Name())
	sentryx.SetSpanTag(spanCtx, "channel", ch.Name())
	sentryx.SetSpanTag(spanCtx, "command", ev.Command)

	backoff := e.retryInitialBackoff
	var lastErr error
	attempts := 0
	for attempt := 1; attempt <= e.retryMax; attempt++ {
		attempts = attempt
		sendCtx, cancel := context.WithTimeout(spanCtx, e.timeout)
		err := ch.Send(sendCtx, a)
		cancel()

		if err == nil {
			sentryx.SetSpanData(spanCtx, "attempts", attempt)
			finish(nil)
			if e.rep != nil {
				e.rep.AlertSent(ch.Name(), ev.Command)
			}
			if attempt > 1 {
				e.log.Info("alert recovered after retry",
					"channel", ch.Name(),
					"command", ev.Command,
					"attempts", attempt)
			}
			return nil
		}

		lastErr = err
		if e.rep != nil {
			e.rep.AlertError(ch.Name(), err)
		}
		e.log.Warn("alert send attempt failed",
			"channel", ch.Name(),
			"attempt", attempt,
			"max_attempts", e.retryMax,
			"err", err)

		if attempt == e.retryMax {
			break
		}
		if !sleepWithContext(ctx, backoff) {
			lastErr = fmt.Errorf("retry aborted: %w", ctx.Err())
			break
		}
		backoff = nextBackoff(backoff, e.retryMaxBackoff)
	}

	sentryx.SetSpanData(spanCtx, "attempts", attempts)
	finish(lastErr)
	e.log.Error("alert send failed",
		"channel", ch.Name(),
		"attempts", attempts,
		"err", lastErr)
	sentryx.Capture(spanCtx, lastErr,
		"channel", ch.Name(),
		"attempts", attempts,
		"max_attempts", e.retryMax,
		"command", ev.FullCommand(),
		"source_ip", ev.Source.IP,
		"db", ev.DB,
		"reason", a.Reason,
	)
	return lastErr
}

// sleepWithContext sleeps for d unless ctx fires first. Returns false if
// the context was cancelled (the caller should bail out of the retry
// loop), true otherwise.
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextBackoff doubles cur, capping it at maxBackoff. A zero maxBackoff
// means "unbounded"; we still cap at one minute to keep total dispatch
// time finite even when the config is misset.
func nextBackoff(cur, maxBackoff time.Duration) time.Duration {
	next := cur * 2
	if maxBackoff > 0 && next > maxBackoff {
		return maxBackoff
	}
	if maxBackoff == 0 && next > time.Minute {
		return time.Minute
	}
	return next
}

func (e *Engine) match(ev *event.Event) (string, bool) {
	if _, ok := e.commands[ev.Command]; ok {
		return "suspicious command: " + ev.FullCommand(), true
	}
	if len(e.patterns) > 0 {
		line := ev.CommandLine()
		for _, p := range e.patterns {
			if p.MatchString(line) {
				return "pattern match: " + p.String(), true
			}
		}
	}
	return "", false
}

func (e *Engine) rateLimitAllow(ev *event.Event) bool {
	if !e.rateLimit || e.rateMaxAlerts <= 0 || e.rateWindow <= 0 {
		return true
	}
	key := ev.Command + "|" + ev.Source.IP
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.rates[key]
	if !ok || now.Sub(b.windowStart) > e.rateWindow {
		e.rates[key] = &bucket{windowStart: now, count: 1}
		return true
	}
	if b.count >= e.rateMaxAlerts {
		return false
	}
	b.count++
	return true
}

func (e *Engine) closeAll() {
	for _, ch := range e.channels {
		if err := ch.Close(); err != nil {
			e.log.Warn("alert channel close error", "channel", ch.Name(), "err", err)
		}
	}
}

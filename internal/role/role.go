// Package role keeps track of whether the upstream Redis instance is
// currently the primary (master) in a Sentinel-managed deployment. It
// exposes a Gate interface that other components (notably monitor.Client)
// use to pause work during failovers.
//
// The Checker periodically issues `INFO replication` on a dedicated short
// lived connection. INFO is cheap and works on every Redis version since
// 2.x, unlike the newer `ROLE` command which is only available since 2.8.0.
package role

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/resp"
)

// Role is the value of "role:" in `INFO replication`.
type Role string

const (
	// RoleMaster is the writable primary instance.
	RoleMaster Role = "master"
	// RoleReplica is a read-only follower (still called "slave" by Redis
	// for backwards compatibility — we normalize that to "replica" at the
	// observability boundary).
	RoleReplica Role = "replica"
	// RoleUnknown is the initial state before the first successful check
	// completes, or whenever a check fails.
	RoleUnknown Role = "unknown"
)

// Options configures a Checker.
type Options struct {
	Network      string
	Address      string
	Username     string
	Password     string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	Interval     time.Duration
	AllowReplica bool // if true the Checker always reports IsMaster() == true
}

// MetricsSink is implemented by metrics.Registry so the role Checker can
// publish role changes without depending on the metrics package directly.
type MetricsSink interface {
	SetRedisRole(role string)
	RecordRoleTransition(from, to string)
}

// Checker periodically probes Redis for its replication role and notifies
// subscribers when the role flips. Zero-value Checker is unusable; build
// one with New.
type Checker struct {
	opts    Options
	log     *slog.Logger
	metrics MetricsSink

	isMaster atomic.Bool
	current  atomic.Value // Role

	mu       sync.Mutex
	waiters  []chan struct{}
	probeErr error
}

// New constructs a Checker. The Checker does not connect until Run is
// called.
func New(opts Options, log *slog.Logger, metrics MetricsSink) *Checker {
	if log == nil {
		log = slog.Default()
	}
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 3 * time.Second
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = 3 * time.Second
	}
	c := &Checker{opts: opts, log: log, metrics: metrics}
	c.current.Store(RoleUnknown)
	return c
}

// Role returns the most recently observed role.
func (c *Checker) Role() Role {
	if v, ok := c.current.Load().(Role); ok {
		return v
	}
	return RoleUnknown
}

// IsMaster reports whether the upstream Redis is currently considered the
// primary. When AllowReplica is true this always returns true.
func (c *Checker) IsMaster() bool {
	if c.opts.AllowReplica {
		return true
	}
	return c.isMaster.Load()
}

// IsActive satisfies monitor.Gate. It is an alias of IsMaster.
func (c *Checker) IsActive() bool { return c.IsMaster() }

// Wait blocks until IsMaster() is true or ctx is done.
func (c *Checker) Wait(ctx context.Context) error {
	if c.IsMaster() {
		return nil
	}
	for {
		ch, cancel := c.subscribe()
		// Re-check after subscribing to close the race window between the
		// IsMaster() probe above and the registration of our channel.
		if c.IsMaster() {
			cancel()
			return nil
		}
		select {
		case <-ctx.Done():
			cancel()
			return ctx.Err()
		case <-ch:
			cancel()
		}
		if c.IsMaster() {
			return nil
		}
	}
}

// Subscribe returns a channel that receives a signal every time the role
// changes (in either direction). The returned cancel function MUST be
// invoked by the caller when it is done with the subscription.
func (c *Checker) Subscribe() (<-chan struct{}, func()) {
	return c.subscribe()
}

func (c *Checker) subscribe() (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.waiters = append(c.waiters, ch)
	c.mu.Unlock()
	cancel := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, w := range c.waiters {
			if w == ch {
				c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
				return
			}
		}
	}
	return ch, cancel
}

func (c *Checker) notify() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Run drives the periodic probe loop until ctx is done.
func (c *Checker) Run(ctx context.Context) error {
	c.probeOnce(ctx)
	t := time.NewTicker(c.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.probeOnce(ctx)
		}
	}
}

func (c *Checker) probeOnce(ctx context.Context) {
	role, err := c.queryRole(ctx)
	if err != nil {
		c.mu.Lock()
		c.probeErr = err
		c.mu.Unlock()
		c.log.Warn("role probe failed", "err", err)
		return
	}

	c.mu.Lock()
	c.probeErr = nil
	c.mu.Unlock()

	oldRole := c.Role()
	newIsMaster := role == RoleMaster

	c.isMaster.Store(newIsMaster)
	c.current.Store(role)

	if c.metrics != nil {
		c.metrics.SetRedisRole(string(role))
	}

	if oldRole != role {
		c.log.Info("redis role transition",
			"from", string(oldRole),
			"to", string(role),
			"is_master", newIsMaster)
		if c.metrics != nil {
			c.metrics.RecordRoleTransition(string(oldRole), string(role))
		}
		c.notify()
	}
}

// LastError returns the most recent probe error, or nil. Used by health
// checks so /readyz can fail-closed during an outage.
func (c *Checker) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.probeErr
}

func (c *Checker) queryRole(ctx context.Context) (Role, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.opts.DialTimeout)
	defer cancel()
	d := &net.Dialer{Timeout: c.opts.DialTimeout}
	conn, err := d.DialContext(dialCtx, c.opts.Network, c.opts.Address)
	if err != nil {
		return RoleUnknown, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(c.opts.ReadTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	if c.opts.Password != "" {
		if err := resp.WriteCommand(w, resp.AuthArgs(c.opts.Username, c.opts.Password)...); err != nil {
			return RoleUnknown, fmt.Errorf("AUTH: %w", err)
		}
		if err := resp.ReadSimpleString(r); err != nil {
			return RoleUnknown, fmt.Errorf("AUTH: %w", err)
		}
	}

	if err := resp.WriteCommand(w, "INFO", "replication"); err != nil {
		return RoleUnknown, fmt.Errorf("INFO: %w", err)
	}
	body, err := resp.ReadBulkString(r)
	if err != nil {
		return RoleUnknown, fmt.Errorf("INFO read: %w", err)
	}
	return parseRoleFromInfo(body)
}

// parseRoleFromInfo extracts the "role:" line from an `INFO replication`
// response. Redis still prints "slave" for backwards compatibility — we
// normalize that to "replica" for our own observability surface.
func parseRoleFromInfo(body string) (Role, error) {
	for _, line := range strings.Split(body, "\r\n") {
		if !strings.HasPrefix(line, "role:") {
			continue
		}
		v := strings.TrimSpace(strings.TrimPrefix(line, "role:"))
		switch v {
		case "master":
			return RoleMaster, nil
		case "slave", "replica":
			return RoleReplica, nil
		default:
			return Role(v), nil
		}
	}
	return RoleUnknown, errors.New("role: not found in INFO replication output")
}

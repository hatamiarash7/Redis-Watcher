package output

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// NetSink writes events to a UDP or TCP endpoint, reconnecting transparently.
type NetSink struct {
	mu        sync.Mutex
	network   string
	address   string
	timeout   time.Duration
	keepalive time.Duration
	format    string

	conn net.Conn
}

// NetOptions configures a NetSink.
type NetOptions struct {
	Network   string // "udp" or "tcp"
	Address   string
	Timeout   time.Duration
	Keepalive time.Duration
	Format    string
}

// NewNetSink builds a NetSink without connecting; the connection is opened
// lazily on first write.
func NewNetSink(o NetOptions) (*NetSink, error) {
	if o.Network != "udp" && o.Network != "tcp" {
		return nil, fmt.Errorf("network must be udp or tcp, got %q", o.Network)
	}
	if o.Address == "" {
		return nil, errors.New("address required")
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	return &NetSink{
		network: o.Network, address: o.Address,
		timeout: o.Timeout, keepalive: o.Keepalive, format: o.Format,
	}, nil
}

// Name implements Sink.
func (s *NetSink) Name() string { return s.network }

// Write implements Sink. On error the underlying connection is reset so the
// next call will dial again.
func (s *NetSink) Write(ev *event.Event) error {
	buf, err := encode(ev, s.format)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		c, err := s.dial()
		if err != nil {
			return err
		}
		s.conn = c
	}

	_ = s.conn.SetWriteDeadline(time.Now().Add(s.timeout))
	if _, err := s.conn.Write(buf); err != nil {
		_ = s.conn.Close()
		s.conn = nil
		return fmt.Errorf("write to %s/%s: %w", s.network, s.address, err)
	}
	return nil
}

// Close implements Sink.
func (s *NetSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

func (s *NetSink) dial() (net.Conn, error) {
	d := &net.Dialer{Timeout: s.timeout, KeepAlive: s.keepalive}
	return d.Dial(s.network, s.address)
}

//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/monitor"
)

func redisAddr() string {
	if a := os.Getenv("REDIS_WATCHER_TEST_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
}

func redisPassword() string { return os.Getenv("REDIS_WATCHER_TEST_PASSWORD") }

// sendCommand opens a fresh connection to Redis, optionally authenticates
// and sends a single inline command. Used by the integration tests to drive
// traffic that the MONITOR consumer must observe.
func sendCommand(t *testing.T, args ...string) {
	t.Helper()
	d := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.Dial("tcp", redisAddr())
	if err != nil {
		t.Fatalf("dial redis: %v", err)
	}
	defer conn.Close()

	if pw := redisPassword(); pw != "" {
		mustWrite(t, conn, fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(pw), pw))
		readSimple(t, conn)
	}
	var msg = fmt.Sprintf("*%d\r\n", len(args))
	for _, a := range args {
		msg += fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)
	}
	mustWrite(t, conn, msg)
	readSimple(t, conn)
}

func mustWrite(t *testing.T, conn net.Conn, s string) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte(s)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readSimple(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	_, _ = conn.Read(buf)
}

func TestIntegrationMonitor(t *testing.T) {
	// Quick connectivity probe.
	c, err := net.DialTimeout("tcp", redisAddr(), time.Second)
	if err != nil {
		t.Skipf("Redis not reachable at %s: %v", redisAddr(), err)
	}
	_ = c.Close()

	ch := make(chan *event.Event, 64)
	mon := monitor.New(monitor.Options{
		Network:     "tcp",
		Address:     redisAddr(),
		Password:    redisPassword(),
		DialTimeout: 3 * time.Second,
		BackoffMin:  100 * time.Millisecond,
		BackoffMax:  time.Second,
	}, ch, nil, nil, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = mon.Run(ctx) }()

	// Give MONITOR a moment to register.
	time.Sleep(200 * time.Millisecond)

	sendCommand(t, "SET", "rw:test:key", "value")
	sendCommand(t, "GET", "rw:test:key")
	sendCommand(t, "DEL", "rw:test:key")

	seen := map[string]bool{}
	deadline := time.After(3 * time.Second)
loop:
	for {
		select {
		case ev := <-ch:
			seen[ev.Command] = true
			if seen["SET"] && seen["GET"] && seen["DEL"] {
				break loop
			}
		case <-deadline:
			t.Fatalf("did not observe SET/GET/DEL within timeout, seen=%v", seen)
		}
	}
}

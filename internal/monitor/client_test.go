package monitor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

// fakeRedis is a minimal RESP server that supports AUTH and MONITOR for
// testing. It accepts a single client and writes the supplied script lines
// after MONITOR is issued.
type fakeRedis struct {
	t        *testing.T
	ln       net.Listener
	password string
	script   []string // lines (without leading '+') to emit after MONITOR
	conns    chan net.Conn
}

func newFakeRedis(t *testing.T, password string, script []string) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{t: t, ln: ln, password: password, script: script, conns: make(chan net.Conn, 4)}
	go f.serve()
	return f
}

func (f *fakeRedis) addr() string { return f.ln.Addr().String() }

func (f *fakeRedis) close() {
	_ = f.ln.Close()
}

func (f *fakeRedis) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.conns <- conn
		go f.handle(conn)
	}
}

func (f *fakeRedis) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	authed := f.password == ""
	for {
		args, err := readArray(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		switch args[0] {
		case "AUTH":
			if !authed && (len(args) == 2 && args[1] == f.password) ||
				(len(args) == 3 && args[2] == f.password) {
				_, _ = conn.Write([]byte("+OK\r\n"))
				authed = true
				continue
			}
			_, _ = conn.Write([]byte("-WRONGPASS\r\n"))
			return
		case "MONITOR":
			if !authed {
				_, _ = conn.Write([]byte("-NOAUTH\r\n"))
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
			for _, line := range f.script {
				_, _ = conn.Write([]byte("+" + line + "\r\n"))
			}
			// Hold the connection open until client disconnects.
			_, _ = io.Copy(io.Discard, conn)
			return
		default:
			_, _ = conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func readArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("not an array: %q", line)
	}
	var n int
	if _, err := fmt.Sscanf(line, "*%d\r\n", &n); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		head, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		var sz int
		if _, err := fmt.Sscanf(head, "$%d\r\n", &sz); err != nil {
			return nil, err
		}
		buf := make([]byte, sz+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:sz]))
	}
	return out, nil
}

func TestClientReceivesEvents(t *testing.T) {
	srv := newFakeRedis(t, "", []string{
		`1.0 [0 127.0.0.1:11111] "PING"`,
		`2.0 [3 127.0.0.1:22222] "SET" "k" "v"`,
	})
	defer srv.close()

	ch := make(chan *event.Event, 4)
	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		DialTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
	}, ch, nil, nil, true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	for i, want := range []string{"PING", "SET"} {
		select {
		case ev := <-ch:
			if ev.Command != want {
				t.Errorf("event %d: got %q want %q", i, ev.Command, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for event %d", i)
		}
	}
}

func TestClientAuthenticates(t *testing.T) {
	srv := newFakeRedis(t, "s3cret", []string{
		`1.0 [0 127.0.0.1:11111] "PING"`,
	})
	defer srv.close()

	ch := make(chan *event.Event, 1)
	c := New(Options{
		Network: "tcp", Address: srv.addr(), Password: "s3cret",
		DialTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
	}, ch, nil, nil, true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	select {
	case ev := <-ch:
		if ev.Command != "PING" {
			t.Errorf("got %q", ev.Command)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestClientReconnects(t *testing.T) {
	srv := newFakeRedis(t, "", []string{
		`1.0 [0 127.0.0.1:11111] "PING"`,
	})
	defer srv.close()

	// On the first server, after sending PING the connection will be closed
	// when the test closes the conn manually. The client should reconnect to
	// the same listener (which re-accepts) and we observe a second PING.
	ch := make(chan *event.Event, 8)
	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		DialTimeout: time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
	}, ch, nil, nil, true)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	// First event arrives, then we close the connection from the server side.
	<-ch
	conn := <-srv.conns
	_ = conn.Close()

	// Second event after reconnect.
	select {
	case ev := <-ch:
		if ev.Command != "PING" {
			t.Errorf("got %q", ev.Command)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not reconnect")
	}

	if got := c.Stats().Reconnections; got == 0 {
		t.Errorf("expected at least 1 reconnect, got %d", got)
	}
}

func TestRedactHidesPort(t *testing.T) {
	if got := redact("127.0.0.1:6379"); got != "127.0.0.1:***" {
		t.Errorf("redact: %q", got)
	}
	if got := redact("/tmp/redis.sock"); got != "/tmp/redis.sock" {
		t.Errorf("redact: %q", got)
	}
}

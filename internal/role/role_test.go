package role

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeInfoServer is a minimal Redis server that responds to AUTH and
// INFO replication. The role returned can be flipped at runtime by tests.
type fakeInfoServer struct {
	t        *testing.T
	ln       net.Listener
	password string

	mu   sync.Mutex
	role string // "master" or "slave"

	requests atomic.Int64
}

func newFakeInfoServer(t *testing.T, role, password string) *fakeInfoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeInfoServer{t: t, ln: ln, role: role, password: password}
	go f.serve()
	return f
}

func (f *fakeInfoServer) addr() string { return f.ln.Addr().String() }
func (f *fakeInfoServer) close()       { _ = f.ln.Close() }

func (f *fakeInfoServer) setRole(role string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.role = role
}

func (f *fakeInfoServer) currentRole() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.role
}

func (f *fakeInfoServer) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeInfoServer) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	authed := f.password == ""
	for {
		args, err := readArray(r)
		if err != nil {
			return
		}
		f.requests.Add(1)
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "AUTH":
			ok := false
			switch len(args) {
			case 2:
				ok = args[1] == f.password
			case 3:
				ok = args[2] == f.password
			}
			if !ok {
				_, _ = conn.Write([]byte("-WRONGPASS\r\n"))
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
			authed = true
		case "INFO":
			if !authed {
				_, _ = conn.Write([]byte("-NOAUTH\r\n"))
				return
			}
			payload := fmt.Sprintf("# Replication\r\nrole:%s\r\nconnected_slaves:0\r\n", f.currentRole())
			_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(payload), payload)
		default:
			_, _ = conn.Write([]byte("-ERR unknown\r\n"))
			return
		}
	}
}

func readArray(r *bufio.Reader) ([]string, error) {
	head, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(head) == 0 || head[0] != '*' {
		return nil, fmt.Errorf("not array: %q", head)
	}
	var n int
	if _, err := fmt.Sscanf(head, "*%d\r\n", &n); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		bulk, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		var sz int
		if _, err := fmt.Sscanf(bulk, "$%d\r\n", &sz); err != nil {
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

type recordingMetrics struct {
	mu          sync.Mutex
	lastRole    string
	transitions []string
}

func (m *recordingMetrics) SetRedisRole(role string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastRole = role
}

func (m *recordingMetrics) RecordRoleTransition(from, to string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transitions = append(m.transitions, from+"->"+to)
}

func (m *recordingMetrics) LastRole() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastRole
}

func TestParseRoleFromInfoMaster(t *testing.T) {
	r, err := parseRoleFromInfo("# Replication\r\nrole:master\r\nconnected_slaves:2\r\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if r != RoleMaster {
		t.Errorf("got %q", r)
	}
}

func TestParseRoleFromInfoReplica(t *testing.T) {
	r, err := parseRoleFromInfo("role:slave\r\nmaster_host:10.0.0.1\r\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if r != RoleReplica {
		t.Errorf("got %q", r)
	}
}

func TestParseRoleFromInfoMissing(t *testing.T) {
	if _, err := parseRoleFromInfo("# no role here\r\n"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCheckerDetectsMaster(t *testing.T) {
	srv := newFakeInfoServer(t, "master", "")
	defer srv.close()

	m := &recordingMetrics{}
	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		Interval: 10 * time.Millisecond,
	}, nil, m)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, c.IsMaster, time.Second)
	if c.Role() != RoleMaster {
		t.Errorf("role: %q", c.Role())
	}
	if got := m.LastRole(); got != "master" {
		t.Errorf("metrics role: %q", got)
	}
}

func TestCheckerDetectsReplica(t *testing.T) {
	srv := newFakeInfoServer(t, "slave", "")
	defer srv.close()

	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		Interval: 10 * time.Millisecond,
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, func() bool { return c.Role() == RoleReplica }, time.Second)
	if c.IsMaster() {
		t.Error("IsMaster should be false for replica")
	}
}

func TestCheckerWaitUnblocksOnPromotion(t *testing.T) {
	srv := newFakeInfoServer(t, "slave", "")
	defer srv.close()

	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		Interval: 10 * time.Millisecond,
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, func() bool { return c.Role() == RoleReplica }, time.Second)

	waitDone := make(chan struct{})
	go func() {
		_ = c.Wait(ctx)
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("Wait should be blocking while replica")
	case <-time.After(50 * time.Millisecond):
	}

	srv.setRole("master")

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after promotion")
	}
}

func TestCheckerNotifiesOnTransition(t *testing.T) {
	srv := newFakeInfoServer(t, "master", "")
	defer srv.close()

	m := &recordingMetrics{}
	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		Interval: 10 * time.Millisecond,
	}, nil, m)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, c.IsMaster, time.Second)

	notify, unsub := c.Subscribe()
	defer unsub()

	srv.setRole("slave")

	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive transition")
	}
	if c.IsMaster() {
		t.Error("IsMaster should be false")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.transitions) == 0 {
		t.Error("expected at least one transition recorded")
	}
}

func TestCheckerAllowReplicaShortCircuits(t *testing.T) {
	srv := newFakeInfoServer(t, "slave", "")
	defer srv.close()

	c := New(Options{
		Network: "tcp", Address: srv.addr(),
		Interval: 10 * time.Millisecond, AllowReplica: true,
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if !c.IsMaster() {
		t.Error("AllowReplica should force IsMaster=true regardless of probe")
	}
	if err := c.Wait(ctx); err != nil {
		t.Errorf("Wait should be immediate: %v", err)
	}
}

func TestCheckerFailureLeavesLastError(t *testing.T) {
	// Listener never accepts -- dial succeeds (or times out) but read fails.
	c := New(Options{
		Network: "tcp", Address: "127.0.0.1:1", // port 1 is almost certainly closed
		Interval:    50 * time.Millisecond,
		DialTimeout: 50 * time.Millisecond,
		ReadTimeout: 50 * time.Millisecond,
	}, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, func() bool { return c.LastError() != nil }, time.Second)
	if c.IsMaster() {
		t.Error("IsMaster should remain false when probe fails")
	}
}

func waitFor(t *testing.T, fn func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

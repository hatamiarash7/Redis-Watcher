package output

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

func sampleEvent() *event.Event {
	return &event.Event{
		Timestamp: time.Unix(1700000000, 0).UTC(),
		DB:        0,
		Source:    event.Source{Raw: "127.0.0.1:1234", IP: "127.0.0.1", Port: "1234"},
		Command:   "SET",
		Args:      []string{"key", "value"},
	}
}

func TestEncodeJSON(t *testing.T) {
	buf, err := encode(sampleEvent(), "json")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.HasSuffix(string(buf), "\n") {
		t.Error("expected trailing newline")
	}
	var got event.Event
	if err := json.Unmarshal(buf, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Command != "SET" {
		t.Errorf("cmd: %q", got.Command)
	}
}

func TestEncodeText(t *testing.T) {
	buf, err := encode(sampleEvent(), "text")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	s := string(buf)
	if !strings.Contains(s, "cmd=SET") || !strings.Contains(s, "src=127.0.0.1:1234") {
		t.Errorf("text encoding: %q", s)
	}
}

func TestFileSinkWritesAndRotates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	sink := NewFileSink(FileOptions{Path: path, MaxSizeMB: 1, Format: "json"})
	defer sink.Close()

	for i := 0; i < 10; i++ {
		if err := sink.Write(sampleEvent()); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Count(string(data), "\n")
	if lines != 10 {
		t.Errorf("lines: %d", lines)
	}
}

// fakeStringWriter records bytes for stdout-like sinks.
type recordingWriter struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (r *recordingWriter) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *recordingWriter) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

func TestStdoutSinkWrites(t *testing.T) {
	rec := &recordingWriter{}
	sink := &StdoutSink{w: rec, format: "json"}
	if err := sink.Write(sampleEvent()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !strings.Contains(rec.String(), `"command":"SET"`) {
		t.Errorf("unexpected: %q", rec.String())
	}
}

func TestNetSinkUDP(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	sink, err := NewNetSink(NetOptions{Network: "udp", Address: pc.LocalAddr().String(), Format: "json"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer sink.Close()

	if err := sink.Write(sampleEvent()); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = pc.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 4096)
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(buf[:n]), `"command":"SET"`) {
		t.Errorf("payload: %q", string(buf[:n]))
	}
}

func TestNetSinkTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var got atomic.Pointer[string]
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		line, _ := bufio.NewReader(c).ReadString('\n')
		got.Store(&line)
	}()

	sink, err := NewNetSink(NetOptions{Network: "tcp", Address: ln.Addr().String(), Format: "json"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer sink.Close()

	if err := sink.Write(sampleEvent()); err != nil {
		t.Fatalf("write: %v", err)
	}
	<-done
	if got.Load() == nil || !strings.Contains(*got.Load(), `"command":"SET"`) {
		t.Errorf("payload: %v", got.Load())
	}
}

func TestConsumerDropsWhenFull(t *testing.T) {
	sink := &blockingSink{ready: make(chan struct{})}
	c := NewConsumer(sink, 1, true, nil)

	c.Submit(sampleEvent()) // first goes into queue
	c.Submit(sampleEvent()) // dropped (queue full + sink blocked)
	c.Submit(sampleEvent()) // dropped
	close(sink.ready)

	if c.Counters().Dropped < 2 {
		t.Errorf("expected drops, got %d", c.Counters().Dropped)
	}
}

type blockingSink struct {
	ready chan struct{}
}

func (*blockingSink) Name() string { return "blocking" }
func (b *blockingSink) Write(*event.Event) error {
	<-b.ready
	return nil
}
func (*blockingSink) Close() error { return nil }

func TestManagerDispatchesToAll(t *testing.T) {
	s1 := &countingSink{}
	s2 := &countingSink{}
	m := NewManager(nil)
	m.Add(NewConsumer(s1, 8, false, nil))
	m.Add(NewConsumer(s2, 8, false, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = m.Run(ctx) }()

	for i := 0; i < 3; i++ {
		m.Dispatch(sampleEvent())
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s1.n.Load() == 3 && s2.n.Load() == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("s1=%d s2=%d", s1.n.Load(), s2.n.Load())
}

type countingSink struct{ n atomic.Int64 }

func (*countingSink) Name() string               { return "count" }
func (c *countingSink) Write(*event.Event) error { c.n.Add(1); return nil }
func (*countingSink) Close() error               { return nil }

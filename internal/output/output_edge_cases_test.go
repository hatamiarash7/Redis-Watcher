package output

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

func TestEncodeFormatsAndErrors(t *testing.T) {
	ev := sampleEvent()
	ev.Command = "CONFIG"
	ev.Subcommand = "SET"
	ev.Args = []string{"SET", "key", "a \"quoted\" \\ value"}
	got, err := encode(ev, "TEXT")
	if err != nil {
		t.Fatal(err)
	}
	want := "2023-11-14T22:13:20Z db=0 src=127.0.0.1:1234 cmd=CONFIG sub=SET SET key \"a \\\"quoted\\\" \\\\ value\"\n"
	if string(got) != want {
		t.Errorf("text = %q, want %q", got, want)
	}
	for _, format := range []string{"", "JSON"} {
		if data, err := encode(ev, format); err != nil || !strings.Contains(string(data), `"subcommand":"SET"`) {
			t.Errorf("encode(%q) = %s, %v", format, data, err)
		}
	}
	if _, err := encode(ev, "xml"); err == nil {
		t.Error("expected unknown format error")
	}
	ev.Timestamp = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := encode(ev, "json"); err == nil {
		t.Error("expected JSON timestamp error")
	}
}

func TestQuoteText(t *testing.T) {
	for _, tt := range []struct{ input, want string }{
		{"plain", "plain"},
		{"hello world", `"hello world"`},
		{"a\tb\nc\r", "\"a\tb\nc\r\""},
		{`a"b`, `"a\"b"`},
		{`a \ b`, `"a \\ b"`},
	} {
		if got := quoteText(tt.input); got != tt.want {
			t.Errorf("quoteText(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

type recordingMetrics struct {
	written, dropped, errors int
	durations                []time.Duration
	states                   []bool
	names                    []string
}

func (m *recordingMetrics) IncrOutputWritten(name string) {
	m.written++
	m.names = append(m.names, name)
}
func (m *recordingMetrics) IncrOutputDropped(name string) {
	m.dropped++
	m.names = append(m.names, name)
}
func (m *recordingMetrics) IncrOutputError(name string) { m.errors++; m.names = append(m.names, name) }
func (m *recordingMetrics) SetOutputFailing(name string, failing bool) {
	m.states = append(m.states, failing)
	m.names = append(m.names, name)
}
func (m *recordingMetrics) ObserveOutputWrite(name string, d time.Duration) {
	m.durations = append(m.durations, d)
	m.names = append(m.names, name)
}

func TestConsumerMetricsAndQueue(t *testing.T) {
	sink := &flakySink{script: []error{errBoom("first"), errBoom("second"), nil}}
	c := NewConsumer(sink, 2, true, nil)
	m := &recordingMetrics{}
	c.SetMetricsSink(m)
	if c.Name() != "flaky" || c.QueueCapacity() != 2 || c.QueueDepth() != 0 {
		t.Fatalf("unexpected consumer: %s, capacity=%d, depth=%d", c.Name(), c.QueueCapacity(), c.QueueDepth())
	}
	c.Submit(sampleEvent())
	c.Submit(sampleEvent())
	c.Submit(sampleEvent())
	if c.QueueDepth() != 2 || m.dropped != 1 || c.Counters().Dropped != 1 {
		t.Fatalf("depth=%d, drops=%d, counters=%+v", c.QueueDepth(), m.dropped, c.Counters())
	}
	for i := 0; i < 3; i++ {
		c.writeOne(sampleEvent())
	}
	if m.written != 1 || m.errors != 2 || len(m.durations) != 3 || !reflect.DeepEqual(m.states, []bool{true, false}) {
		t.Errorf("unexpected metrics: %+v", m)
	}
	for _, name := range m.names {
		if name != "flaky" {
			t.Errorf("metric attributed to %q, want flaky", name)
		}
	}
	for _, d := range m.durations {
		if d < 0 {
			t.Errorf("negative duration: %v", d)
		}
	}
	c.SetMetricsSink(nil)
	c.writeOne(sampleEvent())
	if m.written != 1 {
		t.Error("nil metrics sink should detach previous reporter")
	}
}

func TestConsumerShutdownDrain(t *testing.T) {
	for _, tt := range []struct {
		name       string
		script     []error
		failing    bool
		written    uint64
		errors     uint64
		remaining  int
		wantStates []bool
	}{
		{"success", nil, false, 3, 0, 0, nil},
		{"recovers", nil, true, 3, 0, 0, []bool{false}},
		{"stops on failure", []error{errBoom("failed")}, false, 0, 1, 2, []bool{true}},
		{"existing failure", []error{errBoom("failed")}, true, 0, 1, 2, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewConsumer(&flakySink{script: tt.script}, 3, true, nil)
			m := &recordingMetrics{}
			c.SetMetricsSink(m)
			c.failing.Store(tt.failing)
			for i := 0; i < 3; i++ {
				c.Submit(sampleEvent())
			}
			c.drainAfterShutdown()
			if got := c.Counters(); got.Written != tt.written || got.Errors != tt.errors {
				t.Errorf("counters = %+v, want written=%d errors=%d", got, tt.written, tt.errors)
			}
			if c.QueueDepth() != tt.remaining || !reflect.DeepEqual(m.states, tt.wantStates) {
				t.Errorf("remaining=%d states=%v, want remaining=%d states=%v", c.QueueDepth(), m.states, tt.remaining, tt.wantStates)
			}
			if m.written != int(tt.written) || m.errors != int(tt.errors) || len(m.durations) != int(tt.written+tt.errors) {
				t.Errorf("shutdown metrics = %+v", m)
			}
		})
	}
}

type closeErrorSink struct {
	written int
	closed  bool
}

func (*closeErrorSink) Name() string { return "close-error" }
func (s *closeErrorSink) Write(*event.Event) error {
	s.written++
	return nil
}
func (s *closeErrorSink) Close() error {
	s.closed = true
	return errors.New("close failed")
}

func TestConsumerRunDrainsAndClosesOnCancellation(t *testing.T) {
	sink := &closeErrorSink{}
	c := NewConsumer(sink, 2, true, nil)
	c.Submit(sampleEvent())
	c.Submit(sampleEvent())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.written != 2 || !sink.closed || c.QueueDepth() != 0 || c.Counters().Written != 2 {
		t.Errorf("shutdown: sink=%+v depth=%d counters=%+v", sink, c.QueueDepth(), c.Counters())
	}
}

func TestSinksRejectInvalidEncoding(t *testing.T) {
	file := NewFileSink(FileOptions{Path: filepath.Join(t.TempDir(), "audit.log"), Format: "xml"})
	network, err := NewNetSink(NetOptions{Network: "tcp", Address: "127.0.0.1:1", Format: "xml"})
	if err != nil {
		t.Fatal(err)
	}
	stdout := NewStdoutSink("xml")
	for _, sink := range []Sink{file, network, stdout} {
		t.Run(sink.Name(), func(t *testing.T) {
			defer sink.Close()
			if err := sink.Write(sampleEvent()); err == nil || !strings.Contains(err.Error(), "unknown format") {
				t.Errorf("Write error = %v, want unknown format", err)
			}
		})
	}
	if file.w.MaxSize != 100 {
		t.Errorf("default file max size = %d, want 100", file.w.MaxSize)
	}
	if stdout.w != os.Stdout {
		t.Error("stdout sink must use os.Stdout")
	}
}

func TestNetSinkValidation(t *testing.T) {
	for _, opts := range []NetOptions{
		{Network: "unix", Address: "redis.sock"},
		{Network: "tcp"},
	} {
		if sink, err := NewNetSink(opts); err == nil || sink != nil {
			t.Errorf("NewNetSink(%+v) = %v, %v; want nil and error", opts, sink, err)
		}
	}
}

func TestNetSinkResetsFailedConnection(t *testing.T) {
	sink, err := NewNetSink(NetOptions{Network: "tcp", Address: "invalid-address"})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	defer client.Close()
	_ = server.Close()
	sink.conn = client
	if err := sink.Write(sampleEvent()); err == nil || !strings.Contains(err.Error(), "write to tcp/invalid-address") {
		t.Errorf("write error = %v, want wrapped connection error", err)
	}
	if sink.conn != nil {
		t.Fatal("failed connection was not reset")
	}
	// A subsequent write must attempt to dial again, not reuse the dead pipe.
	if err := sink.Write(sampleEvent()); err == nil || strings.Contains(err.Error(), "write to") {
		t.Errorf("redial error = %v, want dial error", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

type failingStdoutWriter struct{ err error }

func (w failingStdoutWriter) Write([]byte) (int, error) { return 0, w.err }

func TestStdoutSinkPropagatesWriteError(t *testing.T) {
	want := errors.New("stdout failed")
	sink := &StdoutSink{w: failingStdoutWriter{want}, format: "json"}
	if err := sink.Write(sampleEvent()); !errors.Is(err, want) {
		t.Errorf("Write error = %v, want %v", err, want)
	}
}

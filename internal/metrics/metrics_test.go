package metrics

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/hatamiarash7/redis-watcher/internal/event"
)

func TestRegistryRecord(t *testing.T) {
	r := New(nil, true, "dev", "abcdef")
	ev := &event.Event{
		Timestamp: time.Now(),
		DB:        2,
		Source:    event.Source{IP: "10.0.0.1", Port: "1"},
		Command:   "SET",
	}
	r.Record(ev)
	r.Record(ev)

	if got := testutil.ToFloat64(r.CommandsTotal.WithLabelValues("SET", "2")); got != 2 {
		t.Errorf("commands_total: %v", got)
	}
	if got := testutil.ToFloat64(r.CommandsByIPTotal.WithLabelValues("SET", "10.0.0.1")); got != 2 {
		t.Errorf("by_ip: %v", got)
	}
	if got := testutil.ToFloat64(r.CommandsByDBTotal.WithLabelValues("2")); got != 2 {
		t.Errorf("by_db: %v", got)
	}
}

func TestRegistryIgnoreList(t *testing.T) {
	r := New([]string{"PING"}, true, "dev", "abc")
	r.Record(&event.Event{Command: "PING", Source: event.Source{IP: "1.1.1.1"}})
	if got := testutil.ToFloat64(r.CommandsTotal.WithLabelValues("PING", "0")); got != 0 {
		t.Errorf("ignored command counted: %v", got)
	}
	if got := testutil.ToFloat64(r.EventsProcessed); got != 1 {
		t.Errorf("events_processed should still count: %v", got)
	}
}

func TestServerExposesMetrics(t *testing.T) {
	r := New(nil, true, "v", "c")
	r.Record(&event.Event{Command: "GET", Source: event.Source{IP: "1.1.1.1"}})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer(ln.Addr().String(), "/metrics", r, nil)
	go func() { _ = srv.srv.Serve(ln) }()
	defer srv.srv.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "redis_watcher_commands_total") {
		t.Errorf("missing metric")
	}
	if !strings.Contains(string(body), "redis_watcher_build_info") {
		t.Errorf("missing build info")
	}
}

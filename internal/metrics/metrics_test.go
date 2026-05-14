package metrics

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestRegistrySinkInterfacesUpdateMetrics(t *testing.T) {
	r := New(nil, true, "v", "c")

	r.SetMonitorConnected(true)
	if got := testutil.ToFloat64(r.MonitorConnected); got != 1 {
		t.Errorf("monitor_connected: %v", got)
	}
	r.SetMonitorConnected(false)
	if got := testutil.ToFloat64(r.MonitorConnected); got != 0 {
		t.Errorf("monitor_connected: %v", got)
	}

	now := time.Unix(1700000123, 0)
	r.ObserveLastEvent(now)
	if got := testutil.ToFloat64(r.LastEventTimestampSeconds); got != float64(now.Unix()) {
		t.Errorf("last_event ts: %v", got)
	}

	r.IncrParseError()
	r.IncrReconnect()
	r.IncrMonitorDropped()
	if got := testutil.ToFloat64(r.ParseErrorsTotal); got != 1 {
		t.Errorf("parse errors: %v", got)
	}
	if got := testutil.ToFloat64(r.ReconnectsTotal); got != 1 {
		t.Errorf("reconnects: %v", got)
	}
	if got := testutil.ToFloat64(r.MonitorDroppedTotal); got != 1 {
		t.Errorf("monitor dropped: %v", got)
	}

	r.IncrOutputWritten("stdout")
	r.IncrOutputDropped("stdout")
	r.IncrOutputError("stdout")
	r.SetOutputFailing("stdout", true)
	r.ObserveOutputWrite("stdout", 50*time.Millisecond)
	if got := testutil.ToFloat64(r.OutputWrittenTotal.WithLabelValues("stdout")); got != 1 {
		t.Errorf("output written: %v", got)
	}
	if got := testutil.ToFloat64(r.DroppedEventsTotal.WithLabelValues("stdout")); got != 1 {
		t.Errorf("dropped: %v", got)
	}
	if got := testutil.ToFloat64(r.OutputErrorsTotal.WithLabelValues("stdout")); got != 1 {
		t.Errorf("output errors: %v", got)
	}
	if got := testutil.ToFloat64(r.OutputFailing.WithLabelValues("stdout")); got != 1 {
		t.Errorf("output failing: %v", got)
	}

	r.AlertSent("telegram", "FLUSHALL")
	r.AlertError("telegram", io.EOF)
	r.AlertRetry("telegram")
	r.AlertDropped()
	r.AlertRateLimited("FLUSHALL")
	r.ObserveAlertSend("telegram", 200*time.Millisecond)
	r.SetAlertChannelFailing("telegram", true)
	if got := testutil.ToFloat64(r.AlertsSentTotal.WithLabelValues("telegram", "FLUSHALL")); got != 1 {
		t.Errorf("alerts_sent: %v", got)
	}
	if got := testutil.ToFloat64(r.AlertSendErrors.WithLabelValues("telegram")); got != 1 {
		t.Errorf("alert errors: %v", got)
	}
	if got := testutil.ToFloat64(r.AlertRetryAttempts.WithLabelValues("telegram")); got != 1 {
		t.Errorf("retry attempts: %v", got)
	}
	if got := testutil.ToFloat64(r.AlertDroppedTotal); got != 1 {
		t.Errorf("alerts dropped: %v", got)
	}
	if got := testutil.ToFloat64(r.AlertRateLimitedTotal.WithLabelValues("FLUSHALL")); got != 1 {
		t.Errorf("alerts rate limited: %v", got)
	}
	if got := testutil.ToFloat64(r.AlertChannelFailing.WithLabelValues("telegram")); got != 1 {
		t.Errorf("alert channel failing: %v", got)
	}

	r.RecordRoleProbe(true, 5*time.Millisecond)
	r.RecordRoleProbe(false, 10*time.Millisecond)
	if got := testutil.ToFloat64(r.RoleProbeSuccessTotal); got != 1 {
		t.Errorf("probe success: %v", got)
	}
	if got := testutil.ToFloat64(r.RoleProbeFailuresTotal); got != 1 {
		t.Errorf("probe failures: %v", got)
	}
	if got := testutil.ToFloat64(r.LastRoleProbeTimestampSecs); got == 0 {
		t.Errorf("last probe ts not set")
	}
}

func TestReadinessSnapshot(t *testing.T) {
	r := New(nil, true, "v", "c")

	// Initial: nothing healthy yet.
	rs := r.Readiness()
	if rs.MonitorConnected || rs.RoleKnown {
		t.Fatalf("expected unhealthy initial state: %+v", rs)
	}

	r.SetMonitorConnected(true)
	r.SetRedisRole("master")
	r.SetOutputFailing("file", true)
	r.SetAlertChannelFailing("telegram", true)

	rs = r.Readiness()
	if !rs.MonitorConnected || !rs.RoleKnown {
		t.Fatalf("expected monitor/role healthy: %+v", rs)
	}
	if len(rs.FailingOutputs) != 1 || rs.FailingOutputs[0] != "file" {
		t.Errorf("failing outputs: %+v", rs.FailingOutputs)
	}
	if len(rs.FailingAlertChannels) != 1 || rs.FailingAlertChannels[0] != "telegram" {
		t.Errorf("failing channels: %+v", rs.FailingAlertChannels)
	}

	// Recovery clears the sets.
	r.SetOutputFailing("file", false)
	r.SetAlertChannelFailing("telegram", false)
	rs = r.Readiness()
	if len(rs.FailingOutputs) != 0 || len(rs.FailingAlertChannels) != 0 {
		t.Errorf("expected recovery to clear failing sets: %+v", rs)
	}
}

func TestReadyzReflectsState(t *testing.T) {
	r := New(nil, true, "v", "c")
	srv := NewServer("", "/metrics", r, nil)

	probe := func() (int, map[string]any) {
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}

	// Initially: not connected, no role → 503.
	code, body := probe()
	if code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 initially, got %d (%v)", code, body)
	}
	if body["ready"] != false {
		t.Errorf("ready=true unexpectedly: %v", body)
	}

	// Healthy state → 200.
	r.SetMonitorConnected(true)
	r.SetRedisRole("master")
	code, body = probe()
	if code != http.StatusOK || body["ready"] != true {
		t.Errorf("expected 200/ready=true, got %d %v", code, body)
	}

	// Output failure → 503 with the failing output listed.
	r.SetOutputFailing("file", true)
	code, body = probe()
	if code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when output failing, got %d", code)
	}
	failing, _ := body["failing_outputs"].([]any)
	if len(failing) != 1 || failing[0] != "file" {
		t.Errorf("expected failing_outputs=[file], got %v", body["failing_outputs"])
	}
}

func TestQueueDepthCollector(t *testing.T) {
	r := New(nil, true, "v", "c")
	c := NewQueueDepthCollector()
	ch := make(chan int, 8)
	ch <- 1
	ch <- 2
	c.Add("events", func() int { return len(ch) }, cap(ch))
	if err := r.RegisterCollector(c); err != nil {
		t.Fatalf("register: %v", err)
	}

	srv := NewServer("", "/metrics", r, nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `redis_watcher_queue_depth{queue="events"} 2`) {
		t.Errorf("missing queue_depth line; body excerpt:\n%s", excerpt(body, "redis_watcher_queue_"))
	}
	if !strings.Contains(body, `redis_watcher_queue_capacity{queue="events"} 8`) {
		t.Errorf("missing queue_capacity line; body excerpt:\n%s", excerpt(body, "redis_watcher_queue_"))
	}
}

func TestMetricsExposesGoAndProcessCollectors(t *testing.T) {
	r := New(nil, true, "v", "c")
	srv := NewServer("", "/metrics", r, nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	for _, want := range []string{
		"redis_watcher_process_resident_memory_bytes",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected %s in /metrics scrape", want)
		}
	}
}

func excerpt(body, prefix string) string {
	var keep []string
	for _, l := range strings.Split(body, "\n") {
		if strings.Contains(l, prefix) {
			keep = append(keep, l)
		}
	}
	return strings.Join(keep, "\n")
}

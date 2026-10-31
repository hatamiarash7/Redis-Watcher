package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/hatamiarash7/redis-watcher/internal/alert"
	"github.com/hatamiarash7/redis-watcher/internal/config"
	"github.com/hatamiarash7/redis-watcher/internal/event"
	"github.com/hatamiarash7/redis-watcher/internal/metrics"
	"github.com/hatamiarash7/redis-watcher/internal/monitor"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestBuildOutputs(t *testing.T) {
	cfg := config.Default()
	cfg.Pipeline.ConsumerBuffer = 3
	cfg.Outputs = []config.OutputConfig{
		{Type: "unknown", Enabled: false},
		{Type: "stdout", Enabled: true, Format: "json"},
		{Type: "file", Enabled: true, Format: "text", Path: filepath.Join(t.TempDir(), "audit.log")},
		{Type: "udp", Enabled: true, Address: "127.0.0.1:1"},
		{Type: "tcp", Enabled: true, Address: "127.0.0.1:1"},
	}
	mgr, err := buildOutputs(cfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	consumers := mgr.Consumers()
	wantNames := []string{"stdout", "file", "udp", "tcp"}
	if len(consumers) != len(wantNames) {
		t.Fatalf("consumer count = %d, want %d", len(consumers), len(wantNames))
	}
	for i, c := range consumers {
		if c.Name() != wantNames[i] || c.QueueCapacity() != 3 {
			t.Errorf("consumer %d = %s, capacity=%d", i, c.Name(), c.QueueCapacity())
		}
	}
	// Constructors are lazy: an already-cancelled run closes all sinks
	// without contacting the configured network addresses.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mgr.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBuildOutputsDisabledAndInvalid(t *testing.T) {
	for _, tt := range []struct {
		name    string
		outputs []config.OutputConfig
		wantErr string
	}{
		{name: "empty"},
		{name: "disabled", outputs: []config.OutputConfig{{Type: "stdout", Enabled: false}}},
		{name: "unknown type", outputs: []config.OutputConfig{{Type: "unknown", Enabled: true}}, wantErr: "unsupported output type"},
		{name: "invalid network sink", outputs: []config.OutputConfig{{Type: "tcp", Enabled: true}}, wantErr: "address required"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Outputs = tt.outputs
			mgr, err := buildOutputs(cfg, testLogger())
			if mgr != nil {
				t.Error("expected nil manager")
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestBuildAlertEngineDisabledAndInvalid(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enabled bool
		channel bool
		pattern string
		wantErr bool
	}{
		{name: "disabled"},
		{name: "no channels", enabled: true},
		{name: "invalid pattern", enabled: true, channel: true, pattern: "[", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.Alerts.Enabled = tt.enabled
			cfg.Alerts.Webhook.Enabled = tt.channel
			cfg.Alerts.Patterns = []string{tt.pattern}
			engine, err := buildAlertEngine(cfg, metrics.New(nil, true, "test", "test"), testLogger())
			if engine != nil || (err != nil) != tt.wantErr {
				t.Errorf("buildAlertEngine = %v, %v; want nil engine, error=%v", engine, err, tt.wantErr)
			}
		})
	}
}

type alertRequest struct {
	method, path, header, username, password, body string
}

func TestBuildAlertEngineDeliversConfiguredChannels(t *testing.T) {
	for _, channel := range []string{"telegram", "webhook", "pushgateway"} {
		t.Run(channel, func(t *testing.T) {
			requests := make(chan alertRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				user, pass, _ := r.BasicAuth()
				requests <- alertRequest{r.Method, r.URL.Path, r.Header.Get("X-Test"), user, pass, string(body)}
				_, _ = io.WriteString(w, `{"ok":true}`)
			}))
			defer server.Close()
			cfg := config.Default()
			cfg.Pipeline.ConsumerBuffer = 2
			cfg.Alerts.Enabled = true
			cfg.Alerts.SuspiciousCommands = []string{"FLUSHALL"}
			cfg.Alerts.Retry.MaxAttempts = 1
			switch channel {
			case "telegram":
				cfg.Alerts.Telegram = config.TelegramConfig{Enabled: true, Endpoint: server.URL, BotToken: "test-token", ChatID: "123", ThreadID: 42}
			case "webhook":
				cfg.Alerts.Webhook = config.WebhookConfig{Enabled: true, URL: server.URL + "/hook", Method: "PUT", Headers: map[string]string{"X-Test": "configured"}}
			case "pushgateway":
				cfg.Alerts.Pushgateway = config.PushgatewayConfig{Enabled: true, URLs: []string{server.URL}, Job: "test-job", Username: "test-user", Password: "test-password"}
			}
			reg := metrics.New(nil, true, "test", "test")
			engine, err := buildAlertEngine(cfg, reg, testLogger())
			if err != nil || engine == nil {
				t.Fatalf("buildAlertEngine = %v, %v", engine, err)
			}
			if engine.QueueCapacity() != 2 {
				t.Errorf("queue capacity = %d, want 2", engine.QueueCapacity())
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- engine.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("engine.Run: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("engine did not stop")
				}
			})
			engine.Submit(&event.Event{Timestamp: time.Unix(1700000000, 0).UTC(), Command: "FLUSHALL", Source: event.Source{IP: "192.0.2.1"}})
			var req alertRequest
			select {
			case req = <-requests:
			case <-ctx.Done():
				t.Fatal("no alert request received")
			}
			switch channel {
			case "telegram":
				var payload struct {
					ChatID   string `json:"chat_id"`
					ThreadID int    `json:"message_thread_id"`
				}
				if err := json.Unmarshal([]byte(req.body), &payload); err != nil {
					t.Fatal(err)
				}
				if req.path != "/bottest-token/sendMessage" || req.method != "POST" || payload.ChatID != "123" || payload.ThreadID != 42 {
					t.Errorf("telegram request = %+v, payload=%+v", req, payload)
				}
			case "webhook":
				if req.path != "/hook" || req.method != "PUT" || req.header != "configured" || !strings.Contains(req.body, "FLUSHALL") {
					t.Errorf("webhook request = %+v", req)
				}
			case "pushgateway":
				if !strings.Contains(req.path, "/metrics/job/test-job") || req.username != "test-user" || req.password != "test-password" {
					t.Errorf("pushgateway request = %+v", req)
				}
			}
		})
	}
}

func TestWireMetricsQueuesAndOutputSink(t *testing.T) {
	for _, withDownstream := range []bool{false, true} {
		t.Run(map[bool]string{false: "monitor only", true: "all queues"}[withDownstream], func(t *testing.T) {
			cfg := config.Default()
			cfg.Pipeline.ConsumerBuffer = 2
			cfg.Pipeline.DropOnFull = true
			cfg.Outputs = []config.OutputConfig{{Type: "stdout", Enabled: true}}
			cfg.Alerts.Enabled = true
			cfg.Alerts.Webhook.Enabled = true
			reg := metrics.New(nil, true, "test", "test")
			events := make(chan *event.Event, 3)
			mon := monitor.New(monitor.Options{}, events, testLogger(), nil, true)
			var engine *alert.Engine
			mgr, err := buildOutputs(cfg, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			if withDownstream {
				engine, err = buildAlertEngine(cfg, reg, testLogger())
				if err != nil {
					t.Fatal(err)
				}
			} else {
				mgr = nil
			}
			if err := wireMetrics(reg, mon, mgr, engine); err != nil {
				t.Fatal(err)
			}
			events <- &event.Event{Command: "GET"}
			want := "\n" +
				"# HELP redis_watcher_queue_capacity Capacity of an internal channel.\n" +
				"# TYPE redis_watcher_queue_capacity gauge\n" +
				"redis_watcher_queue_capacity{queue=\"events\"} 3\n" +
				"# HELP redis_watcher_queue_depth Instantaneous depth of an internal channel. Compare against `redis_watcher_queue_capacity` to detect back-pressure.\n" +
				"# TYPE redis_watcher_queue_depth gauge\n" +
				"redis_watcher_queue_depth{queue=\"events\"} 1\n"
			if withDownstream {
				for i := 0; i < 3; i++ {
					mgr.Dispatch(&event.Event{Command: "GET"})
				}
				engine.Submit(&event.Event{Command: "GET"})
				if got := testutil.ToFloat64(reg.DroppedEventsTotal.WithLabelValues("stdout")); got != 1 {
					t.Errorf("output dropped metric = %v, want 1", got)
				}
				want = "\n" +
					"# HELP redis_watcher_queue_capacity Capacity of an internal channel.\n" +
					"# TYPE redis_watcher_queue_capacity gauge\n" +
					"redis_watcher_queue_capacity{queue=\"events\"} 3\n" +
					"redis_watcher_queue_capacity{queue=\"output:stdout\"} 2\n" +
					"redis_watcher_queue_capacity{queue=\"alerts\"} 2\n" +
					"# HELP redis_watcher_queue_depth Instantaneous depth of an internal channel. Compare against `redis_watcher_queue_capacity` to detect back-pressure.\n" +
					"# TYPE redis_watcher_queue_depth gauge\n" +
					"redis_watcher_queue_depth{queue=\"events\"} 1\n" +
					"redis_watcher_queue_depth{queue=\"output:stdout\"} 2\n" +
					"redis_watcher_queue_depth{queue=\"alerts\"} 1\n"
			}
			if err := testutil.GatherAndCompare(reg.Gatherer(), strings.NewReader(want), "redis_watcher_queue_capacity", "redis_watcher_queue_depth"); err != nil {
				t.Error(err)
			}
			if err := wireMetrics(reg, mon, mgr, engine); err == nil || !strings.Contains(err.Error(), "register queue depth collector") {
				t.Errorf("duplicate registration error = %v", err)
			}
		})
	}
}

func TestBuildRoleChecker(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		enabled, allowReplica bool
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true},
		{name: "allow replica", enabled: true, allowReplica: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.RoleCheck.Enabled = tt.enabled
			cfg.RoleCheck.AllowReplica = tt.allowReplica
			reg := metrics.New(nil, true, "test", "test")
			mon := monitor.New(monitor.Options{}, nil, testLogger(), nil, true)
			checker := buildRoleChecker(cfg, reg, mon, testLogger())
			if !tt.enabled {
				if checker != nil {
					t.Fatal("expected nil checker")
				}
				return
			}
			if checker == nil || checker.IsActive() != tt.allowReplica {
				t.Errorf("checker = %v, want initial active=%v", checker, tt.allowReplica)
			}
		})
	}
}

func TestDispatcherFansOutOnlyUnfilteredEvents(t *testing.T) {
	cfg := config.Default()
	cfg.Outputs = []config.OutputConfig{{Type: "stdout", Enabled: true}}
	cfg.Alerts.Enabled = true
	cfg.Alerts.Webhook.Enabled = true
	reg := metrics.New(nil, true, "test", "test")
	mgr, err := buildOutputs(cfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	engine, err := buildAlertEngine(cfg, reg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	source := make(chan *event.Event, 3)
	source <- &event.Event{Command: "CLIENT", Subcommand: "LIST"}
	source <- &event.Event{Command: "GET"}
	source <- &event.Event{Command: "SET"}
	close(source)
	d := dispatcher{reg: reg, source: source, outputs: mgr, alerts: engine, ignored: map[string]struct{}{"CLIENT": {}}}
	d.run(context.Background())
	if got := testutil.ToFloat64(reg.EventsProcessed); got != 2 {
		t.Errorf("processed = %v, want 2", got)
	}
	if got := testutil.ToFloat64(reg.IgnoredEventsTotal.WithLabelValues("CLIENT")); got != 1 {
		t.Errorf("ignored = %v, want 1", got)
	}
	if mgr.Consumers()[0].QueueDepth() != 2 || engine.QueueDepth() != 2 {
		t.Errorf("output depth=%d alert depth=%d, want 2 each", mgr.Consumers()[0].QueueDepth(), engine.QueueDepth())
	}
}

func TestRunRejectsMissingConfig(t *testing.T) {
	err := Run(context.Background(), filepath.Join(t.TempDir(), "missing.yaml"), BuildInfo{})
	if err == nil || !strings.Contains(err.Error(), "load config:") {
		t.Errorf("Run error = %v, want wrapped config error", err)
	}
}

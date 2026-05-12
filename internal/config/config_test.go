package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestDefaultIsValid(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}
}

func TestLoadValidConfig(t *testing.T) {
	path := writeTempConfig(t, `
redis:
  network: tcp
  address: 127.0.0.1:6379
  password: secret
log:
  level: debug
  format: text
metrics:
  enabled: true
  address: ":9100"
outputs:
  - type: file
    enabled: true
    path: /tmp/audit.log
alerts:
  enabled: true
  suspicious_commands: [FLUSHALL]
  telegram:
    enabled: true
    bot_token: t
    chat_id: c
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Redis.Network != "tcp" || cfg.Redis.Address != "127.0.0.1:6379" {
		t.Errorf("redis section not parsed: %+v", cfg.Redis)
	}
	if cfg.Log.Level != "debug" || cfg.Log.Format != "text" {
		t.Errorf("log section not parsed: %+v", cfg.Log)
	}
	if len(cfg.Outputs) != 1 || cfg.Outputs[0].Format != "json" {
		t.Errorf("output defaults not applied: %+v", cfg.Outputs)
	}
	if cfg.Outputs[0].Rotation.MaxSizeMB != 100 {
		t.Errorf("default rotation size not applied: %d", cfg.Outputs[0].Rotation.MaxSizeMB)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeTempConfig(t, `
redis:
  network: tcp
  address: 127.0.0.1:6379
typo_field: nope
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestValidateRejectsBadNetwork(t *testing.T) {
	c := Default()
	c.Redis.Network = "smtp"
	if err := c.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateRequiresTelegramFields(t *testing.T) {
	c := Default()
	c.Alerts.Enabled = true
	c.Alerts.Telegram.Enabled = true
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for telegram without bot_token/chat_id")
	}
}

func TestValidateRequiresSentryDSN(t *testing.T) {
	c := Default()
	c.Sentry.Enabled = true
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for sentry without dsn")
	}
}

func TestValidateRejectsUnknownOutput(t *testing.T) {
	c := Default()
	c.Outputs = []OutputConfig{{Type: "smoke-signal", Enabled: true}}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for unknown output type")
	}
}

func TestEnvOverridesApplied(t *testing.T) {
	t.Setenv("REDIS_WATCHER_REDIS_ADDRESS", "/tmp/foo.sock")
	t.Setenv("REDIS_WATCHER_LOG_LEVEL", "warn")
	t.Setenv("REDIS_WATCHER_SENTRY_ENABLED", "true")
	t.Setenv("REDIS_WATCHER_SENTRY_DSN", "https://example/1")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Redis.Address != "/tmp/foo.sock" {
		t.Errorf("env address not applied: %q", cfg.Redis.Address)
	}
	if cfg.Log.Level != "warn" {
		t.Errorf("env level not applied: %q", cfg.Log.Level)
	}
	if !cfg.Sentry.Enabled {
		t.Errorf("sentry should be enabled")
	}
}

func TestTelegramThreadIDFromYAML(t *testing.T) {
	path := writeTempConfig(t, `
redis:
  network: tcp
  address: 127.0.0.1:6379
alerts:
  enabled: true
  telegram:
    enabled: true
    bot_token: t
    chat_id: c
    thread_id: 42
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Alerts.Telegram.ThreadID != 42 {
		t.Errorf("thread_id: %d", cfg.Alerts.Telegram.ThreadID)
	}
}

func TestTelegramThreadIDFromEnv(t *testing.T) {
	t.Setenv("REDIS_WATCHER_ALERTS_TELEGRAM_THREAD_ID", "7")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Alerts.Telegram.ThreadID != 7 {
		t.Errorf("env thread_id: %d", cfg.Alerts.Telegram.ThreadID)
	}
}

func TestTelegramEndpointFromYAML(t *testing.T) {
	path := writeTempConfig(t, `
redis:
  network: tcp
  address: 127.0.0.1:6379
alerts:
  enabled: true
  telegram:
    enabled: true
    endpoint: https://bot-api.internal:8081
    bot_token: t
    chat_id: c
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Alerts.Telegram.Endpoint; got != "https://bot-api.internal:8081" {
		t.Errorf("endpoint: %q", got)
	}
}

func TestTelegramEndpointFromEnv(t *testing.T) {
	t.Setenv("REDIS_WATCHER_ALERTS_TELEGRAM_ENDPOINT", "https://tg-proxy.example.com")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Alerts.Telegram.Endpoint; got != "https://tg-proxy.example.com" {
		t.Errorf("env endpoint: %q", got)
	}
}

func TestFilterIgnoredSetNormalizesAndDeduplicates(t *testing.T) {
	f := FilterConfig{IgnoredCommands: []string{"ping", "PING", "  Info ", "", "AUTH"}}
	got := f.IgnoredSet()
	want := []string{"PING", "INFO", "AUTH"}
	for _, w := range want {
		if _, ok := got[w]; !ok {
			t.Errorf("missing %q in set %v", w, got)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 unique entries, got %d: %v", len(got), got)
	}
}

func TestFilterLoadedFromYAML(t *testing.T) {
	path := writeTempConfig(t, `
redis:
  network: tcp
  address: 127.0.0.1:6379
filter:
  ignored_commands:
    - PING
    - publish
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	set := cfg.Filter.IgnoredSet()
	if _, ok := set["PING"]; !ok {
		t.Error("PING not in filter set")
	}
	if _, ok := set["PUBLISH"]; !ok {
		t.Error("PUBLISH not normalized to uppercase")
	}
}

func TestDurationsParsed(t *testing.T) {
	path := writeTempConfig(t, `
redis:
  network: tcp
  address: 127.0.0.1:6379
  dial_timeout: 7s
  backoff_max: 1m
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Redis.DialTimeout != 7*time.Second {
		t.Errorf("dial_timeout: %v", cfg.Redis.DialTimeout)
	}
	if cfg.Redis.BackoffMax != time.Minute {
		t.Errorf("backoff_max: %v", cfg.Redis.BackoffMax)
	}
}

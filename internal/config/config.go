// Package config loads, validates and exposes the runtime configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// envPrefix is the prefix for environment variable overrides.
const envPrefix = "REDIS_WATCHER_"

// Config is the top-level configuration.
type Config struct {
	Redis     RedisConfig     `yaml:"redis"`
	Log       LogConfig       `yaml:"log"`
	Filter    FilterConfig    `yaml:"filter"`
	RoleCheck RoleCheckConfig `yaml:"role_check"`
	Outputs   []OutputConfig  `yaml:"outputs"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Alerts    AlertsConfig    `yaml:"alerts"`
	Sentry    SentryConfig    `yaml:"sentry"`
	Pipeline  PipelineConfig  `yaml:"pipeline"`
}

// RedisConfig holds the upstream Redis connection settings.
type RedisConfig struct {
	Network     string        `yaml:"network"`
	Address     string        `yaml:"address"`
	Username    string        `yaml:"username"`
	Password    string        `yaml:"password"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
	BackoffMin  time.Duration `yaml:"backoff_min"`
	BackoffMax  time.Duration `yaml:"backoff_max"`
}

// LogConfig configures the internal application logger.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	File   string `yaml:"file"`
}

// FilterConfig configures pipeline-wide event filtering. Commands listed
// here are dropped before any downstream subsystem sees them, so they will
// NOT appear in outputs, metrics or alerts. Use this to silence high-volume,
// low-value noise such as PING/PUBLISH/INFO/AUTH on busy production hosts.
//
// NOTE: this is different from `metrics.ignored_commands`, which only
// suppresses Prometheus counters but still writes the event to outputs and
// evaluates alert rules.
type FilterConfig struct {
	IgnoredCommands []string `yaml:"ignored_commands"`
}

// IgnoredSet returns the configured commands as an upper-cased lookup set
// suitable for O(1) membership testing on the hot path.
func (f FilterConfig) IgnoredSet() map[string]struct{} {
	set := make(map[string]struct{}, len(f.IgnoredCommands))
	for _, c := range f.IgnoredCommands {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c != "" {
			set[c] = struct{}{}
		}
	}
	return set
}

// RoleCheckConfig configures the Sentinel-aware role detector. When the
// detector is enabled, Redis Watcher pauses all work (MONITOR consumption,
// outputs, metrics, alerts) while the upstream Redis is not the primary.
// This is the right default in any Sentinel-managed deployment: the watcher
// would otherwise produce duplicate audit trails on every replica.
type RoleCheckConfig struct {
	Enabled      bool          `yaml:"enabled"`
	Interval     time.Duration `yaml:"interval"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	AllowReplica bool          `yaml:"allow_replica"`
}

// OutputConfig describes one audit-log sink.
type OutputConfig struct {
	Type      string         `yaml:"type"`
	Enabled   bool           `yaml:"enabled"`
	Format    string         `yaml:"format"`
	Path      string         `yaml:"path,omitempty"`
	Address   string         `yaml:"address,omitempty"`
	Timeout   time.Duration  `yaml:"timeout,omitempty"`
	Keepalive time.Duration  `yaml:"keepalive,omitempty"`
	Rotation  RotationConfig `yaml:"rotation,omitempty"`
}

// RotationConfig configures log rotation for the `file` output type.
type RotationConfig struct {
	MaxSizeMB  int  `yaml:"max_size_mb"`
	MaxBackups int  `yaml:"max_backups"`
	MaxAgeDays int  `yaml:"max_age_days"`
	Compress   bool `yaml:"compress"`
	LocalTime  bool `yaml:"local_time"`
}

// MetricsConfig configures the Prometheus exporter.
type MetricsConfig struct {
	Enabled         bool     `yaml:"enabled"`
	Address         string   `yaml:"address"`
	Path            string   `yaml:"path"`
	IgnoredCommands []string `yaml:"ignored_commands"`
	TrackSourceIP   bool     `yaml:"track_source_ip"`
}

// AlertsConfig configures the alerting engine.
type AlertsConfig struct {
	Enabled            bool              `yaml:"enabled"`
	SuspiciousCommands []string          `yaml:"suspicious_commands"`
	Patterns           []string          `yaml:"patterns"`
	IgnoredSourceIPs   []string          `yaml:"ignored_source_ips"`
	RateLimit          RateLimitConfig   `yaml:"rate_limit"`
	Telegram           TelegramConfig    `yaml:"telegram"`
	Webhook            WebhookConfig     `yaml:"webhook"`
	Pushgateway        PushgatewayConfig `yaml:"pushgateway"`
}

// RateLimitConfig limits the rate of outbound notifications.
type RateLimitConfig struct {
	Enabled   bool          `yaml:"enabled"`
	Window    time.Duration `yaml:"window"`
	MaxAlerts int           `yaml:"max_alerts"`
}

// TelegramConfig configures Telegram bot notifications.
type TelegramConfig struct {
	Enabled  bool          `yaml:"enabled"`
	BotToken string        `yaml:"bot_token"`
	ChatID   string        `yaml:"chat_id"`
	Timeout  time.Duration `yaml:"timeout"`
}

// WebhookConfig configures generic HTTP webhook notifications.
type WebhookConfig struct {
	Enabled bool              `yaml:"enabled"`
	URL     string            `yaml:"url"`
	Method  string            `yaml:"method"`
	Timeout time.Duration     `yaml:"timeout"`
	Headers map[string]string `yaml:"headers"`
}

// PushgatewayConfig configures Prometheus Pushgateway notifications.
type PushgatewayConfig struct {
	Enabled bool              `yaml:"enabled"`
	URL     string            `yaml:"url"`
	Job     string            `yaml:"job"`
	Timeout time.Duration     `yaml:"timeout"`
	Labels  map[string]string `yaml:"labels"`
}

// SentryConfig configures error tracking.
type SentryConfig struct {
	Enabled          bool    `yaml:"enabled"`
	DSN              string  `yaml:"dsn"`
	Environment      string  `yaml:"environment"`
	Release          string  `yaml:"release"`
	SampleRate       float64 `yaml:"sample_rate"`
	TracesSampleRate float64 `yaml:"traces_sample_rate"`
	AttachStacktrace bool    `yaml:"attach_stacktrace"`
}

// PipelineConfig tunes the internal channels.
type PipelineConfig struct {
	EventBuffer    int  `yaml:"event_buffer"`
	ConsumerBuffer int  `yaml:"consumer_buffer"`
	DropOnFull     bool `yaml:"drop_on_full"`
}

// Load parses a YAML configuration file and applies environment overrides.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path) // #nosec G304 -- path is operator-supplied
		if err != nil {
			return nil, fmt.Errorf("read config %q: %w", path, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(data)))
		dec.KnownFields(true)
		if err := dec.Decode(cfg); err != nil {
			return nil, fmt.Errorf("parse config %q: %w", path, err)
		}
	}

	applyEnv(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Default returns the default configuration with sane production values.
func Default() *Config {
	return &Config{
		Redis: RedisConfig{
			Network:     "unix",
			Address:     "/var/run/redis/redis.sock",
			DialTimeout: 5 * time.Second,
			ReadTimeout: 0,
			BackoffMin:  1 * time.Second,
			BackoffMax:  30 * time.Second,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
		RoleCheck: RoleCheckConfig{
			Enabled:      true,
			Interval:     5 * time.Second,
			DialTimeout:  3 * time.Second,
			ReadTimeout:  3 * time.Second,
			AllowReplica: false,
		},
		Metrics: MetricsConfig{
			Enabled:       true,
			Address:       ":9100",
			Path:          "/metrics",
			TrackSourceIP: true,
		},
		Alerts: AlertsConfig{
			Enabled: false,
			RateLimit: RateLimitConfig{
				Enabled:   true,
				Window:    60 * time.Second,
				MaxAlerts: 5,
			},
		},
		Sentry: SentryConfig{
			Enabled:          false,
			Environment:      "production",
			SampleRate:       1.0,
			AttachStacktrace: true,
		},
		Pipeline: PipelineConfig{
			EventBuffer:    10000,
			ConsumerBuffer: 2000,
			DropOnFull:     true,
		},
	}
}

// Validate checks the configuration for inconsistencies and missing values.
func (c *Config) Validate() error {
	if c.Redis.Network != "unix" && c.Redis.Network != "tcp" {
		return fmt.Errorf("redis.network must be 'unix' or 'tcp', got %q", c.Redis.Network)
	}
	if c.Redis.Address == "" {
		return errors.New("redis.address is required")
	}
	if c.Redis.BackoffMin <= 0 {
		c.Redis.BackoffMin = time.Second
	}
	if c.Redis.BackoffMax < c.Redis.BackoffMin {
		c.Redis.BackoffMax = c.Redis.BackoffMin
	}

	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("log.level invalid: %q", c.Log.Level)
	}
	switch strings.ToLower(c.Log.Format) {
	case "", "json", "text":
	default:
		return fmt.Errorf("log.format invalid: %q", c.Log.Format)
	}

	enabledOutputs := 0
	for i := range c.Outputs {
		o := &c.Outputs[i]
		if !o.Enabled {
			continue
		}
		enabledOutputs++
		switch o.Type {
		case "stdout":
		case "file":
			if o.Path == "" {
				return fmt.Errorf("outputs[%d]: file path is required", i)
			}
			if o.Rotation.MaxSizeMB == 0 {
				o.Rotation.MaxSizeMB = 100
			}
		case "udp", "tcp":
			if o.Address == "" {
				return fmt.Errorf("outputs[%d]: address is required for %s", i, o.Type)
			}
			if o.Timeout == 0 {
				o.Timeout = 5 * time.Second
			}
		default:
			return fmt.Errorf("outputs[%d]: unknown type %q", i, o.Type)
		}
		if o.Format == "" {
			o.Format = "json"
		}
		if o.Format != "json" && o.Format != "text" {
			return fmt.Errorf("outputs[%d]: format must be json or text", i)
		}
	}

	if c.Metrics.Enabled && c.Metrics.Address == "" {
		return errors.New("metrics.address is required when metrics.enabled")
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}

	if c.Alerts.Enabled {
		if c.Alerts.Telegram.Enabled {
			if c.Alerts.Telegram.BotToken == "" || c.Alerts.Telegram.ChatID == "" {
				return errors.New("alerts.telegram requires bot_token and chat_id")
			}
		}
		if c.Alerts.Webhook.Enabled && c.Alerts.Webhook.URL == "" {
			return errors.New("alerts.webhook requires url")
		}
		if c.Alerts.Pushgateway.Enabled && c.Alerts.Pushgateway.URL == "" {
			return errors.New("alerts.pushgateway requires url")
		}
		if c.Alerts.Webhook.Enabled && c.Alerts.Webhook.Method == "" {
			c.Alerts.Webhook.Method = "POST"
		}
	}

	if c.Sentry.Enabled && c.Sentry.DSN == "" {
		return errors.New("sentry.dsn is required when sentry.enabled")
	}

	if c.Pipeline.EventBuffer <= 0 {
		c.Pipeline.EventBuffer = 10000
	}
	if c.Pipeline.ConsumerBuffer <= 0 {
		c.Pipeline.ConsumerBuffer = 2000
	}

	if c.RoleCheck.Enabled {
		if c.RoleCheck.Interval <= 0 {
			c.RoleCheck.Interval = 5 * time.Second
		}
		if c.RoleCheck.DialTimeout <= 0 {
			c.RoleCheck.DialTimeout = 3 * time.Second
		}
		if c.RoleCheck.ReadTimeout <= 0 {
			c.RoleCheck.ReadTimeout = 3 * time.Second
		}
	}
	return nil
}

// applyEnv overlays known environment variables on top of cfg.
//
// Supported variables (a non-exhaustive but practical subset):
//
//	REDIS_WATCHER_REDIS_NETWORK
//	REDIS_WATCHER_REDIS_ADDRESS
//	REDIS_WATCHER_REDIS_USERNAME
//	REDIS_WATCHER_REDIS_PASSWORD
//	REDIS_WATCHER_LOG_LEVEL
//	REDIS_WATCHER_LOG_FORMAT
//	REDIS_WATCHER_METRICS_ADDRESS
//	REDIS_WATCHER_SENTRY_DSN
//	REDIS_WATCHER_SENTRY_ENABLED
//	REDIS_WATCHER_SENTRY_ENVIRONMENT
//	REDIS_WATCHER_SENTRY_RELEASE
//	REDIS_WATCHER_ALERTS_TELEGRAM_BOT_TOKEN
//	REDIS_WATCHER_ALERTS_TELEGRAM_CHAT_ID
//	REDIS_WATCHER_ALERTS_WEBHOOK_URL
//	REDIS_WATCHER_ALERTS_PUSHGATEWAY_URL
func applyEnv(c *Config) {
	str := func(key string, dst *string) {
		if v, ok := os.LookupEnv(envPrefix + key); ok {
			*dst = v
		}
	}
	boolVar := func(key string, dst *bool) {
		if v, ok := os.LookupEnv(envPrefix + key); ok {
			if b, err := strconv.ParseBool(v); err == nil {
				*dst = b
			}
		}
	}

	str("REDIS_NETWORK", &c.Redis.Network)
	str("REDIS_ADDRESS", &c.Redis.Address)
	str("REDIS_USERNAME", &c.Redis.Username)
	str("REDIS_PASSWORD", &c.Redis.Password)
	str("LOG_LEVEL", &c.Log.Level)
	str("LOG_FORMAT", &c.Log.Format)
	str("METRICS_ADDRESS", &c.Metrics.Address)
	str("SENTRY_DSN", &c.Sentry.DSN)
	boolVar("SENTRY_ENABLED", &c.Sentry.Enabled)
	str("SENTRY_ENVIRONMENT", &c.Sentry.Environment)
	str("SENTRY_RELEASE", &c.Sentry.Release)
	str("ALERTS_TELEGRAM_BOT_TOKEN", &c.Alerts.Telegram.BotToken)
	str("ALERTS_TELEGRAM_CHAT_ID", &c.Alerts.Telegram.ChatID)
	str("ALERTS_WEBHOOK_URL", &c.Alerts.Webhook.URL)
	str("ALERTS_PUSHGATEWAY_URL", &c.Alerts.Pushgateway.URL)
}

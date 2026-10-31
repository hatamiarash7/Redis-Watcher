package logging

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hatamiarash7/redis-watcher/internal/config"
)

func TestBuildLevels(t *testing.T) {
	for _, tt := range []struct {
		name  string
		level slog.Level
	}{
		{"", slog.LevelInfo}, {"info", slog.LevelInfo},
		{"DEBUG", slog.LevelDebug}, {"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn}, {"ERROR", slog.LevelError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			log, closer, err := Build(config.LogConfig{Level: tt.name})
			if err != nil {
				t.Fatal(err)
			}
			if closer != nil {
				t.Fatal("stderr logger must not return a closer")
			}
			if !log.Enabled(context.Background(), tt.level) || log.Enabled(context.Background(), tt.level-1) {
				t.Errorf("logger does not enforce threshold %v", tt.level)
			}
		})
	}
}

func TestBuildRejectsInvalidSettings(t *testing.T) {
	for _, tt := range []struct {
		cfg  config.LogConfig
		want string
	}{
		{config.LogConfig{Level: "verbose"}, "unknown log level"},
		{config.LogConfig{Format: "xml"}, "unknown log format"},
	} {
		log, closer, err := Build(tt.cfg)
		if err == nil || !strings.Contains(err.Error(), tt.want) || log != nil || closer != nil {
			t.Errorf("Build(%+v) = %v, %v, %v; want nil logger/closer and %q", tt.cfg, log, closer, err, tt.want)
		}
	}
}

func TestBuildWritesFile(t *testing.T) {
	for _, format := range []string{"", "JSON", "TEXT"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watcher.log")
			log, closer, err := Build(config.LogConfig{File: path, Format: format, Level: "warn"})
			if err != nil {
				t.Fatal(err)
			}
			if closer == nil {
				t.Fatal("file logger must return a closer")
			}
			t.Cleanup(func() { _ = closer.Close() })
			log.Info("filtered message")
			log.Warn("visible message", "component", "monitor")
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "filtered message") || strings.Count(string(data), "\n") != 1 {
				t.Fatalf("unexpected log records: %s", data)
			}
			if format == "TEXT" {
				if !strings.Contains(string(data), `msg="visible message"`) || !strings.Contains(string(data), "component=monitor") {
					t.Errorf("text log = %s", data)
				}
				return
			}
			var record map[string]any
			if err := json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			if record["msg"] != "visible message" || record["level"] != "WARN" || record["component"] != "monitor" {
				t.Errorf("JSON record = %v", record)
			}
		})
	}
}

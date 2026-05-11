// Package logging configures the process-wide slog.Logger.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/hatamiarash7/redis-watcher/internal/config"
)

// Build creates a *slog.Logger from the supplied LogConfig.
func Build(cfg config.LogConfig) (*slog.Logger, io.Closer, error) {
	var (
		out    io.Writer = os.Stderr
		closer io.Closer
	)
	if cfg.File != "" {
		lj := &lumberjack.Logger{
			Filename:   cfg.File,
			MaxSize:    50,
			MaxBackups: 5,
			MaxAge:     14,
			Compress:   true,
		}
		out = lj
		closer = lj
	}

	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, nil, err
	}

	handlerOpts := &slog.HandlerOptions{Level: level, AddSource: false}
	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "text":
		handler = slog.NewTextHandler(out, handlerOpts)
	case "", "json":
		handler = slog.NewJSONHandler(out, handlerOpts)
	default:
		return nil, nil, fmt.Errorf("unknown log format %q", cfg.Format)
	}
	return slog.New(handler), closer, nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

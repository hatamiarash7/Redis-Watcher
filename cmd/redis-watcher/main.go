// Command redis-watcher is an audit and monitoring daemon for Redis. It
// connects to a Redis instance (typically via unix socket), runs the
// MONITOR command, parses every observed Redis operation and fans the
// result out to logs, Prometheus metrics and configurable alert channels.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/hatamiarash7/redis-watcher/internal/app"
)

// Populated at link time via -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	var (
		configPath  string
		showVersion bool
	)
	flag.StringVar(&configPath, "config", defaultConfigPath(), "path to configuration file")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("redis-watcher %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	info := app.BuildInfo{Version: version, Commit: commit, Date: date}
	if err := app.Run(context.Background(), configPath, info); err != nil {
		fmt.Fprintf(os.Stderr, "redis-watcher: %v\n", err)
		os.Exit(1)
	}
}

func defaultConfigPath() string {
	if p := os.Getenv("REDIS_WATCHER_CONFIG"); p != "" {
		return p
	}
	if _, err := os.Stat("/etc/redis-watcher/config.yaml"); err == nil {
		return "/etc/redis-watcher/config.yaml"
	}
	return "config.yaml"
}

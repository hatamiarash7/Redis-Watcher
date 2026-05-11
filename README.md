# Redis Watcher

[![Go Version](https://img.shields.io/badge/go-1.23+-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Redis Watcher is a small, production-minded daemon that subscribes to a Redis
server's `MONITOR` stream, parses every command it observes and forwards the
result to **logs**, **Prometheus metrics**, and **alert channels** (Telegram,
generic webhooks, Prometheus Pushgateway).

> **Performance note** — `MONITOR` is expensive on busy Redis instances
> because the server has to serialize every command into ASCII for the
> watcher. Use Redis Watcher on a side replica or on hosts where the
> additional CPU cost is acceptable. See [Redis docs on MONITOR][redis-monitor].

## Features

- Connects to Redis over **unix socket** or TCP (with optional AUTH)
- Streams `MONITOR` events with automatic exponential-backoff reconnect
- Parses timestamp, DB number, source IP/port, command + arguments
- Multiple **outputs** in parallel:
  - rotated **file** (lumberjack)
  - **stdout** (JSON or text)
  - **UDP/TCP** forwarder (Fluent Bit, Fluentd, syslog, …)
- **Prometheus metrics** (commands, per-IP, per-DB, alerts, drops, reconnects)
- **Alerts** on suspicious commands (`FLUSH*`, `CONFIG`, `ACL`, `KEYS`,
  `EVAL`, `SCRIPT`, `SHUTDOWN`, `DEBUG`, …) with per-(command, IP) rate
  limiting; delivered via Telegram, webhook, or Pushgateway
- **Sentry** integration for runtime error visibility
- **Docker** image (distroless, non-root) + `docker-compose.yml`
- Unit tests + integration tests behind a build tag
- Drop-on-full backpressure policy to protect the MONITOR connection

## Architecture

```text
                        +--------------------+
                        |   Redis (MONITOR)  |
                        +---------+----------+
                                  |
                                  v
                       +----------+----------+
                       |  monitor.Client     |
                       |  (RESP, reconnect)  |
                       +----------+----------+
                                  |
                          events chan *Event
                                  |
                                  v
                         +--------+--------+
                         |   Dispatcher    |
                         +--------+--------+
                                  |
        +-------------------------+-------------------------+
        |                         |                         |
        v                         v                         v
+---------------+        +-----------------+        +-----------------+
| Prom metrics  |        |    Outputs      |        |  Alert engine   |
| /metrics:9100 |        | file/stdout/    |        |  Telegram /     |
|               |        | UDP / TCP       |        |  Webhook / PGW  |
+---------------+        +-----------------+        +-----------------+
```

Each sink runs in its own goroutine with a bounded buffered channel. When
`pipeline.drop_on_full: true` (recommended), a slow downstream cannot back
up the MONITOR consumer.

## Quick start

### Local Go build

```bash
git clone https://github.com/hatamiarash7/Redis-Watcher.git
cd Redis-Watcher
cp config.example.yaml config.yaml      # edit to taste
make build
./bin/redis-watcher --config config.yaml
```

### Docker Compose (recommended for kicking the tires)

```bash
cp config.example.yaml config.yaml
make docker-compose-up
curl http://localhost:9100/metrics      # metrics
docker logs -f rw-watcher               # process logs
```

The compose stack starts a Redis instance with both a TCP port and a unix
socket (shared via a named volume) so the watcher can connect over either
transport.

## Configuration

Configuration is loaded in this order (later sources override earlier ones):

1. Built-in defaults
2. YAML file passed via `--config` (or `REDIS_WATCHER_CONFIG`)
3. Environment variables prefixed with `REDIS_WATCHER_`

See [`config.example.yaml`](config.example.yaml) for an exhaustive,
commented example.

### Useful environment variables

| Variable                                  | Purpose                              |
|-------------------------------------------|--------------------------------------|
| `REDIS_WATCHER_CONFIG`                    | Path to the config file              |
| `REDIS_WATCHER_REDIS_NETWORK`             | `unix` or `tcp`                      |
| `REDIS_WATCHER_REDIS_ADDRESS`             | Socket path or `host:port`           |
| `REDIS_WATCHER_REDIS_PASSWORD`            | AUTH password (avoid checking-in)    |
| `REDIS_WATCHER_LOG_LEVEL`                 | `debug`, `info`, `warn`, `error`     |
| `REDIS_WATCHER_METRICS_ADDRESS`           | `host:port` to expose metrics on     |
| `REDIS_WATCHER_SENTRY_DSN`                | Sentry DSN                           |
| `REDIS_WATCHER_ALERTS_TELEGRAM_BOT_TOKEN` | Telegram bot token                   |
| `REDIS_WATCHER_ALERTS_TELEGRAM_CHAT_ID`   | Telegram chat ID                     |
| `REDIS_WATCHER_ALERTS_WEBHOOK_URL`        | Webhook URL                          |
| `REDIS_WATCHER_ALERTS_PUSHGATEWAY_URL`    | Pushgateway URL                      |

## Filtering noisy commands

Busy production hosts emit a lot of `PING`, `INFO`, `AUTH`, `SELECT`,
`SUBSCRIBE` and similar housekeeping traffic that is almost never worth
auditing. Use the top-level `filter` section to drop these commands as
early as possible:

```yaml
filter:
  ignored_commands:
    - PING
    - INFO
    - AUTH
    - SELECT
    - HELLO
    - COMMAND
    - SUBSCRIBE
    - UNSUBSCRIBE
    - PSUBSCRIBE
    - PUNSUBSCRIBE
```

Matching is case-insensitive on the command name (the first token of the
Redis command). Listing a parent command like `CLIENT` also silences every
`CLIENT <subcommand>` invocation.

Filtered events do not reach outputs, metrics or alerts. They are counted
separately in `redis_watcher_ignored_events_total{command="..."}` so you
can still verify the filter is doing what you expect.

> 💡 `filter.ignored_commands` is the right knob for silencing noise.
> `metrics.ignored_commands` is a narrower setting that only suppresses
> Prometheus labels while still writing the event to outputs and the
> alert engine -- useful if you want to keep audit logs for, say, `PING`
> but not pay the metric-cardinality price.

## Prometheus metrics

All metrics are exposed at `/metrics` on the configured `metrics.address`
(default `:9100`). Notable series:

| Metric                                          | Type    | Labels                                |
|-------------------------------------------------|---------|---------------------------------------|
| `redis_watcher_commands_total`                  | counter | `command`, `db`                       |
| `redis_watcher_commands_by_ip_total`            | counter | `command`, `source_ip`                |
| `redis_watcher_commands_by_db_total`            | counter | `db`                                  |
| `redis_watcher_suspicious_commands_total`       | counter | `command`, `source_ip`                |
| `redis_watcher_alerts_sent_total`               | counter | `channel`, `command`                  |
| `redis_watcher_alert_send_errors_total`         | counter | `channel`                             |
| `redis_watcher_dropped_events_total`            | counter | `consumer`                            |
| `redis_watcher_ignored_events_total`            | counter | `command`                             |
| `redis_watcher_monitor_reconnects_total`        | counter | —                                     |
| `redis_watcher_parse_errors_total`              | counter | —                                     |
| `redis_watcher_events_processed_total`          | counter | —                                     |
| `redis_watcher_build_info`                      | gauge   | `version`, `commit`                   |

Health endpoints `/healthz` and `/readyz` are also exposed for liveness /
readiness probes.

> 💡 If you operate Redis with many client IPs, set
> `metrics.track_source_ip: false` to keep per-command time series
> cardinality bounded.

## Alerts

The alert engine matches each event against:

1. **`alerts.suspicious_commands`** — exact command names (e.g. `FLUSHALL`,
   `CONFIG`). For commands that route by subcommand (`CONFIG`, `CLIENT`,
   `ACL`, `SCRIPT`, …) the watcher exposes the joined name (e.g.
   `CONFIG SET`) as the alert title.
2. **`alerts.patterns`** — case-insensitive regular expressions applied to
   the reconstructed command line.

Rate limiting is applied per `(command, source_ip)` tuple. When the rate is
exceeded the event is recorded in metrics but not pushed downstream.

## Production checklist

- [ ] Run on a Redis **replica** (the primary should not pay the MONITOR
      tax). Replicas still see every write because of replication.
- [ ] Use `network: unix` when watching the local instance — no port, no
      network round-trip, easier filesystem permissions.
- [ ] Set `metrics.track_source_ip: false` if clients use many IPs.
- [ ] Keep `pipeline.drop_on_full: true`. Back-pressuring `MONITOR` causes
      Redis to drop the watcher.
- [ ] Scrape `/metrics`, alert on:
      `rate(redis_watcher_dropped_events_total[5m])`,
      `rate(redis_watcher_monitor_reconnects_total[5m]) > 0`,
      `rate(redis_watcher_suspicious_commands_total[5m])`.
- [ ] Capture Sentry events for the `monitor`, `metrics_server`, and
      `outputs` components.
- [ ] Make sure the unix socket has restricted permissions
      (`unixsocketperm 770` in `redis.conf`).

## Development

```bash
make tidy            # go mod tidy
make fmt             # go fmt + goimports
make vet lint        # static checks
make test            # unit tests with race detector
make test-cover      # unit tests + coverage report
make test-integration  # against a running Redis instance
make docker          # build the OCI image
```

See [`Makefile`](Makefile) for the full list of targets.

## License

MIT — see [LICENSE](LICENSE).

[redis-monitor]: https://redis.io/docs/latest/commands/monitor/

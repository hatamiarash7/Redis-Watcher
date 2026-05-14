# Redis Watcher

[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Image size](https://img.shields.io/docker/image-size/hatamiarash7/redis-watcher/latest?maxAge=30)](https://hub.docker.com/r/hatamiarash7/redis-watcher/)

Redis Watcher is a small, production-minded daemon that subscribes to a Redis
server's `MONITOR` stream, parses every command it observes and forwards the
result to **logs**, **Prometheus metrics**, and **alert channels** (Telegram,
generic webhooks, Prometheus Pushgateway).

> [!caution]
> **Performance note** — `MONITOR` is expensive on busy Redis instances because the server has to serialize every command into ASCII for the watcher. Use Redis Watcher on a side replica or on hosts where the additional CPU cost is acceptable. See [Redis docs on MONITOR](https://redis.io/docs/latest/commands/monitor/).

- [Redis Watcher](#redis-watcher)
  - [Features](#features)
  - [Architecture](#architecture)
  - [Quick start](#quick-start)
    - [Local Go build](#local-go-build)
    - [Docker Compose (recommended for kicking the tires)](#docker-compose-recommended-for-kicking-the-tires)
  - [Configuration](#configuration)
    - [Useful environment variables](#useful-environment-variables)
  - [Sentinel-aware role detection](#sentinel-aware-role-detection)
  - [Filtering noisy commands](#filtering-noisy-commands)
  - [Prometheus metrics](#prometheus-metrics)
    - [Health gauges (primary alert sources)](#health-gauges-primary-alert-sources)
    - [Throughput counters](#throughput-counters)
    - [Error / drop counters](#error--drop-counters)
    - [Latency histograms](#latency-histograms)
    - [Pipeline depth (back-pressure)](#pipeline-depth-back-pressure)
    - [Process + Go runtime](#process--go-runtime)
    - [Health endpoints](#health-endpoints)
  - [Alerts](#alerts)
    - [Retry (`alerts.retry`)](#retry-alertsretry)
  - [Production checklist](#production-checklist)
  - [Development](#development)
  - [💛 Support](#-support)
  - [🤝 Contributing](#-contributing)
  - [🐛 Issues](#-issues)

## Features

- Connects to Redis over **unix socket** or TCP
- Streams `MONITOR` events with automatic exponential-backoff reconnect
- Parses timestamp, DB number, source IP/port, command + arguments
- Multiple **outputs** in parallel:
  - rotated **file** (lumberjack)
  - **stdout** (JSON or text)
  - **UDP/TCP** forwarder (Fluent Bit, Fluentd, syslog, …)
- **Prometheus metrics** with deep operational insight — health gauges
  (`monitor_connected`, `output_failing`, `alert_channel_failing`),
  freshness gauges (`last_event_timestamp_seconds`), latency histograms
  (alert send / output write / role probe), pipeline-depth gauges,
  per-channel retry/drop/rate-limit counters, and the Go runtime +
  process collectors. `/healthz` is a pure liveness probe; `/readyz`
  fail-closes on degraded state
- **Alerts** on suspicious commands (`FLUSH*`, `CONFIG`, `ACL`, `KEYS`,
  `EVAL`, `SCRIPT`, `SHUTDOWN`, `DEBUG`, …) with per-(command, IP) rate
  limiting and **per-channel retry with exponential backoff** (configurable
  via `alerts.retry`); delivered via Telegram, webhook, or Pushgateway
- **Sentry** integration for runtime error visibility — all subsystem
  failures (alert send failures, MONITOR disconnects, role-probe failures,
  output write failures, parse errors, panics on the metrics server) are
  captured. Performance tracing is opt-in via `traces_sample_rate` and
  emits `alert.dispatch`, `role.probe` and `http.server` transactions
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

| Variable                                  | Purpose                           |
| ----------------------------------------- | --------------------------------- |
| `REDIS_WATCHER_CONFIG`                    | Path to the config file           |
| `REDIS_WATCHER_REDIS_NETWORK`             | `unix` or `tcp`                   |
| `REDIS_WATCHER_REDIS_ADDRESS`             | Socket path or `host:port`        |
| `REDIS_WATCHER_REDIS_PASSWORD`            | AUTH password (avoid checking-in) |
| `REDIS_WATCHER_LOG_LEVEL`                 | `debug`, `info`, `warn`, `error`  |
| `REDIS_WATCHER_METRICS_ADDRESS`           | `host:port` to expose metrics on  |
| `REDIS_WATCHER_SENTRY_DSN`                | Sentry DSN                        |
| `REDIS_WATCHER_ALERTS_TELEGRAM_BOT_TOKEN` | Telegram bot token                |
| `REDIS_WATCHER_ALERTS_TELEGRAM_CHAT_ID`   | Telegram chat ID                  |
| `REDIS_WATCHER_ALERTS_WEBHOOK_URL`        | Webhook URL                       |
| `REDIS_WATCHER_ALERTS_PUSHGATEWAY_URL`    | Pushgateway URL                   |

## Sentinel-aware role detection

In Redis Sentinel deployments the primary may move between hosts at any
time. Running Redis Watcher on every node would duplicate audit trails
across the fleet and trigger spurious alerts from the replication
command stream itself. Also, the real source IP:PORT will be shown only when the instance is master

Redis Watcher therefore ships with a built-in role detector. It probes
the upstream Redis with `INFO replication` every few seconds:

- While the instance is the **primary**, the pipeline runs normally.
- When the instance becomes a **replica** (e.g. after a Sentinel failover)
  the MONITOR connection is dropped immediately and outputs/metrics/alerts
  pause until the role flips back.

```yaml
role_check:
  enabled: true
  interval: 5s
  dial_timeout: 3s
  read_timeout: 3s
  allow_replica: false   # set to true only for debugging or non-Sentinel setups
```

Observability:

```text
redis_watcher_redis_is_master                       # gauge: 1=master, 0=replica
redis_watcher_redis_role_info{role="master"}        # 1 for the currently observed role
redis_watcher_redis_role_transitions_total{from,to} # counter of role flips
```

Deployment pattern: install Redis Watcher as a sidecar on **every** node
that can become a primary (e.g. as a DaemonSet, or alongside each Redis
unit in your service manager). With `role_check` enabled this is safe:
only one Redis Watcher in the cluster will actively audit at any time,
and the active one automatically follows the primary across failovers.

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

> [!note]
> `filter.ignored_commands` is the right knob for silencing noise.
> `metrics.ignored_commands` is a narrower setting that only suppresses Prometheus labels while still writing the event to outputs and the alert engine -- useful if you want to keep audit logs for, say, `PING` but not pay the metric-cardinality price.

## Prometheus metrics

All metrics are exposed at `/metrics` on the configured `metrics.address`
(default `:9100`). The same handler also serves `go_*` (Go runtime) and
`redis_watcher_process_*` (process collector, RSS / CPU / FDs) so a
single scrape gives you a full health picture without a node-exporter
sidecar.

### Health gauges (primary alert sources)

| Metric                                            | Type  | Labels              | What to alert on                                         |
| ------------------------------------------------- | ----- | ------------------- | -------------------------------------------------------- |
| `redis_watcher_monitor_connected`                 | gauge | —                   | `== 0 for 1m` → MONITOR session down                     |
| `redis_watcher_last_event_timestamp_seconds`      | gauge | —                   | `time() - this > expected_idle` → ingest stalled         |
| `redis_watcher_last_role_probe_timestamp_seconds` | gauge | —                   | `time() - this > 2 * role_check.interval` → probe stuck  |
| `redis_watcher_redis_is_master`                   | gauge | —                   | `== 0` → upstream failed over to replica                 |
| `redis_watcher_output_failing`                    | gauge | `output`            | `== 1` → that output is broken right now                 |
| `redis_watcher_alert_channel_failing`             | gauge | `channel`           | `== 1` → that channel cannot deliver alerts              |
| `redis_watcher_start_time_seconds`                | gauge | —                   | `time() - this < 60` after expected start = restart loop |
| `redis_watcher_build_info`                        | gauge | `version`, `commit` | use as info series in dashboards                         |
| `redis_watcher_redis_role_info`                   | gauge | `role`              | dashboard breakdown of replication role                  |

### Throughput counters

| Metric                                       | Type    | Labels                 |
| -------------------------------------------- | ------- | ---------------------- |
| `redis_watcher_events_processed_total`       | counter | —                      |
| `redis_watcher_commands_total`               | counter | `command`, `db`        |
| `redis_watcher_commands_by_ip_total`         | counter | `command`, `source_ip` |
| `redis_watcher_commands_by_db_total`         | counter | `db`                   |
| `redis_watcher_suspicious_commands_total`    | counter | `command`, `source_ip` |
| `redis_watcher_ignored_events_total`         | counter | `command`              |
| `redis_watcher_output_written_total`         | counter | `output`               |
| `redis_watcher_alerts_sent_total`            | counter | `channel`, `command`   |
| `redis_watcher_redis_role_transitions_total` | counter | `from`, `to`           |

### Error / drop counters

| Metric                                       | Type    | Labels     | Meaning                                                       |
| -------------------------------------------- | ------- | ---------- | ------------------------------------------------------------- |
| `redis_watcher_parse_errors_total`           | counter | —          | MONITOR line couldn't be parsed                               |
| `redis_watcher_monitor_reconnects_total`     | counter | —          | one per MONITOR reconnect attempt                             |
| `redis_watcher_monitor_dropped_events_total` | counter | —          | events dropped at the MONITOR→dispatcher boundary             |
| `redis_watcher_dropped_events_total`         | counter | `consumer` | events dropped at the dispatcher→output boundary (per output) |
| `redis_watcher_output_errors_total`          | counter | `output`   | `Sink.Write` returned an error                                |
| `redis_watcher_alert_send_errors_total`      | counter | `channel`  | one per failed `channel.Send` attempt (incl. each retry)      |
| `redis_watcher_alert_retry_attempts_total`   | counter | `channel`  | times the engine slept and tried again                        |
| `redis_watcher_alert_dropped_total`          | counter | —          | alerts dropped because the engine buffer was full             |
| `redis_watcher_alert_rate_limited_total`     | counter | `command`  | alerts suppressed by the per-(command, source_ip) limiter     |
| `redis_watcher_role_probe_failures_total`    | counter | —          | INFO replication probe failed                                 |
| `redis_watcher_role_probe_successes_total`   | counter | —          | INFO replication probe succeeded                              |

### Latency histograms

| Metric                                           | Type      | Labels    | Buckets summary                             |
| ------------------------------------------------ | --------- | --------- | ------------------------------------------- |
| `redis_watcher_output_write_duration_seconds`    | histogram | `output`  | 100µs … 5s (covers stdout to slow TCP)      |
| `redis_watcher_alert_send_duration_seconds`      | histogram | `channel` | 5ms … 10s (one bucket per realistic SLO)    |
| `redis_watcher_role_probe_duration_seconds`      | histogram | —         | 1ms … 5s                                    |
| `redis_watcher_monitor_session_duration_seconds` | histogram | —         | 1s … 1d (short sessions = Redis kicking us) |

### Pipeline depth (back-pressure)

`redis_watcher_queue_depth{queue}` and `redis_watcher_queue_capacity{queue}`
are exposed by a custom collector that reads `len()` and `cap()` of each
internal channel on every scrape. Queues:

- `events` — MONITOR → dispatcher
- `output:<name>` — dispatcher → each output consumer
- `alerts` — dispatcher → alert engine

> The healthy pattern is `depth / capacity` staying close to 0. A sustained
> ratio above ~0.5 means the consumer is too slow; >0.9 means drops are
> imminent.

### Process + Go runtime

The standard collectors are wired in:

- `go_goroutines`, `go_memstats_*`, `go_gc_duration_seconds`, `go_sched_*`
- `redis_watcher_process_resident_memory_bytes`,
  `redis_watcher_process_cpu_seconds_total`,
  `redis_watcher_process_open_fds`, etc.

### Health endpoints

- `GET /healthz` — always 200 while the HTTP server is alive. Use as a
  Kubernetes **liveness** probe (don't fail it on transient downstream
  outages or you'll restart-loop yourself).
- `GET /readyz` — returns 503 with a JSON body when any of the following
  is true:
  - MONITOR is not currently connected,
  - the role checker has not yet determined the role,
  - any output is currently in a failing state,
  - any alert channel is currently in a failing state.
  Use as a Kubernetes **readiness** probe and as the gate for blue/green
  promotions.

> [!warning]
> If you operate Redis with many client IPs, set `metrics.track_source_ip: false` to keep per-command time series cardinality bounded.

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

### Retry (`alerts.retry`)

Each configured channel is invoked independently. On a failed `Send` the
engine sleeps for `initial_backoff`, doubles on every subsequent failure
(capped at `max_backoff`), and retries up to `max_attempts` times. The
wait is context-aware, so shutdowns abort retries promptly.

- `max_attempts` counts the initial try (`1` = no retries, `3` = initial
  - up to 2 retries). Defaults to `3`.
- Initial / max backoff default to `500ms` / `5s`.
- `redis_watcher_alert_send_errors_total` bumps once **per failed
  attempt** so dashboards visibly react to retry storms.
- Sentry only captures **after** all attempts for a channel are
  exhausted — a single recovered hiccup does not page anyone. The
  captured event's `attempts`/`max_attempts` context shows how persistent
  the failure was.

Override via env (Go-duration strings for the backoffs):

```bash
REDIS_WATCHER_ALERTS_RETRY_MAX_ATTEMPTS=5
REDIS_WATCHER_ALERTS_RETRY_INITIAL_BACKOFF=250ms
REDIS_WATCHER_ALERTS_RETRY_MAX_BACKOFF=30s
```

## Production checklist

- [ ] Run on a Redis **replica** (the primary should not pay the MONITOR
      tax). Replicas still see every write because of replication.
- [ ] Use `network: unix` when watching the local instance — no port, no
      network round-trip, easier filesystem permissions.
- [ ] Set `metrics.track_source_ip: false` if clients use many IPs.
- [ ] Keep `pipeline.drop_on_full: true`. Back-pressuring `MONITOR` causes
      Redis to drop the watcher.
- [ ] Scrape `/metrics`. Suggested alerts:
      - `redis_watcher_monitor_connected == 0 for 1m` (ingest down)
      - `time() - redis_watcher_last_event_timestamp_seconds > 60`
        (ingest stalled even though we think we're connected)
      - `max by (output) (redis_watcher_output_failing) == 1 for 5m`
      - `max by (channel) (redis_watcher_alert_channel_failing) == 1`
      - `rate(redis_watcher_dropped_events_total[5m]) > 0`
      - `rate(redis_watcher_monitor_dropped_events_total[5m]) > 0`
      - `histogram_quantile(0.99, rate(redis_watcher_alert_send_duration_seconds_bucket[5m])) > 2`
      - `rate(redis_watcher_suspicious_commands_total[5m]) > 0`
- [ ] Use `/readyz` (not `/healthz`) as the readiness probe in
      Kubernetes / blue-green deployers.
- [ ] Configure Sentry. Errors are captured automatically for every
      subsystem (monitor connection, role probes, alert sends, output
      writes, /metrics handlers). For performance tracing set
      `sentry.traces_sample_rate` to a small value (0.01–0.05 is plenty);
      transactions named `alert.dispatch`, `role.probe`, and `http.server`
      surface latency for the operationally interesting paths.
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

---

## 💛 Support

[![Donate with Bitcoin](https://img.shields.io/badge/Bitcoin-bc1qmmh6vt366yzjt3grjxjjqynrrxs3frun8gnxrz-orange)](https://donatebadges.ir/donate/Bitcoin/bc1qmmh6vt366yzjt3grjxjjqynrrxs3frun8gnxrz)
[![Donate with Ethereum](https://img.shields.io/badge/Ethereum-0x0831bD72Ea8904B38Be9D6185Da2f930d6078094-blueviolet)](https://donatebadges.ir/donate/Ethereum/0x0831bD72Ea8904B38Be9D6185Da2f930d6078094)

<div><a href="https://payping.ir/@hatamiarash7"><img src="https://cdn.payping.ir/statics/Payping-logo/Trust/blue.svg" height="128" width="128"></a></div>

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/my-new-feature`
3. Install development dependencies: `make install-dev`
4. Make your changes and add tests
5. Run checks: `make check`
6. Commit your changes: `git commit -am 'Add some feature'`
7. Push to the branch: `git push origin feature/my-new-feature`
8. Submit a pull request

## 🐛 Issues

Found a bug or have a suggestion? Please [open an issue](https://github.com/hatamiarash7/Redis-Watcher/issues).

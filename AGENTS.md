# AGENTS.md — Redis Watcher

Audit/monitoring daemon for Redis. Subscribes to `MONITOR`, parses every
observed command, and fans events out to logs / Prometheus / alert channels.
In Sentinel deployments a role checker pauses the whole pipeline whenever
the upstream Redis is a replica.

For the feature overview, architecture diagram, full metrics catalogue,
configuration reference, and the production checklist, see
[`README.md`](README.md). **Read it before touching anything non-trivial** —
most operational context lives there and is not duplicated here.

## Layout (only the non-obvious bits)

- `cmd/redis-watcher/main.go` — intentionally thin. Flags, `-ldflags`
  build vars (`version`, `commit`, `date`), and one call into
  `internal/app.Run`. **Do not add wiring here**; it lives in `app/`.
- `internal/app/app.go` — composition root. Builds outputs, alerts,
  metrics, monitor, role checker; owns the `dispatcher` goroutine;
  handles signals. `wireMetrics` plumbs the `*metrics.Registry` into
  every component as their sink and registers the queue-depth
  collector — extend it (not `Run`) when adding instrumentation.
- `internal/monitor/` — speaks **raw RESP** on a dedicated connection
  because `go-redis` (and any pooled client) cannot expose `MONITOR`
  cleanly. The file header in `client.go` explains why. The client
  takes an optional `Gate` (set via `SetGate`); when the gate is
  inactive the read loop pauses and disconnects from Redis.
- `internal/role/` — Sentinel-aware role detector. Periodically polls
  `INFO replication` on its own short-lived connection and implements
  `monitor.Gate` so the pipeline runs **only on the primary**. Exposes
  `Subscribe`/`Wait` for change notifications.
- `internal/resp/` — minimal RESP2 codec shared by `monitor` and
  `role` (AUTH, MONITOR, INFO, ROLE only). **Don't grow it.** If a new
  feature needs SUBSCRIBE/transactions/pipelining, pull in
  `github.com/redis/go-redis` instead — the package doc says so.
- `internal/event/` — the canonical `*Event` type. Every other package
  consumes this. Fields are JSON-tagged; renaming them is a breaking
  change for downstream log consumers.
- `internal/output/` — `Sink` interface + per-sink `Consumer` (own
  goroutine, own buffered channel, own drop counter). Consumers take
  a `MetricsSink` via `SetMetricsSink`.
- `internal/alert/` — rule engine + per-channel retry with exponential
  backoff. Channels (`telegram.go`, `webhook.go`, `pushgateway.go`)
  implement `alert.Channel`. The engine emits a Sentry transaction
  per fired alert (`alert.dispatch`) with child spans per channel
  (`alert.send.<name>`); Sentry capture happens **only after all
  retries are exhausted**.
- `internal/metrics/` — `Registry` (owns its own non-default
  `*prometheus.Registry`) + `Server` (`/metrics`, `/healthz`,
  `/readyz`). The Registry implements every sink interface used
  elsewhere (see "Instrumentation" below). `collectors.go` holds the
  `QueueDepthCollector` — a scrape-time collector that reads `len()`
  and `cap()` of channels via closures so producers stay lock-free.
- `internal/sentryx/` — thin Sentry wrapper exposing `Init`,
  `Report`/`Capture`, `StartSpan` + `Set{SpanData,SpanTag}`,
  `WrapHTTP`, `Recover`. **Every other production package must go
  through it** — do not import `getsentry/sentry-go` elsewhere. The
  helpers are safe no-ops when Sentry is disabled.
- `internal/sentryx/sentryxtest/` — the **only sanctioned** place
  outside `sentryx` that imports `sentry-go` (tests only). `Swap(t)`
  installs a recording transport; assert via `Recorder.Events()` /
  `WaitFor`.
- `internal/config/config.go` — `Validate()` both validates **and**
  fills in defaults (mutates the receiver). Add new fields by editing
  the struct, `Default()`, `Validate()`, `applyEnv()`, and
  [`config.example.yaml`](config.example.yaml) together. `applyEnv`
  has typed helpers for string / bool / int / slice / duration.
- `test/integration/` — gated by `//go:build integration`. Requires a
  running Redis; honors `REDIS_WATCHER_TEST_ADDR` /
  `REDIS_WATCHER_TEST_PASSWORD`. `make ci` now runs these too.

## Commands

Routine tasks have Makefile targets — see [`Makefile`](Makefile)
(`make help` for the full list). The common ones:

```bash
make tidy fmt vet lint   # format + static checks
make test                # unit tests with -race
make test-integration    # requires a running Redis (host:port via REDIS_WATCHER_TEST_ADDR)
make ci                  # what CI runs: vet + lint + unit + integration
make build               # binary into ./bin/
make docker-compose-up   # creates ./redis-socket, then brings up Redis + watcher
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs
`go vet`, `golangci-lint`, unit tests with race + coverage, integration
tests against a real Redis service, and a Docker image build.

## Conventions

- **Module path** `github.com/hatamiarash7/redis-watcher`. Go version
  pinned in [`go.mod`](go.mod); when bumping it, also update the
  Dockerfile base image and the README badge.
- **Imports**: `goimports` with `-local github.com/hatamiarash7/redis-watcher`
  (third-party group is separate from the project's own packages).
  `make fmt` handles this.
- **Linters**: see [`.golangci.yml`](.golangci.yml). `gocyclo` cap is
  20; `revive` requires doc comments on every exported identifier
  (`revive.exported`). When you add an exported type/func, add the
  godoc comment in the same change.
- **Logging**: `log/slog` via `internal/logging.Build`. Pass `*slog.Logger`
  into constructors; do not call `slog.Default()` deep in libraries.
- **Errors**: wrap with `%w`; never log-and-swallow on the hot path
  (MONITOR read loop, dispatcher, alert engine). Use
  `sentryx.Capture(ctx, err, "stage", "...")` when a `context.Context`
  is available (so the event lands on the right hub); `sentryx.Report`
  is the context-less fallback.
- **Goroutines**: every long-running component takes a `context.Context`
  and returns when it's cancelled. Follow the `Run(ctx) error` shape
  used by `monitor.Client`, `output.Consumer`, `alert.Engine`,
  `role.Checker`, `metrics.Server`. New goroutines that we own should
  start with `defer sentryx.Recover()` so panics land in Sentry.
- **Constructor options**: prefer a typed `XxxOptions` struct over a
  long positional argument list. See `alert.TelegramOptions`,
  `alert.PushgatewayOptions`, `output.FileOptions`, `monitor.Options`,
  `role.Options`.
- **Tests** live next to the code as `*_test.go`. Integration tests
  must carry the `//go:build integration` tag at the top of the file.
  Sentry-touching tests use `sentryxtest.Swap(t, tracesEnabled)` to
  install a recording transport.

## Instrumentation pattern (metrics & sinks)

A single `*metrics.Registry` is constructed in `app.Run` and threaded
into every component as that component's narrow sink interface
(`monitor.MetricsSink`, `output.MetricsSink`, `alert.Reporter`,
`role.MetricsSink`, …). The Registry implements all of them. New
components should follow the same shape:

1. Declare an interface in the consumer package containing only the
   methods you actually need (keeps the package decoupled and
   testable with a small fake).
2. Add the matching methods on `metrics.Registry` (with comments
   pointing at the consumer interface), wire them into the right
   metric — counter / gauge / histogram.
3. Pass `reg` from `app.Run` (it satisfies the new interface
   automatically).

**Optional capabilities use a type-asserted extension interface.** See
`alert.RetryReporter` and `role.ProbeMetricsSink`: the base interface
stays small and stable; richer telemetry is opt-in via the assert. Use
this idiom when adding telemetry you don't want to force on every
consumer.

**Health gauges drive `/readyz`.** The handler reads
`Registry.Readiness()` which is populated by `SetMonitorConnected`,
`SetRedisRole`, `SetOutputFailing` and `SetAlertChannelFailing`. If
your new component contributes to readiness, publish state through one
of these (or add a sibling) — don't add a private boolean in `app/`.

## Sentry tracing

Tracing is opt-in via `sentry.traces_sample_rate > 0`. When enabled the
watcher emits transactions at these op / description pairs:

| Op              | Description                | Where                    |
| --------------- | -------------------------- | ------------------------ |
| `task.alert`    | `alert.dispatch`           | `alert.Engine.handle`    |
| `http.client`   | `alert.send.<channel>`     | `alert.Engine.sendOne`   |
| `db.redis.info` | `role.probe`               | `role.Checker.probeOnce` |
| `http.server`   | per-request (when enabled) | `sentryx.WrapHTTP`       |

When adding a new long-running unit of work, wrap it in
`sentryx.StartSpan(ctx, op, description)` and always invoke the
returned `FinishSpan`. Pass the returned `ctx` down so nested spans
nest correctly. Use `SetSpanTag` for low-cardinality, indexed values
(channel name, command); `SetSpanData` for free-form context.

## Pitfalls (read before changing pipeline code)

1. **Never back-pressure `MONITOR`.** If a downstream consumer blocks
   the read loop, Redis disconnects the watcher. The repo-wide default
   is `pipeline.drop_on_full: true` and every consumer must honour it.
   When adding a new sink, plug it in through `output.Consumer`
   (which already implements the drop policy) rather than reading
   directly from the events channel.
2. **MONITOR is expensive on busy Redis.** Don't add features that
   assume cheap operation; see the README "Performance note".
3. **`Validate()` mutates `Config`.** It applies defaults (timeouts,
   buffer sizes, output format, role-check intervals). Don't treat it
   as a pure check.
4. **Two different "ignore" knobs.** `filter.ignored_commands`
   (top-level) drops events before they reach metrics / outputs /
   alerts and increments `redis_watcher_ignored_events_total`.
   `metrics.ignored_commands` only suppresses Prometheus counters but
   still writes the event downstream. Pick the right one.
5. **Per-source-IP cardinality.** Metrics that label by source IP are
   guarded by `metrics.track_source_ip`. New IP-labelled series must
   honour that flag — see how `Registry.Record` consumes it.
6. **Subcommand parsing.** Commands in `commandsWithSubcommand`
   (see `monitor/parser.go`) split their first argument into
   `Event.Subcommand`. Add new ones there if you need alerting /
   metrics to treat them as separate (e.g. `CLIENT KILL` vs
   `CLIENT LIST`).
7. **Alert retry semantics.** `AlertError` fires **once per failed
   attempt** (so retry storms light up dashboards). `AlertSent` fires
   **once on eventual success**. Sentry capture happens **once after
   all retries are exhausted** — don't add a per-attempt capture.
   When introducing a new failure surface, follow the same
   "metrics-on-every-failure, Sentry-on-final-failure" rule.
8. **Role-aware components.** Anything that opens a connection to
   Redis on its own (currently just `role.Checker`) should keep its
   timeouts independent of `RedisConfig.ReadTimeout` (which is `0`
   for MONITOR). Use a `RoleCheckConfig`-style sub-section, not the
   global redis config, for new probes.
9. **Secrets in logs.** `monitor.redact` masks the port of TCP
   addresses before logging. Don't log `cfg.Redis.Password`,
   `Telegram.BotToken`, `Pushgateway.Password`, webhook URLs, or
   webhook header values. `config.yaml` is gitignored — keep it that
   way; never copy snippets containing real tokens into commits, PR
   descriptions, or tests.
10. **`internal/resp` is intentionally minimal.** Don't extend it
    with new RESP types or pipelining. If you need a real client,
    add `github.com/redis/go-redis` and use it from a new package.
11. **Build-info ldflags.** `version`, `commit`, `date` are injected
    via `-X main.*` and surfaced in `/metrics` and Sentry releases.
    Keep the variable names in `cmd/redis-watcher/main.go` in sync
    with the Makefile and Dockerfile.

## Adding things — quick recipes

- **New config field**: edit the struct in `internal/config/config.go`,
  give it a `yaml:` tag, set its default in `Default()`, validate +
  normalise it in `Validate()`, optionally expose an env override in
  `applyEnv()` (use `strVar` / `boolVar` / `intVar` / `sliceVar` /
  `durVar`), and document it in
  [`config.example.yaml`](config.example.yaml) with a comment.
- **New output sink**: implement `output.Sink`
  (`Name`/`Write`/`Close`), register a new `type` in
  `app.buildOutputs`, extend `config.OutputConfig` if you need extra
  fields, and document the new type in `config.example.yaml`. Wire
  up `IncrOutputWritten` / `ObserveOutputWrite` / `SetOutputFailing`
  on the per-consumer metrics sink so `/readyz` and dashboards reflect
  the new sink automatically.
- **New alert channel**: implement `alert.Channel` (a `Name` method,
  a `Send(ctx, Alert) error` method, a `Close`), add a typed
  `XxxOptions` struct, wire it in `app.buildAlertEngine`, add its
  config struct under `AlertsConfig`, and bump the README "Alerts"
  section. The retry/backoff/Sentry behaviour is provided by the
  engine — don't reimplement it.
- **New Prometheus metric**: declare and register it in
  `internal/metrics/metrics.go` (under the right `register*` group),
  add a typed method on `Registry`, and document it in the README
  table. Use the existing histogram bucket lists as a guide so
  dashboards remain consistent across channels/outputs.
- **New gated component** (something that should pause with the role
  checker): take a `monitor.Gate` (or `role.Checker`) in your
  constructor and call `IsActive()` / `Wait(ctx)` at the start of
  each iteration. Don't create a new role-detection loop.

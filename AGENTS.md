# AGENTS.md — Redis Watcher

Audit/monitoring daemon for Redis. Subscribes to `MONITOR`, parses every
observed command, and fans events out to logs / Prometheus / alert channels.

For a feature overview, architecture diagram, configuration reference,
metrics list, and the production checklist, see [`README.md`](README.md).
**Read it before touching anything non-trivial** — most operational
context lives there and is not duplicated here.

## Layout (only the non-obvious bits)

- `cmd/redis-watcher/main.go` — intentionally thin. Flags, `-ldflags`
  build vars (`version`, `commit`, `date`), and one call into
  `internal/app.Run`. **Do not add wiring here**; it lives in `app/`.
- `internal/app/app.go` — composition root. Builds outputs, alerts,
  metrics, monitor; owns the dispatch goroutine; handles signals.
- `internal/monitor/` — speaks **raw RESP** on a dedicated connection
  because `go-redis` (and any pooled client) cannot expose `MONITOR`
  cleanly. **Do not introduce a Redis client library on this path** —
  the file header in `client.go` explains why.
- `internal/event/` — the canonical `*Event` type. Every other package
  consumes this. Fields are JSON-tagged; renaming them is a breaking
  change for downstream log consumers.
- `internal/output/` — `Sink` interface + per-sink `Consumer` (own
  goroutine, own buffered channel, own drop counter).
- `internal/alert/` — rule engine, rate limiter, channels
  (`telegram.go`, `webhook.go`, `pushgateway.go`).
- `internal/metrics/` — Prometheus registry + `/metrics`, `/healthz`,
  `/readyz` server. All series are listed in README.
- `internal/sentryx/` — thin Sentry wrapper. `Init` and `Report` are
  **safe no-ops when disabled**; always go through this package, never
  import `getsentry/sentry-go` elsewhere.
- `internal/config/config.go` — `Validate()` both validates **and**
  fills in defaults (mutates the receiver). Add new fields by editing
  the struct, `Default()`, `Validate()`, `applyEnv()`, and
  [`config.example.yaml`](config.example.yaml) together.
- `test/integration/` — gated by `//go:build integration`. Requires a
  running Redis; honors `REDIS_WATCHER_TEST_ADDR` /
  `REDIS_WATCHER_TEST_PASSWORD`.

## Commands

All routine tasks have Makefile targets — see [`Makefile`](Makefile)
(`make help` for the full list). The common ones:

```bash
make tidy fmt vet lint   # format + static checks
make test                # unit tests with -race
make test-integration    # requires a running Redis
make ci                  # what CI runs (vet + lint + test)
make build               # binary into ./bin/
make docker-compose-up   # Redis + watcher, talking over unix socket
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs
`go vet`, `golangci-lint`, unit tests with race + coverage, integration
tests against a real Redis service, and a Docker image build.

## Conventions

- **Module path** `github.com/hatamiarash7/redis-watcher`. Go version
  pinned in [`go.mod`](go.mod); update both there and in the Dockerfile
  base image when bumping.
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
  (MONITOR read loop, dispatch). Use `sentryx.Report(err, "stage",
  "...")` for runtime issues that need ops visibility.
- **Goroutines**: every long-running component takes a `context.Context`
  and returns when it's cancelled. New components should follow the
  `Run(ctx) error` shape used by `monitor.Client`, `output.Consumer`,
  `alert.Engine`, `metrics.Server`.
- **Tests** live next to the code as `*_test.go`. Integration tests
  must carry the `//go:build integration` tag at the top of the file.

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
   buffer sizes, output format). Don't treat it as a pure check.
4. **Per-source-IP cardinality.** Metrics that label by source IP are
   guarded by `metrics.track_source_ip`. New IP-labelled series must
   honour that flag — see how `metrics.New` consumes it.
5. **Subcommand parsing.** Commands in
   `commandsWithSubcommand` (see `monitor/parser.go`) split their
   first argument into `Event.Subcommand`. Add new ones there if you
   need alerting/metrics to treat them as separate (e.g. `CLIENT KILL`
   vs `CLIENT LIST`).
6. **Secrets in logs.** `monitor.redact` masks the port of TCP
   addresses before logging. Don't log `cfg.Redis.Password`,
   webhook URLs, or Telegram tokens.
7. **Build-info ldflags.** `version`, `commit`, `date` are injected via
   `-X main.*` and surfaced in `/metrics` and Sentry releases. Keep
   the variable names in `cmd/redis-watcher/main.go` in sync with the
   Makefile and Dockerfile.

## Adding things — quick recipes

- **New config field**: edit the struct in `internal/config/config.go`,
  give it a `yaml:` tag, set its default in `Default()`, validate it in
  `Validate()`, optionally expose an env override in `applyEnv()`, and
  document it in [`config.example.yaml`](config.example.yaml) with a
  comment.
- **New output sink**: implement `output.Sink`
  (`Name`/`Write`/`Close`), register a new `type` in
  `app.buildOutputs`, extend `config.OutputConfig` if you need extra
  fields, and document the new type in `config.example.yaml`.
- **New alert channel**: implement `alert.Channel`, wire it in
  `app.buildAlertEngine`, add its config struct under `AlertsConfig`,
  and bump the README "Alerts" section.
- **New Prometheus metric**: declare it in `internal/metrics/metrics.go`,
  register it in `New`, document it in the README table.

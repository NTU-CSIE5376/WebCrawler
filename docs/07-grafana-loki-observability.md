# 07. Grafana + Loki Observability

This document covers the Loki/Grafana log-monitoring stack added to this
project: what it is, how it is wired into the existing services, how to
run it in production, and how to extend the dashboard with new panels.

## 7.1 What this gives you

Every Python service (crawler, offerer, ingestor, router) writes
structured JSON log lines via `libs/obslog`. The Docker `loki` log
driver picks those lines up directly from each container's stdout,
parses the JSON, and pushes it to a Loki server. Grafana then queries
Loki and renders the **Crawler — Live (Loki)** dashboard.

End-to-end flow:

```
[Python service] --json log line--> stdout
        |
        v
[Docker loki driver]  -- pipeline parses JSON, sets stream labels
        |
        v
[Loki ingester]  -- stores chunks, indexed by labels
        |
        v
[Grafana]  -- LogQL queries, dashboard panels
```

No extra agent (Promtail, Fluentd) is required. The Docker plugin
handles ingestion.

## 7.2 What we configured

### 7.2.1 Application-side (`libs/obslog`)

`libs/obslog/configure(service, worker_id, shard)` is called from each
service's main entry point. It:

- Removes Scrapy / SQLAlchemy default handlers from the root logger
- Installs a single `python-json-logger` handler on stdout
- Adds a `_ContextFilter` that injects `service` / `worker_id` / `shard`
  into every log record

Every service then logs with `extra={"event": "...", ...}`. The output
line is one-line JSON with at least:

```json
{
  "ts": "...",
  "level": "INFO",
  "logger": "crawler",
  "message": "...",
  "event": "...",
  "service": "crawler",
  "worker_id": 7,
  ...event-specific fields...
}
```

### 7.2.2 Docker compose (`docker-compose.yml`)

There is one shared logging anchor `x-loki-logging` reused by all three
deployable services (`scheduler_control`, `scheduler_ingest`,
`crawler`). `postgres` keeps the default `json-file` driver because we
have no application observability concern there.

Key options on the anchor:

| Option | Value | Why |
|---|---|---|
| `loki-url` | `http://localhost:3100/loki/api/v1/push` | Assumes Loki on the host. Adjust if Loki lives elsewhere. |
| `loki-batch-size` | `1048576` (1 MiB) | Batches roughly 1 second of log volume. Smaller values cause burst-drop on high-rate events; larger values waste memory. See §7.5. |
| `mode` | `non-blocking` | Driver never blocks the container. Trade-off: under extreme load the driver buffer can drop entries. |
| `keep-file: true` | – | Container stdout is also kept as a `json-file` for emergency `docker logs` access if Loki goes down. |
| `loki-pipeline-stages` | see §7.2.3 | JSON-parses each line and promotes a small set of fields to stream labels. |

### 7.2.3 Pipeline stages and labels

The pipeline runs at the Docker driver, before lines hit Loki:

```yaml
- json:
    expressions:
      level, service, worker_id, shard, event, domain, status, latency_ms
- labels:
    level
    service
    worker_id
    event
```

Stream **labels** become indexed dimensions in Loki. Keep this list
small; every distinct label combination is a separate stream.

- `service`, `worker_id`, `level`, `event` are bounded sets (a few dozen
  values), so they are safe as labels.
- `domain`, `status`, `latency_ms` are extracted but **not promoted to
  labels**. They live in the log line and are reachable at query time
  via `| json` (see §7.4 for an example).

**Do not** add `domain` to labels: with thousands of unique hostnames
crawled, Loki's stream cardinality would explode and crater both ingest
and query performance.

### 7.2.4 Loki server config (`ops/loki/loki-config.yml`)

Single-binary mode. TSDB schema, filesystem chunks, 30-day retention,
structured-metadata enabled. Tuned for a single-host deployment, not a
cluster. Edit `retention_period` and `ingestion_rate_mb` if your
volume changes substantially.

### 7.2.5 Grafana provisioning (`ops/grafana/`)

- `provisioning/datasources/loki.yml` – wires Loki as the default
  datasource at `http://loki:3100`.
- `provisioning/dashboards/dashboards.yml` – auto-loads any JSON under
  `/etc/grafana/dashboards`. `updateIntervalSeconds: 30` means edits to
  the JSON file take effect within 30 s without restarting Grafana.
- `dashboards/crawler.json` – the dashboard described in §7.4.

## 7.3 First-time deployment

These are one-time host steps. Repeat per host that runs the stack.

### 7.3.1 Install the Loki Docker driver plugin

```bash
docker plugin install grafana/loki-docker-driver:3.0.0 \
    --alias loki --grant-all-permissions
docker plugin ls | grep loki   # expect: loki:latest, ENABLED=true
```

### 7.3.2 Run Loki and Grafana

Both services are referenced by `docker-compose.yml` only as the log
*destination* (Loki) and the *viewer* (Grafana); they are not part of
that compose file and must be brought up separately. Two options:

- Run them on the same host with a small standalone compose (mount
  `ops/loki/loki-config.yml` to `/etc/loki/loki-config.yml`, mount
  `ops/grafana/provisioning` and `ops/grafana/dashboards`, expose 3100
  and 3000), or
- Point `loki-url` at an existing managed Loki and import the dashboard
  into an existing Grafana.

Either way, the `loki-url` in `x-loki-logging` must be reachable from
inside the application containers.

### 7.3.3 Bring up the application stack

```bash
docker compose up -d --build
```

Verify the loki driver is taking the logs (you should not see plain
text from `docker logs crawler` after a few seconds — the driver
captures logs into Loki and keeps a json-file copy):

```bash
docker logs crawler --tail 5    # JSON lines, one per row
```

### 7.3.4 Open the dashboard

Browse to `http://<host>:3000`. The **Crawler — Live (Loki)**
dashboard is auto-provisioned under the *Crawler* folder. Default
admin / admin on first login (change immediately).

## 7.4 Existing dashboard panels

| Panel | What it shows | Query (simplified) |
|---|---|---|
| QPS per crawler worker | `request.start` rate per worker — how many HTTPs/sec each crawler issues | `sum by (worker_id) (rate({event="request.start"}[1m]))` |
| QPS per domain (top 10) | Top 10 hostnames by issuing rate | `topk(10, sum by (domain) (count_over_time({event="request.start"} \| json domain="domain" [1m])) / 60)` |
| Success rate (last 5m) | Successful HTTPs / total attempts. Bounded `[0, 1]`. | `rate(end & status<400) / (rate(end) + rate(fail and not HttpError))` |
| Download latency p50/p95/p99 | Per-worker quantiles of `latency_ms` | `quantile_over_time(0.5, ... \| unwrap latency_ms [1m]) by (worker_id)` |
| Inflight / Pending / Active domains / Transferring / Slot queue | Per-worker spider-internals from `spider.heartbeat` events (one every 5 s) | `avg by (worker_id) (avg_over_time({event="spider.heartbeat"} \| json \| unwrap <field> [1m]))` |
| Errors & warnings (live tail) | Raw log tail for `level=ERROR` or `WARNING` across all services | `{service=~".+", level=~"ERROR\|WARNING"}` |

### 7.4.1 Event vocabulary

Events emitted by the crawler spider:

| Event | When | Useful fields |
|---|---|---|
| `request.scheduled` | Scrapy `request_scheduled` signal. **Unreliable** — does not fire on every code path. Avoid. | url, domain |
| `request.start` | `request_reached_downloader` — the request actually leaves Scrapy for HTTP. **Use this for QPS.** | url, domain |
| `request.end` | `response_received` — HTTP response came back (any status). | url, domain, status, latency_ms |
| `request.fail` | Spider `errback` — request failed (timeout, DNS, robots block, HttpError, etc.). | url, domain, fail_reason, status |
| `spider.heartbeat` | Periodic, every `OBSLOG_HEARTBEAT_SEC` (default 5 s). Snapshot of internal counters. | inflight, pending, transferring, slot_queue, active_domains, … |
| `spider.stats` | Emitted from `_log()` on top-up / queue events. Same fields as heartbeat plus a free-form message. | … |

Events emitted by the offerer:

| Event | Notes |
|---|---|
| `offer.refill` | One per scan cycle. Includes `picked_urls`, `new_domains`. |
| `offer.error` | DB / disk error path. |

Events emitted by ingestor / router (`ingest.*`, `route.*`) are similar
in shape — see the call sites in `containers/scheduler_ingest/` for the
exact fields.

## 7.5 Adding a new panel

The provisioned dashboard is loaded from
`ops/grafana/dashboards/crawler.json`. You can either:

1. **Edit in Grafana, export back into the repo.** Open the dashboard,
   click *Edit*, save, then *Dashboard settings → JSON Model → Save to
   file*. Replace `ops/grafana/dashboards/crawler.json` and commit.
   Provisioning will reload within 30 s.
2. **Hand-edit the JSON.** For small additions this is often quicker.
   Copy an existing panel, change `id`, `title`, `gridPos`, and
   `targets[].expr`. Validate with `python -c "import json;
   json.load(open('ops/grafana/dashboards/crawler.json'))"` before
   committing.

### 7.5.1 LogQL idioms used in this dashboard

- **Rate of an event** (events / sec):
  ```
  sum by (worker_id) (rate({service="crawler", event="X"}[1m]))
  ```

- **Quantile over an unwrapped numeric field**, grouped by worker:
  ```
  quantile_over_time(0.95, {... event="request.end"} | json | unwrap latency_ms [1m]) by (worker_id)
  ```

- **Group by an extracted (non-label) field** like `domain`, *with* an
  explicit pull so cardinality stays sane:
  ```
  sum by (domain) (count_over_time({event="request.start"} | json domain="domain" [1m])) / 60
  ```

- **Filter by an extracted field** without making it a label:
  ```
  rate({event="request.fail"} | json | fail_reason !~ "HttpError.*" [5m])
  ```

### 7.5.2 Tips and gotchas

- **Always include both `service` and `event` in the stream selector**
  whenever possible — both are real labels, the stream selector is the
  only fast index. `{event="request.end"}` alone is fine; `{service=~".+"}`
  is *very* expensive.
- **`$__rate_interval` does not always expand for Loki queries.** It
  has bitten this dashboard once already. Prefer hard-coded ranges
  (`[1m]`, `[5m]`) or `$__interval` if you need an automatic step.
- **Beware the 10 000-series limit.** If you see `maximum of series
  (10000) reached`, your query is producing a series per source stream
  (e.g. one per filename × worker × domain). Push grouping into the
  range aggregation: `quantile_over_time(...) by (worker_id)`, not
  `avg by (worker_id) (quantile_over_time(...))`.
- **Long-range queries on `| json` are expensive.** For heavy panels
  set `interval` and `maxDataPoints` on the panel JSON to keep the
  number of sub-queries Grafana fires per refresh in check (see the
  *QPS per domain* panel for an example).
- **Adding a new pipeline label needs a Loki driver restart on every
  host.** Recreate the relevant containers (`docker compose up -d
  --force-recreate <service>`) so the new pipeline takes effect. Be
  conservative: every label is a stream cardinality multiplier.

## 7.6 Operational notes

### 7.6.1 What happens when Loki is down

The driver is in `non-blocking` mode and `keep-file: true` is set. So:

- The container keeps running normally; nothing blocks on logging.
- `docker logs <container>` continues to work via the local json-file.
- Once Loki is reachable again, **previously buffered lines are
  retried** up to `loki-retries`; older lines may be dropped.
- For long Loki outages you will lose dashboard data for the gap.

### 7.6.2 Tuning the batch size

If you change crawler concurrency or the spider starts emitting
significantly more events, watch for:

- Spikes of dropped lines in the dashboard (e.g. `request.fail` count
  obviously below `request.scheduled` for sustained periods).
- The Loki driver's `state` file under
  `/var/run/docker/plugins/<id>/state` (key `dropped_events` if your
  driver build exposes it).

If you hit drops, raise `loki-batch-size` first (it amortises HTTP
overhead). Only raise `ingestion_rate_mb` in `loki-config.yml` if the
Loki ingester itself is rejecting pushes (visible in
`docker logs <loki-container>` as `rate limited`).

### 7.6.3 Where logs live

- In Loki: TSDB chunks under `/loki/chunks` inside the Loki container.
  Persist via a named volume; do not bind to a host path you want to
  prune.
- On the host (json-file fallback): `/var/lib/docker/containers/<id>/`
  with `max-size: 50m, max-file: 10` rotation per service.

### 7.6.4 Removing all observability for a single service

Set that service's `logging:` block back to `driver: json-file` (or
just delete the `logging:` line — Docker default is json-file). Other
services keep going; the dashboard panels for the removed service will
quietly show "No data".

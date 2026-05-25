# 08. Sitemap Patroller

This document covers the `sitemap_patroller` container added in
NTU-CSIE5376/WebCrawler#30: what it does, why it lives as a separate
service, how it plugs into the existing crawler pipeline, and how to
operate and observe it.

## 8.1 What and why

The main crawler discovers URLs only by following outlinks from pages it
has already fetched. That works well for the bulk of the URL space but
misses a specific population: **fresh URLs on domains that we already
know are golden but which our crawl has not yet reached.** A news site
publishes an article at 9 a.m.; the article is in the site's sitemap
within minutes; the natural-discovery crawl might not reach it for hours
or days. The sitemap patroller closes that gap.

Strategy: every 24 hours, look up which sitemap URLs each
**T0/T1 golden domain** exposes (`domain_state.domain_score >= 0.95` per
`update_golden_domain_scores.py`). Then every 10 minutes, fetch due
sitemaps and feed every `<loc>` URL into the existing ingestor — exactly
the same IPC contract the router uses for natural-discovery outlinks.

The design intentionally:

- **Does not** modify the spider, scorer, offerer, ingestor, or any of
  the 1,280 `url_state_*` sharded tables.
- **Does not** invent a new HTTP fetcher with its own QPS / robots
  machinery; it runs at a conservative global rate (≤0.5 req/s) that is
  small enough to ignore politeness budgeting at the URL level.
- **Does** land discovered URLs in `url_state_current_*` through the
  same upsert path natural-discovery uses, so the rest of the pipeline
  inherits them with zero code changes.

Issue thread for the design discussion: NTU-CSIE5376/WebCrawler#30
(includes the two-approach comparison and per-domain vs per-sitemap
cadence reasoning).

## 8.2 Architecture

```
┌─ sitemap_patroller container (docker-compose service) ─────┐
│                                                            │
│  ┌─ sitemap_discover (loop, sleep 24h) ───────────────┐    │
│  │ SELECT domain_id, domain                           │    │
│  │   FROM domain_state                                │    │
│  │  WHERE domain_score >= 0.95                        │    │
│  │                                                    │    │
│  │ for domain in result:                              │    │
│  │   robots = GET https://{domain}/robots.txt         │    │
│  │   urls   = parse 'Sitemap:' lines                  │ ───┼──► domain_sitemap
│  │   urls or= ['https://{domain}/sitemap.xml']        │    │   (new table)
│  │   INSERT ... ON CONFLICT (sitemap_url) DO NOTHING  │    │
│  └────────────────────────────────────────────────────┘    │
│                                                            │
│  ┌─ sitemap_patrol (loop, sleep 10m) ──────────────────┐   │
│  │ SELECT due rows FROM domain_sitemap                 │   │
│  │  WHERE last_patrolled_at IS NULL                    │   │
│  │     OR last_patrolled_at < NOW()-INTERVAL '24h'     │   │
│  │                                                     │   │
│  │ for sm in due:                                      │   │
│  │   resp = conditional GET sm.sitemap_url             │   │
│  │          (If-None-Match=sm.etag,                    │   │
│  │           If-Modified-Since=sm.last_modified)       │   │
│  │   if 304: bump last_patrolled_at; continue          │   │
│  │   kind, locs = parse_sitemap(resp.body)             │   │
│  │   if kind == 'sitemapindex':                        │   │
│  │     INSERT each child into domain_sitemap           │   │
│  │   elif kind == 'urlset':                            │   │
│  │     for loc in locs:                                │   │
│  │       sid = compute_shard(loc, ...)                 │ ──┼──► /data/ipc/
│  │       iid = sid // shards_per_ingestor              │   │   crawl_result/
│  │       ensure_domain(loc)                            │   │   ingestor_{NN}/
│  │       emit {"status":"new", "url":loc, ...}         │   │   {date}/{time}/
│  │           into ingestor_{iid} IPC dir               │   │   *_sitemap_*.jsonl
│  │   UPDATE domain_sitemap SET ... etag, ...           │   │           │
│  └─────────────────────────────────────────────────────┘   │           ▼
└────────────────────────────────────────────────────────────┘   existing ingestor
                                                                 → url_state_current_{shard}
                                                                 (no downstream changes)
```

### 8.2.0 Shared routing pieces

Both the patrol worker and the scheduler_ingest `router` need to translate
a URL into the same `(domain_key, shard_id, ingestor_id)` triple, and both
need to emit the same JSON record shape. Those two pieces live in shared
libs so the two callers can't drift:

- `libs/db/sharding/router.py:ShardRouter` — the sole owner of the
  `host → shard_id → ingestor_id` math. `patrol/main.py` constructs one
  at startup from `ingest.yaml`'s `router.num_shards`,
  `router.shards_per_ingestor`, `router.domain_overrides`, and the
  `shard_split_subdomain` DB whitelist — exactly what the router itself
  reads.
- `libs/ipc/new_link_record.py:build_new_link_record(...)` — the canonical
  builder for the "new outlink candidate" record schema documented in
  `docs/03-data-flow-and-ipc.md` §3.3. Router calls it with
  `discovery_source_type=DISCOVERY_SOURCE_PAGE_OUTLINK (=1)`; patrol calls
  it with `DISCOVERY_SOURCE_SITEMAP (=2)`.

If a future schema change is needed (new field, renamed field), changing
those two files updates both writers at once.

### 8.2.1 Why one container, two workers

Discover and patrol share a Postgres connection profile, the same
sharding wiring (loaded from `containers/scheduler_ingest/config/ingest.yaml`),
and the same logging config. Co-locating them under one supervisord
matches the precedent set by `scheduler_ingest` (which runs 49 workers
in one container) and lets a single `docker compose restart
sitemap_patroller` recover both.

### 8.2.2 Why the patrol worker writes JSONL files instead of HTTP

The ingestor's input contract is a directory of time-bucketed JSONL
files written into `/data/ipc/crawl_result/ingestor_{NN}/{YYYYMMDD}/{HHMM}/`
(see [03-data-flow-and-ipc.md](./03-data-flow-and-ipc.md) §3.2 / §3.3).
No HTTP endpoint exists, and the issue thread explicitly accepted
external writers into that directory (`@fmb123456`). So the patroller
emits in exactly the format `router` already produces, and the existing
`ingestor` picks the files up unchanged.

## 8.3 Data model

### 8.3.1 New table: `domain_sitemap`

Non-sharded, low-cardinality. One row per discovered sitemap URL.

| Column              | Type          | Notes |
|---------------------|---------------|-------|
| `id`                | `BIGSERIAL`   | Primary key. |
| `domain_id`         | `BIGINT`      | FK → `domain_state(domain_id)`. The owning domain (used by the discover worker; the patroller looks up the `<loc>` URL's own shard independently). |
| `sitemap_url`       | `TEXT`        | `UNIQUE`. Idempotent rediscovery — `ON CONFLICT (sitemap_url) DO NOTHING` keeps patrol state intact. |
| `last_patrolled_at` | `TIMESTAMPTZ` | NULL = never patrolled. Indexed `NULLS FIRST` so the patrol selector hits new rows first. |
| `last_url_count`    | `INTEGER`     | Number of `<loc>` entries the last successful parse saw. |
| `last_new_count`    | `INTEGER`     | Number of URLs we actually emitted as `status="new"` records. |
| `etag`              | `TEXT`        | Verbatim `ETag` response header. Replayed as `If-None-Match` next time. |
| `last_modified`     | `TEXT`        | Verbatim `Last-Modified` header. Replayed as `If-Modified-Since`. |
| `status`            | `TEXT`        | Terminal outcome of the last fetch: `ok`, `not_modified`, `parse_error`, `http_{code}`, `err_{ExceptionName}`, `timeout`. |
| `created_at`        | `TIMESTAMPTZ` | Default `NOW()`. |

Indexes: `idx_domain_sitemap_due ON (last_patrolled_at NULLS FIRST)` for
selection, `idx_domain_sitemap_domain_id` for per-domain lookups.

Migration: `scripts/migrate_add_domain_sitemap.py` — idempotent
`IF NOT EXISTS`.

### 8.3.2 IPC record emitted to the ingestor

Identical to the router's "new outlink candidate" schema documented in
[03-data-flow-and-ipc.md](./03-data-flow-and-ipc.md) §3.3, with two
sitemap-specific tags:

```json
{
  "url": "https://example.com/article-42",
  "status": "new",
  "shard_id": 98,
  "domain_id": 123,
  "domain_score": 0.95,
  "discovered_from": "https://example.com/sitemap.xml",
  "discovery_source_type": 2,
  "inlink_count_approx": 1,
  "inlink_count_external": 0,
  "anchor_text": null
}
```

- `discovery_source_type = 2` (= `DISCOVERY_SOURCE_SITEMAP` from
  `containers/sitemap_patroller/__init__.py`) lets future analyses
  separate sitemap-discovered URLs from page-outlink-discovered URLs
  (`= 1`) without joining a side table. The ingestor accepts this field
  unchanged.
- `discovered_from` is the sitemap URL itself, mirroring how the router
  records the parent page URL for natural outlinks.

The patroller writes one JSONL file per ingestor per run-bucket per
process at:

```
/data/ipc/crawl_result/ingestor_{NN}/{YYYYMMDD}/{HHMM}/{HHMM}_sitemap_{pid}_{ts}.jsonl
```

The `{ts}` suffix prevents concurrent or repeated runs from clobbering
each other.

## 8.4 Component reference

### 8.4.1 `sitemap_discover` worker

| File | Purpose |
|------|---------|
| `containers/sitemap_patroller/discover/main.py` | argparse, `libs.obslog.configure(service="sitemap_discover")`, `while True: run_once(); time.sleep(loop_interval_sec)`. |
| `containers/sitemap_patroller/discover/service.py` | `DiscoverConfig`, `run_once()`, `fetch_robots_txt`, `parse_sitemap_directives`, `discover_for_domain`, `upsert_sitemap`, `fetch_golden_domains`. |

Notable behaviors:

- robots.txt parser is **case-insensitive** on the `Sitemap:` prefix and
  accepts **absolute http(s) URLs only**. Relative paths and commented
  lines are dropped — they are almost always editor bugs.
- If robots.txt is unreachable or has no `Sitemap:` directive, falls
  back to `https://{domain}/sitemap.xml`. The fallback row still enters
  the patrol cycle so we get a definitive `http_404` outcome instead of
  silent absence.

### 8.4.2 `sitemap_patrol` worker

| File | Purpose |
|------|---------|
| `containers/sitemap_patroller/patrol/main.py` | argparse, `libs.obslog.configure(service="sitemap_patrol")`, while-loop. Loads `ingest_config_path` via `load_sharding_config` for the same `overrides` + `split_subdomains` the router uses. |
| `containers/sitemap_patroller/patrol/service.py` | `PatrolConfig` (holds a shared `ShardRouter`), `run_once`, `process_row` (uses the shared `build_new_link_record`), `fetch_sitemap` (conditional GET), `parse_sitemap`, `IngestorEmitter`, `ensure_domain`. |

Notable behaviors:

- **Conditional GET** uses stored `etag` and `last_modified`. A 304
  bumps `last_patrolled_at` to `NOW()` and records `status =
  not_modified`. No body is read or parsed.
- **XML sniff** before parsing: looks for `<urlset` or `<sitemapindex`
  in the first 4 KB. Non-matching responses (HTML 404 pages, JSON, etc.)
  are flagged `parse_error` instead of crashing the loop.
- **Sitemap-index handling** is **deferred, not inline**. Each nested
  sitemap URL is inserted into `domain_sitemap`; the next patrol pass
  picks it up like any other row. This bounds bursts on a single domain
  across runs — a sitemap index with 200 children does not turn into
  200 fetches in the same minute.
- **Per-domain cooldown** within one run: if we've already fetched a
  URL on this domain in the past 60s, the next row from the same
  domain is skipped (its `last_patrolled_at` is left alone, so it
  comes up again on the next pass).
- **Sharding agreement** with the router: the patroller calls
  `compute_shard(host, ...)` with the same `overrides` and
  `split_subdomains` the router loaded, so a sitemap row for
  `news.example.com` lands in the same ingestor as a page outlink to
  the same URL. Without this, the ingestor's per-shard upsert would
  silently miss.
- **Domain bootstrap**: when a `<loc>` URL points at a domain not yet
  in `domain_state` (cross-site sitemap entry, CDN host, etc.), the
  patroller `ensure_domain`s the row in-place — same approach
  `scripts/golden_inject.py` and the router use.

### 8.4.3 Migration

`scripts/migrate_add_domain_sitemap.py` is the only piece that lives
outside the container, because it must run **before** the container can
start successfully. Idempotent and safe to rerun.

## 8.5 Relationship to existing recurring jobs

The patroller is **additive**, not a replacement for any existing job.

| Job | Population it owns | Why we still need it |
|-----|-------------------|----------------------|
| `update_golden_domain_scores.py` (daily, docs §6.12) | `domain_state.domain_score` from `metricdb.metric_batches` presence | The discover worker filters domains by `domain_score`. This job only UPDATEs `domain_state`, never INSERTs. |
| `golden_inject.py` (weekly, docs §6.2) | URL-level: historical golden URLs (>4 weeks old) from `metricdb.metric_url`; bootstraps `domain_state` rows for metric_url-only domains; tags rows `source = SOURCE_GOLDEN`. | Covers a different URL population: archived URLs that are no longer in any sitemap, plus bootstrap of domains the crawler has never reached naturally. The patroller cannot cover these. |
| `sitemap_patroller / discover` (every 24h) | Which sitemap URLs each T0/T1 domain exposes | Future-likely-golden URLs that have not yet been metric-queried. |
| `sitemap_patroller / patrol` (every 10m) | The actual `<loc>` URLs from those sitemaps | Same as above. |

Deploy order on a fresh cluster: apply the migration, ensure
`update_golden_domain_scores.py` has run at least once (so `domain_score`
is populated), then start `sitemap_patroller`. The discover worker's
first sweep populates `domain_sitemap`, then the patrol worker picks
those rows up on its next pass.

## 8.6 Configuration

Single YAML at `containers/sitemap_patroller/config/sitemap.yaml`. Both
workers load the same file; each reads its own section.

```yaml
postgres:
  dsn: "postgresql://crawler:crawler@172.16.191.1:5432/crawlerdb"

ingest_config_path: "/app/containers/scheduler_ingest/config/ingest.yaml"

discover:
  score_min: 0.95           # T0+T1 threshold
  domain_limit: null        # null = no cap
  global_delay_sec: 0.5     # robots.txt fetch rate
  loop_interval_sec: 86400  # 24h between sweeps

patrol:
  due_interval_hours: 24
  batch_limit: 500
  global_delay_sec: 2.0          # ≤0.5 req/s globally
  per_domain_cooldown_sec: 60
  loop_interval_sec: 600         # 10 min between passes
```

Tunables most likely to be touched in production:

- `discover.score_min` — drop to `0.8` to include T2 (any historical
  batch). Roughly doubles the domain count.
- `patrol.batch_limit` — raise to clear backlog after long downtime.
- `patrol.global_delay_sec` — raise if politeness budget tightens.

Anything touching `ingest_config_path` should match the router's actual
runtime config; the patroller intentionally loads from the same file
to avoid drift.

## 8.7 Politeness budget

Concrete numbers at default settings, ~5,000 T0+T1 domains:

| Worker | Rate ceiling | Daily volume |
|--------|--------------|--------------|
| discover | 1 robots.txt fetch / 0.5s → ≤2 req/s, runs once per 24h | ~5,000 fetches → ~40 min per sweep |
| patrol | 1 sitemap fetch / 2.0s → ≤0.5 req/s, runs every 10 min | ~7,200 fetches/day budget; in practice bounded by `batch_limit × 144 passes/day ≤ 72k`, and by `domain_sitemap` row count |

Steady-state real load assuming ~1.2 sitemaps/domain and 24h cadence:
**~6,000 fetches/day ≈ 0.07 req/s global**. Per-domain ceiling on a
single day is bounded by the number of distinct sitemap URLs the domain
exposes; the 60s in-run cooldown plus 24h due-interval keep this at a
handful of fetches/day per domain.

The downstream URL crawl that those discoveries trigger inherits Scrapy's
existing `domain_qps.json` controls — sitemap discovery does not bypass
per-domain politeness on the actual page fetches.

## 8.8 Operations

### 8.8.1 Build + start

```bash
# One-shot migration (host with crawlerdb access):
uv run scripts/migrate_add_domain_sitemap.py --dry-run
uv run scripts/migrate_add_domain_sitemap.py

# Container:
docker compose build sitemap_patroller
docker compose up -d sitemap_patroller
docker exec sitemap_patroller supervisorctl status
```

`supervisorctl status` should show two `RUNNING` programs:
`sitemap_discover` and `sitemap_patrol`.

### 8.8.2 Ad-hoc single run (debugging)

Both workers support `--run-once` for one-pass invocation. Useful for
smoke-testing config changes or backfilling after downtime.

```bash
docker exec sitemap_patroller \
  python -m containers.sitemap_patroller.discover.main \
    --config /app/containers/sitemap_patroller/config/sitemap.yaml --run-once

docker exec sitemap_patroller \
  python -m containers.sitemap_patroller.patrol.main \
    --config /app/containers/sitemap_patroller/config/sitemap.yaml --run-once
```

### 8.8.3 Common operational queries (psql)

```sql
-- How much sitemap discovery have we accumulated?
SELECT COUNT(*)               AS sitemaps,
       COUNT(DISTINCT domain_id) AS domains,
       COUNT(*) FILTER (WHERE last_patrolled_at IS NULL) AS never_patrolled
FROM domain_sitemap;

-- Where is patrol failing?
SELECT status, COUNT(*)
FROM domain_sitemap
WHERE last_patrolled_at > NOW() - INTERVAL '24 hours'
GROUP BY status ORDER BY 2 DESC;

-- Top-10 sitemaps by emitted URLs in the last day:
SELECT sitemap_url, last_url_count, last_new_count, last_patrolled_at
FROM domain_sitemap
WHERE last_patrolled_at > NOW() - INTERVAL '24 hours'
ORDER BY last_new_count DESC NULLS LAST
LIMIT 10;
```

### 8.8.4 Common failure modes

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `discover.done` reports `domain_count = 0` | `update_golden_domain_scores.py` has not run, or `score_min` is too high | Run the scorer; verify `SELECT COUNT(*) FROM domain_state WHERE domain_score >= 0.95` |
| `patrol.done` reports `urls_emitted = 0` for many runs | Sitemaps are unchanged (304s) — this is normal | Confirm via `status = 'not_modified'` rows in `domain_sitemap` |
| Many `parse_error` outcomes on a single domain | Server returns HTML / gzip / non-sitemap XML | v1 marks `parse_error`; gzip is out of scope for v1 (see §8.10) |
| `urls_emitted > 0` but `url_state_current_*` row count not growing | Ingestor backlogged | Check `scheduler_ingest` Loki logs; the patroller's output lands in `{YYYYMMDD}/{HHMM}` folders that the ingestor reads only after `2 × interval_minutes` |

## 8.9 Observability (Loki + Grafana)

Both workers route logs through `libs/obslog`, which writes JSON to
stdout. The Docker `loki` driver (see
[05-docker-settings-and-deployment.md](./05-docker-settings-and-deployment.md)
and [07-grafana-loki-observability.md](./07-grafana-loki-observability.md))
parses those lines and labels by `service`, `level`, `event`.

### 8.9.1 Event vocabulary

| Event | Worker | Extras |
|-------|--------|--------|
| `discover.start` | discover | `domain_count`, `score_min`, `limit` |
| `discover.done` | discover | `domains`, `new_sitemaps`, `existing_sitemaps`, `errors`, `elapsed_sec` |
| `discover.robots_fetch_fail` | discover | `domain`, `err` (exception class) |
| `discover.domain_error` | discover | `domain`, `err` |
| `discover.run_error` | discover | `err` (loop-level — should be rare) |
| `patrol.start` | patrol | `rows_due`, `due_interval_hours`, `batch_limit` |
| `patrol.done` | patrol | `rows_due`, `ok`, `not_modified`, `parse_error`, `http_err`, `other_err`, `skipped_cooldown`, `urls_emitted`, `nested_registered`, `elapsed_sec` |
| `patrol.xml_parse_error` | patrol | `err` |
| `patrol.run_error` | patrol | `err` (loop-level) |

### 8.9.2 Suggested LogQL queries

```logql
# Run cadence and counters
{service="sitemap_patrol",    event="patrol.done"}
{service="sitemap_discover",  event="discover.done"}

# Errors
{service="sitemap_patrol",    event=~"patrol.xml_parse_error|patrol.run_error"}
{service="sitemap_discover",  event="discover.robots_fetch_fail"}
| json | line_format "{{.domain}}: {{.err}}"

# Throughput over time (panel)
sum by (service) (
  rate({service=~"sitemap_.*", event=~".*\\.done"} | json | unwrap urls_emitted [5m])
)
```

A dedicated dashboard JSON under `ops/grafana/dashboards/` is a
follow-up item (§8.10).

## 8.10 Future work

- **Adaptive cadence** — halve `due_interval_hours` per row on
  new-URL hits (floor 6h), double on no-change (ceiling 7d). The
  schema already has the columns; only the patrol selector and
  `update_row` need to learn the math.
- **Inline sitemap-index recursion** — currently deferred to the next
  pass. Inline-with-cap would shorten lag between an index update and
  the first URL emission, at the cost of larger single-run bursts.
- **gzip support** — many large sitemaps ship as `*.xml.gz`. v1 marks
  these `parse_error`; adding `gzip.decompress` is mechanically easy
  once we decide whether to gate on URL suffix, `Content-Type`, or
  `Content-Encoding`.
- **Cleanup pass** — remove `domain_sitemap` rows when a domain falls
  out of T0/T1. Discover currently only INSERTs.
- **`source` column traceability** — currently sitemap-discovered URLs
  land with `source = 0` (natural). Threading `source =
  SOURCE_SITEMAP` through the `_bulk_links` upsert would let queries
  separate the two cohorts directly in `url_state_current_*`.
- **Dedicated Grafana dashboard JSON** — provision a panel set
  (rows-due, urls-emitted, error mix, per-domain top-K) into
  `ops/grafana/dashboards/`.

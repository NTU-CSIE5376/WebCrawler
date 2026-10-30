# 05. Docker Settings and Deployment Notes

## 5.1 Compose Service Definitions

From `docker-compose.yml`, services are:

- `postgres`
- `scheduler_control`
- `scheduler_ingest`
- `crawler`

Common runtime properties on non-DB services:

- `init: true`
- `restart: unless-stopped`
- bind mount source code: `./:/app`
- shared IPC mount: `/data/ipc:/data/ipc`
- JSON file log driver with rotation:
  - `max-size: 50m`
  - `max-file: 10`

Crawler-specific networking:

- DNS explicitly set to `8.8.8.8` and `1.1.1.1`.

## 5.2 Dockerfiles

### `containers/scheduler_control/Dockerfile`

- Base: `python:3.12-slim`
- Installs: `supervisor`
- Python deps: `sqlalchemy`, `psycopg2-binary`, `pyyaml`, `joblib`, `numpy`, `scipy`, `scikit-learn`
- Entrypoint: supervisord

### `containers/scheduler_ingest/Dockerfile`

- Base: `python:3.12-slim`
- Installs: `supervisor`
- Python deps: `tldextract`, `pyyaml`, `sqlalchemy`, `psycopg2-binary`, `joblib`, `numpy`, `scipy`, `scikit-learn`
- Entrypoint: supervisord

### `containers/crawler/Dockerfile`

- Base: `python:3.12-slim`
- Installs: `supervisor`
- Python deps: `scrapy`, `requests`, `tldextract`
- `PYTHONPATH=/app`
- Entrypoint: supervisord

## 5.3 Supervisor Process Topology

- `scheduler_control`: 16 offerers + 1 accounting rolloff worker + optional Golden Discovery Ranker v1 workers
- `scheduler_ingest`: 16 routers + 16 ingestors + 16 extractors + 1 stats aggregator
- `crawler`: 16 spiders total:
  - 15 baseline AutoThrottle spiders (`crawler_id=0..14`)
  - 1 independently configurable AutoThrottle canary spider (`crawler_id=15`)

Total long-running app processes (excluding postgres internals):

- Default: `17 + 49 + 16 = 82`
- With Golden Discovery Ranker v1 enabled: `21 + 49 + 16 = 86`

Supervisor keeps per-process global concurrency and AutoThrottle target/start/max
tuning. Baseline workers use global concurrency 256; the canary uses 384 and a
150,000,000-byte scraper buffer. The default scraper buffer is 50,000,000 bytes;
`CRAWLER_SCRAPER_SLOT_MAX_ACTIVE_SIZE` overrides it.

### 5.3.1 Middleware

Crawler policies are loaded from `containers/crawler/config/site_policies.yml`.
`default.crawler_mode` is `autothrottle`, and
`default.request_limits.max_qps` is `4` (a 0.25-second minimum interval).
Domains and exact hostnames can override the mode with `fixed` or `autothrottle`
and provide partial `request_limits`; each field inherits from default to domain
to host. Domain rules match the registered domain and its subdomains; host rules
match an exact hostname.

```yaml
default:
  crawler_mode: autothrottle
  request_limits:
    max_qps: 4
    max_concurrent_requests: 16
domains:
  wikipedia.org:
    crawler_mode: autothrottle
    middleware: crawler.middlewares.wikipedia.WikipediaMiddleware
    request_limits:
      download_slot: wikipedia
      max_qps: 15
      max_concurrent_requests: 10
  youtube.com:
    middleware: crawler.middlewares.youtube.YouTubeMiddleware
hosts:
  example.com:
    crawler_mode: fixed
    request_limits:
      max_qps: 1
```

`SiteMiddlewareDispatcher` selects handlers from these declarative bindings for
request, downloaded-response, and spider-output hooks. Exact host rules inherit
the domain middleware; `middleware: null` disables the custom handler while
keeping the resolved crawl policy. `request_limits: null` resets limits to the
global default. Headers inherit one field at a time; a null header suppresses it.

The policy-aware extension reuses Scrapy's AutoThrottle calculation and applies
the resolved site's minimum interval, calculated as `1 / max_qps`. AutoThrottle
starts at the larger of that minimum and `AUTOTHROTTLE_START_DELAY`; it may slow
down as latency rises. Fixed slots use their configured interval and opt out of
adaptive adjustments. Concurrency is enforced by Scrapy slots per crawler
process. Standard Scrapy delay randomization still applies, so `max_qps` controls
nominal pacing rather than a strict rolling-window or shared multi-process QPS
budget. `download_slot` groups requests into a shared slot within a crawler;
rules sharing a slot must have consistent policies. Normal and manual API
requests use the same downloader and site policies.

The YAML is the source of truth for mode, minimum interval, and slot concurrency;
the old `domain_qps.json`, `CRAWLER_USE_AUTOTHROTTLE`, `CRAWLER_DOWNLOAD_DELAY`, and
`CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN` settings are no longer used.

#### Wikipedia

Wikipedia uses AutoThrottle with `max_qps: 15` (a minimum interval of about
0.0667 seconds) and a shared `wikipedia` slot with concurrency 10 per process.
The middleware normalizes `/wiki/Title` URLs by removing query parameters and
fragments, trimming surrounding title whitespace, and replacing title spaces
with underscores. It converts `/w/index.php?title=Title` URLs to `/wiki/Title`.
Revision selectors are discarded so requests retrieve current content. URLs
without a usable title and write methods are rejected; internal robots.txt
checks are allowed. Each rewrite emits `wikipedia.url_normalized` with
`original_url` and `normalized_url`; canonical URLs do not emit this event.

Wikipedia requests use the configured bot identity and gzip, suppress browser
navigation headers, and disable cookies. Scrapy does not execute JavaScript.
Robots.txt checks and standard bounded retries remain enabled.

On HTTP 429, the middleware pauses the affected Wikipedia download slot for at
least 10 seconds, honoring a longer `Retry-After` value (seconds or an HTTP date).
Queued Wikipedia requests and retries wait together; other domains continue.
Further 429s extend the cooldown without shortening an existing wait. Requests
already in flight can finish. Cooldowns are logged as `wikipedia.rate_limited`
and counted in `wikipedia/cooldowns`.

#### YouTube

The YouTube middleware runs the existing video outlink extractor on normal
HTML `PageItem` output. It preserves the extractor's parsing behavior and adds
video links to the page's outlinks. Manual bulk crawl results and non-HTML
responses bypass this enrichment. The former standalone `youtube_extractor.py`
implementation now lives in the middleware module.

## 5.4 Operational Configuration Coupling

The following must remain aligned:

- `offerer.total_shards`, `router.num_shards`, and actual number of shard tables.
- `offerer.shards_per_offerer` and offerer process count.
- `router.shards_per_ingestor` and ingest/extractor process counts.
- queue/result/progress path templates across all services.

## 5.5 Current DSN and Network Assumption

Configured DSN in YAML:

- `postgresql+psycopg2://crawler:crawler@172.16.191.1:5432/crawlerdb`

Implication:

- Services currently expect PostgreSQL at host IP `172.16.191.1`, not `postgres` service DNS name.
- If using compose-internal networking for DB, update DSN host accordingly.

## 5.6 Startup and Health Considerations

- `postgres` has healthcheck but application services do not declare `depends_on` with health conditions.
- Services rely on internal retry/restart behavior.
- Router has explicit transient DB retry (3 attempts with backoff).
- Accounting rolloff runs daily in UTC and also supports one-shot execution (`--once`) for manual verification.

## 5.7 Golden Discovery Ranker v1 Rollout Notes

- Run `scripts/migrate_add_url_score_updated_at.py` before deploying code that reads or writes `url_score_updated_at`; ingest and accounting paths reference the column even when the ranker is disabled.
- The migration also creates partial `*_golden_discovery_v1_unscored` and `*_golden_discovery_v1_selection` indexes on `url_state_current_*` with `CREATE INDEX CONCURRENTLY`. These indexes protect the background ranker lookup and offerer selection path but add disk usage and modest write-maintenance overhead.
- Keep `golden_discovery_ranker_v1.enabled` / `GOLDEN_DISCOVERY_RANKER_V1_ENABLED` false until the column, indexes, and mounted artifact are verified.
- Keep `GOLDEN_DISCOVERY_RANKER_V1_INGEST_INLINE_ENABLED=false` until the ranker artifact is also mounted in `scheduler_ingest`.
- Bound inline ingest scoring with `GOLDEN_DISCOVERY_RANKER_V1_INGEST_INLINE_SCORE_TIMEOUT_SEC`; URLs not scored within the budget are inserted with `url_score_updated_at=NULL` for the background scorer to handle later.
- Enable the ranker before switching the offerer strategy. The expected sign of progress is a growing count of rows where `url_score_updated_at IS NOT NULL`.
- Switch `OFFERER_STRATEGY=golden_discovery_ranker_v1` only after ranker progress looks healthy. If DB load rises, disable the ranker first, then revert the offerer strategy to the prior value.

## 5.8 Data Durability

- DB durability: persisted via `/data/postgres` mount.
- IPC data durability: persisted via `/data/ipc` mount.
- Queue/crawl/stats files survive container restarts if host paths persist.

## 5.9 Deployment Checklist

1. Ensure `/data/postgres` and `/data/ipc` exist with writable permissions.
2. Initialize DB schema (all non-sharded + 256 shard table families).
3. Verify YAML DSN host resolves from containers.
4. Confirm shard parameters match schema generation.
5. Start compose stack and verify all supervisord child processes are healthy.
6. Monitor `/data/ipc/stats/bad` for aggregation failures.

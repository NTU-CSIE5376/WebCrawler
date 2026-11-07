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
      target_concurrent_requests: 8
      max_qps: 15
      max_concurrent_requests: 8
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

`allow` is a shared policy option with the same default → domain → exact-host
precedence. It defaults to `true`; `false` makes the common dispatcher raise
`IgnoreRequest` before robots fetching, site handlers, or acquiring a request
budget. It covers normal requests, manual jobs, retries, and redirect destinations
in every crawler process. Domain denials include all subdomains unless an exact
host explicitly sets `allow: true`. Denials remain failures in crawl and manual
job results. Discovery and patrol workers do not use this crawler policy.

```yaml
domains:
  instagram.com:
    allow: false
  facebook.com:
    allow: false
  reddit.com:
    allow: false
```

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

`request_limits.target_concurrent_requests` optionally overrides the AutoThrottle
target for a domain or exact host, inheriting through the same policy hierarchy.
When omitted, it uses `AUTOTHROTTLE_TARGET_CONCURRENCY` (2 by default). This target
controls adaptive pacing; `max_concurrent_requests` remains the hard cap.

The YAML is the source of truth for mode, minimum interval, and slot concurrency;
the old `domain_qps.json`, `CRAWLER_USE_AUTOTHROTTLE`, `CRAWLER_DOWNLOAD_DELAY`, and
`CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN` settings are no longer used.

#### Wikipedia

Wikipedia uses AutoThrottle with `target_concurrent_requests: 8`, `max_qps: 15`
(a minimum interval of about 0.0667 seconds), and `max_concurrent_requests: 8`. When explicitly configured
under `domains`, `max_concurrent_requests` is enforced by the generic
`SharedConcurrencyMiddleware` across all crawler processes and subdomains.
This includes robots.txt, redirects, retries, and manual jobs, regardless of
shard routing. Host overrides cannot relax a domain's shared cap; a lower host
limit still applies locally. Default and host-only concurrency limits remain
per process. Domain rules without an explicit concurrency setting do not
create a shared budget.

The shared limit covers crawler requests. The unchanged synchronous sitemap
discovery and patrol workers can each add one concurrent request; with a crawler
cap of eight, the combined maximum is ten. Use a crawler cap of seven to keep
the combined maximum at nine for the current one-instance-per-worker deployment.

The middleware uses file locks under `/data/ipc/request_budgets`, on the IPC
bind mount shared by the crawler containers. Workers wait asynchronously for a
lease and fail closed if the shared directory is unavailable. Responses and
download exceptions release leases; process death releases locks automatically.
For local runs, set `SHARED_REQUEST_BUDGET_DIR` to a writable directory shared by
all participating processes. Do not delete lock files while crawlers run.
Deploy by restarting all crawler processes together so no old workers bypass
the middleware. Existing split whitelist entries can remain: all crawler
processes share the same domain budget regardless of their shard assignment.
The whitelist refresh script excludes Wikipedia from all scopes, avoiding
additional parallel probes and new language-host split entries.

The Wikipedia middleware preserves the requested URL, including its path,
query parameters, and fragment. URL normalization and selection belong
upstream; redirects are handled by Scrapy. Requests must use GET or HEAD,
and robots.txt checks remain enabled.

Wikipedia requests use the configured bot identity and gzip, suppress browser
navigation headers, and disable cookies. Scrapy does not execute JavaScript.
Robots.txt checks and standard bounded retries remain enabled.

On HTTP 429, the common `RetryAfterMiddleware` pauses the affected download slot for at
least 10 seconds, honoring a longer `Retry-After` value (seconds or an HTTP date).
Queued requests and retries for that slot wait together; other slots continue.
Further 429s extend the cooldown without shortening an existing wait. Requests
already in flight can finish.
Cooldowns are local to the downloader slot in its crawler process, consistent
with host routing. They are logged as `crawler.rate_limited` and counted in
`retry_after/cooldowns`.

#### YouTube

The YouTube middleware runs the existing video outlink extractor on normal
HTML `PageItem` output. It preserves the extractor's parsing behavior and adds
video links to the page's outlinks. Manual bulk crawl results and non-HTML
responses bypass this enrichment. The former standalone `youtube_extractor.py`
implementation now lives in the middleware module.

#### ESPN

`crawler.middlewares.espn.ESPNMiddleware` skips login, members, and personalization
paths on ESPN's public frontend before downloading. It preserves content URLs
and query parameters, identifies as `WebCrawlerBot/1.0`, requests gzip, and disables
cookies. Configured site headers can override the identity. Robots checks,
bounded retries, and the common 429 cooldown remain enabled.

The `espn.com` domain policy uses an AutoThrottle target of 32, nominal 64 QPS,
a shared domain concurrency cap of 16, and one `espn.com` download slot. The target
controls pacing; the hard cap remains 16 across crawler processes and subdomains.
Global defaults remain unchanged.

A fresh 10,000-URL benchmark on `www.espn.com` completed in 377.75 seconds with
9,826 successful HTTP-200 pages (26.01 pages/sec), no HTTP 429s, and no cooldowns.
First-half and second-half rates were 26.05 and 25.98 pages/sec. The run used one
crawler with a two-CPU quota and 4 GiB RAM, retaining the production middleware,
HTML parser, and JSON output pipeline. These are measured tuning values, not
limits published by ESPN; the benchmark did not validate rates for other ESPN
hosts. Other hosts share the domain slot, so their cooldowns also affect it.

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

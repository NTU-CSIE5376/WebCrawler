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
  - 15 baseline AutoThrottle spiders (`crawler_id=0..6,8..15`)
  - 1 independently configurable AutoThrottle canary spider (`crawler_id=7`)

Total long-running app processes (excluding postgres internals):

- Default: `17 + 49 + 16 = 82`
- With Golden Discovery Ranker v1 enabled: `21 + 49 + 16 = 86`

Crawler throttle mode is selected per supervisord program through environment
variables. All production workers currently set `CRAWLER_USE_AUTOTHROTTLE=true`.
Workers `0..6` and `8..15` share the baseline AutoThrottle values; worker `7`
is intentionally kept in its own program so it can be tuned independently for
future canary experiments. Fixed-QPS mode remains available by setting
`CRAWLER_USE_AUTOTHROTTLE=false`, which reads `domain_qps.json`.
All crawler workers default Scrapy's `SCRAPER_SLOT_MAX_ACTIVE_SIZE` to
`50000000` bytes to avoid downloader stalls from scraper-slot backpressure; set
`CRAWLER_SCRAPER_SLOT_MAX_ACTIVE_SIZE` to override it.

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

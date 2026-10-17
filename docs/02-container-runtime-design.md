# 02. Container Runtime Design

## 2.1 `postgres` Container

Defined in `docker-compose.yml`:

- Image: `postgres:16`
- DB: `crawlerdb`
- User/password: `crawler/crawler`
- Tunings:
  - `max_connections=500`
  - `max_wal_size=20GB`
  - `checkpoint_timeout=15min`
- Persistent volume: `/data/postgres:/var/lib/postgresql/data`
- Exposed port: `5432`

Primary role:

- Stores all domain, URL state/history, event counters, content features, and aggregated daily stats.

## 2.2 `scheduler_control` Container

Runs `supervisord` with four program types:

- `offerer` with `numprocs=16` (`offerer_00..offerer_15`)
- `accounting_rolloff` with `numprocs=1`
- `frontier_gc` with `numprocs=1`
- `golden_discovery_ranker_v1` with `numprocs=4` (disabled unless `GOLDEN_DISCOVERY_RANKER_V1_ENABLED=true`)

Runtime behavior per offerer:

1. Check queue depth in `/data/ipc/url_queue/crawler_{id:02d}`.
2. If below `low_watermark_batches`, query its 16 assigned shards in DB.
3. Select URLs where `should_crawl=TRUE` using strategy ordering:
   - Phase A: score-aware (`url_score`, `domain_score`, `last_scheduled`, `first_seen`)
   - Golden Discovery Ranker v1 mode (`OFFERER_STRATEGY=golden_discovery_ranker_v1`): prefer rows with refreshed `url_score_updated_at`, then highest `url_score`
   - Phase B: fairness (`last_scheduled`, `first_seen`)
4. Atomically update selected rows:
   - `should_crawl=FALSE`
   - `last_scheduled=NOW()`
   - `num_scheduled_90d += 1`
5. Upsert daily per-URL schedule event in `url_event_counter_{shard}`.
6. Emit queue files containing URL batches (`batch_size=512`).
7. Write stats delta with `num_scheduled` and per-domain schedule counts.

Runtime behavior of `accounting_rolloff`:

1. Wake up by configured polling interval and check daily UTC schedule.
2. For each shard (`0..255`), read `url_event_counter_{shard}` rows where:
   - `accounted=TRUE`
   - `event_date <= CURRENT_DATE - event_retention_days` (default 90)
3. Process in batches (`batch_size`, configurable) with `FOR UPDATE SKIP LOCKED`:
   - aggregate picked event rows by URL within the batch,
   - subtract aggregated values from `url_state_current_{shard}` 90-day counters (floor at 0),
   - append snapshots into `url_state_history_{shard}`,
   - set processed (and missing-current-row) event rows to `accounted=FALSE`.
4. Commit each batch independently to reduce lock duration and avoid long transactions.
5. History retention: the same daily pass provisions the current/next month partition of `url_state_history_{shard}` and `content_feature_history_{shard}` (RANGE-partitioned on `snapshot_at`, monthly) and DROPs whole partitions older than `history_retention_days` (default 30, `0` disables). These are append-only audit logs with no pipeline read path. DROP is an O(1) metadata operation that returns space to the OS immediately, with no row scan or bloat. The current-table 90-day rolling counters are unaffected. Requires the tables to be partitioned first (`scripts/migrate_partition_history.py`); un-migrated tables are skipped.

Runtime behavior of `frontier_gc`:

The crawl frontier (`should_crawl=TRUE` rows) is never evicted by the pipeline, so low-yield domains accumulate an unbounded never-fetched backlog. Each daily pass, per shard:

1. Wake up by polling interval and check daily UTC schedule (same pattern as `accounting_rolloff`).
2. Refresh the frontier budget: sample to find oversized domains, exact-verify by `domain_id`, set `domain_state.discovery_frozen` when `pending > frontier_cap` AND fetch yield `< yield_floor`, and unfreeze domains that no longer qualify.
3. Evict never-scheduled frontier rows in `batch_size` trickle deletes (`FOR UPDATE SKIP LOCKED`): frozen-domain rows older than `frozen_pending_days`, plus general unscored rows older than `stale_pending_days`. Does not touch `url_state_history`.

Parallel query is disabled on its connection (the DB host is disk-constrained; parallel workers' shared-memory segments can fail to allocate).

Runtime behavior of `golden_discovery_ranker_v1`:

1. Loads a mounted Golden Discovery Ranker artifact when `GOLDEN_DISCOVERY_RANKER_V1_ENABLED=true`.
2. Finds crawlable URLs where `url_score_updated_at IS NULL`.
3. Scores those URLs with the artifact heads and writes the combined ranker score into the existing `url_score`.
4. Sets `url_score_updated_at=NOW()` as the only completion marker; it does not append experiment logs or score-history rows.

## 2.3 `crawler` Container

Runs `supervisord` with:

- 15 baseline AutoThrottle spiders (`crawler_id=0..6,8..15`)
- 1 independently configurable AutoThrottle canary spider (`crawler_id=7`)
- each spider reads/writes its own IPC queue/result path by `crawler_id`

Crawler worker behavior:

1. Pop earliest queue batch JSON from `/data/ipc/url_queue/crawler_{id:02d}`.
2. Delete queue file immediately after read.
3. Crawl URLs with Scrapy AutoThrottle (robots obeyed, retries enabled).
   - Baseline workers and the `crawler_id=7` canary currently use the same AutoThrottle values.
   - `crawler_id=7` remains a separate supervisord program so future canary parameters can be adjusted independently.
   - Fixed-QPS mode remains available through crawler settings when `CRAWLER_USE_AUTOTHROTTLE=false`.
   - Scrapy scraper-slot backpressure defaults to `SCRAPER_SLOT_MAX_ACTIVE_SIZE=50000000` for every crawler worker.
4. For each response:
   - if HTML/XHTML: save full content + extracted outlinks.
   - otherwise: emit failure record (`NonHTML content-type`).
5. For request errors: emit failure record with normalized reason (`HttpError`, `IgnoreRequest`, etc.).
6. Write records as JSONL into `/data/ipc/crawl_result/crawler_{id:02d}/{YYYYMMDD}/{HHMM}/HHMM.jsonl`.

### API Jobs in the Existing Crawler Pool

Input: accepted job messages in `MANUAL_BULK_CRAWL_QUEUE_DIR`
(default `/data/manual_bulk_crawls/pending`).

The existing `html_spider` workers also consume API jobs. The API resolves each
URL to the crawler that owns its domain's shard, using read-only routing data.
It neither inserts domains nor updates URL scheduling flags. API requests use
the same downloader, domain slots, robots.txt rules, retry policy and
AutoThrottle settings as regular requests. Normal crawl requests have higher
scheduler priority than API requests.

Responsibilities:

- Exclusively claim this worker's portions of multiple accepted jobs with
  nonblocking file locks; different workers can process one job concurrently.
- Bound scheduled API URLs with `MANUAL_BULK_CRAWL_PREFETCH_URLS` (default 256)
  and claimed jobs with `MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS` (default 64),
  both per worker in the crawler settings. Override them with the corresponding
  `CRAWLER_MANUAL_BULK_CRAWL_PREFETCH_URLS` and
  `CRAWLER_MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS` environment variables.
  Refill as each URL completes so other jobs progress while slow fetches finish.
- Fetch only submitted URLs. Dedicated callbacks save HTML and metadata without
  emitting PageItems, discovering outlinks or entering the normal result pipeline.
- Atomically update per-worker progress and aggregate it for job status.
- Read the output root from each job message and publish under `result_dir/{job_id}`
  only after every assigned worker finishes, then remove the pending file.
- Resume interrupted jobs from durable per-URL records. Process exit releases
  claims; a fetch interrupted before its record was saved may run again.
- On a job failure, retain its claim until outstanding callbacks drain before
  retrying. Other jobs can continue to publish their results.

The API and normal crawler container share a separate host directory, configured
by `MANUAL_BULK_CRAWL_DATA_DIR` in Compose (default `/data/manual_bulk_crawls`). The
crawler does not read API configuration. The API publishes URLs, assigned crawler
IDs and the output root in each job message. The crawler reads only its own
settings; `CRAWLER_MANUAL_BULK_CRAWL_QUEUE_DIR` selects the input queue and must
match the API publisher's `manual_bulk_crawl.pending_dir`. No new crawler processes or SQL dispatcher threads
are started. API jobs share crawler capacity and politeness budgets, while their
results stay outside `/data/ipc/crawl_result` and never enter ingestion or feature
extraction. Completed HTML and status files currently have no automatic retention
cleanup.

## 2.4 `scheduler_ingest` Container

The container has an 8-CPU limit shared by 4 program families via `supervisord`:

- `router` x16
- `ingestor` x16
- `feature_extractor` x16
- `stats_aggregator` x1

### Router (x16)

Input: crawler result folders `crawler_{id}`.

Responsibilities:

- Read crawler JSON/JSONL records from ready time buckets.
- Resolve source URL domain into `domain_state` (insert if missing).
- Compute shard/ingestor destination.
- For fetch-success records, compute `content_hash` (SHA-1 of content).
- For each outlink, ensure destination domain exists and emit separate `status="new"` records.
- Write transformed records to `ingestor_{id}` bucket JSONL files.

### Ingestor (x16)

Input: router output folders `ingestor_{id}`.

Responsibilities:

- `status="new"` records: insert new URL candidates into `url_state_current_{shard}` and history.
  - Links whose domain has `domain_state.discovery_frozen=TRUE` are skipped (frontier budget; see `frontier_gc` in 2.2), so frozen domains stop accumulating new frontier rows.
  - If `GOLDEN_DISCOVERY_RANKER_V1_INGEST_INLINE_ENABLED=true`, check which URLs are truly new, score those with the mounted ranker artifact before writing them to DB, and fall back to unscored insert when the inline scoring time budget is exhausted.
  - If inline scoring is disabled, new URLs keep `url_score_updated_at=NULL` and the background ranker can score them later.
- Fetch result records:
  - upsert `url_state_current_{shard}` counters and status fields,
  - append snapshots to `url_state_history_{shard}`,
  - upsert `url_event_counter_{shard}` daily fetch/update events.
- Emit stats deltas (`new_links`, fetch OK/fail, content updates, fail reasons, ingest errors).

### Feature Extractor (x16)

Input: same `ingestor_{id}` folders.

Responsibilities:

- For `status="ok"` records, derive basic features:
  - content length
  - content hash
  - outlink count
- Upsert latest features into `content_feature_current_{shard}`.
- Append snapshots into `content_feature_history_{shard}`.

### Stats Aggregator (x1)

Input: `/data/ipc/stats/*.json` deltas.

Responsibilities:

- Apply deltas to:
  - `summary_daily` (global counters)
  - `domain_stats_daily` (per-domain counters)
- Aggregate `fail_reasons` JSONB maps via `jsonb_set` increments.
- Move malformed/unprocessable files into `/data/ipc/stats/bad`.
- Emit `stats_error` delta if aggregation fails.

## 2.5 `api` Container

The API container replaces the Nginx gateway and runs Flask with Gunicorn.
Compose binds host `127.0.0.1:4000` to container port `8080`; set
`API_BIND_ADDRESS` and `API_PORT` to change the host binding.

### API Server (x5)

Input: HTTP requests under `/v1/manual-bulk-crawls`.

Responsibilities:

- Run 1 Gunicorn master and 4 API workers, configured in `containers/api/config/config.yml`.
- Validate and deduplicate the complete batch before accepting any URLs.
- Publish one immutable job file with an atomic rename and return its job ID.
- Report job status and serve completed HTML and metadata for download.
- Apply the configured 300 MiB request body limit using Flask's built-in handling.
- Read API paths and limits from `manual_bulk_crawl` in `containers/api/config/config.yml`.
- Include the output root in each job message so crawlers need no API configuration.

The API has an 8-CPU limit and shares `/data/manual_bulk_crawls` with the existing
crawler pool. It reads shard routing from the existing scheduler_ingest YAML
selected by `ingest_config_path` in `containers/api/config/config.yml`;
PostgreSQL connection settings remain in the API configuration.
It does not submit ingestion records or change scheduler state.

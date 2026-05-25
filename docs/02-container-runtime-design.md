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
5. History retention: the same daily pass also trickle-deletes `url_state_history_{shard}` and `content_feature_history_{shard}` snapshots older than `history_retention_days` (default 30, `0` disables). These are append-only audit logs with no pipeline read path; each batch is ordered by `snapshot_id` (the PK, monotonic with time) so it reads the oldest rows first. The current-table 90-day rolling counters are unaffected. Deletes release space to the table free list; repack/vacuum returns it to the OS.

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

## 2.4 `scheduler_ingest` Container

Runs 4 program families via `supervisord`:

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

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

1. Query unfinished API-job inventory and fresh per-domain crawler feedback.
2. Peek eligible frontier candidates in the offerer’s assigned shards, then probe bounded usable URL counts and frontier value.
3. Allocate new work with latency-aware MIP and a feasible greedy fallback. Observed capacity is a soft target; frontier, batch, domain and global outstanding limits are hard bounds.
4. Claim selected URLs atomically in scored-first order, updating scheduling state and daily event counters.
5. Persist a local HTTP outbox and submit `processing_mode: ingest` jobs through the API. Reconcile lost replies using the exact unique client reference.
6. Write scheduling stats deltas. The offerer decides business priority; crawler policy remains inside the crawler.

See [16-offerer-api-job-allocation.md](16-offerer-api-job-allocation.md) for the objective, limits and remaining claim/outbox crash window.

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

With domain-priority steering enabled, the ranker discovers candidate URLs
without locking the per-domain candidates consumed by the global sort. It then
claims at most `batch_size` rows once with `FOR UPDATE OF u SKIP LOCKED`,
rechecks eligibility, and reads current feature values. A partial batch is scored,
updated, and committed immediately; the ranker never refills it by searching again
while retaining row locks. An empty claim advances to the next shard. Age, TTL,
manual-priority, and domain-tier gates apply to both discovery and claiming.
Claims remain held through scoring and the score update. Each SQL statement in
the scoring transaction has a 30-second timeout, configured by
`golden_discovery_ranker_v1.statement_timeout_sec` or
`GOLDEN_DISCOVERY_RANKER_V1_STATEMENT_TIMEOUT_SEC`. This is a per-statement limit,
not a deadline for Python model inference or the whole transaction. A canceled
statement rolls back the batch, releases its locks, logs
`golden_discovery_ranker_v1.statement_timeout`, and advances to the next shard.

## 2.3 `crawler` Container

Runs `supervisord` with:

- 15 baseline AutoThrottle spiders (`crawler_id=0..14`)
- 1 independently configurable AutoThrottle canary spider (`crawler_id=15`)
- each spider reads/writes its own IPC queue/result path by `crawler_id`

Crawler worker behavior:

1. Poll this crawler’s unfinished API-job partitions. Legacy domain-file input is retired.
2. Admit bounded fetch/ingest work using the existing service-time heuristic, per-site policies and source sharing. Requests use Scrapy robots handling, retries and downloader rate/concurrency enforcement.
3. For fetch jobs, persist HTML job results without extraction or downstream ingest records.
4. For ingest jobs, parse HTML/XHTML, extract links and metadata, and emit the existing success/failure records. Parser processes and the optional bounded writer process preserve backpressure.
5. Append ingest records to `/data/ipc/crawl_result/crawler_{id:02d}/{YYYYMMDD}/{HHMM}/HHMM.jsonl`; acknowledge the job URL after JSONL fsync and checkpoint persistence.
6. Publish bounded per-domain processing feedback, exposed through the API for offerer allocation. Heartbeat pending inventory describes ingest jobs.

The JSONL contract for router, ingestor and feature extraction is unchanged.
See [15-ingest-job-api.md](15-ingest-job-api.md) and the coordinated
[migration procedure](17-api-job-input-migration.md) before deploying this layer.

## 2.4 `scheduler_ingest` Container

The container has a 32-CPU limit shared by 4 program families via `supervisord`:

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

During Host2 memory-incident recovery, routers use the original direct routing
and per-record parent-score queries. The hostname routing cache and batched
score prefetch are disabled. Domain resolution and output writes remain per
record. `route.folder_done` retains its input/output counts and duration fields
to measure throughput and compare it with the ten-minute input interval.

This rollback does not bound the existing whole-file/whole-folder reads or
shrink queued buckets. Do not restart the pipeline solely because the rollback
tests pass: inspect queued bucket sizes and coordinate recovery with its owner.

On TERM or INT, a router finishes its current folder and saves progress before
exiting. Supervisor allows 20 minutes before forcing termination; Compose gives
the scheduler-ingest container 21 minutes. Rebuild/recreate the container to
apply these deployment settings. A crash or forced kill can still replay the
last incomplete folder under the existing folder-level checkpoint scheme.

### Ingestor (x16)

Input: router output folders `ingestor_{id}`.

The ingestor snapshots input filenames and streams the folder twice: discoveries
first, then crawl results. Each shard buffers at most 128,000 discovery records or
2,000 crawl results, with a shared
2 GiB estimated decoded-record budget per ingestor. Reaching either limit
flushes a database batch; the byte limit flushes the largest shard buffer first.
One oversized record can exceed the budget while it is decoded, but is flushed
immediately. JSONL readers in the router and extractor also yield one record at
a time. Ready input folders must remain unchanged during the two passes.

`ingestor.discovery_batch_size` configures discovery batches; the
`INGESTOR_DISCOVERY_BATCH_SIZE` environment variable overrides it.
`ingestor.max_buffer_mib` configures the shared buffer budget in MiB; the
`INGESTOR_MAX_BUFFER_MIB` environment variable overrides it. Both settings must
be positive. For example, `max_buffer_mib: 2048` allows 2 GiB of estimated input
buffers per worker, or 32 GiB across 16 workers, in addition to model and database
working memory. Changes apply when the ingestor worker starts.
The configured inline-scoring budget is 30 seconds per discovery batch, controlled
by `golden_discovery_ranker_v1.ingest_inline_score_timeout_sec` or the
`GOLDEN_DISCOVERY_RANKER_V1_INGEST_INLINE_SCORE_TIMEOUT_SEC` environment override.
Scoring still runs in 5,000-URL chunks. Startup logs include the effective scoring
budget and chunk size alongside the record and memory limits.
`ingest.batch_done` records input count, count
after deduplication/filtering, kind, and batch processing duration (including
inline scoring and database work) for tuning.

Discovery deduplication and URL normalization run within each flushed batch.
Duplicates in later batches use the existing database UPSERT, preserving additive
inlink counts and parent/priority conflict handling. This can increase database
work and audit-event counts compared with folder-wide deduplication. Statistics
still accumulate per domain until the folder finishes, so the byte budget bounds
record buffers rather than the entire process RSS.

Responsibilities:

- `status="new"` records: insert new URL candidates into `url_state_current_{shard}` and history.
  - Trap filter (`ingestor/trap_filter.py`; on by default, optional `trap_filter` block in `ingest.yaml` to override, see [10](10-frontier-trap-rules.md#configuration-and-rollout)), applied before any DB work: strip the approved params in `normalize_params` (default only `srsltid`, the same policy as `golden_inject.py`), merge links that collapse to the same URL, log what stripping every other tracking / session param would change without applying it (`would_normalize`), then drop links hitting a high-confidence trap rule (`libs/frontier/trap_rules.HIGH_CONFIDENCE`: path repeat, too many path segments, nested redirect, faceted navigation, calendar plugin). Every affected link is appended to `/data/ipc/trap_filter/ingestor_{id}/YYYY-MM-DD.jsonl.gz`; `scripts/trap_filter_golden_check.py` measures golden false kills from these logs. Crawl results are never filtered. Rule definitions: [10-frontier-trap-rules.md](10-frontier-trap-rules.md).
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

The `api` container hosts the system's external HTTP APIs using FastAPI and Uvicorn.
It has an 8-CPU limit. Compose binds host `127.0.0.1:4000` to container port `8080`;
set `API_BIND_ADDRESS` and `API_PORT` to change the host binding.

### API Server (x5)

Input: external HTTP requests.

Responsibilities:

- Run 1 Uvicorn parent and 4 API workers, configured in `containers/api/config/config.yml`.
- Route requests to the registered FastAPI endpoint handlers.
- Return JSON error responses and enforce the configured request body limit
  (default 300 MiB), including streamed uploads.
- Load server and feature settings from `containers/api/config/config.yml`.

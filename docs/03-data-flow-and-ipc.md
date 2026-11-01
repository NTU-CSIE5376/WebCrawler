# 03. Data Flow and IPC Contracts

## 3.1 End-to-End Flow

1. `offerer` reads eligible URLs from sharded DB tables and writes queue batch JSON files.
2. `crawler` consumes queue JSON files, fetches pages, writes crawl JSONL results.
3. `router` transforms crawl records, ensures domain IDs, computes shard targets, emits normalized ingest JSONL.
4. `ingestor` updates URL state/history/event-counter tables from normalized records.
5. `feature_extractor` computes content metrics and updates feature tables.
6. All producer components write stats deltas.
7. `stats_aggregator` consumes deltas and updates daily summary tables.
8. `accounting_rolloff` (DB-only, daily) rolls off aged per-URL event counters from `url_state_current_{shard}` and marks event rows accounted.

## 3.2 Filesystem Layout Under `/data/ipc`

- URL queue
  - `/data/ipc/url_queue/crawler_{id:02d}/*.json`
- Crawl results
  - `/data/ipc/crawl_result/crawler_{id:02d}/{YYYYMMDD}/{HHMM}/*.jsonl`
- Router output / Ingest input
  - `/data/ipc/crawl_result/ingestor_{id:02d}/{YYYYMMDD}/{HHMM}/*.jsonl`
- Progress checkpoints
  - `/data/ipc/progress/router/{id:02d}.json`
  - `/data/ipc/progress/ingestor/{id:02d}.json`
  - `/data/ipc/progress/extractor/{id:02d}.json`
- Stats deltas
  - `/data/ipc/stats/*.json`
  - bad files: `/data/ipc/stats/bad/*.json`

Notes:

- `accounting_rolloff` does not use filesystem IPC; it reads/writes only SQL tables.

## 3.3 File Record Schemas

### Offerer Queue Batch (`*.json`)

```json
{
  "generated_at": "2026-01-01T01:01:01+00:00",
  "urls": ["https://example.com/a", "https://example.com/b"]
}
```

### Crawler Result Record (`*.jsonl`)

```json
{
  "url": "https://example.com/page",
  "domain": "example.com",
  "fetched_at": "2026-01-01T01:02:03+00:00",
  "status": "ok",
  "fail_reason": null,
  "content": "<html>...",
  "outlinks": [
    {
      "url": "https://another.example/p",
      "domain": "another.example",
      "anchor": "read more"
    }
  ]
}
```

### Router Output Record (result)

```json
{
  "url": "https://example.com/page",
  "status": "ok",
  "fetched_at": "2026-01-01T01:02:03+00:00",
  "fail_reason": null,
  "content": "<html>...",
  "outlinks": [
    {"url": "https://another.example/p", "domain_id": 123, "anchor": "read more"}
  ],
  "shard_id": 37,
  "domain_id": 55,
  "content_hash": "sha1hex..."
}
```

### Router Output Record (new outlink candidate)

```json
{
  "url": "https://another.example/p",
  "status": "new",
  "shard_id": 98,
  "domain_id": 123,
  "domain_score": 0.0,
  "discovered_from": "https://example.com/parent",
  "discovery_source_type": 1,
  "parent_page_score": 0.5,
  "inlink_count_approx": 1,
  "inlink_count_external": 0,
  "anchor_text": "click here"
}
```

The single canonical builder for this record lives at
`libs/ipc/new_link_record.py:build_new_link_record(...)`. Both the
`router` (`discovery_source_type = 1`) and the `sitemap_patroller`
(`discovery_source_type = 2`) call it. Adding a writer to this IPC dir
in the future SHOULD reuse the builder rather than hand-roll the dict,
so the schema stays in sync with the ingestor's `_bulk_links` reader.
`URL → (shard_id, ingestor_id)` routing is shared the same way through
`libs/db/sharding/router.py:ShardRouter`.

### Stats Delta (`*.json`)

```json
{
  "generated_at": "2026-01-01T01:05:00+00:00",
  "source": "ingestor",
  "counters": {
    "new_links": 80,
    "num_fetch_ok": 200,
    "num_fetch_fail": 12,
    "fail_reasons": {"HttpError 404": 6}
  },
  "domains": {
    "55": {
      "num_fetch_ok": 120,
      "num_fetch_fail": 4,
      "fail_reasons": {"HttpError 404": 3}
    }
  }
}
```

## 3.4 Processing Windows and Progress Tracking

- Time bucket unit: `interval_minutes` (configured as 10).
- A folder is considered ready only after `2 * interval_minutes` from its timestamp.
- Each worker stores last processed `(date, time)` in progress JSON.
- This gives deterministic forward-only scanning and prevents reading partially written folders.

## 3.5 Reliability Characteristics

- Atomic write pattern (`*.tmp` then `os.replace`) used for JSON file creation.
- Queue consumer deletes selected batch file immediately after read; failures after delete may lose that batch (tradeoff for simplicity).
- DB interactions include retries for router domain resolution on transient DB transport errors.
- Supervisor autorestart is enabled for all long-running processes.

## 3.6 External API

The external API lets other services submit URLs for crawling. The default
local base URL is `http://localhost:4000`; remote clients use the host address
provided by the deployment. Request bodies are limited to 300 MiB; larger ones
return `413` with `{"error":"Request body too large"}`. Errors always use a JSON
`error` field.

### 3.6.1 Crawl Request API (bulk crawl v2)

Every `/v1/crawl-requests*` call needs `Authorization: Bearer <token>`.
`crawl_requests.tokens_file` (YAML, token -> tenant) decides the tenant, which a
body cannot set. A missing or unknown token returns `401`; another tenant's request
returns `404`. Bodies must be `application/json`.

```text
POST   /v1/crawl-requests            {name, result_quota_gb?, expires_days?}
       -> 201 {request_id, name, result_quota_gb, expires_at}
GET    /v1/crawl-requests            -> 200 {requests: [<status>, ...]}
POST   /v1/crawl-requests/{id}/urls  {batch_id, urls: [{id, url, priority}]}   (1..max_urls)
       -> 202 {accepted, skipped: [{index, id, reason: "invalid"|"duplicate"}]}
POST   /v1/crawl-requests/{id}/seal  -> 200 {sealed: true}
DELETE /v1/crawl-requests/{id}       -> 202 {cancelled: true}
GET    /v1/crawl-requests/{id}       -> 200 <status>
GET    /v1/crawl-requests/{id}/segments?wait=<s>   -> 200 {segments: [{name, bytes}]}
GET    /v1/crawl-requests/{id}/segments/{name}     -> 200 application/gzip
POST   /v1/crawl-requests/{id}/ack   {segments: [name, ...]} -> 200 {deleted}
```

- `result_quota_gb` defaults to, and is capped at, `max_result_quota_gb` (GB =
  10^9 bytes). `expires_days` defaults to 30 and is capped at 60.
- URLs: each item is validated and canonicalized like other API URLs (HTTP(S),
  public host, no credentials, at most `max_url_length`). A bad URL is skipped
  as `invalid`; a repeated `id` within the batch is skipped as `duplicate`.
  Skipped items never get a record. Only a malformed body returns `400`.
- `priority` is an integer; lower is crawled first within a host.
- The same `batch_id` again returns the stored first response and writes
  nothing. An `id` already pending in the request (queued, downloading or not
  yet in a closed segment) is dropped silently; once its record is published,
  the same `id` can be sent again and is crawled again.
- `409`: the request is sealed, cancelled or expired. `404`: unknown request.
- `503` with `Retry-After` (`retry_after_s`): a target crawler's inbox holds more
  than `max_inbox_files` files, free space under the data root is below
  `min_free_gb`, the tenant has more than `max_tenant_queued_urls` queued URLs,
  or the routing lookup failed. Nothing is written in that case.
- `DELETE` cancels queued URLs; each gets an `error: "cancelled"` record. URLs
  already downloading finish normally. Unacked segments stay listable,
  downloadable and ackable.
- Status: `{request_id, name, created_at, expires_at, sealed, cancelled, queued,
  leased, unacked_segments, unacked_bytes, top_hosts: [{host, remaining,
  done_1h}], updated_at}`. Counts are summed from crawler snapshots taken every
  `MANUAL_BULK_CRAWL_STATUS_S`; `updated_at` is the oldest snapshot time.
- `segments` lists closed, unacked segments oldest first. `wait` (capped at
  `long_poll_max_s`) long-polls until the list is non-empty. `ack` deletes the
  named segments and ignores unknown names.

Segment name: `{crawler_id:02d}-{seq:012d}.jsonl.gz`. Each gzip line is one
terminal record:

```text
{id, url, priority, final_url, status_code, error, fetched_at | null, finished_at, html | null}
```

- `url` is the canonical submitted URL; `final_url` is after redirects.
- `html` is the decoded body of an HTML 2xx response, otherwise null.
- `error`: null (HTML 2xx) | `non_html` (other 2xx) | `http_<code>` | `dns` |
  `timeout` | `connection` | `robots` | `retries_exhausted` | `expired` |
  `cancelled`.
- `fetched_at` is null without a response; every record has `finished_at`.
- Delivery is at least once: every accepted URL gets one or more records.

## 3.7 Crawl Request Processing

Layout under `crawl_requests.root` / `MANUAL_BULK_CRAWL_ROOT`
(`/data/manual_bulk_crawls`, shared by the api and crawler containers). Every
JSON file is written as tmp + fsync + rename.

```text
requests/{id}.json, {id}.sealed, {id}.cancel     API: meta (tenant, expiry, quota) and markers
requests/{id}/batches/{batch_key}.json           API: stored 202 response
inbox/crawler_NN/{id}-{batch_key}.json           API writes, crawler NN ingests and deletes
frontier/crawler_NN.sqlite                       crawler NN only (the API never opens it)
segments/{id}/crawler_NN/{NN}-{seq}.jsonl.gz     crawler NN writes (.part while open); API deletes on ack
status/crawler_NN.json                           crawler NN snapshot
```

Ownership follows the normal IPC rule: one consumer per partition. The API
validates, routes each URL to the crawler that owns its domain (read-only
`domain_state` lookup, cached per host for `route_cache_ttl_s`), writes one
inbox file per target crawler, then the batch record. A retry rewrites the
same inbox files.

Each crawler process runs one frontier thread that owns its SQLite file (WAL)
and one segment thread. The reactor only schedules in memory:

1. Ingest: up to `MANUAL_BULK_CRAWL_INGEST_FILES` inbox files per poll go into
   SQLite with `INSERT OR IGNORE` on `(request, id)`. Files of a missing or
   cancelled request are deleted unread.
2. Dispatch, while manual in-flight < `CONCURRENT_REQUESTS`: pick the eligible
   request with the lowest stride pass (equal weights; a new or idle request
   does not bank turns). In it, pick the ready slot (hostname) with the largest
   `remaining * seconds_per_url + jitter`. A slot is ready below its cap,
   `min(slot concurrency, ceil(latency / delay) + MANUAL_BULK_CRAWL_HOST_HEADROOM)`,
   shared by all requests, and when not blocked. Each (request, slot) keeps a
   small leased buffer (about 2 x cap rows) that the frontier thread refills.
3. The `FairScheduler` then shares downloader dispatch between normal and
   manual work (`MANUAL_BULK_CRAWL_SCHEDULING_POLICY`, `weighted` with
   `MANUAL_BULK_CRAWL_SHARE_PERCENT`). Manual and normal requests use the same
   downloader slot, robots.txt rules, retries and AutoThrottle.
4. 429/503: the slot is blocked for `Retry-After` (at most
   `MANUAL_BULK_CRAWL_MAX_BLOCK_S`) or `MANUAL_BULK_CRAWL_BLOCK_BASE_S * 2**n`,
   and the URL is requeued without a record, up to
   `MANUAL_BULK_CRAWL_MAX_REQUEUES` times.
5. Records go to the open segment of their request. A segment closes (gzip
   flush, fsync, rename) at `MANUAL_BULK_CRAWL_SEGMENT_BYTES` or after
   `MANUAL_BULK_CRAWL_SEGMENT_S`; only then are its rows deleted from SQLite.
6. Cancel or expiry: queued rows are closed in chunks of
   `MANUAL_BULK_CRAWL_CLOSE_CHUNK` per poll and get `cancelled` / `expired`
   records.

Backpressure: a crawler pauses a request while its unacked bytes in that
crawler (closed segments plus the open one) exceed
`result_quota_bytes / MANUAL_BULK_CRAWL_SHARDS`; an ack resumes it on the next
poll. All manual dispatch pauses while free space is below
`MANUAL_BULK_CRAWL_MIN_FREE_GB`. The heartbeat reports `manual_queued`,
`manual_outstanding`, `manual_unacked_bytes`, `manual_blocked_slots` and
`manual_paused`.

Restart: the crawler resets leased and closing rows to queued and deletes its
own `.part` files, so unpublished work is redone (at least once). Segment
numbers continue from the highest one ever closed, so acked names are not reused.

GC (`containers.api.manual_bulk_crawl_gc`, every `gc_interval_sec`) removes a
request `gc_grace_days` after it expires, or once it is cancelled, all its
segments are acked, no inbox file or open segment remains, and every crawler
snapshot taken after the cancel reports nothing queued or leased.

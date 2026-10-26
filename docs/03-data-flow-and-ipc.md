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
provided by the deployment. Authentication is not required.

The API limits request bodies to 300 MiB. Oversized requests return
`413 Content Too Large` with a JSON body:

```json
{"error":"Request body too large"}
```

### 3.6.1 Manual Bulk Crawl API

`POST /v1/manual-bulk-crawls`

The `manual-bulk-crawl-api` creates an independent crawl job for one or more URLs.
Jobs fetch the submitted pages and save their HTML in separate storage without
updating scheduler state, discovering links or running feature extraction. The request must use `Content-Type: application/json`.

**Request body**

```json
{
  "urls": [
    "https://example.com/article",
    "https://example.org/page"
  ],
  "ttl_days": 14
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `urls` | Array of strings | Yes | Nonempty list of absolute HTTP or HTTPS URLs; default maximum 10,000 per request. |
| `ttl_days` | Integer | No | Retention from submission, 1–60 days inclusive; defaults to 14 days. |

The default URL length limit is 2,500
characters, checked both before and after normalization.
Deployments may configure a smaller length limit or a different per-request
URL count limit. Each URL must have a valid hostname and port and contain no
credentials, whitespace, or control characters. If any URL is invalid, the
entire request is rejected.

**Successful response**

`200 OK`

```json
{"job_id":"ab12cd34ef56ab78cd90ef12ab34cd56"}
```

Submission is all-or-nothing: `200 OK` means the entire batch has been
accepted for processing. It does not mean the URLs have already been
crawled. Crawling is asynchronous, respects robots.txt and crawler politeness,
and uses the existing crawler workers alongside normal crawling.
API jobs share crawler capacity and domain politeness limits with regular work. No completion time is guaranteed.

URLs are normalized, and duplicates within the request are collapsed. Every
job fetches its own submitted URLs, including previously crawled URLs. Requests
from different jobs are independent.

**Job status**

`GET /v1/manual-bulk-crawls/{job_id}`

```json
{
  "job_id": "ab12cd34ef56ab78cd90ef12ab34cd56",
  "status": "completed",
  "total_urls": 1000,
  "completed_urls": 1000,
  "succeeded_urls": 990,
  "failed_urls": 10,
  "html_files": 993,
  "created_at": "2026-10-17T10:00:00+00:00",
  "started_at": "2026-10-17T10:00:01+00:00",
  "completed_at": "2026-10-17T10:01:00+00:00",
  "results_url": "/v1/manual-bulk-crawls/ab12cd34ef56ab78cd90ef12ab34cd56/results"
}
```

`status` is `pending`, `running` or `completed`. Unknown job IDs return `404`.
Counts refer to normalized, unique URLs. `completed` means every URL has a
terminal result, including failures; it does not mean every fetch succeeded.
`succeeded_urls` counts successful HTML responses; `failed_urls` includes HTTP
errors, transport failures and non-HTML responses. `html_files` can exceed
`succeeded_urls` because HTML error pages are also saved. `started_at` and
`completed_at` are null until those events occur. `results_url` is null until the job
completes. Interrupted jobs resume from saved per-URL results; a fetch
interrupted before its result was saved may run again.

**Reading HTML results**

Completed results are available over HTTP through the API.
Clients on another machine can download them without mounting the server's
filesystem. Use the API's reachable hostname and port as the base URL;
all returned download URLs are relative to that origin.

`GET /v1/manual-bulk-crawls/{job_id}/results`

Follow the status response's `results_url` to list available files:

```json
{
  "job_id": "ab12cd34ef56ab78cd90ef12ab34cd56",
  "files": [
    {
      "name": "00000.html",
      "url": "https://example.com/",
      "size_bytes": 12345,
      "download_url": "/v1/manual-bulk-crawls/ab12cd34ef56ab78cd90ef12ab34cd56/results/00000.html"
    }
  ]
}
```

The list includes downloaded HTML, per-URL JSON metadata, `request.json` and
`status.json`. Each entry gives its byte size, download URL and original
submitted URL in `url` (after validation and normalization, before redirects).
Job-wide files (`request.json` and `status.json`) have `url: null`.

`GET /v1/manual-bulk-crawls/{job_id}/results/{filename}`

Follow a file's `download_url` to download its original bytes. Files are served
as attachments. Downloads support HTTP byte ranges for resuming transfers.
A result-listing or file request returns `409` until the job completes, `404`
for an unknown job or unavailable filename, or `503` if storage is unavailable.
Errors use a JSON `error` field.

`request.json` contains the normalized URL list in order. Each numbered JSON
file identifies the original URL, final URL after redirects, HTTP status (null
for transport failures), fetch time, error and HTML filename. A URL that did
not return HTML has no `.html` file. HTML files contain the downloaded response
bytes. All results become available together after all URLs finish.

**Submission error responses**

| HTTP status | Meaning |
|-------------|---------|
| `400 Bad Request` | Malformed JSON, unexpected fields, an invalid URL, or a URL list outside the permitted size. |
| `415 Unsupported Media Type` | Content type is not JSON. |
| `500 Internal Server Error` | An unexpected internal failure prevented submission. The entire batch was rejected. |
| `503 Service Unavailable` | A temporary service or resource availability problem prevented submission, such as full temporary storage. The entire batch was rejected. |

API errors use an `error` field, for example:

```json
{"error":"Invalid HTTP(S) URL at index 1"}
```

URL indices are zero-based.

For `500`, the response is:

```json
{"error":"Submission failed; no URLs accepted"}
```

For `503`, the response is:

```json
{"error":"Submission unavailable; no URLs accepted"}
```

**Retries and batch behavior**

- A `200` response accepts the entire batch. Do not resubmit it merely because
  crawling has not completed yet.
- A `500` or `503` response accepts none of the URLs. There is no partial-success
  response or per-URL acceptance count.
- After `503`, retry the entire request with a delay between attempts.
- After `500`, investigate the failure before retrying; immediate retries may
  encounter the same internal error.
- After `400` or `415`, correct the request before resubmitting.
- If the connection is lost without an HTTP response, acceptance is unknown.
  Retrying may create a second job and fetch the URLs again; this endpoint
  does not provide cross-request idempotency.

## 3.7 Manual Bulk Crawl Processing

Input: per-crawler job entries under `MANUAL_BULK_CRAWL_QUEUE_DIR/crawler_{id:02d}`
(default `/data/manual_bulk_crawls/pending/crawler_{id:02d}`).

The existing `html_spider` workers also consume API jobs. The API resolves each
URL to the crawler that owns its domain's shard, using read-only routing data.
The routing configuration is selected by `ingest_config_path` in
`containers/api/config/config.yml`; PostgreSQL connection settings remain in
that API configuration. It neither inserts domains nor updates URL scheduling
flags. API requests use
the same downloader, domain slots, robots.txt rules, retry policy and
AutoThrottle settings as regular requests.

Responsibilities:

- Consume only this crawler's queue partition. Run one live crawler process per
  crawler ID, as with normal IPC. Different crawlers can process one job
  concurrently without file locks.
- Configure `manual_bulk_crawl.scheduling_policy` in `containers/api/config/config.yml`,
  optionally overridden by `MANUAL_BULK_CRAWL_SCHEDULING_POLICY` on the API process
  (also passed through Compose). The API publishes the policy with each new job;
  crawlers read no API config. Existing jobs retain their original policy, and
  jobs without the field default to `manual_first` (API work first).
  In `balanced` mode, when both sources have queued requests, take one normal request and one manual
  request in turn. Skip an empty source so the other uses all available dispatch
  capacity; an arriving source receives the next turn. `manual_first` gives that
  job's requests priority over normal and balanced work. `normal_first` dispatches
  that job's requests only when the other queues have no request to dispatch.
  These are dispatch preferences, not preemption of downloads or changes to QPS.
  Priority modes can starve lower-priority queues under continuous load.
  No dispatch ratio or manual
  percentage concurrency cap is configured. Each source retains Scrapy's
  downloader-aware domain selection and uses FIFO queues at equal priority,
  including startup requests. Retries retain their source and priority so new
  URLs within the same queue cannot indefinitely overtake them.
  Manual prefetch is bounded by `CONCURRENT_REQUESTS`; active jobs remain bounded
  by `MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS` (default 64), overridable with
  `CRAWLER_MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS`.
- Admit manual jobs in submission order up to the active-job limit, then group
  their unfinished URLs by Scrapy downloader slot (normally hostname). Within a
  slot, preserve submission order and original URL indexes. Across slots, prefer
  the longest estimated remaining service time: remaining URLs divided by
  estimated QPS. QPS is estimated as `1 / max(delay, latency / concurrency)`,
  using live downloader slot settings when available, otherwise crawler settings
  and `DOWNLOAD_SLOTS` overrides. Latency starts at one second and uses an
  exponentially smoothed `download_latency` from responses (20% new sample).
  Among slots below their prefetch cap, select the largest estimated remaining
  service time; break equal scores by least-recent dispatch order.
  Each slot's manual outstanding URLs (queued, downloading or retrying), across
  all jobs, are capped at its configured concurrency. Refill immediately on
  completion, and refresh live slot settings on each poll. These are prefetch
  decisions; the shared downloader still enforces actual politeness for both
  normal and manual work. Jobs outside the active window wait for admission.
- Fetch only submitted URLs. Dedicated callbacks save HTML and metadata without
  emitting PageItems, discovering outlinks or entering the normal result pipeline.
- Write results and atomically replace progress only within
  `result_dir/{job_id}/crawler_{id:02d}`. Each partition has one writer.
- Remove only this crawler's queue entry after its completed progress is saved.
  Resume unfinished entries from durable per-URL records after restart; a fetch
  interrupted before its record was saved may run again.
- On a job failure, drain its outstanding callbacks before retrying. Other jobs
  and crawler partitions continue independently.

The API creates output partitions and queue entries before atomically publishing
an immutable job manifest. Entries without a manifest are ignored, so a failed
submission cannot expose part of a batch. Concurrent API workers publish distinct
jobs without sharing a mutable queue file.

The API computes job status by summing each assigned crawler's progress. A job is
completed only when every partition has a completion timestamp; result listing
and downloads remain unavailable until then. No crawler publishes a shared final
status or moves the whole result directory. Download filenames stay flat; the API
resolves each numbered file to its owning crawler partition.

Only the API and its GC process delete jobs. They atomically retire the manifest
to hide the job, then move its output directory out of the visible namespace
before removing it. Crawlers never create output directories, so late callbacks
cannot recreate deleted results. GC retries interrupted cleanup and removes
expired, uncommitted preparations too.

The API and normal crawler container share a separate host directory, configured
by `MANUAL_BULK_CRAWL_DATA_DIR` in Compose (default `/data/manual_bulk_crawls`). The
crawler does not read API configuration. The API publishes URLs, assigned crawler
IDs and the output root in each job message. The crawler reads only its own
settings; `CRAWLER_MANUAL_BULK_CRAWL_QUEUE_DIR` selects the input queue and must
match the API publisher's `manual_bulk_crawl.pending_dir`. No new crawler processes or SQL dispatcher threads
are started. API jobs share crawler capacity and politeness budgets, while their
results stay outside `/data/ipc/crawl_result` and never enter ingestion or feature
extraction.

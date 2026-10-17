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

### Manual Bulk Crawl Job (`*.json`)

The API atomically publishes a job in its pending directory. Crawler workers
consume this IPC message without reading the API's configuration:

```json
{
  "version": 1,
  "urls": ["https://example.com/article"],
  "crawler_ids": [0],
  "result_dir": "/data/manual_bulk_crawls/results",
  "created_at": "2026-10-17T10:00:00+00:00"
}
```

The filename stem is the job ID. `crawler_ids` assigns each URL to its existing
crawler worker. `result_dir` is an absolute output root supplied by the publisher;
the complete job is published under `{result_dir}/{job_id}`. The crawler's own
settings control queue discovery and resource limits. API and crawler containers
must mount the output storage at the same path.

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
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `urls` | Array of strings | Yes | Nonempty list of absolute HTTP or HTTPS URLs; default maximum 10,000 per request. |

The body must contain only `urls`. The default URL length limit is 2,500
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
  "result_dir": "/data/manual_bulk_crawls/results/ab12cd34ef56ab78cd90ef12ab34cd56",
  "results_url": "/v1/manual-bulk-crawls/ab12cd34ef56ab78cd90ef12ab34cd56/results"
}
```

`status` is `pending`, `running` or `completed`. Unknown job IDs return `404`.
Counts refer to normalized, unique URLs. `completed` means every URL has a
terminal result, including failures; it does not mean every fetch succeeded.
`succeeded_urls` counts successful HTML responses; `failed_urls` includes HTTP
errors, transport failures and non-HTML responses. `html_files` can exceed
`succeeded_urls` because HTML error pages are also saved. `started_at` and
`completed_at` are null until those events occur. `result_dir` is null until the
entire result directory is available. `results_url` is also null until the job
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
      "size_bytes": 12345,
      "download_url": "/v1/manual-bulk-crawls/ab12cd34ef56ab78cd90ef12ab34cd56/results/00000.html"
    }
  ]
}
```

The list includes downloaded HTML, per-URL JSON metadata, `request.json` and
`status.json`. Each entry gives its byte size and download URL.

`GET /v1/manual-bulk-crawls/{job_id}/results/{filename}`

Follow a file's `download_url` to download its original bytes. Files are served
as attachments. Downloads support HTTP byte ranges for resuming transfers.
A result-listing or file request returns `409` until the job completes, `404`
for an unknown job or unavailable filename, or `503` if storage is unavailable.
Errors use a JSON `error` field.

Files are also accessible directly through the server's `result_dir`:

```text
{result_dir}/
  request.json
  status.json
  00000.html
  00000.json
  00001.json
  ...
```

`request.json` contains the normalized URL list in order. Each numbered JSON
file identifies the original URL, final URL after redirects, HTTP status (null
for transport failures), fetch time, error and HTML filename. A URL that did
not return HTML has no `.html` file. HTML files contain the downloaded response
bytes. The entire job directory becomes visible together after all URLs finish.
Completed job files are retained until explicitly removed; there is currently
no automatic expiration.

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

Input: accepted job messages in `MANUAL_BULK_CRAWL_QUEUE_DIR`
(default `/data/manual_bulk_crawls/pending`).

The existing `html_spider` workers also consume API jobs. The API resolves each
URL to the crawler that owns its domain's shard, using read-only routing data.
The routing configuration is selected by `ingest_config_path` in
`containers/api/config/config.yml`; PostgreSQL connection settings remain in
that API configuration. It neither inserts domains nor updates URL scheduling
flags. API requests use
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

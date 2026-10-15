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
Interactive API documentation is available at `/scheduler_ingest/docs`.

### 3.6.1 Crawl Request API

`POST /scheduler_ingest/v1/crawl-requests`

The `crawl-request-api` accepts one or more URLs for the crawler to discover
and schedule. The request must use `Content-Type: application/json`.

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
| `urls` | Array of strings | Yes | Nonempty list of absolute HTTP or HTTPS URLs; default maximum 1,000 per request. |

The body must contain only `urls` and must not exceed 4 MiB. The default URL
length limit is 2,500 characters, checked both before and after normalization.
Deployments may configure a smaller length limit or a different per-request
URL count limit. Each URL must have a valid hostname and port and contain no
credentials, whitespace, or control characters. If any URL is invalid, the
entire request is rejected.

**Successful response**

`200 OK`

```json
{"status":"ok"}
```

Submission is all-or-nothing: `200 OK` means the entire batch has been
accepted for processing. It does not mean the URLs have already been
crawled. Crawling is asynchronous and subject to
scheduling priority and crawl policy. No completion time is guaranteed.

URLs are normalized before submission, and duplicates within the same
request are collapsed. Already-known URLs are marked eligible for crawling
again; new URLs follow the normal discovery process. A result from an ongoing crawl may
satisfy a request without another crawl. This does not create duplicate URL
records.
Scheduling priority, domain pauses, and crawler politeness controls still
apply. This endpoint does not return crawl results or provide submission-status
tracking.

**Error responses**

| HTTP status | Meaning |
|-------------|---------|
| `400 Bad Request` | Malformed JSON, unexpected fields, an invalid URL, or a URL list outside the permitted size. |
| `413 Content Too Large` | Request body exceeds 4 MiB. |
| `415 Unsupported Media Type` | Content type is not JSON. |
| `500 Internal Server Error` | An unexpected internal failure prevented submission. The entire batch was rejected. |
| `503 Service Unavailable` | A temporary service or resource availability problem prevented submission, such as a database connection outage. The entire batch was rejected. |

API errors use an `error` field, for example:

```json
{"error":"Invalid HTTP(S) URL at index 1"}
```

URL indices are zero-based. Oversized requests may receive an HTML error
response instead of JSON.

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
- After `400`, `413`, or `415`, correct the request before resubmitting.
- If the connection is lost without an HTTP response, acceptance is unknown.
  Retrying may submit the batch again; URL records remain unique, but this
  endpoint does not provide cross-request idempotency.

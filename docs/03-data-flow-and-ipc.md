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

## 3.6 URL Submission API (PoC)

`scheduler_ingest` runs `url_submission_api` (Flask under Gunicorn) on port
8080. It has no authentication. The `api_gateway` Nginx container publishes
host loopback port 4000 and proxies `/scheduler_ingest/` to this Flask server,
stripping the prefix. Thus `POST /scheduler_ingest/v1/urls` forwards to
`POST http://scheduler_ingest:8080/v1/urls`. Flask only hosts `/v1/urls` and
`/healthz`; it does not handle a service prefix. Other Compose services can
call Flask directly at `http://scheduler_ingest:8080/v1/urls`.
Set `API_GATEWAY_BIND_ADDRESS` and `API_GATEWAY_PORT` when a different
host interface or port is needed.

```bash
curl -X POST http://localhost:4000/scheduler_ingest/v1/urls \
  -H 'Content-Type: application/json' \
  -d '{"urls":["https://example.com/article"]}'
```

The request must contain only `urls`, a nonempty list of HTTP(S) URLs.
The whole batch is validated before writes. Limits in `ingest.yaml` are
1,000 entries and a 1 MiB request body; URLs are limited to 2,500 characters
both before and after canonicalization. Credentials in URLs are rejected.
Canonical duplicates within one request are collapsed.

A successful response is `200 OK`:

```json
{"status":"ok"}
```

The API loads shared YAML overrides and the DB subdomain whitelist, resolves
domains through the existing `DomainResolver`, commits those domains, then
publishes `status="new"` records using `build_new_link_record`. Records have
unknown/seed discovery type (`0`) and zero inlink counts. Each destination
gets a uniquely named `api_{uuid}.jsonl` in its current UTC bucket;
the completed, flushed file is renamed from `.tmp` before readers see it.

`200` means published to IPC, not inserted or crawled. Existing URLs are not
automatically requeued; discovery-frozen domains are skipped by the ingestor.
Normal bucket delays and scheduler priority apply. There is no request-status
endpoint, forced recrawl, or cross-request idempotency in this PoC.

Flask returns JSON `400` for invalid input, `415` for a non-JSON content type,
`413` for an oversized body, and `503` for storage failures. The gateway also
enforces a 1 MiB body limit and may return its own HTML `413` before Flask.
Publication across destinations is
not atomic: a publication failure returns
`published_count` for files already published. A crash may also leave partial
publication. Retrying creates a new submission and may repeat discovery
signals; there is no durable submission/outbox table.

`GET /scheduler_ingest/healthz` through the gateway (`/healthz` on Flask)
is a process liveness check; it does not check DB availability.
`INGEST_CONFIG_PATH` overrides the API's default ingest config path.

Build and run with the existing source bind mount:

```bash
docker compose up -d --build scheduler_ingest api_gateway
docker compose exec scheduler_ingest python -m pytest containers/scheduler_ingest/tests -q
```

Tests use temporary IPC files and SQLite for domain resolution, with the DB
subdomain whitelist stubbed. They require neither a live PostgreSQL instance
nor external HTTP access. Dependencies, including pytest, are pinned in
`containers/scheduler_ingest/requirements.txt`, shared by the Dockerfile and
the host test environment.

Both services expose the same Task interface for local development and CI.
Install [Task](https://taskfile.dev/docs/installation) and Python 3.12, then
run from either service directory (gateway tests also require Docker):

```bash
task install
task test
```

Or use `task --dir containers/scheduler_ingest install` / `test` and
`task --dir containers/api_gateway install` / `test` from the repository root.
Both services install Python dependencies in service-local `.venv` directories
and execute pytest on the host, matching the CI runner. The scheduler reads
its requirements file and requires neither Docker nor PostgreSQL. Gateway tasks
install pytest, pull Nginx and
the mock-backend images, and run real proxy tests in temporary containers.
Gateway tests are always enabled through `task test`; their temporary
containers/network are removed at teardown. Extra pytest arguments can be
passed after `--`, for example `task test -- -v`. To choose a Python executable,
use `task install PYTHON_BIN=python3.12`.

`.github/workflows/ci.yml` runs both services as independent matrix jobs on
pushes and pull requests, and supports manual dispatch. Each job calls
`task install` followed by `task test`.

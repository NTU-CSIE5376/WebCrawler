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

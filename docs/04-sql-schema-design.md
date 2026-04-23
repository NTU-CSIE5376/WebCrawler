# 04. SQL Schema Design

## 4.1 Table Families Overview

The schema has two categories:

- **Non-sharded tables**: global domain and daily summary metadata.
- **Sharded tables**: 256-way split URL and content state tables.

With `num_shards=256`, total sharded tables are:

- `url_state_current_000..255` (256)
- `url_state_history_000..255` (256)
- `url_event_counter_000..255` (256)
- `content_feature_current_000..255` (256)
- `content_feature_history_000..255` (256)

Total sharded table count: **1280**.

## 4.2 Non-Sharded Tables

### `domain_state`

Purpose:

- Canonical domain registry used by router for `domain -> domain_id` resolution.

Columns:

- `domain_id BIGINT PRIMARY KEY`
- `domain VARCHAR NOT NULL UNIQUE`
- `shard_id INTEGER NOT NULL`
- `domain_score FLOAT DEFAULT 0.0`

Usage:

- Router performs `INSERT ... ON CONFLICT(domain) DO NOTHING`, then `SELECT domain_id, domain_score`.

### `domain_stats_daily`

Purpose:

- Daily per-domain aggregates from stats deltas.

PK:

- `(domain_id, event_date)`

Columns:

- `domain_id BIGINT`
- `event_date DATE`
- `shard_id INTEGER NOT NULL`
- `num_scheduled INTEGER DEFAULT 0`
- `num_fetch_ok INTEGER DEFAULT 0`
- `num_fetch_fail INTEGER DEFAULT 0`
- `num_content_update INTEGER DEFAULT 0`
- `fail_reasons JSONB DEFAULT '{}'::jsonb`

### `summary_daily`

Purpose:

- Daily global aggregates across the entire system.

PK:

- `event_date`

Main counters:

- `new_links`
- `num_scheduled`
- `num_fetch_ok`
- `num_fetch_fail`
- `num_content_update`
- `fail_reasons JSONB`

Error counters:

- `error_count`
- `offer_error`
- `route_error`
- `ingest_error`
- `stats_error`
- `extract_error`

### `url_link`

Purpose:

- Link graph edge table model (currently not written by active pipeline code).

PK:

- `(src_url, dst_url, anchor_hash)`

Columns:

- `src_url VARCHAR`
- `dst_url VARCHAR`
- `anchor_hash BYTEA GENERATED ALWAYS AS decode(md5(anchor_text), 'hex')`
- `anchor_text VARCHAR`
- `first_seen TIMESTAMPTZ DEFAULT now()`
- `last_seen TIMESTAMPTZ DEFAULT now()`

## 4.3 Sharded URL Tables

### `url_state_current_{shard}`

Purpose:

- Current mutable row per URL used by scheduler decisions.

PK:

- `url`

Key columns:

- identity and assignment: `url`, `domain_id`
- scheduler times: `first_seen`, `last_scheduled`, `last_fetch_ok`, `last_content_update`
- rolling counters: `num_scheduled_90d`, `num_fetch_ok_90d`, `num_fetch_fail_90d`, `num_content_update_90d`
- quality/failure: `num_consecutive_fail`, `last_fail_reason`, `content_hash`
- scheduling flags/signals: `should_crawl`, `url_score`, `domain_score`
- provenance: `source SMALLINT NOT NULL DEFAULT 0` (`0` = natural discovery, `1` = golden set membership; see `scripts/golden_inject.py`)
- provenance: `discovered_from VARCHAR` (parent page URL on first discovery; NULL for golden-injected and seed URLs; first parent wins via `ON CONFLICT DO NOTHING`)
- page metadata: `title VARCHAR` (`<title>` trimmed to 500 chars by the spider; NULL on fail / non-HTML; latest successful fetch wins, fails keep the previous value via `COALESCE`)

Write patterns:

- Offerer: selects `should_crawl=TRUE`, then updates scheduling fields.
- Ingestor: upserts fetch outcomes and resets/extends failure counters.
- Router-discovered links: inserted with initial `domain_score`.
- Golden set injection (`scripts/golden_inject.py`): inserts new URLs with `source=1`, or upserts `source=1` onto existing rows so that golden set membership is identifiable even when the crawler discovered the URL naturally first.

### `url_state_history_{shard}`

Purpose:

- Append-only snapshot history of URL state changes.

PK and metadata:

- `snapshot_id BIGINT PRIMARY KEY`
- `snapshot_at TIMESTAMPTZ DEFAULT now()`
- `url VARCHAR NOT NULL`
- plus all columns from `UrlStateMixin`

Write pattern:

- Ingestor inserts a row after each current-table upsert (full snapshot copy).
- Accounting rolloff appends snapshots after each maintenance update batch.
- Golden set injection (`scripts/golden_inject.py`): inserts a snapshot only when a new row is added to `url_state_current_{shard}`. Source-only updates on existing URLs do not generate a history entry.

### `url_event_counter_{shard}`

Purpose:

- Daily per-URL event counters for scheduled/fetch/update event accounting.

PK:

- `(url, event_date)`

Columns:

- `num_scheduled`
- `num_fetch_ok`
- `num_fetch_fail`
- `num_content_update`
- `accounted BOOLEAN DEFAULT TRUE`

Write pattern:

- Offerer increments `num_scheduled`.
- Ingestor increments fetch/update counters.
- Accounting rolloff marks aged rows `accounted=FALSE` after subtracting them from current 90-day counters.

## 4.4 Sharded Content Feature Tables

### `content_feature_current_{shard}`

Purpose:

- Latest extracted content features for each URL.

PK:

- `url`

Columns:

- `domain_id BIGINT NOT NULL`
- `fetched_at TIMESTAMPTZ DEFAULT now()`
- `content_length INTEGER DEFAULT 0`
- `content_hash VARCHAR`
- `num_links INTEGER DEFAULT 0`

Write pattern:

- Feature extractor upserts latest values for each successful fetch.

### `content_feature_history_{shard}`

Purpose:

- Append-only historical feature snapshots.

PK and metadata:

- `snapshot_id BIGINT PRIMARY KEY`
- `snapshot_at TIMESTAMPTZ DEFAULT now()`
- `url VARCHAR NOT NULL`
- plus shared content feature fields

Write pattern:

- Feature extractor inserts one history row per processed successful fetch.

## 4.5 Consistency and Transaction Boundaries

- Ingestor wraps current/history/event writes for one record in a single DB transaction.
- Offerer selection/update is transactionally atomic inside each selection phase.
- Stats aggregator applies one delta file per transaction.
- Router domain ensure/select for each record runs inside transactional block.
- Accounting rolloff uses short batched transactions with `FOR UPDATE SKIP LOCKED` to minimize lock contention.

## 4.6 Recommended Physical Indexes

To support current query patterns efficiently, maintain indexes such as:

- `domain_state(domain)` unique (already implied)
- `url_state_current_{shard}(should_crawl, last_scheduled, url_score, domain_score)`
- `url_state_current_{shard}(domain_id)`
- `domain_stats_daily(event_date)`
- `summary_daily(event_date)` primary key (already)
- `url_event_counter_{shard}(accounted, event_date)` for accounting rolloff scans

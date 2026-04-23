# 06. Maintenance Scripts

One-off and recurring maintenance scripts under `scripts/`.

## 6.1 `migrate_add_source.py`

- One-time migration.
- Adds `source SMALLINT NOT NULL DEFAULT 0` to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}` (512 ALTERs total).
- Idempotent via `IF NOT EXISTS`.
- PG 11+ treats this as metadata-only, no table rewrite.

```bash
uv run scripts/migrate_add_source.py [--dry-run]
```

## 6.2 `golden_inject.py`

- Recurring job (intended weekly).
- Force-injects golden set URLs older than 4 weeks from metricdb into crawlerdb.
- Shard resolution: goes through `libs.db.sharding.key.compute_shard` (single source of truth), which honors `domain_overrides` in `ingest.yaml` and `split_etld1` in `shard_split.yaml`; overrides for split eTLD+1s are stripped automatically.
- Writes to `domain_state`, `url_state_current_{shard}`, `url_state_history_{shard}`.
- Existing rows are flipped to `source = 1` so golden set membership is identifiable. New rows are also mirrored into history (matches `db_ops.process_link`).
- Does not write to metricdb.

```bash
uv run scripts/golden_inject.py [--dry-run]
```

## 6.3 `migrate_add_discovered_from.py`

- One-time migration.
- Adds `discovered_from VARCHAR` (nullable, no default) to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}` (512 ALTERs total).
- Idempotent via `IF NOT EXISTS`.
- PG 11+ treats this as metadata-only, no table rewrite.
- Phase 1 of NTU-CSIE5376/WebCrawler#6: ingestor `_bulk_links` writes the parent page URL on first discovery; `ON CONFLICT DO NOTHING` preserves the first writer.

```bash
uv run scripts/migrate_add_discovered_from.py [--dry-run]
```

## 6.4 `migrate_add_title.py`

- One-time migration.
- Adds `title VARCHAR` (nullable, no default) to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}` (512 ALTERs total).
- Idempotent via `IF NOT EXISTS`.
- PG 11+ treats this as metadata-only, no table rewrite.
- Spider captures `<title>` trimmed to 500 chars on successful HTML fetches; ingestor upserts with `COALESCE(EXCLUDED.title, ...)` so failed re-fetches keep the previous value.

```bash
uv run scripts/migrate_add_title.py [--dry-run]
```

## 6.5 `migrate_merge_subdomain_rows.py`

- One-time migration.
- Cleans up legacy `domain_state` rows in subdomain form (e.g. `en.wikipedia.org`) left by an older `golden_inject` that used `urlparse().hostname` instead of eTLD+1.
- For each dirty row, merges per-shard `url_state_current`, `url_event_counter`, `content_feature_current`, and `domain_stats_daily` into the canonical `(shard, domain_id)`. URL conflicts keep the canonical row and bump `source` to `GREATEST`. History tables are left untouched (append-only).
- Skips rows whose `domain` value is not a valid DNS hostname (anchor-text leakage).
- Default is `--dry-run`; pass `--execute` to mutate. `--domain-like` limits scope.

```bash
uv run scripts/migrate_merge_subdomain_rows.py --dry-run
uv run scripts/migrate_merge_subdomain_rows.py --execute
```

## 6.6 `migrate_shard_split.py`

- Recurring / on-demand.
- For each eTLD+1 listed in `containers/scheduler_ingest/config/shard_split.yaml`, moves `url_state_current_{old}`, `url_state_history_{old}`, and `url_event_counter_{old}` rows to new per-hostname shards (`md5(hostname) % 256`). `domain_state` is upserted per hostname with the new `shard_id`.
- `content_feature_*` and `domain_stats_daily` are not migrated (feature rows regenerate on next fetch; daily stats restart per new host row).
- Default is `--dry-run` (reports per-hostname row counts and projected new-shard distribution). Pass `--execute` to perform the move. Batches of 5000 per table, idempotent on conflict.
- Pre-req for `--execute`: pause `scheduler_ingest` (router + ingestor) for the affected eTLD+1, or live writes race the migration.

```bash
uv run scripts/migrate_shard_split.py             # dry-run
uv run scripts/migrate_shard_split.py --execute   # actually move rows
```

## 6.7 `constants.py`

Shared constants:

- `NUM_SHARDS = 256`
- `CRAWLERDB`, `METRICDB`: psycopg2 connection kwargs
- `SOURCE_NATURAL = 0`, `SOURCE_GOLDEN = 1`: values for `url_state_current.source`

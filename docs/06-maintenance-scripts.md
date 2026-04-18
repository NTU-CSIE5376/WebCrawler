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
- Source resolution: `domain_overrides` from `containers/scheduler_ingest/config/ingest.yaml`, fallback to `MD5(hostname) % 256`. Mirrors `ShardRouter.domain_to_shard`.
- Writes to `domain_state`, `url_state_current_{shard}`, `url_state_history_{shard}`.
- Existing rows are flipped to `source = 1` so golden set membership is identifiable. New rows are also mirrored into history (matches `db_ops.process_link`).
- Does not write to metricdb.

```bash
uv run scripts/golden_inject.py [--dry-run]
```

## 6.3 `migrate_merge_subdomain_rows.py`

- One-time migration.
- Cleans up legacy `domain_state` rows in subdomain form (e.g. `en.wikipedia.org`) left by an older `golden_inject` that used `urlparse().hostname` instead of eTLD+1.
- For each dirty row, merges per-shard `url_state_current`, `url_event_counter`, `content_feature_current`, and `domain_stats_daily` into the canonical `(shard, domain_id)`. URL conflicts keep the canonical row and bump `source` to `GREATEST`. History tables are left untouched (append-only).
- Skips rows whose `domain` value is not a valid DNS hostname (anchor-text leakage).
- Default is `--dry-run`; pass `--execute` to mutate. `--domain-like` limits scope.

```bash
uv run scripts/migrate_merge_subdomain_rows.py --dry-run
uv run scripts/migrate_merge_subdomain_rows.py --execute
```

## 6.4 `constants.py`

Shared constants:

- `NUM_SHARDS = 256`
- `CRAWLERDB`, `METRICDB`: psycopg2 connection kwargs
- `SOURCE_NATURAL = 0`, `SOURCE_GOLDEN = 1`: values for `url_state_current.source`

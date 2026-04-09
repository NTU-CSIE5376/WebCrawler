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

## 6.3 `constants.py`

Shared constants:

- `NUM_SHARDS = 256`
- `CRAWLERDB`, `METRICDB`: psycopg2 connection kwargs
- `SOURCE_NATURAL = 0`, `SOURCE_GOLDEN = 1`: values for `url_state_current.source`

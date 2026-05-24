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

## 6.5 `migrate_add_url_score_updated_at.py`

- One-time migration.
- Adds `url_score_updated_at TIMESTAMPTZ` (nullable, no default) to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}`.
- Existing rows stay NULL so the Golden Discovery Ranker v1 can refresh them into the existing `url_score` without adding score-version columns.
- Creates two partial Golden Discovery Ranker v1 indexes per current shard:
  - `idx_url_state_current_{shard}_golden_discovery_v1_unscored`
  - `ON url_state_current_{shard}(first_seen ASC NULLS LAST)`
  - `WHERE should_crawl = TRUE AND url_score_updated_at IS NULL`
  - `idx_url_state_current_{shard}_golden_discovery_v1_selection`
  - `ON url_state_current_{shard}(domain_id, score-refresh flag, url_score DESC, domain_score DESC, last_scheduled ASC, first_seen ASC)`
  - `WHERE should_crawl = TRUE`
- These indexes are not new data columns. They are PostgreSQL lookup structures for the background ranker's "find unscored crawlable URLs" query and the Golden Discovery offerer's per-domain selection query.
- The indexes intentionally do not include the full URL text in their keys, keeping write churn lower for high discovery volume.
- Indexes are created with `CREATE INDEX CONCURRENTLY` after the column transaction commits. This reduces write blocking, but index creation can still take time and consume IO.
- Runtime cost after creation: additional disk usage and a small write/update overhead on matching `url_state_current_*` rows.
- The migration is idempotent via `IF NOT EXISTS`. If `CREATE INDEX CONCURRENTLY` is interrupted, inspect for invalid indexes before rerunning, because PostgreSQL can leave an invalid same-name index behind.

```bash
uv run scripts/migrate_add_url_score_updated_at.py [--dry-run]
```

Local smoke test against a disposable schema in a PostgreSQL database:

```bash
GOLDEN_DISCOVERY_LOCAL_DB_SMOKE_DSN='postgresql://crawler:crawler@127.0.0.1:5432/crawlerdb' \
  python -m unittest tests.test_golden_discovery_local_db_smoke -v
```

Recommended rollout order:

1. Run `uv run scripts/migrate_add_url_score_updated_at.py --dry-run`.
2. Run the migration before deploying code that reads/writes `url_score_updated_at`.
3. Verify all `url_state_current_*` and `url_state_history_*` shards have the column.
4. Verify all `url_state_current_*` shards have the `*_golden_discovery_v1_unscored` index and that no invalid index remains.
5. Deploy the image/code with `GOLDEN_DISCOVERY_RANKER_V1_ENABLED=false` first.
6. Mount the ranker artifact, then enable `GOLDEN_DISCOVERY_RANKER_V1_ENABLED=true`.
7. After ranker progress is visible through increasing non-NULL `url_score_updated_at` rows, switch `OFFERER_STRATEGY=golden_discovery_ranker_v1`.

## 6.6 `migrate_add_url_metadata.py`

- One-time migration.
- Adds lightweight discovery and response metadata columns to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}` (6144 ALTERs total).
- Columns: `last_modified TIMESTAMPTZ`, `etag VARCHAR`, `cache_control VARCHAR`, `is_redirect BOOLEAN`, `redirect_hop_count SMALLINT`, `discovery_source_type SMALLINT NOT NULL DEFAULT 0`, `parent_page_score DOUBLE PRECISION`, `inlink_count_approx INTEGER NOT NULL DEFAULT 0`, `inlink_count_external INTEGER NOT NULL DEFAULT 0`, `anchor_text VARCHAR`, `robots_bits SMALLINT NOT NULL DEFAULT 0`, `hreflang_count INTEGER`.
- Idempotent via `IF NOT EXISTS`.
- PG 11+ treats these as metadata-only, no table rewrite.
- Spider records HTTP cache/redirect metadata; router tags outlink discoveries with `discovery_source_type=1`, source-page score, and external-link status; ingestor preserves previous response metadata when a refetch does not provide a value.
- `inlink_count_approx` and `inlink_count_external` are no-dedup observed outlink counters from the time this migration is deployed; repeated observations of the same edge increment again.
- `anchor_text` stores the first non-null outlink anchor observed for a URL; later observations only fill it if the current value is NULL.
- `robots_bits` uses `0=unknown`, `1=crawl allowed`, `2=robots.txt disallowed`; unknown crawl failures do not overwrite an existing value.
- `hreflang_count` stores the number of alternate hreflang links found on successful HTML fetches.

```bash
uv run scripts/migrate_add_url_metadata.py [--dry-run]
```

## 6.7 `migrate_add_has_json_ld.py`

- One-time migration.
- Adds `has_json_ld BOOLEAN` (nullable, no default) to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}` (512 ALTERs total).
- Idempotent via `IF NOT EXISTS`.
- PG 11+ treats this as metadata-only, no table rewrite.
- Spider sets `True` when a successful HTML response contains `<script type="application/ld+json">`, `False` otherwise; ingestor leaves the column NULL on failed fetches and uses `COALESCE(EXCLUDED, current)` so refetches preserve the previous value when a fetch fails.

```bash
uv run scripts/migrate_add_has_json_ld.py [--dry-run]
```

## 6.8 `migrate_merge_subdomain_rows.py`

- One-time migration.
- Cleans up legacy `domain_state` rows in subdomain form (e.g. `en.wikipedia.org`) left by an older `golden_inject` that used `urlparse().hostname` instead of eTLD+1.
- For each dirty row, merges per-shard `url_state_current`, `url_event_counter`, `content_feature_current`, and `domain_stats_daily` into the canonical `(shard, domain_id)`. URL conflicts keep the canonical row and bump `source` to `GREATEST`. History tables are left untouched (append-only).
- Skips rows whose `domain` value is not a valid DNS hostname (anchor-text leakage).
- Default is `--dry-run`; pass `--execute` to mutate. `--domain-like` limits scope.

```bash
uv run scripts/migrate_merge_subdomain_rows.py --dry-run
uv run scripts/migrate_merge_subdomain_rows.py --execute
```

## 6.9 `migrate_shard_split.py`

- Recurring / on-demand.
- For each eTLD+1 listed in `containers/scheduler_ingest/config/shard_split.yaml`, moves `url_state_current_{old}`, `url_state_history_{old}`, and `url_event_counter_{old}` rows to new per-hostname shards (`md5(hostname) % 256`). `domain_state` is upserted per hostname with the new `shard_id`.
- `content_feature_*` and `domain_stats_daily` are not migrated (feature rows regenerate on next fetch; daily stats restart per new host row).
- Default is `--dry-run` (reports per-hostname row counts and projected new-shard distribution). Pass `--execute` to perform the move. Batches of 5000 per table, idempotent on conflict.
- Pre-req for `--execute`: pause `scheduler_ingest` (router + ingestor) for the affected eTLD+1, or live writes race the migration.

```bash
uv run scripts/migrate_shard_split.py             # dry-run
uv run scripts/migrate_shard_split.py --execute   # actually move rows
```

## 6.10 `migrate_add_domain_pause.py`

- One-time migration.
- Adds `crawl_paused_until TIMESTAMPTZ` and `domain_fail_count INT NOT NULL DEFAULT 0` to `domain_state`.
- Idempotent via `IF NOT EXISTS`.
- PG 11+ treats this as metadata-only, no table rewrite.

```bash
uv run scripts/migrate_add_domain_pause.py [--dry-run]
```

## 6.11 `constants.py`

Shared constants:

- `NUM_SHARDS = 256`
- `CRAWLERDB`, `METRICDB`: psycopg2 connection kwargs
- `SOURCE_NATURAL = 0`, `SOURCE_GOLDEN = 1`: values for `url_state_current.source`

## 6.12 `update_golden_domain_scores.py`

- Recurring job (intended daily; new metric batches land roughly every two weeks so daily is comfortably more frequent than needed).
- Writes `domain_state.domain_score` from each domain's presence across `metric_batches`.
- Tiers (highest wins):
  - `1.0` (T0) — domain appears in every metric batch.
  - `0.95` (T1) — domain appears in the last 2 metric batches (consecutive by batch id), and is not T0.
  - `0.8` (T2) — domain appears in any metric batch, and is not T0 / T1.
  - `0.0` (T3) — default; domain never appeared in a golden batch.
- Host collapse: hosts in `metric_url` go through `libs.db.sharding.key.shard_key(host, split_subdomains)`, the same key used by the rest of the pipeline. Non-split subdomains roll up to eTLD+1; entries in `shard_split.yaml` stay as full host. This guarantees the tier is applied to the same `domain_state` row that URLs in that domain point to.
- Idempotent: previously tier-scored rows (`domain_score IN (1.0, 0.95, 0.8)`) are reset to `0.0` before re-application, so domains that drop out of a tier are demoted correctly.
- Whole run executes in a single transaction (`commit` on success, `rollback` on any exception).
- Reads metricdb, writes crawlerdb (`domain_state` only). Does not touch `url_state_current_*` or `url_state_history_*`.

```bash
uv run scripts/update_golden_domain_scores.py [--dry-run]
```

Recommended scheduling: host crontab, daily.

```cron
0 3 * * * cd /app && uv run scripts/update_golden_domain_scores.py >> /var/log/golden_tier.log 2>&1
```

The job is cheap (~5–10 s end-to-end on production scale: ~14k hosts collapsed to ~13k domain keys, plus an `UPDATE ... WHERE domain = ANY(...)` against the unique `domain_state_domain_key` index), so a daily cadence is conservative.

## 6.13 `migrate_add_discovery_frozen.py`

- One-time migration.
- Adds `discovery_frozen BOOLEAN NOT NULL DEFAULT FALSE` to `domain_state`.
- Idempotent via `IF NOT EXISTS`. PG 11+ treats this as metadata-only, no table rewrite.
- Frontier budget flag: maintained by the `frontier_gc` program (see 2.2), read by ingestor `_bulk_links` to stop inserting new links for frozen domains. Run before deploying code that reads the column.

```bash
uv run scripts/migrate_add_discovery_frozen.py [--dry-run]
```

## 6.14 `oneoff_drop_trap_frontier.py`

- One-off cleanup for the pre-existing frontier backlog (what `frontier_gc` does going forward, applied immediately to the current pile-up).
- Finds trap domains (estimated `pending > --cap` AND fetch yield `< --yield-floor`, via per-shard sampling so it stays off full table scans), then per target: sets `discovery_frozen=TRUE`, pushes `crawl_paused_until` far out, and deletes `should_crawl=TRUE` rows in batches by `domain_id`, committing per batch (resumable, no long lock).
- Fetched rows are kept. `url_state_history` is only touched with `--include-history`.
- Tuning defaults: `--cap 1000000`, `--yield-floor 0.01`, `--sample-pct 0.5`, `--batch 20000`. Production traps sit at <0.1% yield with tens of millions pending while healthy domains clear ~2%+, so the 1% floor separates them with margin.
- Deletes release space to the table free list only; the script prints the `pg_repack` / `VACUUM FULL` command per affected shard to return it to the OS. On a disk-constrained host, run frontier-only first to free space before `--include-history`.
- Pre-req: run `migrate_add_discovery_frozen.py` first (the `--execute` path writes `discovery_frozen`).

```bash
uv run scripts/oneoff_drop_trap_frontier.py                      # dry-run, list targets
uv run scripts/oneoff_drop_trap_frontier.py --execute            # freeze + drain frontier
uv run scripts/oneoff_drop_trap_frontier.py --execute --include-history
```

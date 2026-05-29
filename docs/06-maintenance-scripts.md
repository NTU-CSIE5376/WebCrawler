# 06. Maintenance Scripts

One-off and recurring maintenance scripts under `scripts/`.

## 6.1 `migrate_add_source.py`

- One-time migration.
- Adds `source SMALLINT NOT NULL DEFAULT 0` to all 256 shards of `url_state_current_{shard}` and `url_state_history_{shard}` (512 ALTERs total).
- Known values are `0` natural discovery, `1` golden set injection, and `2` weekly pageview injection.
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
- For each dirty row, merges per-shard `url_state_current`, `url_event_counter`, `content_feature_current`, and `domain_stats_daily` into the canonical `(shard, domain_id)`. URL conflicts keep the canonical row and preserve golden source membership over pageview source membership. History tables are left untouched (append-only).
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
- `SOURCE_NATURAL = 0`, `SOURCE_GOLDEN = 1`, `SOURCE_PAGEVIEW = 2`: values for `url_state_current.source`

The `DISCOVERY_SOURCE_*` constants for the "new outlink candidate" IPC
record live in `libs/ipc/new_link_record.py` (`DISCOVERY_SOURCE_UNKNOWN = 0`,
`DISCOVERY_SOURCE_PAGE_OUTLINK = 1`, `DISCOVERY_SOURCE_SITEMAP = 2`). Both
the router and the sitemap patroller import from there. Sitemap-specific
`SITEMAP_USER_AGENT` stays in `containers/sitemap_patroller/__init__.py`.

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

## 6.13 `migrate_add_domain_sitemap.py`

- One-time migration for the sitemap patroller (NTU-CSIE5376/WebCrawler#30).
- Creates `domain_sitemap` (non-sharded), plus two indexes: `idx_domain_sitemap_due` on `(last_patrolled_at NULLS FIRST)` for the patrol's due-row selection, and `idx_domain_sitemap_domain_id`.
- Schema:
  - `id BIGSERIAL PRIMARY KEY`
  - `domain_id BIGINT NOT NULL REFERENCES domain_state(domain_id)`
  - `sitemap_url TEXT NOT NULL UNIQUE`
  - `last_patrolled_at TIMESTAMPTZ`, `last_url_count INTEGER`, `last_new_count INTEGER`
  - `etag TEXT`, `last_modified TEXT` (verbatim from the previous response, used as `If-None-Match` / `If-Modified-Since` next time)
  - `status TEXT` (`ok`, `not_modified`, `parse_error`, `http_<code>`, `err_<exc>`, `timeout`)
  - `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
- Idempotent via `IF NOT EXISTS` on table and indexes.
- Non-sharded — total rows bounded by `golden_domains × few sitemaps each` (low thousands).

```bash
uv run scripts/migrate_add_domain_sitemap.py [--dry-run]
```

## 6.14 `sitemap_patroller` container

The recurring sitemap workload runs as a dedicated `sitemap_patroller`
service (see `docker-compose.yml` and `containers/sitemap_patroller/`),
not as host crons. The container runs supervisord with two worker
processes; both emit JSON via `libs.obslog`, so Loki labels (`service`,
`level`, `event`) work the same as the other containers.

### Workers

| Worker | Loop period (config: `loop_interval_sec`) | Role |
|--------|-------------------------------------------|------|
| `sitemap_discover` (`containers/sitemap_patroller/discover/main.py`) | `86400` (24 h) | Selects domains from `domain_state` where `domain_score >= score_min` (default `0.95` = T0+T1, per NTU-CSIE5376/WebCrawler#30). Fetches `robots.txt` for `Sitemap:` directives (case-insensitive, absolute URLs only); falls back to `https://{domain}/sitemap.xml`. Upserts each candidate into `domain_sitemap` with `ON CONFLICT (sitemap_url) DO NOTHING`, so reruns preserve patrol state. |
| `sitemap_patrol` (`containers/sitemap_patroller/patrol/main.py`) | `600` (10 min) | Selects rows where `last_patrolled_at IS NULL` or older than `due_interval_hours` (default `24`), capped at `batch_limit` (default `500`). Conditional GET (`If-None-Match` + `If-Modified-Since` from stored `etag` / `last_modified`). On `<urlset>`: routes each `<loc>` via `libs.db.sharding.key.compute_shard`, ensures `domain_state`, appends a "new outlink candidate" record to `/data/ipc/crawl_result/ingestor_{NN}/{YYYYMMDD}/{HHMM}/{HHMM}_sitemap_{pid}_{ts}.jsonl`. On `<sitemapindex>`: registers each child sitemap URL into `domain_sitemap` for the next pass (bounds bursts). On error or 304: updates `status` (`http_404`, `parse_error`, `timeout`, `not_modified`, ...) and bumps `last_patrolled_at`. |

IPC record schema matches `docs/03-data-flow-and-ipc.md` §3.3 "Router
Output Record (new outlink candidate)" plus `discovery_source_type = 2`
(sitemap) and `discovered_from = <sitemap URL>`. The existing
`scheduler_ingest/ingestor` consumes these files unchanged — discovered
URLs land in `url_state_current_*` through the same upsert path as
natural discovery, so the offerer, scorer, and crawler need no changes.

### Config

`containers/sitemap_patroller/config/sitemap.yaml`:

- `postgres.dsn` — psycopg2 DSN for crawlerdb.
- `ingest_config_path` — pointer to `scheduler_ingest/config/ingest.yaml`
  so the patrol worker uses the same `num_shards`, `shards_per_ingestor`,
  `domain_overrides`, `ingestor_dir_template`, and `interval_minutes` the
  router does. Routing must agree, otherwise emitted records land in the
  wrong ingestor's directory.
- `discover.{score_min, domain_limit, global_delay_sec, loop_interval_sec}`
- `patrol.{due_interval_hours, batch_limit, global_delay_sec, per_domain_cooldown_sec, loop_interval_sec}`

Politeness: default global delay `0.5 s` for discover (robots.txt fetches)
and `2.0 s` for patrol (sitemap fetches → ≤0.5 req/s). Per-run per-domain
cooldown `60 s` for patrol. Steady-state load at ~5k T0+T1 domains and
24 h cadence is well under 0.1 req/s globally.

### Dependency on existing jobs

`sitemap_discover` reads `domain_state.domain_score`, which is populated
by `update_golden_domain_scores.py` (§6.12). On a fresh deploy the
recommended order is:

1. Apply this migration and start the container.
2. Run `update_golden_domain_scores.py` so T0/T1 domains are scored.
3. The discover worker's first sweep then populates `domain_sitemap`,
   and the patrol worker picks rows up on its next pass.

### Build + run

```bash
docker compose build sitemap_patroller
docker compose up -d sitemap_patroller
docker exec sitemap_patroller supervisorctl status
```

### Ad-hoc single run (debugging)

```bash
docker exec sitemap_patroller \
  python -m containers.sitemap_patroller.discover.main \
    --config /app/containers/sitemap_patroller/config/sitemap.yaml --run-once

docker exec sitemap_patroller \
  python -m containers.sitemap_patroller.patrol.main \
    --config /app/containers/sitemap_patroller/config/sitemap.yaml --run-once
```

### Loki / Grafana

Both workers emit structured events that the Loki driver labels by
`service`, `level`, `event`. Useful queries:

- `{service="sitemap_patrol", event="patrol.done"}` — counters per run
  (`rows_due`, `ok`, `urls_emitted`, `nested_registered`, `elapsed_sec`).
- `{service="sitemap_discover", event="discover.done"}` — discovery
  sweep totals (`domain_count`, `new_sitemaps`, `elapsed_sec`).
- `{service="sitemap_discover", event="discover.robots_fetch_fail"}` —
  per-domain robots.txt failures (group by `domain`).
- `{service="sitemap_patrol", event=~"patrol.xml_parse_error|patrol.run_error"}` — parsing / loop errors.

## 6.15 `migrate_add_discovery_frozen.py`

- One-time migration.
- Adds `discovery_frozen BOOLEAN NOT NULL DEFAULT FALSE` to `domain_state`.
- Idempotent via `IF NOT EXISTS`. PG 11+ treats this as metadata-only, no table rewrite.
- Frontier budget flag: maintained by the `frontier_gc` program (see 2.2), read by ingestor `_bulk_links` to stop inserting new links for frozen domains. Run before deploying code that reads the column.

```bash
uv run scripts/migrate_add_discovery_frozen.py [--dry-run]
```

## 6.16 `oneoff_drop_trap_frontier.py`

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

## 6.17 `wiki_pageview_inject.py`

- Recurring job (intended weekly).
- Downloads the latest available Wikimedia `pageview_complete` automated dump, extracts the top pageview URLs, and injects them into crawlerdb with pageview-derived `url_score`.
- Stores dump and extracted artifacts under `/data/wiki_pageviews/` by default instead of the process working directory. `docker-compose.yml` mounts the same host path into `scheduler_ingest` for compose-run maintenance jobs.
- Downloads to `*.part`, resumes incomplete dumps with HTTP `Range` requests, validates the compressed bzip2 dump, then atomically renames it into place. Transient Wikimedia/network failures keep resumable partial files and retry with exponential backoff.
- Writes extracted top files through `*.part` and atomically renames them after validating the extracted rows.
- Applies retention cleanup to old `pageviews-*-automated.bz2`, `top*_pageviews_*.txt`, and stale `*.part` artifacts. Default retention is 45 days; use `--retention-days` or `--no-cleanup` to adjust.

```bash
uv run scripts/wiki_pageview_inject.py [--dry-run] [--skip-inject]
docker compose exec scheduler_ingest python scripts/wiki_pageview_inject.py --retention-days 30
```

# 08. Golden Parent Patrol

Adaptive patrol of parent pages that have produced golden child URLs, so
the crawler keeps refetching the most productive sources to expand golden
set discovery coverage.

This document captures the design decisions for the patrol service. PR-by-PR
implementation lands the schema first (this PR), followed by backfill
scripts, the cron service, and finally the deployment config.

## Motivation

Today's golden set discovery flow is:

- Crawler organically fetches pages and follows outlinks.
- After 4 weeks, `scripts/golden_inject.py` force-injects any golden URL the
  crawler has not yet discovered (see [06.2](06-maintenance-scripts.md)).

`golden_inject` is **eventual catch-up** — it backfills URLs that organic
crawl missed. The patrol's complementary role is **proactive discovery**:
identify parent pages with a track record of producing new golden URLs and
re-fetch them on a learned cadence so new golden URLs are discovered
*before* the 4-week inject window.

**KPI**: per-batch **discovery coverage** of golden URLs against the SERP-
defined ground truth (`metric_url.is_discovered` joined to `metric_queries
.batch_id`). Patrol is effective iff this number rises batch over batch for
domains in scope. We do NOT use "fewer URLs for `golden_inject` to backfill"
as the primary KPI — that indirection hides which lever moved coverage.

## Scope of this design

The patrol watchlist enrolls one source category for the MVP:

- `live_observed` — production crawler organically discovered the parent
  and one of its outlinks landed in the golden set
  (`url_state_current_*.discovered_from = parent` AND `source = 1`).

`source_type` is kept as a column for future-proofing (WAT-style historical
seeds, manual seeds, sitemap-derived parents) but the MVP only emits and
reads `'live_observed'`. Adding a new source later does not require a
schema migration — only loader code + an optional cadence-bias config.

Each cycle the patrol writes `should_crawl = TRUE` on the parent's
`url_state_current_*` row so the existing offerer / crawler / ingestor
pipeline re-fetches it. New outlinks flow through that pipeline normally
into the URL frontier; this design adds no new fetch path.

## Decisions

The headline trade-offs grilled in design:

### D1. Patrol writes priority directly into `url_score`

When a parent is due, the cron writes
`url_score = 1.0`, `url_score_updated_at = NOW()`, `should_crawl = TRUE`,
`source = 3` (`SOURCE_GOLDEN_PARENT_PATROL`). Value `3` because `2` is
already taken by `SOURCE_PAGEVIEW` (`scripts/wiki_pageview_inject.py`,
landed on main after this design was drafted).

No new column. Aligned with the production ranker's design ("the ranker
writes operational priority into url_score" — see
`golden_discovery_ranker_v1_strategy.py`). The v1 background scorer did not
overwrite patrol writes because it only scored rows where
`url_score_updated_at IS NULL`.

⚠️ **Stale-invariant warning (v2 scorer)**: after the v2 continuous
re-scorer landed (LATERAL query with `url_score_updated_at < NOW() -
rescore_ttl_days`), patrol writes are no longer durable — v2 will pick the
row back up after `rescore_ttl_days` (currently 5 d) and overwrite
`url_score` with its own [0,1] score. Mitigation options:
- (a) add `AND u.source <> 3` to the v2 steering query so v2 skips patrol-
  written rows; cleanest but couples v2 to patrol.
- (b) accept v2 will eventually overwrite; patrol re-asserts on next cron
  tick (≤ 6 h), so the score only stays "wrong" for up to one cadence.
Decision deferred to the cron-service PR review; current MVP behaviour is
(b).

**Implication**: `url_score` becomes shared between the ranker and patrol;
to attribute by writer, filter on `source = 3`.

### D2. Identity = normalized URL, fetch URL = raw URL

Two URL strings travel together for each parent:

- `parent_key` (PRIMARY KEY): output of `normalize_parent_url()` —
  forced https, lowercased host, stripped `www.`, stripped default port,
  stripped trailing slash, fragment dropped, tracking params dropped via
  whitelist (`utm_*`, `fbclid`, `gclid`, `mc_cid`, `mc_eid`, `ref`).
- `fetch_url` (NOT NULL): the raw URL string we have observed working in
  `url_state_current_*`. Used when writing `should_crawl = TRUE`.

Normalization is for dedup only. We never invent a fetch URL string we have
not seen: a normalize bug can degrade dedup precision but cannot produce a
URL that fails to fetch.

`consecutive_fetch_fail` is the failure backstop — if the chosen
`fetch_url` does start failing repeatedly, the row is retired.

### D3. Golden child count uses first-discovery edges only

`lifetime_golden_child_count` and per-cycle hit counting both query
`url_state_current_* WHERE discovered_from IN aliases AND source = 1`.

Outlink ingest is `ON CONFLICT (url) DO NOTHING`, so `discovered_from`
records the **first** parent to discover a URL. Parents that re-link to
already-known golden URLs receive no credit. This under-counts edges but
aligns with the goal: marginal coverage growth is what matters; redundant
re-discovery has zero coverage value.

**Known limitation**: golden URLs injected by `golden_inject.py` carry
`discovered_from = NULL` (the inject path skips that column). Edges where
both parent and golden URL exist but the parent never first-discovered any
of them are not creditable. Closing this gap requires enabling the
currently-unused `url_link` edge table; out of scope for the patrol MVP.

### D4. Cadence — short-loop signal drives re-pacing, long-loop drives retire

Four cadence buckets; intervals are config-driven defaults. The `fast`
bucket (1 h) was dropped in design review to avoid hammering anti-bot
WAFs on the productive parent pages — `medium` (6 h) is the fastest
cadence.

| bucket   | interval |
|----------|----------|
| `medium` | 6 h      |
| `slow`   | 1 d      |
| `trial`  | 2 d      |
| `cold`   | 7 d      |

Initial bucket is set per `lifetime_golden_child_count` in config; all
parents start in `trial` and earn faster cadence by producing.

**Short loop (every patrol cycle):**

- New URL count from this fetch ≥ promote threshold → bucket up one step
  (capped at `medium`).
- `consecutive_no_new_url ≥` demote threshold → bucket down one step.
- Otherwise unchanged.

If the crawler has not yet fetched the parent since the last patrol mark
(`last_observed_fetch_at <= last_patrol_at`), the cadence does not
transition this round; the cron just re-confirms `should_crawl = TRUE`
and pushes `next_patrol_at` out by a short grace period.

**Long loop (each new golden batch):**

- `cycle_hits = 0` → `consecutive_miss_batches += 1`; demote on first
  miss; retire on second miss.
- `cycle_hits ≥ 1` → reset miss counter, increment lifetime count.

### D5. New `source` enum value

`scripts/constants.py` adds:

```python
SOURCE_GOLDEN_PARENT_PATROL = 3
```

Parent rows are tagged `source = 3` whenever the patrol cron writes them
(force overwrite, matching `golden_inject`'s precedent). Children of
patrolled parents inherit `source = 0` via the natural ingest path;
provenance is reconstructed via `discovered_from`.

### D6. Cron-driven script, no new container

`scripts/run_golden_parent_patrol.py` (lands in PR 3) follows the
`golden_inject.py` pattern: short-lived process, idempotent run, scheduled
by cron at ~10 minute granularity. No supervisord change, no new
long-running process, no per-shard worker partitioning (the patrol table
is global, on the order of thousands of rows).

### D7. Enrollment threshold = 1 golden child

Any parent with at least one historical first-discovery golden child is
eligible. Weak parents are not filtered at enrollment — instead the
cadence policy assigns them to `trial` / `cold` so they consume little
budget and self-retire if they fail to produce.

10 M URL/day production crawl budget makes patrol budget effectively free
(thousands of parent fetches per day is < 1 % of capacity), so the
threshold's job is signal collection, not budget protection.

### D8. Patrol priority is a constant `1.0`

Patrol writes a constant `url_score = 1.0`. Per-row differentiation
(productive parents vs cold ones) is expressed entirely through the
cadence policy (how often to re-fetch) and the lifecycle policy (when to
retire), not through the `url_score` value (where in the offerer queue).
When future source types land, they will follow the same convention.

### D9. INSERT-or-UPDATE on enqueue

When the cron makes a parent due, the enqueue write is
`INSERT … ON CONFLICT (url) DO UPDATE` against `url_state_current_*`,
mirroring `golden_inject`'s pattern. For the MVP `live_observed` source
this is effectively always the UPDATE branch (the parent is already in
`url_state_current_*` because that is where we observed it). The INSERT
branch is kept as a forward-compat safety net for future source types
whose parents may not exist in `url_state_current_*` yet, and as a
defence against ad-hoc row pruning.

### D10. Delayed evaluation runs as soon as a new metric batch lands

Evaluation does not wait for `golden_inject.py`'s 4-week stamping window.
When a new batch appears in `metricdb.metric_batches`, the cron:

1. Reads the batch's golden URLs from metricdb.
2. Buckets them by shard (`compute_shard(domain)`).
3. Per shard, joins on `url = ANY(:batch_urls)` against
   `url_state_current_*` to recover `discovered_from` for any URL whose
   `first_seen` falls in the cycle window.
4. Aggregates hits per parent and updates patrol state.

This requires opening both metricdb and crawlerdb connections in app
code — same pattern as `golden_inject.py`. No FDW, no DBA setup.

Newly enrolled parents bootstrap with
`last_eval_batch_id = MAX(metric_batches.id)` so they are not retroactively
penalised for batches that closed before they joined the watchlist.

## Schema

Single global table `golden_parent_patrol_state` (created in this PR).
Columns:

- **Identity**: `parent_key` (PK), `fetch_url`, `aliases TEXT[]`,
  `parent_domain`, `shard_id`.
- **Source / lifecycle**: `source_type`, `first_enrolled_at`,
  `status`, `cadence_bucket`.
- **Cron timestamps**: `last_patrol_at`, `next_patrol_at`,
  `last_observed_fetch_at`.
- **Short-loop signals**: `lifetime_golden_child_count`,
  `last_seen_new_url_count`, `consecutive_no_new_url`.
- **Long-loop signals**: `last_eval_batch_id`,
  `consecutive_miss_batches`.
- **Backstop**: `consecutive_fetch_fail`.
- **Audit**: `updated_at`, `metadata_json` (JSONB; per-row provenance or
  free-form notes; reserved for future loaders).

ORM: `libs/db/models/patrol/state.py` (`GoldenParentPatrolState`).
Migration: `scripts/migrate_add_golden_parent_patrol.py`.

Two indexes:

- `idx_golden_parent_patrol_state_due` — partial on `(next_patrol_at)
  WHERE status <> 'retired'`. Hot path for the cron's due query.
- `idx_golden_parent_patrol_state_domain` — on `(parent_domain)`. Ad-hoc
  analysis only.

## PR plan

This feature lands in four PRs, each independently mergeable. Earlier
merges do not activate the feature; activation happens in the final PR
when the cron is enabled.

1. **PR 1 (this PR) — Schema and helpers.** Migration script, `source`
   enum, ORM model, `normalize_parent_url`, plus this design doc.
   No behaviour change.
2. **PR 2 — Backfill script.** `backfill_golden_parent_patrol.py` (live).
   Populates the watchlist from `url_state_current_*.discovered_from`;
   does not touch `url_state_current_*`. The WAT-based loader from the
   original design was dropped in review — see §Scope.
3. **PR 3 — Cron service.** `run_golden_parent_patrol.py` plus the
   `libs/patrol/cron_loop`, `cadence`, and `evaluation` modules. Includes
   unit tests for cadence transitions and evaluation logic. Disabled
   by default.
4. **PR 4 — Config + deployment.** `golden_parent_patrol.yaml`,
   operational runbook, cron entry. Enables the feature.

## Open items deferred

- `url_link` activation (PR D3 limitation): could enable parent → child
  edge attribution for known golden URLs but is a larger change and
  separate proposal.
- Sub-6h cadences: out of scope. The original `fast = 1 h` bucket was
  dropped in review (anti-bot WAFs on productive parents make sub-6h
  unsafe); `medium = 6 h` is the fastest the MVP will go.
- Cross-DB direct join via PostgreSQL FDW: skipped in favour of the
  app-code pattern that matches `golden_inject.py`.

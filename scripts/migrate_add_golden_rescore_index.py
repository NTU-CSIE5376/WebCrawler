"""
Migration: per-shard index for the Golden Discovery Ranker v2 re-scoring query.

The v2 background scorer no longer treats `url_score` as write-once. Because the
ranker's features (inlink_count_*, anchor_text) drift over time, the steering
query continuously re-picks rows:

    WHERE u.should_crawl = TRUE
      AND d.domain_score > 0                          -- JOIN domain_state
      AND u.first_seen < NOW() - make_interval(...)
      AND (u.url_score_updated_at IS NULL
           OR u.url_score_updated_at < NOW() - make_interval(...))
    ORDER BY d.domain_score DESC,
             u.url_score_updated_at ASC NULLS FIRST
    FOR UPDATE OF u SKIP LOCKED
    LIMIT N

The original v1 indexes do NOT serve this:
  * `..._unscored` is partial on `url_score_updated_at IS NULL` only — it covers
    the first pass but nothing once every row has been scored once (steady
    state, where every batch is a TTL refresh).
  * `..._selection` is the offerer's path (ordered by url_score), unrelated.

Without a supporting index, every steady-state batch degenerates into a
sequential scan of the (up to ~256M-row) shard, run by all scorer workers at
once.

The primary sort key `domain_score DESC` lives on the joined `domain_state`
table, so it cannot go into a single-table index. The plan we want is a nested
loop: outer = golden domains in domain_score DESC order (a small set), inner =
this shard's rows for that domain_id walked in `url_score_updated_at` order,
stopping at LIMIT. The composite `(domain_id, url_score_updated_at ASC NULLS
FIRST)` index is what lets that inner scan be index-ordered (matching the
ORDER BY's NULLS FIRST) and bounded. The `should_crawl = TRUE` partial predicate
keeps the index small (already-crawled rows are excluded).

Confirm with EXPLAIN (ANALYZE, BUFFERS) on a large golden shard that the planner
actually uses this index (nested loop, no full sort) before scaling up workers.

Indexes are created concurrently (outside a transaction block), idempotently.

Usage:
    uv run scripts/migrate_add_golden_rescore_index.py [--dry-run]
"""

import argparse
import logging

import psycopg2

try:
    from scripts.constants import CRAWLERDB, NUM_SHARDS
except ModuleNotFoundError:
    from constants import CRAWLERDB, NUM_SHARDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CURRENT_PREFIX = "url_state_current"


def golden_rescore_index_name(shard_id: int) -> str:
    return f"idx_url_state_current_{shard_id:03d}_golden_discovery_v2_rescore"


def create_golden_rescore_index_sql(shard_id: int) -> str:
    table = f"{CURRENT_PREFIX}_{shard_id:03d}"
    return (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        f"{golden_rescore_index_name(shard_id)} "
        f"ON {table} (domain_id, url_score_updated_at ASC NULLS FIRST) "
        "WHERE should_crawl = TRUE"
    )


def create_golden_rescore_indexes(conn, dry_run: bool) -> int:
    count = 0
    previous_autocommit = conn.autocommit

    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    if not dry_run:
        conn.autocommit = True

    try:
        with conn.cursor() as cur:
            for shard_id in range(NUM_SHARDS):
                sql = create_golden_rescore_index_sql(shard_id)
                count += 1
                if dry_run:
                    log.info("[DRY-RUN] %s", sql)
                else:
                    cur.execute(sql)
    finally:
        if not dry_run:
            conn.autocommit = previous_autocommit

    return count


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Create the Golden Discovery Ranker v2 re-scoring index on every "
            "url_state_current shard table"
        )
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)

    try:
        indexed = create_golden_rescore_indexes(conn, args.dry_run)
        if args.dry_run:
            log.info("[DRY-RUN] Would create %d re-scoring indexes", indexed)
        else:
            log.info("Done: created %d Golden Discovery Ranker v2 re-scoring indexes", indexed)
    except Exception:
        if not conn.autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

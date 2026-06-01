"""
Migration: create `golden_parent_patrol_state` table and supporting indexes.

Single global (non-sharded) table that stores the patrol watchlist: which
parent pages the cron should periodically re-enqueue, how often, and how they
are performing across golden batches.

Distinct from `url_state_current_*` (the URL frontier). The cron service
reads this table to find due parents and writes back to `url_state_current_*`
to set `should_crawl = TRUE` with `source = SOURCE_GOLDEN_PARENT_PATROL` (= 3).

Idempotent via `IF NOT EXISTS`. Indexes are created `CONCURRENTLY` outside
the table-creation transaction so a partial run can be rerun safely.

Usage:
    uv run scripts/migrate_add_golden_parent_patrol.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging

import psycopg2

try:
    from scripts.constants import CRAWLERDB
except ModuleNotFoundError:
    from constants import CRAWLERDB


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


TABLE_NAME = "golden_parent_patrol_state"


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    parent_key                   TEXT        PRIMARY KEY,
    fetch_url                    TEXT        NOT NULL,
    aliases                      TEXT[]      NOT NULL DEFAULT '{{}}'::text[],
    parent_domain                TEXT        NOT NULL,
    shard_id                     SMALLINT    NOT NULL,

    source_type                  TEXT        NOT NULL,
    first_enrolled_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    status                       TEXT        NOT NULL DEFAULT 'trial',
    cadence_bucket               TEXT        NOT NULL DEFAULT 'trial',

    last_patrol_at               TIMESTAMPTZ,
    next_patrol_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_observed_fetch_at       TIMESTAMPTZ,

    lifetime_golden_child_count  INTEGER     NOT NULL DEFAULT 0,
    last_seen_new_url_count      INTEGER     NOT NULL DEFAULT 0,
    consecutive_no_new_url       INTEGER     NOT NULL DEFAULT 0,

    last_eval_batch_id           INTEGER,
    consecutive_miss_batches     INTEGER     NOT NULL DEFAULT 0,

    consecutive_fetch_fail       INTEGER     NOT NULL DEFAULT 0,

    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata_json                JSONB
)
"""


def due_index_name() -> str:
    return f"idx_{TABLE_NAME}_due"


def domain_index_name() -> str:
    return f"idx_{TABLE_NAME}_domain"


def create_due_index_sql() -> str:
    """Partial index over (next_patrol_at) for non-retired rows.

    The cron's primary query is `WHERE next_patrol_at <= NOW() AND
    status != 'retired' ORDER BY next_patrol_at`. Partial index keeps the
    retired tail out of the index.
    """
    return (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        f"{due_index_name()} "
        f"ON {TABLE_NAME} (next_patrol_at) "
        "WHERE status <> 'retired'"
    )


def create_domain_index_sql() -> str:
    """Index over parent_domain for ad-hoc analysis grouping.

    Not on the cron hot path; supports dashboards / reports that aggregate
    by domain.
    """
    return (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        f"{domain_index_name()} "
        f"ON {TABLE_NAME} (parent_domain)"
    )


def create_table(conn, dry_run: bool) -> None:
    if dry_run:
        log.info("[DRY-RUN] %s", CREATE_TABLE_SQL.strip())
        return
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def create_indexes(conn, dry_run: bool) -> int:
    count = 0
    previous_autocommit = conn.autocommit

    if not dry_run:
        conn.autocommit = True

    try:
        with conn.cursor() as cur:
            for sql in (create_due_index_sql(), create_domain_index_sql()):
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
            "Create golden_parent_patrol_state table and supporting indexes."
        )
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)

    try:
        create_table(conn, args.dry_run)
        indexed = create_indexes(conn, args.dry_run)

        if args.dry_run:
            log.info(
                "[DRY-RUN] Would create table %s and %d indexes",
                TABLE_NAME,
                indexed,
            )
        else:
            log.info(
                "Done: created table %s and %d indexes",
                TABLE_NAME,
                indexed,
            )

    except Exception:
        if not conn.autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

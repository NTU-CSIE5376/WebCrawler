"""
Migration: add lightweight URL metadata columns to all
url_state_current_{shard} and url_state_history_{shard} tables.

  last_modified TIMESTAMPTZ
  etag VARCHAR
  cache_control VARCHAR
  is_redirect BOOLEAN
  redirect_hop_count SMALLINT
  discovery_source_type SMALLINT NOT NULL DEFAULT 0
  parent_page_score DOUBLE PRECISION
  inlink_count_approx INTEGER NOT NULL DEFAULT 0
  inlink_count_external INTEGER NOT NULL DEFAULT 0
  anchor_text VARCHAR
  robots_bits SMALLINT NOT NULL DEFAULT 0
  hreflang_count INTEGER

PG 11+ handles ADD COLUMN without a volatile DEFAULT as metadata-only,
so this does not rewrite existing table data.

Usage:
    uv run scripts/migrate_add_url_metadata.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import NUM_SHARDS, CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


COLUMNS = (
    ("last_modified", "TIMESTAMPTZ"),
    ("etag", "VARCHAR"),
    ("cache_control", "VARCHAR"),
    ("is_redirect", "BOOLEAN"),
    ("redirect_hop_count", "SMALLINT"),
    ("discovery_source_type", "SMALLINT NOT NULL DEFAULT 0"),
    ("parent_page_score", "DOUBLE PRECISION"),
    ("inlink_count_approx", "INTEGER NOT NULL DEFAULT 0"),
    ("inlink_count_external", "INTEGER NOT NULL DEFAULT 0"),
    ("anchor_text", "VARCHAR"),
    ("robots_bits", "SMALLINT NOT NULL DEFAULT 0"),
    ("hreflang_count", "INTEGER"),
)


def main():
    parser = argparse.ArgumentParser(
        description="Add lightweight URL metadata columns to url_state shard tables"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()

    prefixes = ("url_state_current", "url_state_history")

    try:
        statements = 0
        for prefix in prefixes:
            for i in range(NUM_SHARDS):
                table = f"{prefix}_{i:03d}"
                for name, definition in COLUMNS:
                    sql = (
                        f"ALTER TABLE {table} "
                        f"ADD COLUMN IF NOT EXISTS {name} {definition}"
                    )
                    statements += 1

                    if args.dry_run:
                        log.info("[DRY-RUN] %s", sql)
                    else:
                        cur.execute(sql)

        if not args.dry_run:
            conn.commit()
            log.info("Done: ran %d ALTER TABLE statements", statements)
        else:
            log.info("[DRY-RUN] Would run %d ALTER TABLE statements", statements)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

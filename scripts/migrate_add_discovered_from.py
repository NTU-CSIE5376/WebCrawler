"""
Migration: add `discovered_from` column to all url_state_current_{shard}
and url_state_history_{shard} tables.

  discovered_from VARCHAR
    NULL for golden-injected and seed URLs;
    set to the parent page URL on first discovery via outlink.
    First-parent-only: ON CONFLICT DO NOTHING preserves the first writer.

PG 11+ handles ADD COLUMN without a DEFAULT as metadata-only,
so this does not rewrite any table data.

Usage:
    uv run scripts/migrate_add_discovered_from.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import NUM_SHARDS, CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Add discovered_from column to url_state shard tables"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()

    prefixes = ("url_state_current", "url_state_history")

    try:
        for prefix in prefixes:
            for i in range(NUM_SHARDS):
                table = f"{prefix}_{i:03d}"
                sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS discovered_from VARCHAR"

                if args.dry_run:
                    log.info("[DRY-RUN] %s", sql)
                else:
                    cur.execute(sql)

        total = NUM_SHARDS * len(prefixes)
        if not args.dry_run:
            conn.commit()
            log.info("Done: ran ALTER TABLE on %d tables", total)
        else:
            log.info("[DRY-RUN] Would alter %d tables", total)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

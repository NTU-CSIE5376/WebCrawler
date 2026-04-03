"""
Migration: add `source` column to all url_state_current_{shard} tables.

  source SMALLINT NOT NULL DEFAULT 0
    0 = natural discovery
    1 = golden set injection

PG 11+ handles ADD COLUMN with a non-volatile DEFAULT as metadata-only,
so this does not rewrite any table data.

Usage:
    uv run scripts/migrate_add_source.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import NUM_SHARDS, CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Add source column to url_state_current tables")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()

    added = 0
    skipped = 0

    try:
        for i in range(NUM_SHARDS):
            table = f"url_state_current_{i:03d}"
            sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source SMALLINT NOT NULL DEFAULT 0"

            if args.dry_run:
                log.info("[DRY-RUN] %s", sql)
            else:
                cur.execute(sql)
                # Check if column was actually added (vs already existed)
                if cur.statusmessage == "ALTER TABLE":
                    added += 1
                else:
                    skipped += 1

        if not args.dry_run:
            conn.commit()
            log.info("Done: %d tables altered, %d already had column", added, skipped)
        else:
            log.info("[DRY-RUN] Would alter %d tables", NUM_SHARDS)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
Migration: add domain-level pause columns to `domain_state`.

  crawl_paused_until TIMESTAMPTZ   (NULL = not paused)
  domain_fail_count  INT NOT NULL DEFAULT 0

Used by the ingestor to back off on concentrated fail reasons
(robots.txt, HttpError 403/410/429, connection errors, etc.), and by the
offerer to skip paused domains during selection.

PG 11+ handles ADD COLUMN without a volatile default as metadata-only.

Usage:
    uv run scripts/migrate_add_domain_pause.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Add crawl_paused_until / domain_fail_count to domain_state"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    sql = (
        "ALTER TABLE domain_state "
        "ADD COLUMN IF NOT EXISTS crawl_paused_until TIMESTAMPTZ, "
        "ADD COLUMN IF NOT EXISTS domain_fail_count INT NOT NULL DEFAULT 0"
    )

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()
    try:
        if args.dry_run:
            log.info("[DRY-RUN] %s", sql)
        else:
            cur.execute(sql)
            conn.commit()
            log.info("Done: ALTER TABLE domain_state")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

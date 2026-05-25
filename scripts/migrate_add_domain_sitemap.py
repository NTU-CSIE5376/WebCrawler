"""
Migration: create `domain_sitemap` table for the sitemap patroller.

Tracks, per discovered sitemap URL, the patrol cadence state needed by
scripts/sitemap_patrol.py (last_patrolled_at, etag, last_modified, status).
Non-sharded: total row count is bounded by golden_domains x few sitemaps each
(low thousands).

Idempotent via IF NOT EXISTS.

Usage:
    uv run scripts/migrate_add_domain_sitemap.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS domain_sitemap (
  id                BIGSERIAL PRIMARY KEY,
  domain_id         BIGINT      NOT NULL REFERENCES domain_state(domain_id),
  sitemap_url       TEXT        NOT NULL UNIQUE,
  last_patrolled_at TIMESTAMPTZ,
  last_url_count    INTEGER,
  last_new_count    INTEGER,
  etag              TEXT,
  last_modified     TEXT,
  status            TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

# NULLS FIRST so never-patrolled rows are picked first by sitemap_patrol's
# selection query.
CREATE_DUE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_domain_sitemap_due
  ON domain_sitemap (last_patrolled_at NULLS FIRST)
"""

CREATE_DOMAIN_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_domain_sitemap_domain_id
  ON domain_sitemap (domain_id)
"""

STATEMENTS = (CREATE_TABLE_SQL, CREATE_DUE_INDEX_SQL, CREATE_DOMAIN_INDEX_SQL)


def main():
    parser = argparse.ArgumentParser(
        description="Create domain_sitemap table for the sitemap patroller"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()
    try:
        for sql in STATEMENTS:
            if args.dry_run:
                log.info("[DRY-RUN] %s", sql.strip())
            else:
                cur.execute(sql)
                log.info("Executed: %s", sql.strip().splitlines()[0])
        if not args.dry_run:
            conn.commit()
            log.info("Done: domain_sitemap table + indexes")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

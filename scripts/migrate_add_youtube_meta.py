"""
Migration: create the unsharded `youtube_video_meta` table.

Populated by the feature_extractor for any fetched YouTube video page
(watch / live / shorts) from its server-rendered ytInitialPlayerResponse
blob. Current-only: each row is upserted on video_id, latest fetch wins.

  video_id       VARCHAR PRIMARY KEY
  url            VARCHAR
  channel_id     VARCHAR
  channel_title  VARCHAR
  video_title    VARCHAR
  view_count     BIGINT
  length_seconds INTEGER
  keywords       TEXT[]
  fetched_at     TIMESTAMPTZ

Usage:
    uv run scripts/migrate_add_youtube_meta.py [--dry-run]
"""

import argparse
import logging

import psycopg2

from constants import CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS youtube_video_meta (
      video_id       VARCHAR PRIMARY KEY,
      url            VARCHAR,
      channel_id     VARCHAR,
      channel_title  VARCHAR,
      video_title    VARCHAR,
      view_count     BIGINT,
      length_seconds INTEGER,
      keywords       TEXT[],
      fetched_at     TIMESTAMPTZ
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_youtube_video_meta_channel_id "
    "ON youtube_video_meta (channel_id)",
)


def main():
    parser = argparse.ArgumentParser(description="Create youtube_video_meta table")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print SQL without executing"
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CRAWLERDB)
    cur = conn.cursor()

    try:
        for sql in STATEMENTS:
            if args.dry_run:
                log.info("[DRY-RUN] %s", " ".join(sql.split()))
            else:
                cur.execute(sql)

        if not args.dry_run:
            conn.commit()
            log.info("Done: ran %d statements", len(STATEMENTS))
        else:
            log.info("[DRY-RUN] Would run %d statements", len(STATEMENTS))

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

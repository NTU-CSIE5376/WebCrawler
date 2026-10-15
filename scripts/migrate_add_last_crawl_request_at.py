"""Add the explicit crawl-request marker before deploying the API/ingestor.

Run from the repository root:
    python -m scripts.migrate_add_last_crawl_request_at --dry-run
    python -m scripts.migrate_add_last_crawl_request_at
"""
import argparse

import psycopg2

try:
    from scripts.constants import CRAWLERDB, NUM_SHARDS
except ModuleNotFoundError:
    from constants import CRAWLERDB, NUM_SHARDS


def migrate(conn, num_shards=NUM_SHARDS):
    # One short transaction per shard avoids holding all 256 table locks.
    for shard in range(num_shards):
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(
                    f"ALTER TABLE url_state_current_{shard:03d} "
                    "ADD COLUMN IF NOT EXISTS last_crawl_request_at TIMESTAMPTZ"
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.dry_run:
        for shard in range(NUM_SHARDS):
            print(f"ALTER TABLE url_state_current_{shard:03d} "
                  "ADD COLUMN IF NOT EXISTS last_crawl_request_at TIMESTAMPTZ;")
        return
    with psycopg2.connect(**CRAWLERDB) as conn:
        migrate(conn)


if __name__ == "__main__":
    main()

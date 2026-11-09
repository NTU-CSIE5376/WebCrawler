#!/usr/bin/env python3
"""
Drop the legacy `idx_url_state_current_{shard}_sched` indexes on all 256
shards. They are superseded by `idx_*_golden_discovery_v1_selection`
(partial index WHERE should_crawl=true) and are no longer used by the
scheduler. Reclaims ~361 GB.

Uses DROP INDEX CONCURRENTLY so writes are not blocked. CONCURRENTLY
cannot run inside a transaction, so the connection is set to autocommit.

    scripts/migrate_drop_sched_index.py             # dry-run
    scripts/migrate_drop_sched_index.py --execute   # actually drop
"""
from __future__ import annotations

import argparse
import time

import psycopg2

from constants import CRAWLERDB, NUM_SHARDS

# YSQL does not support DROP INDEX CONCURRENTLY at all (unlike CREATE INDEX
# CONCURRENTLY, which it does support): "DROP INDEX CONCURRENTLY not
# supported yet" (psycopg2.errors.FeatureNotSupported). Fall back to a plain
# DROP INDEX there; CONCURRENTLY is still tried first so real PostgreSQL
# (production today) keeps the non-blocking drop.
_CONCURRENTLY_UNSUPPORTED = "drop index concurrently not supported"


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually drop indexes.")
    args = p.parse_args()

    mode = "DRY-RUN" if args.dry_run else "EXECUTE"
    print(f"mode: {mode}\n")

    conn = psycopg2.connect(**CRAWLERDB)
    conn.autocommit = True
    cur = conn.cursor()

    dropped = 0
    try:
        for shard in range(NUM_SHARDS):
            name = f"idx_url_state_current_{shard:03d}_sched"
            sql = f"DROP INDEX CONCURRENTLY IF EXISTS {name}"

            if args.dry_run:
                cur.execute(
                    "SELECT pg_size_pretty(pg_relation_size(%s::regclass))",
                    (name,),
                )
                size = cur.fetchone()
                size_str = size[0] if size else "missing"
                print(f"  [DRY-RUN] {name}  size={size_str}")
            else:
                t0 = time.time()
                try:
                    cur.execute(sql)
                except psycopg2.errors.FeatureNotSupported as e:
                    if _CONCURRENTLY_UNSUPPORTED not in str(e).lower():
                        raise
                    cur.execute(f"DROP INDEX IF EXISTS {name}")
                print(f"  dropped {name}  ({time.time() - t0:.1f}s)")
            dropped += 1
    finally:
        conn.close()

    print(f"\n{'would drop' if args.dry_run else 'dropped'}: {dropped} indexes")


if __name__ == "__main__":
    main()

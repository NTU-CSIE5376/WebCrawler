#!/usr/bin/env python3
"""
Set `fillfactor=80` on all 256 `url_state_current_{shard}` shards.

Current HOT update ratio is 24-34% (target 70%+). One cause is the
default fillfactor=100, which leaves no free space on the page for
in-place updates. Lower fillfactor lets PostgreSQL keep new row
versions on the same heap page, avoiding non-HOT updates that
re-index every index on the table.

Note: ALTER TABLE SET (fillfactor=N) is metadata-only and instant. It
only affects newly written pages. To realize the change on existing
rows, run `pg_repack` per shard afterward (not done here).

    scripts/migrate_set_fillfactor.py             # dry-run
    scripts/migrate_set_fillfactor.py --execute   # apply
"""
from __future__ import annotations

import argparse
import logging

import psycopg2

from constants import CRAWLERDB, NUM_SHARDS
from libs.obslog import configure as configure_logging


log = logging.getLogger(__name__)

FILLFACTOR = 80


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually apply.")
    args = p.parse_args()
    configure_logging(service="migrate_set_fillfactor")

    mode = "DRY-RUN" if args.dry_run else "EXECUTE"
    log.info(f'mode: {mode}  fillfactor: {FILLFACTOR}')

    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            for shard in range(NUM_SHARDS):
                table = f"url_state_current_{shard:03d}"
                sql = f"ALTER TABLE {table} SET (fillfactor={FILLFACTOR})"
                if args.dry_run:
                    log.info(f'  [DRY-RUN] {sql}')
                else:
                    cur.execute(sql)
                    log.info(f'  set fillfactor on {table}')
        if not args.dry_run:
            conn.commit()

    log.info(f"{('would update' if args.dry_run else 'updated')}: {NUM_SHARDS} tables")


if __name__ == "__main__":
    main()

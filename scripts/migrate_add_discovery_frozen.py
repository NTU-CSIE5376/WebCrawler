#!/usr/bin/env python3
"""
Migration: add `domain_state.discovery_frozen`.

Frontier budgeting needs a per-domain switch the ingest path can read cheaply
to decide whether to keep inserting newly discovered links. A boolean on
`domain_state` (one row per eTLD+1) is enough: the GC service maintains it
(see containers/scheduler_control/gc), the ingestor's `_bulk_links` reads it.

PG 11+ adds a column with a constant DEFAULT as metadata only, so this does
not rewrite the ~5.6M-row table. No index is added: `_bulk_links` looks the
flag up by `domain_id = ANY(...)`, which already uses the primary key.

Usage:
    scripts/migrate_add_discovery_frozen.py             # dry-run
    scripts/migrate_add_discovery_frozen.py --execute
"""
from __future__ import annotations

import argparse

import psycopg2

try:
    from scripts.constants import CRAWLERDB
except ModuleNotFoundError:
    from constants import CRAWLERDB

ADD_COLUMN_SQL = (
    "ALTER TABLE domain_state "
    "ADD COLUMN IF NOT EXISTS discovery_frozen BOOLEAN NOT NULL DEFAULT FALSE"
)


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually run the ALTER TABLE.")
    args = p.parse_args()

    print(f"mode: {'DRY-RUN' if args.dry_run else 'EXECUTE'}")
    print(ADD_COLUMN_SQL)
    if args.dry_run:
        return

    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            cur.execute(ADD_COLUMN_SQL)
        conn.commit()
    print("done")


if __name__ == "__main__":
    main()

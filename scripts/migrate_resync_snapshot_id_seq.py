#!/usr/bin/env python3
"""
Resync url_state_history_NNN.snapshot_id sequences with the table MAX(snapshot_id).

migrate_shard_split.py used INSERT ... SELECT to redistribute url_state_history
rows for Facebook locale subdomains across new per-hostname shards. The
copied rows kept their original snapshot_id values (up to ~80M), but
nextval() was never called, so the per-shard sequences stayed at the
natural ~1-2M range. Subsequent ingestor inserts then collide on the PK
until the sequence climbs past the imported max (which would take years
at current traffic).

This script calls setval(seq, MAX(snapshot_id)) on every shard whose
sequence is behind. content_feature_history was checked and is healthy.

Safe to run while ingestor is live: setval is non-transactional and
takes effect immediately, so concurrent inserts pick up the new value
on their next nextval().

    scripts/migrate_resync_snapshot_id_seq.py             # dry-run
    scripts/migrate_resync_snapshot_id_seq.py --execute   # actually setval
"""
from __future__ import annotations

import argparse
import sys

import psycopg2

from constants import CRAWLERDB, NUM_SHARDS

TABLES = ("url_state_history", "content_feature_history")


def scan(cur, prefix: str) -> list[tuple[int, int, int]]:
    """Return [(shard, seq_last, max_id)] for shards where seq_last < max_id."""
    bad = []
    for s in range(NUM_SHARDS):
        tbl = f"{prefix}_{s:03d}"
        seq = f"{tbl}_snapshot_id_seq"
        cur.execute(f"SELECT last_value FROM {seq}")
        last = cur.fetchone()[0]
        cur.execute(f"SELECT MAX(snapshot_id) FROM {tbl}")
        m = cur.fetchone()[0] or 0
        if m > last:
            bad.append((s, last, m))
    return bad


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true",
                    help="actually run setval (default is dry-run)")
    ap.add_argument("--table", choices=TABLES, default=None,
                    help="limit to one table family")
    args = ap.parse_args()

    targets = [args.table] if args.table else list(TABLES)

    with psycopg2.connect(**CRAWLERDB) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            for prefix in targets:
                print(f"\n=== {prefix} ===")
                bad = scan(cur, prefix)
                if not bad:
                    print(f"  all {NUM_SHARDS} shards healthy, nothing to do.")
                    continue
                print(f"  {len(bad)} shard(s) with seq_last < max(snapshot_id)")
                print(f"  {'shard':>5} {'seq_last':>14} {'max_id':>14} {'gap':>14}")
                for s, last, m in bad:
                    print(f"  {s:>5} {last:>14} {m:>14} {(m-last):>14}")

                if not args.execute:
                    continue

                for s, _, m in bad:
                    seq = f"{prefix}_{s:03d}_snapshot_id_seq"
                    cur.execute("SELECT setval(%s, %s)", (seq, m))
                    new_val = cur.fetchone()[0]
                    print(f"  shard {s:>3} setval -> {new_val}")
            if args.execute:
                conn.commit()
                print("\nCOMMITTED.")
            else:
                print("\nDRY-RUN, no changes. Re-run with --execute to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

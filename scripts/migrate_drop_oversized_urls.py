#!/usr/bin/env python3
"""
One-off cleanup for rows whose URL exceeds the 2500-byte cap introduced in
#16. Filters by octet_length(url), since the PostgreSQL btree index entry
size limit (~2700 bytes) is byte-based, not char-based. octet_length is a
conservative lower bound, the real index tuple also includes overhead and
extra columns (e.g. event_date in url_event_counter), but it is still a
safer filter than length().

Sweeps all 256 shards of url_state_current / url_state_history /
url_event_counter / content_feature_current / content_feature_history,
plus the unsharded url_link (src_url and dst_url).

Safe to run live: PR #16 already blocks new oversized URLs at all three
insertion paths, so no concurrent inserts can land that we'd be racing.

    scripts/migrate_drop_oversized_urls.py             # dry-run
    scripts/migrate_drop_oversized_urls.py --execute   # actually delete
"""
from __future__ import annotations

import argparse

import psycopg2

from constants import CRAWLERDB, NUM_SHARDS

CAP = 2500

SHARDED_TABLES = [
    "url_state_current",
    "url_state_history",
    "url_event_counter",
    "content_feature_current",
    "content_feature_history",
]


def sweep_sharded(cur, dry_run: bool) -> dict[str, int]:
    totals = {t: 0 for t in SHARDED_TABLES}
    for shard in range(NUM_SHARDS):
        for tbl in SHARDED_TABLES:
            name = f"{tbl}_{shard:03d}"
            if dry_run:
                cur.execute(f"SELECT COUNT(*) FROM {name} WHERE octet_length(url) > %s", (CAP,))
                n = cur.fetchone()[0]
            else:
                cur.execute(f"DELETE FROM {name} WHERE octet_length(url) > %s", (CAP,))
                n = cur.rowcount
            totals[tbl] += n
            if n:
                print(f"  shard {shard:03d}  {tbl:25s} {n:>9,}")
    return totals


def sweep_url_link(cur, dry_run: bool) -> int:
    if dry_run:
        cur.execute(
            "SELECT COUNT(*) FROM url_link WHERE octet_length(src_url) > %s OR octet_length(dst_url) > %s",
            (CAP, CAP),
        )
        n = cur.fetchone()[0]
    else:
        cur.execute(
            "DELETE FROM url_link WHERE octet_length(src_url) > %s OR octet_length(dst_url) > %s",
            (CAP, CAP),
        )
        n = cur.rowcount
    if n:
        print(f"  url_link {n:,}")
    return n


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually delete rows.")
    args = p.parse_args()

    mode = "DRY-RUN" if args.dry_run else "EXECUTE"
    print(f"mode: {mode}  cap: octet_length > {CAP}\n")

    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            sharded = sweep_sharded(cur, args.dry_run)
            link = sweep_url_link(cur, args.dry_run)
        if not args.dry_run:
            conn.commit()

    print("\ntotals:")
    for tbl, n in sharded.items():
        print(f"  {tbl:25s} {n:>12,}")
    print(f"  {'url_link':25s} {link:>12,}")


if __name__ == "__main__":
    main()

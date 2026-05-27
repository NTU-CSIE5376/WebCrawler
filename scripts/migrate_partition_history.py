"""One-off: convert a non-partitioned ``url_state_history_{shard}`` table into a
monthly RANGE-partitioned table, keeping only rows within the retention window.

Postgres cannot turn an existing plain table into a partitioned one in place, so
this builds a partitioned clone (``_part``), copies the retained tail into it
(rows older than the retention window are intentionally dropped), then swaps
names and drops the old monolith. The drop returns the old + aged-out space to
the OS immediately, with no row-by-row DELETE, no bloat, and no vacuum/repack.

Pipeline impact: none. The live table name is preserved, the per-table
``snapshot_id`` sequence is preserved, and history inserts are plain appends
(no ``ON CONFLICT`` on snapshot_id), so ingestor/extractor/rolloff are unchanged.
Only the daily prune changes (DROP partition instead of DELETE).

Phases per shard:
  1. (online, no lock) create ``_part`` + month partitions, bulk-copy retained
     rows up to a snapshot_id watermark captured at start.
  2. (ACCESS EXCLUSIVE, brief) copy the delta written since the watermark, detach
     the sequence so it survives the drop, swap names, commit.
  3. (no lock) DROP the old table.

Idempotent-ish: a leftover ``_part`` from a failed run is dropped and rebuilt.
Use --dry-run to print the plan without touching anything.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import psycopg2

from libs.db.sharding.history_partition import (
    create_parent_sql,
    partition_of_sql,
    months_covering,
)


def _table(prefix: str, shard: int) -> str:
    return f"{prefix}_{shard:03d}"


def migrate_shard(conn, prefix: str, shard: int, retention_days: int,
                  dry_run: bool) -> None:
    table = _table(prefix, shard)
    part = f"{table}_part"
    seq = f"{table}_snapshot_id_seq"
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cur = conn.cursor()

    cur.execute(f"SELECT coalesce(max(snapshot_id), 0) FROM {table}")
    watermark = cur.fetchone()[0]
    # Time range to provision partitions for: cutoff month .. newest row.
    cur.execute(f"SELECT max(snapshot_at) FROM {table}")
    newest = cur.fetchone()[0] or datetime.now(timezone.utc)
    months = months_covering(cutoff, newest)

    plan = [
        f"-- shard {shard}: cutoff={cutoff.isoformat()} watermark id={watermark}",
        f"DROP TABLE IF EXISTS {part}",
        *create_parent_sql(table),
        *[partition_of_sql(part, table, m) for m in months],
        f"INSERT INTO {part} SELECT * FROM {table} "
        f"WHERE snapshot_at >= '{cutoff.isoformat()}' AND snapshot_id <= {watermark}",
        "-- [lock] BEGIN; LOCK {t} ACCESS EXCLUSIVE".format(t=table),
        f"INSERT INTO {part} SELECT * FROM {table} "
        f"WHERE snapshot_at >= '{cutoff.isoformat()}' AND snapshot_id > {watermark}",
        f"ALTER SEQUENCE {seq} OWNED BY NONE",
        f"ALTER TABLE {table} RENAME TO {table}_old",
        f"ALTER TABLE {part} RENAME TO {table}",
        f"ALTER SEQUENCE {seq} OWNED BY {table}.snapshot_id",
        "-- COMMIT",
        f"DROP TABLE {table}_old",
    ]
    if dry_run:
        print("\n".join(plan))
        return

    # Phase 1: online build + bulk copy.
    cur.execute(f"DROP TABLE IF EXISTS {part}")
    for stmt in create_parent_sql(table):
        cur.execute(stmt)
    for m in months:
        cur.execute(partition_of_sql(part, table, m))
    cur.execute(
        f"INSERT INTO {part} SELECT * FROM {table} "
        f"WHERE snapshot_at >= %s AND snapshot_id <= %s", (cutoff, watermark)
    )
    conn.commit()

    # Phase 2: brief exclusive swap.
    cur.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    cur.execute(
        f"INSERT INTO {part} SELECT * FROM {table} "
        f"WHERE snapshot_at >= %s AND snapshot_id > %s", (cutoff, watermark)
    )
    cur.execute(f"ALTER SEQUENCE {seq} OWNED BY NONE")
    cur.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
    cur.execute(f"ALTER TABLE {part} RENAME TO {table}")
    cur.execute(f"ALTER SEQUENCE {seq} OWNED BY {table}.snapshot_id")
    conn.commit()

    # Phase 3: reclaim.
    cur.execute(f"DROP TABLE {table}_old")
    conn.commit()
    print(f"shard {shard}: migrated, kept rows since {cutoff.date()}, dropped {table}_old")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="172.16.191.1")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--user", default="crawler")
    ap.add_argument("--password", default="crawler")
    ap.add_argument("--dbname", default="crawlerdb")
    ap.add_argument("--prefix", default="url_state_history",
                    help="table family prefix, e.g. url_state_history")
    ap.add_argument("--shards", required=True,
                    help="comma-separated shard ids, e.g. 56,224")
    ap.add_argument("--retention-days", type=int, default=30)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                            password=args.password, dbname=args.dbname)
    try:
        for s in (int(x) for x in args.shards.split(",")):
            migrate_shard(conn, args.prefix, s, args.retention_days, args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""One-off: create missing monthly partitions on the ``*_history`` shard tables.

On 2026-11-01 no ``url_state_history_NNN_p202611`` partition existed, so every
ingest batch failed ("no partition of relation ... found for row"). Accounting
cannot recover on its own: its rolloff writes history rows and fails before the
daily prune gets to provision partitions. This script breaks the deadlock.

Create-only: unlike accounting's ``_prune_history`` it never drops aged-out
partitions. Existence is checked against the catalog instead of relying on
``CREATE TABLE IF NOT EXISTS``, which on YSQL still raises a duplicate composite
type error when the partition exists, so the script is safe to re-run.

Examples:
  python -m scripts.backfill_history_partitions                       # dry run
  python -m scripts.backfill_history_partitions --apply
  python -m scripts.backfill_history_partitions --apply \\
      --prefixes url_state_history content_feature_history --months 202611 202612
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import psycopg2

from libs.db.sharding.history_partition import (
    list_partitions_sql,
    partition_name,
    partition_of_sql,
)

NUM_SHARDS = 256


def _table(prefix: str, shard: int) -> str:
    return f"{prefix}_{shard:03d}"


def parse_month(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m").replace(tzinfo=timezone.utc)


def missing_partition_sql(table: str, existing: set[str], months: list[datetime]) -> list[str]:
    """DDL for each month partition of ``table`` not already in ``existing``."""
    return [
        partition_of_sql(table, table, m)
        for m in months
        if partition_name(table, m) not in existing
    ]


def _exec_with_retry(conn, sql: str, attempts: int = 3) -> None:
    """Run one DDL statement, retrying on YugabyteDB's "Restart read required"."""
    for attempt in range(attempts):
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            return
        except psycopg2.OperationalError as e:
            conn.rollback()
            if "restart read required" in str(e).lower() and attempt < attempts - 1:
                time.sleep(0.3 * (attempt + 1))
                continue
            raise


def backfill_table(conn, table: str, months: list[datetime], apply: bool) -> tuple[int, int]:
    """Returns (created_or_planned, errors) for one shard table."""
    with conn.cursor() as cur:
        cur.execute("SELECT relkind FROM pg_class WHERE relname = %s", (table,))
        row = cur.fetchone()
        if row is None or row[0] != "p":
            conn.rollback()
            print(f"-- skip {table}: not a partitioned table")
            return 0, 0
        cur.execute(list_partitions_sql(table))
        existing = {r[0] for r in cur.fetchall()}
    conn.rollback()  # end the catalog read before running DDL

    done = errors = 0
    for sql in missing_partition_sql(table, existing, months):
        if not apply:
            print(sql + ";")
            done += 1
            continue
        try:
            _exec_with_retry(conn, sql)
            done += 1
            print(f"ok   {sql.split()[5]}")
        except psycopg2.Error as e:
            errors += 1
            print(f"ERR  {table}: {e}".rstrip())
    return done, errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="172.16.191.3")
    ap.add_argument("--port", type=int, default=5433)
    ap.add_argument("--user", default="yugabyte")
    ap.add_argument("--password", default="yugabyte")
    ap.add_argument("--dbname", default="crawlerdb")
    ap.add_argument("--prefixes", nargs="+", default=["url_state_history"],
                    help="table family prefixes, e.g. url_state_history content_feature_history")
    ap.add_argument("--months", nargs="+", default=["202611", "202612"],
                    help="months to provision as YYYYMM")
    ap.add_argument("--shards", type=int, default=NUM_SHARDS)
    ap.add_argument("--apply", action="store_true",
                    help="execute the DDL (default: print it only)")
    args = ap.parse_args()

    months = sorted({parse_month(m) for m in args.months})
    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                            password=args.password, dbname=args.dbname)
    total = total_errors = 0
    try:
        for prefix in args.prefixes:
            for shard in range(args.shards):
                done, errors = backfill_table(conn, _table(prefix, shard), months, args.apply)
                total += done
                total_errors += errors
    finally:
        conn.close()

    verb = "created" if args.apply else "would create"
    print(f"-- {verb} {total} partitions, {total_errors} errors")
    if total_errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""Helpers for time-range partitioning of the append-only ``*_history`` tables.

Retention on these tables is purely time-based (drop snapshots older than N
days) and they have no pipeline read path, so RANGE partitioning on
``snapshot_at`` + ``DROP PARTITION`` is the right tool: O(1) metadata drop, no
dead tuples, space returned to the OS immediately. This module holds the
SQL-string builders shared by the one-off migration
(``scripts/migrate_partition_history.py``) and the daily prune in the accounting
service, so both agree on partition naming and bounds, plus
``ensure_partitions``, which the accounting pass and the ingestor use to create
missing month partitions.

Partitions are monthly: ``{table}_p{YYYYMM}`` covering ``[month, next month)``.
Monthly granularity keeps the partition count small (256 shards x a few months);
the cost is that a month is only droppable once *all* its days are older than the
retention window, so up to ~1 extra month is retained.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

# Partition child name suffix, e.g. ``..._p202605``.
_PART_RE = re.compile(r"_p(\d{6})$")

# PostgreSQL / YSQL error for an INSERT whose snapshot_at has no partition:
#   no partition of relation "url_state_history_123" found for row
#   DETAIL:  Partition key of the failing row contains (snapshot_at) = (2026-11-03 ...).
_MISSING_PART_RE = re.compile(
    r'no partition of relation "((?:url_state|content_feature)_history_\d{3})" found for row'
    r".*?\(snapshot_at\)\s*=\s*\((\d{4})-(\d{2})-",
    re.S,
)


def month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month(dt: datetime) -> datetime:
    return month_start(dt).replace(year=dt.year + (dt.month // 12),
                                   month=(dt.month % 12) + 1)


def partition_name(table: str, month: datetime) -> str:
    return f"{table}_p{month:%Y%m}"


def partition_month(child: str) -> datetime | None:
    """Inverse of partition_name: the UTC month a partition covers, or None if
    the name is not a ``_pYYYYMM`` partition (e.g. a DEFAULT partition)."""
    m = _PART_RE.search(child)
    return datetime.strptime(m.group(1), "%Y%m").replace(tzinfo=timezone.utc) if m else None


def create_parent_sql(table: str) -> list[str]:
    """DDL to build an empty partitioned clone of ``table`` (suffix ``_part``).

    Clones columns + defaults (incl. the ``nextval`` sequence default) via LIKE,
    then adds the composite PK required by partitioning (the partition key must
    be part of every unique constraint, so ``snapshot_id`` alone is not allowed).
    """
    part = f"{table}_part"
    return [
        f"CREATE TABLE {part} (LIKE {table} INCLUDING DEFAULTS INCLUDING GENERATED) "
        f"PARTITION BY RANGE (snapshot_at)",
        f"ALTER TABLE {part} ADD PRIMARY KEY (snapshot_id, snapshot_at)",
    ]


def partition_of_sql(parent: str, child_table: str, month: datetime) -> str:
    """``CREATE TABLE IF NOT EXISTS`` for one month partition of ``parent``.

    ``child_table`` names the partition (always the live table name so partition
    names stay stable across the migration's ``_part`` -> live rename).
    """
    name = partition_name(child_table, month)
    lo, hi = month_start(month), next_month(month)
    return (
        f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF {parent} "
        f"FOR VALUES FROM ('{lo.isoformat()}') TO ('{hi.isoformat()}')"
    )


def list_partitions_sql(parent: str) -> str:
    """Return the child partition names of ``parent``."""
    return f"""
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE p.relname = '{parent}'
        ORDER BY c.relname
    """


def months_covering(lo: datetime, hi: datetime) -> list[datetime]:
    """All month starts needed to cover the closed range [lo, hi]."""
    out, m = [], month_start(lo)
    last = month_start(hi)
    while m <= last:
        out.append(m)
        m = next_month(m)
    return out


def missing_partition_target(message: str) -> tuple[str, datetime] | None:
    """``(parent, month)`` named by a "no partition of relation" error on a
    ``*_history`` table, or None for any other error."""
    m = _MISSING_PART_RE.search(message)
    if not m:
        return None
    return m.group(1), datetime(int(m.group(2)), int(m.group(3)), 1, tzinfo=timezone.utc)


def _exec_ddl_with_retry(sess, sql: str, attempts: int = 3) -> None:
    """Run one DDL statement, retrying on YugabyteDB's distributed-read
    conflict error ("Restart read required"), which PostgreSQL has no
    equivalent of and which the driver does not retry on its own."""
    for attempt in range(attempts):
        try:
            sess.execute(text(sql))
            sess.commit()
            return
        except OperationalError as e:
            sess.rollback()
            if "restart read required" in str(e).lower() and attempt < attempts - 1:
                time.sleep(0.3 * (attempt + 1))
                continue
            raise


def ensure_partitions(Session, parents: Iterable[str], months: Iterable[datetime]) -> list[str]:
    """Create each missing ``{parent}_pYYYYMM`` partition for ``months``.

    Parents that are not partitioned yet (``scripts/migrate_partition_history.py``
    not run) are skipped. Existence is checked against the catalog rather than
    relying on CREATE TABLE IF NOT EXISTS: on YSQL that clause does not suppress
    the duplicate-composite-type error when the partition already exists. Each
    partition is created in its own transaction, and a failure does not stop the
    rest; the first error is re-raised once every partition has been tried.
    Returns the names of the partitions created.
    """
    parents = sorted(set(parents))
    months = sorted(set(months))
    if not parents or not months:
        return []

    with Session() as sess:
        partitioned = {
            name for (name,) in sess.execute(
                text("SELECT relname FROM pg_class WHERE relkind = 'p' AND relname = ANY(:names)"),
                {"names": parents},
            )
        }
        wanted = {
            partition_name(parent, month): (parent, month)
            for parent in parents if parent in partitioned
            for month in months
        }
        existing = {
            name for (name,) in sess.execute(
                text("SELECT relname FROM pg_class WHERE relname = ANY(:names)"),
                {"names": list(wanted)},
            )
        } if wanted else set()
        sess.rollback()  # end the catalog read before running DDL

        created, first_error = [], None
        for name in sorted(set(wanted) - existing):
            parent, month = wanted[name]
            try:
                _exec_ddl_with_retry(sess, partition_of_sql(parent, parent, month))
                created.append(name)
            except Exception as e:
                sess.rollback()
                first_error = first_error or e
        if first_error is not None:
            raise first_error
        return created

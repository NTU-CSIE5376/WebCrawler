"""Helpers for time-range partitioning of the append-only ``*_history`` tables.

Retention on these tables is purely time-based (drop snapshots older than N
days) and they have no pipeline read path, so RANGE partitioning on
``snapshot_at`` + ``DROP PARTITION`` is the right tool: O(1) metadata drop, no
dead tuples, space returned to the OS immediately. This module holds the pure
SQL-string builders shared by the one-off migration
(``scripts/migrate_partition_history.py``) and the daily prune in the accounting
service, so both agree on partition naming and bounds.

Partitions are monthly: ``{table}_p{YYYYMM}`` covering ``[month, next month)``.
Monthly granularity keeps the partition count small (256 shards x a few months);
the cost is that a month is only droppable once *all* its days are older than the
retention window, so up to ~1 extra month is retained.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

# Partition child name suffix, e.g. ``..._p202605``.
_PART_RE = re.compile(r"_p(\d{6})$")


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

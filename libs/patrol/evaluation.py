"""Long-loop evaluation: credit parents for first-discovering URLs that
ended up in a freshly-published golden batch.

Each batch published in metricdb defines a 14-day cycle window. For every
parent in `golden_parent_patrol_state` that was enrolled before
cycle_start, we count how many URLs from this batch they
first-discovered in `url_state_current_*` during the cycle window.

That count drives long_loop_transition() (libs.patrol.cadence) which
decides demote / retire. Hits accumulate into
`lifetime_golden_child_count`; misses accumulate into
`consecutive_miss_batches`.

Cross-DB pattern matches `scripts/golden_inject.py`: we open both
metricdb and crawlerdb connections and join in app code rather than
running a foreign-data-wrapper. No FDW means no DBA setup.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from libs.patrol.cadence import CadencePolicy, long_loop_transition


logger = logging.getLogger("golden_parent_patrol.evaluation")


# Default cycle window: each metric_batch's evaluation looks back 14 days
# from the batch's created_at. This is per the design doc D10; if the
# bi-weekly cadence ever changes, the value is exposed via PatrolConfig
# rather than hard-coded here.
DEFAULT_CYCLE_DAYS = 14


@dataclass(frozen=True)
class PendingBatch:
    batch_id: int
    created_at: datetime


def find_pending_batches(
    *,
    metric_conn,
    crawler_conn,
    cycle_days: int = DEFAULT_CYCLE_DAYS,
) -> list[PendingBatch]:
    """Returns batches that exist in metricdb past every patrol_state row's
    last_eval_batch_id and whose cycle window has fully closed.

    The "cycle window has closed" requirement means we only evaluate
    batches whose created_at is at least `cycle_days` after every newly
    enrolled parent's first_enrolled_at, otherwise the parent's hit count
    would be artificially low.

    The cursor is `MIN(last_eval_batch_id)` over non-retired rows that
    were enrolled before the current "cycle horizon". Rows enrolled after
    the most recent batch don't constrain the cursor (they're handled by
    the per-row first_enrolled_at filter inside apply_evaluation).
    """
    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MIN(last_eval_batch_id), 0)
            FROM golden_parent_patrol_state
            WHERE status <> 'retired'
            """
        )
        cursor = int(cur.fetchone()[0])

    with metric_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, created_at
            FROM metric_batches
            WHERE id > %s
            ORDER BY id
            """,
            (cursor,),
        )
        rows = cur.fetchall()

    return [PendingBatch(int(b_id), created_at) for b_id, created_at in rows]


def fetch_batch_urls(metric_conn, batch_id: int) -> list[str]:
    """Return all golden URLs published in `batch_id`.

    Mirrors `scripts/golden_inject.py`'s metric_url join structure so
    operators can debug the same query path.
    """
    with metric_conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.url
            FROM metric_url u
            JOIN metric_queries q ON u.query_id = q.id
            WHERE q.batch_id = %s
            """,
            (batch_id,),
        )
        return [r[0] for r in cur.fetchall()]


def bucket_urls_by_shard(
    urls: Iterable[str],
    *,
    extract_domain,
    domain_to_shard,
    overrides,
    split_subdomains,
) -> dict[int, list[str]]:
    """Partition `urls` by their target crawler shard.

    Pure function — sharding helpers are passed in as callables so this
    module does not depend on tldextract / sharding config IO.
    Live tests inject simple domain-based mappers and assert the bucket
    contents.
    """
    by_shard: dict[int, list[str]] = defaultdict(list)
    for url in urls:
        if not url:
            continue
        domain = extract_domain(url)
        if not domain:
            continue
        shard = domain_to_shard(domain, overrides, split_subdomains)
        by_shard[shard].append(url)
    return dict(by_shard)


def fetch_first_parent_hits(
    crawler_conn,
    *,
    shard_id: int,
    urls: list[str],
    cycle_start: datetime,
    cycle_end: datetime,
) -> list[tuple[str, int]]:
    """Per-shard query: which raw parent URLs (`discovered_from`)
    first-discovered any of `urls` during the cycle window?

    Returns (raw_parent_url, count_of_matched_children).
    """
    if not urls:
        return []
    table = f"url_state_current_{shard_id:03d}"
    with crawler_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT discovered_from, COUNT(*) AS hits
            FROM {table}
            WHERE url = ANY(%s)
              AND discovered_from IS NOT NULL
              AND first_seen BETWEEN %s AND %s
            GROUP BY discovered_from
            """,
            (urls, cycle_start, cycle_end),
        )
        return [(row[0], int(row[1])) for row in cur.fetchall()]


def fetch_alias_to_parent_key(crawler_conn) -> dict[str, str]:
    """Return {raw_alias_url: parent_key} for every active patrol row.

    The shard query returns hits keyed by the *raw* parent URL (whatever
    appeared in `discovered_from`); we need to map back to the
    normalized parent_key in patrol_state. Loading the alias map once per
    evaluation avoids a JOIN-per-shard.
    """
    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            SELECT parent_key, aliases
            FROM golden_parent_patrol_state
            """
        )
        out: dict[str, str] = {}
        for parent_key, aliases in cur.fetchall():
            for alias in aliases or ():
                out[alias] = parent_key
    return out


def aggregate_hits_by_parent_key(
    raw_hits: Iterable[tuple[str, int]],
    alias_to_parent_key: dict[str, str],
) -> dict[str, int]:
    """Pure: collapse per-raw-URL hit counts into per-parent_key counts.

    Raw URLs that don't match any patrol alias are dropped (we can't
    credit a parent we never enrolled).
    """
    out: dict[str, int] = defaultdict(int)
    for raw, hits in raw_hits:
        parent_key = alias_to_parent_key.get(raw)
        if parent_key is None:
            continue
        out[parent_key] += hits
    return dict(out)


def apply_evaluation(
    crawler_conn,
    *,
    batch: PendingBatch,
    hits_by_parent_key: dict[str, int],
    policy: CadencePolicy,
    cycle_days: int = DEFAULT_CYCLE_DAYS,
) -> dict[str, int]:
    """Apply long_loop_transition to every relevant patrol_state row.

    Two row sets are updated:

      1. parents in hits_by_parent_key — credited for cycle_hits > 0.
      2. parents not in hits_by_parent_key but enrolled before cycle_start
         and last_eval_batch_id < batch_id — credited with cycle_hits = 0
         (a "miss"); their counters bump and they may demote / retire.

    Returns counts: {"hits": X, "misses": Y, "skipped": Z}.
    """
    cycle_start = batch.created_at - timedelta(days=cycle_days)
    counts = {"hits": 0, "misses": 0, "skipped": 0}

    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            SELECT parent_key, status, cadence_bucket,
                   consecutive_miss_batches, first_enrolled_at,
                   COALESCE(last_eval_batch_id, 0)
            FROM golden_parent_patrol_state
            WHERE status <> 'retired'
            """
        )
        rows = cur.fetchall()

    with crawler_conn.cursor() as cur:
        for (
            parent_key,
            status,
            cadence_bucket,
            consecutive_miss_batches,
            first_enrolled_at,
            last_eval_batch_id,
        ) in rows:
            if last_eval_batch_id >= batch.batch_id:
                counts["skipped"] += 1
                continue
            if first_enrolled_at >= cycle_start:
                # enrolled mid-cycle: we can't fairly assess them on this
                # batch (most of the cycle window is before they joined).
                # advance the cursor without adjusting counters.
                cur.execute(
                    """
                    UPDATE golden_parent_patrol_state
                    SET last_eval_batch_id = %s,
                        updated_at = NOW()
                    WHERE parent_key = %s
                    """,
                    (batch.batch_id, parent_key),
                )
                counts["skipped"] += 1
                continue

            cycle_hits = hits_by_parent_key.get(parent_key, 0)
            outcome = long_loop_transition(
                current_bucket=cadence_bucket,
                current_status=status,
                consecutive_miss_batches=consecutive_miss_batches,
                cycle_hits=cycle_hits,
                policy=policy,
            )

            cur.execute(
                """
                UPDATE golden_parent_patrol_state
                SET cadence_bucket = %s,
                    status = %s,
                    consecutive_miss_batches = %s,
                    lifetime_golden_child_count = lifetime_golden_child_count + %s,
                    last_eval_batch_id = %s,
                    updated_at = NOW()
                WHERE parent_key = %s
                """,
                (
                    outcome.new_bucket,
                    outcome.new_status,
                    outcome.new_consecutive_miss_batches,
                    cycle_hits,
                    batch.batch_id,
                    parent_key,
                ),
            )

            if cycle_hits > 0:
                counts["hits"] += 1
            else:
                counts["misses"] += 1

    crawler_conn.commit()
    return counts

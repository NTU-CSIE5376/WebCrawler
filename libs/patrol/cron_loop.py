"""Cron-driven short loop for golden parent patrol.

Each invocation:
  1. Runs the long-loop evaluation for any new metric_batches.
  2. Picks `cron_batch_size` due parents (next_patrol_at <= NOW()).
  3. For each, syncs `last_observed_fetch_at` from url_state_current,
     counts new URLs first-discovered since `last_patrol_at`, decides
     the next cadence bucket, writes `should_crawl=TRUE` back into
     url_state_current, and updates patrol_state.

The runner script (scripts/run_golden_parent_patrol.py) calls run_once()
and exits; cron schedules the next invocation. No long-running process,
no in-memory state.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from libs.patrol.cadence import (
    CadencePolicy,
    interval_seconds,
    short_loop_transition,
)
from libs.patrol.config import PatrolConfig
from libs.patrol.evaluation import (
    PendingBatch,
    aggregate_hits_by_parent_key,
    apply_evaluation,
    bucket_urls_by_shard,
    fetch_alias_to_parent_key,
    fetch_batch_urls,
    fetch_first_parent_hits,
    find_pending_batches,
)


logger = logging.getLogger("golden_parent_patrol.cron")


# Source enum for url_state_current.source (= 3 for patrol). Hard-coded
# here to avoid an extra config indirection. MUST stay in sync with
# scripts/constants.SOURCE_GOLDEN_PARENT_PATROL — value is 3 (not 2),
# because 2 is already used by SOURCE_PAGEVIEW (wiki_pageview_inject).
SOURCE_GOLDEN_PARENT_PATROL = 3


@dataclass
class DueParent:
    parent_key: str
    fetch_url: str
    aliases: list[str]
    parent_domain: str
    shard_id: int
    cadence_bucket: str
    last_patrol_at: datetime | None
    consecutive_no_new_url: int


def find_due_parents(crawler_conn, *, batch_size: int) -> list[DueParent]:
    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            SELECT parent_key, fetch_url, aliases, parent_domain, shard_id,
                   cadence_bucket, last_patrol_at, consecutive_no_new_url
            FROM golden_parent_patrol_state
            WHERE next_patrol_at <= NOW()
              AND status <> 'retired'
            ORDER BY next_patrol_at
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (batch_size,),
        )
        return [
            DueParent(
                parent_key=row[0],
                fetch_url=row[1],
                aliases=list(row[2] or []),
                parent_domain=row[3],
                shard_id=int(row[4]),
                cadence_bucket=row[5],
                last_patrol_at=row[6],
                consecutive_no_new_url=int(row[7]),
            )
            for row in cur.fetchall()
        ]


def fetch_last_observed_fetch(
    crawler_conn, *, shard_id: int, fetch_url: str
) -> datetime | None:
    """Read url_state_current_{shard}.last_fetch_ok for this parent.

    Returns None if the row does not exist (e.g. a newly enrolled parent
    the crawler has not yet been told to fetch).
    """
    table = f"url_state_current_{shard_id:03d}"
    with crawler_conn.cursor() as cur:
        cur.execute(
            f"SELECT last_fetch_ok FROM {table} WHERE url = %s",
            (fetch_url,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]


def count_new_urls_since(
    crawler_conn,
    *,
    aliases: list[str],
    since: datetime,
    num_shards: int = 256,
) -> int:
    """How many URLs across all `url_state_current_*` shards have a
    `discovered_from` in `aliases` and `first_seen > since`?

    The "since" boundary is the parent's last_patrol_at — anything newer
    is what this patrol cycle observed.

    A patrol parent's children live on shards keyed by the *child* URL's
    domain, not by the parent's domain. So this query has to scan every
    shard, not just the parent's own. We do it in a single UNION ALL so
    Postgres can parallelise across shard partitions.
    """
    if not aliases:
        return 0
    union_parts = [
        f"SELECT discovered_from, first_seen FROM url_state_current_{i:03d} "
        f"WHERE discovered_from = ANY(%s) AND first_seen > %s"
        for i in range(num_shards)
    ]
    sql = f"SELECT COUNT(*) FROM ( {' UNION ALL '.join(union_parts)} ) AS u"
    params: list = []
    for _ in range(num_shards):
        params.extend([aliases, since])
    with crawler_conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


def ensure_domain(crawler_conn, *, domain: str, shard_id: int) -> tuple[int, float]:
    """Fetch (domain_id, domain_score), inserting if missing.

    Inlined from `scripts/golden_inject.ensure_domain` to avoid the
    scripts/-as-package import dance. Refactor candidate: extract this and
    `extract_domain` into `libs/db/domain_helpers.py` and update
    golden_inject too.
    """
    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO domain_state (domain, shard_id)
            VALUES (%s, %s)
            ON CONFLICT (domain) DO NOTHING
            """,
            (domain, shard_id),
        )
        cur.execute(
            "SELECT domain_id, COALESCE(domain_score, 0.0) FROM domain_state WHERE domain = %s",
            (domain,),
        )
        row = cur.fetchone()
        return int(row[0]), float(row[1])


# Order-aware INSERT-or-UPDATE: matches the precedent in
# scripts/golden_inject.py (force-overwrite source, idempotent enqueue).
# patrol writes url_score = 1.0 and url_score_updated_at = NOW() so the
# background scorer (which only scores rows with NULL updated_at) will
# never re-score a patrol-marked row.
ENQUEUE_SQL_TEMPLATE = """
INSERT INTO url_state_current_{shard:03d} (
    url, domain_id, domain_score,
    source, url_score, url_score_updated_at, should_crawl
) VALUES (
    %s, %s, %s,
    %s, %s, NOW(), TRUE
)
ON CONFLICT (url) DO UPDATE SET
    source = EXCLUDED.source,
    url_score = EXCLUDED.url_score,
    url_score_updated_at = NOW(),
    should_crawl = TRUE
"""


def enqueue_parent_for_crawl(
    crawler_conn,
    *,
    parent: DueParent,
    patrol_priority: float,
) -> None:
    """Set should_crawl=TRUE on the patrol parent's url_state_current row.

    INSERT-or-UPDATE. UPDATE path covers live_observed parents the crawler
    has already seen; INSERT path is kept as a safety net for parents whose
    url_state_current row was pruned, or for future source_types whose
    parents the crawler has not yet seen.
    """
    domain_id, domain_score = ensure_domain(
        crawler_conn,
        domain=parent.parent_domain,
        shard_id=parent.shard_id,
    )
    sql = ENQUEUE_SQL_TEMPLATE.format(shard=parent.shard_id)
    with crawler_conn.cursor() as cur:
        cur.execute(
            sql,
            (
                parent.fetch_url,
                domain_id,
                domain_score,
                SOURCE_GOLDEN_PARENT_PATROL,
                float(patrol_priority),
            ),
        )


def write_short_loop_outcome(
    crawler_conn,
    *,
    parent_key: str,
    new_bucket: str,
    next_patrol_at: datetime,
    last_patrol_at: datetime,
    last_observed_fetch_at: datetime | None,
    last_seen_new_url_count: int,
    consecutive_no_new_url: int,
) -> None:
    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE golden_parent_patrol_state SET
                cadence_bucket = %s,
                next_patrol_at = %s,
                last_patrol_at = %s,
                last_observed_fetch_at = %s,
                last_seen_new_url_count = %s,
                consecutive_no_new_url = %s,
                updated_at = NOW()
            WHERE parent_key = %s
            """,
            (
                new_bucket,
                next_patrol_at,
                last_patrol_at,
                last_observed_fetch_at,
                last_seen_new_url_count,
                consecutive_no_new_url,
                parent_key,
            ),
        )


def write_grace_period_only(
    crawler_conn,
    *,
    parent_key: str,
    next_patrol_at: datetime,
) -> None:
    """Push next_patrol_at out by the grace period without transitioning
    cadence (used when the crawler hasn't fetched the parent since our
    last patrol mark)."""
    with crawler_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE golden_parent_patrol_state SET
                next_patrol_at = %s,
                updated_at = NOW()
            WHERE parent_key = %s
            """,
            (next_patrol_at, parent_key),
        )


def process_due_parent(
    crawler_conn,
    *,
    parent: DueParent,
    config: PatrolConfig,
) -> str:
    """Returns 'transitioned' / 'grace' / 'fresh' for logging and tests.

    'fresh': parent has never been patrolled before (last_patrol_at is
        NULL); just enqueue and set last_patrol_at.
    'grace': crawler hasn't fetched since the last patrol mark; do not
        transition cadence, just push next_patrol_at out by the grace
        period.
    'transitioned': crawler fetched since the last mark; count new URLs,
        run short-loop transition, write back.
    """
    last_observed_fetch_at = fetch_last_observed_fetch(
        crawler_conn,
        shard_id=parent.shard_id,
        fetch_url=parent.fetch_url,
    )

    now = datetime.now(timezone.utc)
    bucket_interval = interval_seconds(parent.cadence_bucket, config.cadence_policy)

    # Always re-confirm should_crawl=TRUE on the row (idempotent).
    enqueue_parent_for_crawl(
        crawler_conn,
        parent=parent,
        patrol_priority=config.patrol_priority,
    )

    if parent.last_patrol_at is None:
        # First patrol for this parent.
        write_short_loop_outcome(
            crawler_conn,
            parent_key=parent.parent_key,
            new_bucket=parent.cadence_bucket,
            next_patrol_at=now + timedelta(seconds=bucket_interval),
            last_patrol_at=now,
            last_observed_fetch_at=last_observed_fetch_at,
            last_seen_new_url_count=0,
            consecutive_no_new_url=parent.consecutive_no_new_url,
        )
        return "fresh"

    # If the crawler hasn't actually fetched since we last asked it to,
    # we don't transition cadence. (Avoids penalising parents whose
    # politeness window is still active.)
    if (
        last_observed_fetch_at is None
        or last_observed_fetch_at <= parent.last_patrol_at
    ):
        write_grace_period_only(
            crawler_conn,
            parent_key=parent.parent_key,
            next_patrol_at=now + timedelta(seconds=config.grace_period_seconds),
        )
        return "grace"

    new_url_count = count_new_urls_since(
        crawler_conn,
        aliases=parent.aliases,
        since=parent.last_patrol_at,
    )
    outcome = short_loop_transition(
        current_bucket=parent.cadence_bucket,
        consecutive_no_new_url=parent.consecutive_no_new_url,
        new_url_count=new_url_count,
        policy=config.cadence_policy,
    )

    next_interval = interval_seconds(outcome.new_bucket, config.cadence_policy)
    # Fetch-anchored cadence: the cadence interval is the time we want to
    # give the parent to accumulate new outlinks BETWEEN FETCHES, not
    # between cron marks. If the crawler fetched late (e.g. we marked at
    # T=0, it actually fetched at T=2h on a 6h cadence), the next patrol
    # should be at T=8h (fetch + interval), not at T=6h+6h=T=12h that a
    # `now + interval` formula would give.
    # `max(..., now)` keeps the schedule from sliding into the past when
    # the cron itself ran late.
    next_patrol_at = max(
        last_observed_fetch_at + timedelta(seconds=next_interval),
        now,
    )
    write_short_loop_outcome(
        crawler_conn,
        parent_key=parent.parent_key,
        new_bucket=outcome.new_bucket,
        next_patrol_at=next_patrol_at,
        last_patrol_at=now,
        last_observed_fetch_at=last_observed_fetch_at,
        last_seen_new_url_count=new_url_count,
        consecutive_no_new_url=outcome.new_consecutive_no_new_url,
    )
    return "transitioned"


def run_long_loop_evaluation(
    *,
    metric_conn,
    crawler_conn,
    config: PatrolConfig,
    extract_domain,
    domain_to_shard,
    overrides,
    split_subdomains,
) -> dict[str, int]:
    """Process every batch in metric_batches that hasn't been evaluated
    yet (cursor = MIN(last_eval_batch_id) over non-retired patrol_state).

    Returns aggregated counts across batches.
    """
    pending = find_pending_batches(
        metric_conn=metric_conn,
        crawler_conn=crawler_conn,
        cycle_days=config.cycle_days,
    )
    totals = {"batches": 0, "hits": 0, "misses": 0, "skipped": 0}
    if not pending:
        return totals

    alias_to_parent_key = fetch_alias_to_parent_key(crawler_conn)

    for batch in pending:
        batch_urls = fetch_batch_urls(metric_conn, batch.batch_id)
        urls_by_shard = bucket_urls_by_shard(
            batch_urls,
            extract_domain=extract_domain,
            domain_to_shard=domain_to_shard,
            overrides=overrides,
            split_subdomains=split_subdomains,
        )

        cycle_start = batch.created_at - timedelta(days=config.cycle_days)
        raw_hits: list[tuple[str, int]] = []
        for shard_id, urls in urls_by_shard.items():
            raw_hits.extend(
                fetch_first_parent_hits(
                    crawler_conn,
                    shard_id=shard_id,
                    urls=urls,
                    cycle_start=cycle_start,
                    cycle_end=batch.created_at,
                )
            )

        hits_by_parent_key = aggregate_hits_by_parent_key(
            raw_hits, alias_to_parent_key
        )
        counts = apply_evaluation(
            crawler_conn,
            batch=batch,
            hits_by_parent_key=hits_by_parent_key,
            policy=config.cadence_policy,
            cycle_days=config.cycle_days,
        )
        totals["batches"] += 1
        for k in ("hits", "misses", "skipped"):
            totals[k] += counts[k]

        logger.info(
            "patrol.long_loop.batch_evaluated",
            extra={
                "event": "patrol.long_loop.batch_evaluated",
                "batch_id": batch.batch_id,
                **counts,
            },
        )

    return totals


def run_short_loop(
    *,
    crawler_conn,
    config: PatrolConfig,
) -> dict[str, int]:
    """Process up to `cron_batch_size` due parents in this invocation."""
    counts = {"due": 0, "transitioned": 0, "grace": 0, "fresh": 0}
    due = find_due_parents(crawler_conn, batch_size=config.cron_batch_size)
    counts["due"] = len(due)
    for parent in due:
        action = process_due_parent(crawler_conn, parent=parent, config=config)
        counts[action] += 1
    crawler_conn.commit()
    return counts


def run_once(
    *,
    metric_conn,
    crawler_conn,
    config: PatrolConfig,
    extract_domain,
    domain_to_shard,
    overrides,
    split_subdomains,
) -> dict[str, dict[str, int]]:
    """Top-level cron entry. Always evaluation first, then short loop —
    so retire / demote decisions land before we re-enqueue parents.
    """
    long_loop = run_long_loop_evaluation(
        metric_conn=metric_conn,
        crawler_conn=crawler_conn,
        config=config,
        extract_domain=extract_domain,
        domain_to_shard=domain_to_shard,
        overrides=overrides,
        split_subdomains=split_subdomains,
    )
    short_loop = run_short_loop(crawler_conn=crawler_conn, config=config)
    return {"long_loop": long_loop, "short_loop": short_loop}

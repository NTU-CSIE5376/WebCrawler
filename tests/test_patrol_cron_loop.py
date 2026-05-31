from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from libs.patrol.cadence import CadencePolicy
from libs.patrol.config import PatrolConfig
from libs.patrol.cron_loop import (
    DueParent,
    SOURCE_GOLDEN_PARENT_PATROL,
    count_new_urls_since,
    find_due_parents,
    process_due_parent,
)


def _config() -> PatrolConfig:
    return PatrolConfig(
        cadence_policy=CadencePolicy(
            intervals_sec={
                "medium": 21600,
                "slow": 86400,
                "trial": 172800,
                "cold": 604800,
            },
            promote_new_url_threshold=5,
            demote_no_new_url_threshold=2,
            retire_consecutive_miss_batches=2,
            demote_on_consecutive_miss_batches=1,
        ),
        patrol_priority=1.0,
        grace_period_seconds=900,
        cron_batch_size=1000,
        cycle_days=14,
    )


class _CursorRouter:
    """A psycopg2-style connection mock with a shared fetchone queue.

    Each `with conn.cursor()` block returns a fresh cursor that records
    its execute() calls into the shared `executions` list and drains the
    next item from the shared `_queue` for any fetchone() call. This
    matches real psycopg2 semantics where fetch results are consumed by
    the explicit fetch call, not by entering the cursor context.
    """

    def __init__(self, fetch_results):
        self._queue = list(fetch_results)
        self.executions: list[tuple[str, tuple]] = []

    def cursor(self):
        ctx = MagicMock()
        cur = MagicMock()

        def _fetchone():
            return self._queue.pop(0) if self._queue else None

        def _execute(sql, params=None):
            self.executions.append((sql, params))

        cur.fetchone.side_effect = _fetchone
        cur.fetchall.return_value = []
        cur.execute.side_effect = _execute
        ctx.__enter__ = lambda *_: cur
        ctx.__exit__ = lambda *_: False
        return ctx


def _parent(**overrides) -> DueParent:
    base = dict(
        parent_key="https://example.com/scoreboard",
        fetch_url="https://example.com/scoreboard",
        aliases=["https://example.com/scoreboard"],
        parent_domain="example.com",
        shard_id=42,
        cadence_bucket="medium",
        last_patrol_at=None,
        consecutive_no_new_url=0,
    )
    base.update(overrides)
    return DueParent(**base)


class ProcessDueParentTest(unittest.TestCase):
    def test_fresh_parent_is_enqueued_with_correct_source(self):
        # last_observed_fetch_at lookup returns no row (parent has never
        # been fetched). ensure_domain returns (domain_id, domain_score).
        # last_patrol_at is None → 'fresh' branch.
        conn = _CursorRouter(fetch_results=[
            None,            # fetch_last_observed_fetch SELECT
            (1234, 0.5),     # ensure_domain SELECT
        ])
        action = process_due_parent(conn, parent=_parent(), config=_config())
        self.assertEqual(action, "fresh")

        # Locate the INSERT-or-UPDATE on url_state_current (the enqueue).
        enqueue = next(
            (sql, params)
            for sql, params in conn.executions
            if sql.startswith("\nINSERT INTO url_state_current_")
        )
        self.assertIn("source", enqueue[0])
        self.assertIn("url_score_updated_at = NOW()", enqueue[0])
        self.assertIn("should_crawl = TRUE", enqueue[0])
        self.assertEqual(enqueue[1][0], "https://example.com/scoreboard")
        self.assertEqual(enqueue[1][3], SOURCE_GOLDEN_PARENT_PATROL)
        self.assertEqual(enqueue[1][4], 1.0)

    def test_grace_when_crawler_has_not_fetched_since_last_patrol(self):
        # last_patrol_at is recent; last_observed_fetch_at is older →
        # crawler hasn't picked the parent up yet, so don't transition.
        last_patrol = datetime(2026, 5, 1, tzinfo=timezone.utc)
        last_fetch = last_patrol - timedelta(hours=1)
        conn = _CursorRouter(fetch_results=[
            (last_fetch,),    # fetch_last_observed_fetch
            (1234, 0.5),      # ensure_domain
        ])
        action = process_due_parent(
            conn,
            parent=_parent(last_patrol_at=last_patrol),
            config=_config(),
        )
        self.assertEqual(action, "grace")

        # Grace path uses write_grace_period_only, which only sets
        # next_patrol_at + updated_at. It must NOT touch cadence_bucket.
        grace_update = next(
            (sql, params)
            for sql, params in conn.executions
            if "next_patrol_at" in sql and "cadence_bucket" not in sql
        )
        # The grace UPDATE moves next_patrol_at forward — exact future time
        # depends on now() so we just sanity check the parent_key.
        self.assertEqual(grace_update[1][1], "https://example.com/scoreboard")

    def test_transitioned_when_crawler_fetched_after_last_patrol(self):
        last_patrol = datetime(2026, 5, 1, tzinfo=timezone.utc)
        last_fetch = last_patrol + timedelta(minutes=5)
        # fetch_last_observed_fetch → last_fetch (after patrol mark)
        # ensure_domain → row
        # count_new_urls_since → 7 (above promote threshold = 5)
        conn = _CursorRouter(fetch_results=[
            (last_fetch,),
            (1234, 0.5),
            (7,),
        ])
        action = process_due_parent(
            conn,
            parent=_parent(last_patrol_at=last_patrol, cadence_bucket="slow"),
            config=_config(),
        )
        self.assertEqual(action, "transitioned")

        # Find the full short-loop UPDATE — it is the one that sets
        # cadence_bucket alongside the new next_patrol_at.
        full_update = next(
            (sql, params)
            for sql, params in conn.executions
            if "cadence_bucket = %s" in sql and "last_seen_new_url_count" in sql
        )
        new_bucket = full_update[1][0]
        last_seen_new_url_count = full_update[1][4]
        consecutive_no_new_url = full_update[1][5]
        # Promotion: slow → medium (medium is the fastest bucket post-review).
        self.assertEqual(new_bucket, "medium")
        self.assertEqual(last_seen_new_url_count, 7)
        self.assertEqual(consecutive_no_new_url, 0)

    def test_next_patrol_at_is_anchored_to_fetch_time_not_cron_time(self):
        # Cadence is the time we want to give the parent to accumulate new
        # outlinks BETWEEN FETCHES. If the crawler fetched late, the next
        # patrol must be (fetch_time + interval), not (cron_now + interval),
        # otherwise the actual gap between fetches keeps drifting.
        now = datetime.now(timezone.utc)
        # We marked 6h ago, crawler actually fetched 4h ago (2h late).
        last_patrol = now - timedelta(hours=6)
        last_fetch = now - timedelta(hours=4)
        # 7 new URLs → promote slow → medium (6h interval = 21600s).
        conn = _CursorRouter(fetch_results=[
            (last_fetch,),
            (1234, 0.5),
            (7,),
        ])

        process_due_parent(
            conn,
            parent=_parent(last_patrol_at=last_patrol, cadence_bucket="slow"),
            config=_config(),
        )

        full_update = next(
            (sql, params)
            for sql, params in conn.executions
            if "cadence_bucket = %s" in sql and "last_seen_new_url_count" in sql
        )
        next_patrol_at = full_update[1][1]   # see write_short_loop_outcome param order
        # Expected: last_fetch + 6h (the "medium" bucket post-promote).
        # Allow ±5s for execution time between test setup and the call.
        expected = last_fetch + timedelta(hours=6)
        self.assertLess(
            abs((next_patrol_at - expected).total_seconds()),
            5,
            f"next_patrol_at={next_patrol_at} not anchored to last_fetch+6h={expected}",
        )

    def test_next_patrol_at_clamped_to_now_when_fetch_plus_interval_is_in_past(self):
        # Edge case: crawler fetched long enough ago that fetch+interval has
        # already passed by the time the cron runs. We don't want to schedule
        # in the past — clamp to now so the row stays due and gets re-picked
        # on the next tick.
        now = datetime.now(timezone.utc)
        # Marked 30 days ago, fetched right after — fetch + 6h is ~30 days
        # before now. Without the max(...) clamp, next_patrol_at would be
        # 30 days in the past.
        last_patrol = now - timedelta(days=30)
        last_fetch = last_patrol + timedelta(minutes=5)
        conn = _CursorRouter(fetch_results=[
            (last_fetch,),
            (1234, 0.5),
            (7,),
        ])

        process_due_parent(
            conn,
            parent=_parent(last_patrol_at=last_patrol, cadence_bucket="slow"),
            config=_config(),
        )

        full_update = next(
            (sql, params)
            for sql, params in conn.executions
            if "cadence_bucket = %s" in sql and "last_seen_new_url_count" in sql
        )
        next_patrol_at = full_update[1][1]
        # Should be ~now (the floor), within a small execution window.
        self.assertLess(
            abs((next_patrol_at - now).total_seconds()),
            5,
            f"next_patrol_at={next_patrol_at} not clamped to now={now}",
        )


    def test_transitioned_demotes_when_zero_new_urls_streak_meets_threshold(self):
        # Symmetric counterpart to the promote test: count=0 plus an existing
        # streak of 1 hits demote_no_new_url_threshold=2 → bucket steps from
        # medium to slow and the no-new counter resets.
        now = datetime.now(timezone.utc)
        last_patrol = now - timedelta(hours=6)
        last_fetch = now - timedelta(hours=3)        # crawler did fetch
        conn = _CursorRouter(fetch_results=[
            (last_fetch,),     # fetch_last_observed_fetch
            (1234, 0.5),       # ensure_domain
            (0,),              # count_new_urls_since -> 0
        ])
        action = process_due_parent(
            conn,
            parent=_parent(
                last_patrol_at=last_patrol,
                cadence_bucket="medium",
                consecutive_no_new_url=1,    # already 1 prior zero
            ),
            config=_config(),
        )
        self.assertEqual(action, "transitioned")

        full_update = next(
            (sql, params)
            for sql, params in conn.executions
            if "cadence_bucket = %s" in sql and "last_seen_new_url_count" in sql
        )
        new_bucket = full_update[1][0]
        last_seen_new_url_count = full_update[1][4]
        consecutive_no_new_url = full_update[1][5]
        # Demotion: medium → slow, counter resets so the next demotion needs a
        # fresh streak.
        self.assertEqual(new_bucket, "slow")
        self.assertEqual(last_seen_new_url_count, 0)
        self.assertEqual(consecutive_no_new_url, 0)


class CountNewUrlsSinceSqlTest(unittest.TestCase):
    """Pin the structural invariants of the cross-shard COUNT query:

      * one branch per shard (UNION ALL across all of them) — this is the
        perf-critical query in the cron's short loop, and any silent
        regression in shape (e.g. dropping the discovered_from filter)
        would either inflate or zero out the new_url_count.
      * params re-tile (aliases, since) once per shard so the planner gets
        the parameterised path for every branch.
    """

    def test_sql_unions_one_branch_per_shard_with_correct_filter(self):
        conn = _CursorRouter(fetch_results=[(0,)])
        aliases = ["https://example.com/a"]
        since = datetime(2026, 5, 1, tzinfo=timezone.utc)
        out = count_new_urls_since(
            conn, aliases=aliases, since=since, num_shards=3,
        )
        self.assertEqual(out, 0)
        self.assertEqual(len(conn.executions), 1)
        sql, params = conn.executions[0]

        # Every shard table appears.
        self.assertIn("url_state_current_000", sql)
        self.assertIn("url_state_current_001", sql)
        self.assertIn("url_state_current_002", sql)
        # The shape: 3 shards → 2 UNION ALL joins between them.
        self.assertEqual(sql.count("UNION ALL"), 2)
        # The filter we actually want — silent refactor that drops either
        # of these would invalidate the count.
        self.assertIn("discovered_from = ANY(%s)", sql)
        self.assertIn("first_seen > %s", sql)
        # Params tile (aliases, since) once per shard so each branch gets
        # its own binding.
        self.assertEqual(params, [aliases, since, aliases, since, aliases, since])

    def test_empty_aliases_short_circuits_without_query(self):
        conn = _CursorRouter(fetch_results=[])
        out = count_new_urls_since(
            conn, aliases=[], since=datetime.now(timezone.utc), num_shards=3,
        )
        self.assertEqual(out, 0)
        # No SQL should have been issued — the function must short-circuit
        # so an empty aliases list doesn't generate a 256-branch UNION that
        # returns trivially 0.
        self.assertEqual(conn.executions, [])


class FindDueParentsSqlTest(unittest.TestCase):
    """Pin the SQL structure of the cron's primary selector — the partial
    index `idx_..._due` is partial on `status <> 'retired'` and ordered by
    `next_patrol_at`, so the query must match those exact conditions or it
    silently falls off the index and seq-scans patrol_state."""

    def test_query_uses_partial_index_predicate_and_locks_with_skip(self):
        conn = _CursorRouter(fetch_results=[])
        out = find_due_parents(conn, batch_size=500)
        self.assertEqual(out, [])

        self.assertEqual(len(conn.executions), 1)
        sql, params = conn.executions[0]
        # Conditions the partial index covers.
        self.assertIn("next_patrol_at <= NOW()", sql)
        self.assertIn("status <> 'retired'", sql)
        # Ordering needed for FIFO cron processing (oldest due first).
        self.assertIn("ORDER BY next_patrol_at", sql)
        # Concurrency safety: SKIP LOCKED so multiple cron tickers can run
        # in parallel without colliding on the same parent.
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(params, (500,))


if __name__ == "__main__":
    unittest.main()

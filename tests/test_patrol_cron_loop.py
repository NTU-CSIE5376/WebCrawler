from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from libs.patrol.cadence import CadencePolicy
from libs.patrol.config import PatrolConfig
from libs.patrol.cron_loop import (
    DueParent,
    SOURCE_GOLDEN_PARENT_PATROL,
    process_due_parent,
)


def _config() -> PatrolConfig:
    return PatrolConfig(
        cadence_policy=CadencePolicy(
            intervals_sec={
                "fast": 3600,
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
            parent=_parent(last_patrol_at=last_patrol, cadence_bucket="medium"),
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
        # Promotion: medium → fast.
        self.assertEqual(new_bucket, "fast")
        self.assertEqual(last_seen_new_url_count, 7)
        self.assertEqual(consecutive_no_new_url, 0)


if __name__ == "__main__":
    unittest.main()

"""Tests for GoldenDiscoveryRankerService's manual-priority guard.

What we're protecting:

  * Ranker scores are clipped to [0, 1]; url_score > 1 is a manually
    assigned priority (golden_inject = 3.0, wiki_pageview_inject =
    1 + views/max). The scorer must never pick those rows, on either the
    legacy path or the domain-priority steering path, or it overwrites
    the manual tier with a model score.

The tests record the SQL the scorer issues against a fake session with an
empty result set; no DB or scorer model is needed.
"""
from __future__ import annotations

import unittest

from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig,
    GoldenDiscoveryRankerService,
)

MANUAL_PRIORITY_GUARD = "(url_score IS NULL OR url_score <= 1.0)"


class _RecordingCursor:
    def __init__(self):
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchall(self):
        return []


class _FakeSession:
    def __init__(self, cursor):
        self._cursor = cursor

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self._cursor


class ScorerManualPriorityTest(unittest.TestCase):
    def _picked_sql(self, steering: bool) -> str:
        cfg = GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=1,
            worker_id=0,
            batch_size=10,
            scan_interval_sec=60,
            max_batches_per_shard=1,
            domain_priority_steering_enabled=steering,
        )
        cursor = _RecordingCursor()
        session = _FakeSession(cursor)
        # sess.connection().connection.cursor(): the second hop is an attribute.
        session.connection = lambda: type("C", (), {"connection": session})()
        svc = GoldenDiscoveryRankerService(cfg=cfg, Session=session, scorer=None)
        self.assertEqual(svc._score_batch(0), 0)
        return " ".join(cursor.sql[0].split())

    def test_legacy_path_skips_manual_priority(self):
        self.assertIn(MANUAL_PRIORITY_GUARD, self._picked_sql(False))

    def test_steering_path_skips_manual_priority(self):
        self.assertIn(MANUAL_PRIORITY_GUARD, self._picked_sql(True))


if __name__ == "__main__":
    unittest.main()

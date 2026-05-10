from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from libs.patrol.cadence import CadencePolicy
from libs.patrol.evaluation import (
    DEFAULT_CYCLE_DAYS,
    PendingBatch,
    aggregate_hits_by_parent_key,
    apply_evaluation,
    bucket_urls_by_shard,
)


def _policy() -> CadencePolicy:
    return CadencePolicy(
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
    )


class BucketUrlsByShardTest(unittest.TestCase):
    """Pure shard-bucketing: dependency injection for sharding helpers
    keeps this layer free of tldextract / config IO."""

    def setUp(self):
        # Toy domain extractor: returns the bare host.
        self.extract_domain = lambda u: u.split("/")[2] if "://" in u else None
        # Toy shard mapper: deterministic — first letter of domain.
        # Avoids Python hash() randomization across test runs.
        self.domain_to_shard = lambda d, _o, _s: ord(d[0]) % 4

    def test_groups_urls_by_shard(self):
        out = bucket_urls_by_shard(
            ["https://aaa.com/1", "https://aaa.com/2", "https://zzz.com/3"],
            extract_domain=self.extract_domain,
            domain_to_shard=self.domain_to_shard,
            overrides={},
            split_subdomains=set(),
        )
        a_shard = self.domain_to_shard("aaa.com", {}, set())
        z_shard = self.domain_to_shard("zzz.com", {}, set())
        self.assertNotEqual(a_shard, z_shard, "test domains must hash to different shards")
        self.assertEqual(
            set(out[a_shard]), {"https://aaa.com/1", "https://aaa.com/2"}
        )
        self.assertEqual(out[z_shard], ["https://zzz.com/3"])

    def test_skips_empty_or_unparseable(self):
        out = bucket_urls_by_shard(
            ["", None, "no-scheme", "https://x.com/1"],
            extract_domain=self.extract_domain,
            domain_to_shard=self.domain_to_shard,
            overrides={},
            split_subdomains=set(),
        )
        # Only the well-formed URL should land somewhere.
        flattened = [u for urls in out.values() for u in urls]
        self.assertEqual(flattened, ["https://x.com/1"])


class AggregateHitsByParentKeyTest(unittest.TestCase):
    def test_collapses_aliases_to_parent_key(self):
        alias_map = {
            "http://example.com/scoreboard": "https://example.com/scoreboard",
            "https://www.example.com/scoreboard": "https://example.com/scoreboard",
        }
        raw_hits = [
            ("http://example.com/scoreboard", 3),
            ("https://www.example.com/scoreboard", 2),
        ]
        out = aggregate_hits_by_parent_key(raw_hits, alias_map)
        self.assertEqual(out, {"https://example.com/scoreboard": 5})

    def test_drops_unenrolled_raw_urls(self):
        # Some discovered_from values may belong to parents we never
        # enrolled (e.g. they had 0 lifetime golden children at backfill
        # time). Their hits cannot be attributed to any patrol_state row
        # and must be silently dropped.
        alias_map = {"https://known.com/x": "https://known.com/x"}
        raw_hits = [
            ("https://known.com/x", 4),
            ("https://stranger.com/y", 9),
        ]
        out = aggregate_hits_by_parent_key(raw_hits, alias_map)
        self.assertEqual(out, {"https://known.com/x": 4})


class ApplyEvaluationTest(unittest.TestCase):
    """Mock-driven test of apply_evaluation's UPDATE behaviour.

    apply_evaluation does five things and we verify each:
      1. skip rows whose last_eval_batch_id has already advanced past
         this batch
      2. skip rows enrolled mid-cycle (only advance their cursor)
      3. credit hits and reset the miss streak
      4. demote on first miss when policy.demote_on=1
      5. retire on second consecutive miss
    """

    def setUp(self):
        self.policy = _policy()
        self.batch = PendingBatch(
            batch_id=42,
            created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )
        self.cycle_start = self.batch.created_at - timedelta(days=DEFAULT_CYCLE_DAYS)

    def _mock_conn_returning_rows(self, rows):
        """Construct a mock connection where the first cursor.execute()
        returns the given SELECT rows and subsequent cursor.execute()
        calls (the UPDATE statements) are recorded."""
        select_cur = MagicMock()
        select_cur.fetchall.return_value = rows

        update_cur = MagicMock()

        cursors = iter([select_cur, update_cur])
        conn = MagicMock()
        conn.cursor.return_value.__enter__.side_effect = lambda: next(cursors)
        # Record UPDATEs by inspecting update_cur.execute.call_args_list
        return conn, update_cur

    def test_credits_a_hit_and_resets_streak(self):
        rows = [
            (
                "https://example.com/a",       # parent_key
                "active",                       # status
                "medium",                       # cadence_bucket
                1,                              # consecutive_miss_batches
                self.cycle_start - timedelta(days=30),  # first_enrolled_at
                0,                              # last_eval_batch_id
            )
        ]
        conn, update_cur = self._mock_conn_returning_rows(rows)
        counts = apply_evaluation(
            conn,
            batch=self.batch,
            hits_by_parent_key={"https://example.com/a": 3},
            policy=self.policy,
        )
        self.assertEqual(counts, {"hits": 1, "misses": 0, "skipped": 0})

        self.assertEqual(update_cur.execute.call_count, 1)
        sql, params = update_cur.execute.call_args[0]
        self.assertIn("UPDATE golden_parent_patrol_state", sql)
        # Outcome: bucket unchanged, status unchanged, miss streak reset to 0,
        # lifetime count incremented by 3, last_eval_batch_id advanced.
        self.assertEqual(
            params,
            ("medium", "active", 0, 3, 42, "https://example.com/a"),
        )

    def test_demotes_on_first_miss(self):
        rows = [
            (
                "https://example.com/b",
                "active",
                "medium",
                0,
                self.cycle_start - timedelta(days=30),
                0,
            )
        ]
        conn, update_cur = self._mock_conn_returning_rows(rows)
        counts = apply_evaluation(
            conn,
            batch=self.batch,
            hits_by_parent_key={},
            policy=self.policy,
        )
        self.assertEqual(counts, {"hits": 0, "misses": 1, "skipped": 0})

        sql, params = update_cur.execute.call_args[0]
        # demote_on_consecutive_miss_batches=1 → demote bucket to slow,
        # streak becomes 1, no lifetime increment.
        self.assertEqual(
            params,
            ("slow", "active", 1, 0, 42, "https://example.com/b"),
        )

    def test_retires_on_second_consecutive_miss(self):
        rows = [
            (
                "https://example.com/c",
                "active",
                "slow",
                1,
                self.cycle_start - timedelta(days=30),
                0,
            )
        ]
        conn, update_cur = self._mock_conn_returning_rows(rows)
        counts = apply_evaluation(
            conn,
            batch=self.batch,
            hits_by_parent_key={},
            policy=self.policy,
        )
        self.assertEqual(counts, {"hits": 0, "misses": 1, "skipped": 0})

        sql, params = update_cur.execute.call_args[0]
        # retire_consecutive_miss_batches=2 → status flips to retired,
        # streak becomes 2.
        self.assertEqual(
            params,
            ("slow", "retired", 2, 0, 42, "https://example.com/c"),
        )

    def test_skips_already_evaluated_row(self):
        rows = [
            (
                "https://example.com/d",
                "active",
                "medium",
                0,
                self.cycle_start - timedelta(days=30),
                42,  # already evaluated for this batch
            )
        ]
        conn, update_cur = self._mock_conn_returning_rows(rows)
        counts = apply_evaluation(
            conn,
            batch=self.batch,
            hits_by_parent_key={"https://example.com/d": 5},
            policy=self.policy,
        )
        self.assertEqual(counts, {"hits": 0, "misses": 0, "skipped": 1})
        update_cur.execute.assert_not_called()

    def test_advances_cursor_for_mid_cycle_enrolled_row(self):
        rows = [
            (
                "https://example.com/e",
                "trial",
                "trial",
                0,
                self.cycle_start + timedelta(days=2),  # enrolled mid-cycle
                0,
            )
        ]
        conn, update_cur = self._mock_conn_returning_rows(rows)
        counts = apply_evaluation(
            conn,
            batch=self.batch,
            hits_by_parent_key={},
            policy=self.policy,
        )
        self.assertEqual(counts, {"hits": 0, "misses": 0, "skipped": 1})

        # Mid-cycle rows still get their cursor advanced, but not via the
        # full transition UPDATE.
        sql, params = update_cur.execute.call_args[0]
        self.assertIn("last_eval_batch_id = %s", sql)
        self.assertNotIn("cadence_bucket", sql)
        self.assertEqual(params, (42, "https://example.com/e"))


if __name__ == "__main__":
    unittest.main()

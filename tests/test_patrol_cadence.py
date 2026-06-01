from __future__ import annotations

import unittest

from libs.patrol.cadence import (
    BUCKET_ORDER,
    CadencePolicy,
    LongLoopOutcome,
    ShortLoopOutcome,
    demote_bucket,
    interval_seconds,
    long_loop_transition,
    promote_bucket,
    short_loop_transition,
)


def _policy(**overrides) -> CadencePolicy:
    base = dict(
        intervals_sec={
            "medium": 21600,
            "slow": 86400,
            "trial": 172800,
            "cold": 345600,
        },
        promote_new_url_threshold=5,
        demote_no_new_url_threshold=2,
        retire_consecutive_miss_batches=2,
        demote_on_consecutive_miss_batches=1,
    )
    base.update(overrides)
    return CadencePolicy(**base)


class BucketOrderTest(unittest.TestCase):
    def test_promote_moves_left(self):
        self.assertEqual(promote_bucket("cold"), "trial")
        self.assertEqual(promote_bucket("slow"), "medium")

    def test_promote_at_medium_is_noop(self):
        # "medium" (6h) is the fastest bucket; "fast" (1h) was dropped in
        # review to spare anti-bot rate limits.
        self.assertEqual(promote_bucket("medium"), "medium")

    def test_demote_moves_right(self):
        self.assertEqual(demote_bucket("medium"), "slow")
        self.assertEqual(demote_bucket("slow"), "trial")

    def test_demote_at_cold_is_noop(self):
        self.assertEqual(demote_bucket("cold"), "cold")

    def test_unknown_bucket_passes_through(self):
        self.assertEqual(promote_bucket("invalid"), "invalid")
        self.assertEqual(demote_bucket("invalid"), "invalid")


class IntervalSecondsTest(unittest.TestCase):
    def test_returns_configured_interval(self):
        p = _policy()
        self.assertEqual(interval_seconds("medium", p), 21600)
        self.assertEqual(interval_seconds("cold", p), 345600)

    def test_unknown_bucket_raises(self):
        p = _policy()
        with self.assertRaises(ValueError):
            interval_seconds("not_a_bucket", p)


class ShortLoopTransitionTest(unittest.TestCase):
    def test_promote_when_new_url_count_meets_threshold(self):
        p = _policy(promote_new_url_threshold=5)
        out = short_loop_transition(
            current_bucket="slow",
            consecutive_no_new_url=0,
            new_url_count=5,
            policy=p,
        )
        self.assertEqual(out, ShortLoopOutcome("medium", 0))

    def test_promote_resets_no_new_url_counter(self):
        # Even if the parent had been racking up zeros, a single fruitful
        # patrol breaks the streak and resets the counter.
        p = _policy(promote_new_url_threshold=5)
        out = short_loop_transition(
            current_bucket="slow",
            consecutive_no_new_url=1,
            new_url_count=10,
            policy=p,
        )
        self.assertEqual(out.new_consecutive_no_new_url, 0)

    def test_zero_new_urls_increments_counter_below_threshold(self):
        p = _policy(demote_no_new_url_threshold=2)
        out = short_loop_transition(
            current_bucket="medium",
            consecutive_no_new_url=0,
            new_url_count=0,
            policy=p,
        )
        # First zero → hold bucket, bump counter to 1.
        self.assertEqual(out, ShortLoopOutcome("medium", 1))

    def test_zero_new_urls_demotes_when_streak_meets_threshold(self):
        p = _policy(demote_no_new_url_threshold=2)
        out = short_loop_transition(
            current_bucket="medium",
            consecutive_no_new_url=1,  # one prior zero, this is the second
            new_url_count=0,
            policy=p,
        )
        # Second zero in a row → demote and reset counter.
        self.assertEqual(out, ShortLoopOutcome("slow", 0))

    def test_partial_yield_holds_and_resets(self):
        # 1 <= count < promote_threshold: hold the bucket but reset the
        # counter so accumulated zeros don't carry forward.
        p = _policy(promote_new_url_threshold=5)
        out = short_loop_transition(
            current_bucket="medium",
            consecutive_no_new_url=1,
            new_url_count=2,
            policy=p,
        )
        self.assertEqual(out, ShortLoopOutcome("medium", 0))

    def test_promote_at_medium_caps(self):
        # "medium" is now the fastest bucket (was "fast" before review).
        p = _policy(promote_new_url_threshold=5)
        out = short_loop_transition(
            current_bucket="medium",
            consecutive_no_new_url=0,
            new_url_count=20,
            policy=p,
        )
        self.assertEqual(out.new_bucket, "medium")

    def test_demote_at_cold_caps(self):
        p = _policy(demote_no_new_url_threshold=2)
        out = short_loop_transition(
            current_bucket="cold",
            consecutive_no_new_url=1,
            new_url_count=0,
            policy=p,
        )
        self.assertEqual(out.new_bucket, "cold")


class LongLoopTransitionTest(unittest.TestCase):
    def test_hits_reset_miss_counter_and_hold(self):
        p = _policy()
        out = long_loop_transition(
            current_bucket="medium",
            current_status="active",
            consecutive_miss_batches=1,
            cycle_hits=3,
            policy=p,
        )
        self.assertEqual(out, LongLoopOutcome("medium", "active", 0))

    def test_first_miss_demotes_when_demote_threshold_is_one(self):
        p = _policy(
            demote_on_consecutive_miss_batches=1,
            retire_consecutive_miss_batches=2,
        )
        out = long_loop_transition(
            current_bucket="medium",
            current_status="active",
            consecutive_miss_batches=0,
            cycle_hits=0,
            policy=p,
        )
        self.assertEqual(out.new_bucket, "slow")
        self.assertEqual(out.new_status, "active")
        self.assertEqual(out.new_consecutive_miss_batches, 1)

    def test_second_consecutive_miss_retires(self):
        p = _policy(
            demote_on_consecutive_miss_batches=1,
            retire_consecutive_miss_batches=2,
        )
        out = long_loop_transition(
            current_bucket="slow",
            current_status="active",
            consecutive_miss_batches=1,
            cycle_hits=0,
            policy=p,
        )
        self.assertEqual(out.new_status, "retired")
        # bucket is left alone on retire — analytics still want to know
        # what cadence the parent was on when retired.
        self.assertEqual(out.new_bucket, "slow")
        self.assertEqual(out.new_consecutive_miss_batches, 2)

    def test_miss_below_demote_threshold_just_increments(self):
        # Configure demote_on=2 so the first miss only bumps the counter.
        p = _policy(
            demote_on_consecutive_miss_batches=2,
            retire_consecutive_miss_batches=3,
        )
        out = long_loop_transition(
            current_bucket="medium",
            current_status="active",
            consecutive_miss_batches=0,
            cycle_hits=0,
            policy=p,
        )
        self.assertEqual(out, LongLoopOutcome("medium", "active", 1))


class PolicyOrderTest(unittest.TestCase):
    def test_bucket_order_is_intrinsic_constant(self):
        # Sanity: the cron and tests both rely on this ordering.
        self.assertEqual(BUCKET_ORDER, ("medium", "slow", "trial", "cold"))


if __name__ == "__main__":
    unittest.main()

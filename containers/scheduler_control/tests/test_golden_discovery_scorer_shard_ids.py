"""Tests for GoldenDiscoveryRankerService._shard_ids work-stealing order.

What we're protecting:

  * Every worker visits every shard on each `run_once`. This is the
    headline behaviour change from the previous static partition;
    without it a worker that finishes its slice can't help the busy
    workers.
  * Workers stagger their starting offsets so they aren't all hammering
    shard 0 simultaneously. The offset is deterministic per worker_id
    so reruns of the same worker behave identically.
  * The four production workers (total_shards=256, num_workers=4) start
    at shards 0 / 64 / 128 / 192 — 64 apart, matching the design.
  * Degenerate configs (num_workers=0 or num_workers > total_shards)
    don't crash.

The tests instantiate the service with `Session=None` and `scorer=None`
because `_shard_ids` is a pure function of config; no DB, no fake
session, no scorer mock needed.
"""
from __future__ import annotations

import unittest

from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig,
    GoldenDiscoveryRankerService,
)


def _service(*, num_workers: int, worker_id: int, total_shards: int = 256):
    cfg = GoldenDiscoveryRankerConfig(
        total_shards=total_shards,
        num_workers=num_workers,
        worker_id=worker_id,
        batch_size=1000,
        scan_interval_sec=60,
        max_batches_per_shard=1,
    )
    return GoldenDiscoveryRankerService(cfg=cfg, Session=None, scorer=None)


class ShardIdsCoverageTest(unittest.TestCase):
    """Every worker visits every shard, exactly once per run_once."""

    def test_each_worker_covers_full_shard_range(self):
        for worker_id in range(4):
            with self.subTest(worker_id=worker_id):
                ids = _service(num_workers=4, worker_id=worker_id)._shard_ids()
                self.assertEqual(
                    sorted(ids),
                    list(range(256)),
                    f"worker {worker_id} should cover all 256 shards",
                )

    def test_no_duplicates_in_one_workers_schedule(self):
        ids = _service(num_workers=4, worker_id=2)._shard_ids()
        self.assertEqual(len(ids), len(set(ids)))


class ShardIdsStaggerTest(unittest.TestCase):
    """Workers start at different offsets so they don't synchronously
    contend on shard 0."""

    def test_production_four_workers_start_at_64_apart(self):
        starts = [
            _service(num_workers=4, worker_id=i)._shard_ids()[0] for i in range(4)
        ]
        # 256 / 4 = 64
        self.assertEqual(starts, [0, 64, 128, 192])

    def test_eight_workers_start_at_32_apart(self):
        starts = [
            _service(num_workers=8, worker_id=i)._shard_ids()[0] for i in range(8)
        ]
        self.assertEqual(starts, [0, 32, 64, 96, 128, 160, 192, 224])

    def test_starting_offsets_are_unique_across_workers(self):
        # When num_workers divides total_shards evenly, every worker's
        # starting shard must be distinct — otherwise two workers hit
        # the same shard in lock-step on every iteration of run_once.
        starts = {
            _service(num_workers=8, worker_id=i)._shard_ids()[0]
            for i in range(8)
        }
        self.assertEqual(len(starts), 8)


class ShardIdsDeterminismTest(unittest.TestCase):
    """Each worker's schedule must be stable across calls so cron
    behaviour is predictable for ops debugging."""

    def test_same_worker_id_produces_same_order_on_repeated_calls(self):
        svc = _service(num_workers=4, worker_id=1)
        self.assertEqual(svc._shard_ids(), svc._shard_ids())

    def test_worker_id_one_starts_at_offset_and_wraps(self):
        ids = _service(num_workers=4, worker_id=1)._shard_ids()
        # 256/4 = 64, so worker 1 starts at shard 64 and wraps round to
        # shard 63 last.
        self.assertEqual(ids[0], 64)
        self.assertEqual(ids[-1], 63)
        # Wrap-around: shard 255 appears before shard 0.
        self.assertLess(ids.index(255), ids.index(0))


class ShardIdsDegenerateConfigTest(unittest.TestCase):
    """Configurations that aren't the production happy path should still
    produce a valid schedule, not crash."""

    def test_single_worker_starts_at_zero_and_covers_everything(self):
        ids = _service(num_workers=1, worker_id=0)._shard_ids()
        self.assertEqual(ids, list(range(256)))

    def test_zero_workers_falls_back_to_unstaggered_full_sweep(self):
        # Defensive: callers shouldn't construct this, but the code
        # shouldn't ZeroDivisionError if they do.
        ids = _service(num_workers=0, worker_id=0)._shard_ids()
        self.assertEqual(ids, list(range(256)))

    def test_more_workers_than_shards_clamps_step_to_one(self):
        # Each worker still walks every shard; just starting offsets
        # collide across worker_ids because there are more workers than
        # shards. Avoids a hidden ValueError on small shard counts.
        ids = _service(num_workers=300, worker_id=5, total_shards=256)._shard_ids()
        self.assertEqual(sorted(ids), list(range(256)))
        # Step clamps to 1, offset = worker_id % total_shards.
        self.assertEqual(ids[0], 5)

    def test_uneven_division_still_covers_all_shards(self):
        # 256 / 7 = 36 step, so workers start at 0, 36, 72, 108, 144,
        # 180, 216 — gaps aren't perfectly even but every worker still
        # visits every shard.
        for worker_id in range(7):
            with self.subTest(worker_id=worker_id):
                ids = _service(
                    num_workers=7, worker_id=worker_id
                )._shard_ids()
                self.assertEqual(sorted(ids), list(range(256)))


class ShardIdsReturnTypeTest(unittest.TestCase):
    """The return type used to be `range`. Callers only iterate over the
    result, so list works the same — but pin the contract to surface
    accidental regressions to range (which can't be reversed / sliced /
    inspected by length efficiently)."""

    def test_returns_a_list(self):
        ids = _service(num_workers=4, worker_id=0)._shard_ids()
        self.assertIsInstance(ids, list)


if __name__ == "__main__":
    unittest.main()

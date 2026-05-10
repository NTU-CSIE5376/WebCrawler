from __future__ import annotations

import unittest

from libs.patrol.config import parse_patrol_config


def _valid_raw() -> dict:
    return {
        "enabled": True,
        "patrol_priority": 1.0,
        "cadence_buckets": {
            "fast": 3600,
            "medium": 21600,
            "slow": 86400,
            "trial": 172800,
            "cold": 604800,
        },
        "bucket_transitions": {
            "promote_threshold": 5,
            "demote_threshold": 2,
        },
        "retire_policy": {
            "consecutive_miss_batches_demote": 1,
            "consecutive_miss_batches_retire": 2,
        },
        "grace_period_seconds": 900,
        "cron_batch_size": 1000,
        "cycle_days": 14,
    }


class ParsePatrolConfigTest(unittest.TestCase):
    def test_parses_valid_config(self):
        cfg = parse_patrol_config(_valid_raw())
        self.assertEqual(cfg.patrol_priority, 1.0)
        self.assertEqual(cfg.cron_batch_size, 1000)
        self.assertEqual(cfg.cycle_days, 14)
        self.assertEqual(cfg.grace_period_seconds, 900)

        p = cfg.cadence_policy
        self.assertEqual(p.intervals_sec["fast"], 3600)
        self.assertEqual(p.intervals_sec["cold"], 604800)
        self.assertEqual(p.promote_new_url_threshold, 5)
        self.assertEqual(p.demote_no_new_url_threshold, 2)
        self.assertEqual(p.retire_consecutive_miss_batches, 2)
        self.assertEqual(p.demote_on_consecutive_miss_batches, 1)

    def test_missing_cadence_buckets_raises(self):
        raw = _valid_raw()
        del raw["cadence_buckets"]
        with self.assertRaises(ValueError):
            parse_patrol_config(raw)

    def test_missing_promote_threshold_raises(self):
        raw = _valid_raw()
        del raw["bucket_transitions"]["promote_threshold"]
        with self.assertRaises(ValueError):
            parse_patrol_config(raw)

    def test_missing_retire_policy_field_raises(self):
        raw = _valid_raw()
        del raw["retire_policy"]["consecutive_miss_batches_retire"]
        with self.assertRaises(ValueError):
            parse_patrol_config(raw)

    def test_optional_fields_have_defaults(self):
        raw = _valid_raw()
        del raw["patrol_priority"]
        del raw["grace_period_seconds"]
        del raw["cron_batch_size"]
        del raw["cycle_days"]
        cfg = parse_patrol_config(raw)
        # Defaults documented in libs.patrol.config:
        self.assertEqual(cfg.patrol_priority, 1.0)
        self.assertEqual(cfg.grace_period_seconds, 900)
        self.assertEqual(cfg.cron_batch_size, 1000)
        self.assertEqual(cfg.cycle_days, 14)

    def test_intervals_coerce_to_int(self):
        raw = _valid_raw()
        raw["cadence_buckets"]["fast"] = "3600"
        cfg = parse_patrol_config(raw)
        self.assertEqual(cfg.cadence_policy.intervals_sec["fast"], 3600)
        self.assertIsInstance(cfg.cadence_policy.intervals_sec["fast"], int)


if __name__ == "__main__":
    unittest.main()

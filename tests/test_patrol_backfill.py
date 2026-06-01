from __future__ import annotations

import unittest

from scripts.backfill_golden_parent_patrol import (
    aggregate_by_parent_key,
    UPSERT_SQL as LIVE_UPSERT_SQL,
)


# Sharding config sentinels for the unit test: the helpers we invoke do not
# need a real config file when no overrides / split subdomains apply, so we
# pass empty containers and let domain_to_shard fall back to the default
# hash-mod path.
EMPTY_OVERRIDES: dict[str, int] = {}
EMPTY_SPLIT: set[str] = set()


class LiveBackfillAggregateTest(unittest.TestCase):
    def test_collapses_redirect_variants_into_one_parent_key(self):
        rows = [
            ("http://example.com/scoreboard", 5),
            ("https://www.example.com/scoreboard", 2),
            ("https://example.com/scoreboard/", 7),
        ]
        out = aggregate_by_parent_key(rows, EMPTY_OVERRIDES, EMPTY_SPLIT)

        self.assertEqual(len(out), 1, f"expected 1 key, got {list(out.keys())}")
        agg = next(iter(out.values()))

        self.assertEqual(agg.parent_key, "https://example.com/scoreboard")
        self.assertEqual(agg.child_count, 14)
        self.assertEqual(set(agg.raw_urls), {r[0] for r in rows})
        self.assertEqual(agg.parent_domain, "example.com")
        self.assertIsNotNone(agg.shard_id)

    def test_keeps_distinct_parent_pages_separate(self):
        rows = [
            ("https://example.com/scoreboard", 3),
            ("https://example.com/news", 1),
        ]
        out = aggregate_by_parent_key(rows, EMPTY_OVERRIDES, EMPTY_SPLIT)
        self.assertEqual(len(out), 2)

    def test_drops_rows_with_unparseable_domain(self):
        rows = [
            ("not-a-url", 1),
            ("https://example.com/foo", 2),
        ]
        out = aggregate_by_parent_key(rows, EMPTY_OVERRIDES, EMPTY_SPLIT)
        self.assertEqual(len(out), 1)
        self.assertEqual(next(iter(out.values())).parent_domain, "example.com")

    def test_drops_rows_with_empty_url(self):
        rows = [("", 5), (None, 1)]
        out = aggregate_by_parent_key(rows, EMPTY_OVERRIDES, EMPTY_SPLIT)
        self.assertEqual(out, {})


class LiveBackfillUpsertSqlTest(unittest.TestCase):
    def test_inserts_with_live_observed_source_type(self):
        # MVP-only loader: hardcode 'live_observed' in VALUES (no parameter)
        # so the contract is greppable and future cross-loader precedence
        # logic has a fixed reference point.
        self.assertIn("'live_observed'", LIVE_UPSERT_SQL)

    def test_on_conflict_upgrades_source_to_live_observed(self):
        self.assertIn("ON CONFLICT (parent_key) DO UPDATE", LIVE_UPSERT_SQL)
        self.assertIn("source_type = 'live_observed'", LIVE_UPSERT_SQL)

    def test_on_conflict_refreshes_lifetime_count(self):
        # Live count is authoritative; rerunning the backfill must update
        # the count to match the latest scan.
        self.assertIn(
            "lifetime_golden_child_count = EXCLUDED.lifetime_golden_child_count",
            LIVE_UPSERT_SQL,
        )

    def test_on_conflict_preserves_existing_last_eval_batch_id(self):
        # COALESCE(existing, EXCLUDED) — never overwrite a non-NULL existing
        # cursor. Otherwise reruns would re-open already-closed eval windows.
        self.assertIn(
            "last_eval_batch_id = COALESCE(",
            LIVE_UPSERT_SQL,
        )


if __name__ == "__main__":
    unittest.main()

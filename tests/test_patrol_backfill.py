from __future__ import annotations

import unittest

from scripts.backfill_golden_parent_patrol import (
    aggregate_by_parent_key,
    UPSERT_SQL as LIVE_UPSERT_SQL,
)
from scripts.load_wat_golden_parents import (
    aggregate as aggregate_wat,
    UPSERT_SQL as WAT_UPSERT_SQL,
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
        # The literal 'live_observed' lives in the VALUES clause; the loader
        # must not parameterize source_type to keep the upgrade-from-wat
        # ON CONFLICT branch trustworthy.
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


class WatLoaderAggregateTest(unittest.TestCase):
    def test_unique_children_counted_once_per_parent_key(self):
        rows = [
            ("https://example.com/scoreboard", "https://nba.com/games"),
            ("https://example.com/scoreboard", "https://nba.com/games"),  # dup
            ("https://example.com/scoreboard", "https://nba.com/news"),
            ("https://www.example.com/scoreboard", "https://nba.com/teams"),
        ]
        out = aggregate_wat(iter(rows), EMPTY_OVERRIDES, EMPTY_SPLIT)
        self.assertEqual(len(out), 1)
        agg = next(iter(out.values()))
        self.assertEqual(agg.parent_key, "https://example.com/scoreboard")
        self.assertEqual(len(agg.child_urls), 3)
        self.assertEqual(set(agg.raw_urls), {
            "https://example.com/scoreboard",
            "https://www.example.com/scoreboard",
        })

    def test_distinct_parent_keys_separate_buckets(self):
        rows = [
            ("https://a.com/", "https://x.com/"),
            ("https://b.com/", "https://x.com/"),
        ]
        out = aggregate_wat(iter(rows), EMPTY_OVERRIDES, EMPTY_SPLIT)
        self.assertEqual(len(out), 2)
        keys = sorted(out.keys())
        self.assertEqual(keys, ["https://a.com/", "https://b.com/"])

    def test_skips_rows_without_parent_or_child(self):
        rows = [
            (None, "https://x.com/"),
            ("https://y.com/", None),
            ("https://z.com/", "https://x.com/"),
        ]
        out = aggregate_wat(iter(rows), EMPTY_OVERRIDES, EMPTY_SPLIT)
        self.assertEqual(len(out), 1)
        self.assertIn("https://z.com/", out)


class WatUpsertSqlTest(unittest.TestCase):
    def test_inserts_with_wat_exact_source_type(self):
        self.assertIn("'wat_exact'", WAT_UPSERT_SQL)

    def test_on_conflict_preserves_live_observed_source(self):
        # WAT must never downgrade a live_observed row.
        self.assertIn(
            "WHEN golden_parent_patrol_state.source_type = 'live_observed'",
            WAT_UPSERT_SQL,
        )
        self.assertIn("THEN 'live_observed'", WAT_UPSERT_SQL)

    def test_on_conflict_preserves_live_observed_count(self):
        # Same precedence applied to the count: live_observed wins.
        self.assertIn(
            "THEN golden_parent_patrol_state.lifetime_golden_child_count",
            WAT_UPSERT_SQL,
        )

    def test_on_conflict_unions_aliases(self):
        # Both loaders must merge aliases as a deduped union; otherwise
        # rerunning loses information.
        self.assertIn("unnest(", WAT_UPSERT_SQL)
        self.assertIn("DISTINCT v", WAT_UPSERT_SQL)

    def test_metadata_json_is_merged_not_replaced(self):
        # WAT runs accumulate provenance over time; replacing would lose
        # history. The SQL uses jsonb concat to merge.
        self.assertIn("|| COALESCE(EXCLUDED.metadata_json", WAT_UPSERT_SQL)


if __name__ == "__main__":
    unittest.main()

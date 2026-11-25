from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from scripts.backfill_golden_parent_patrol import (
    aggregate_by_parent_key,
    disable_statement_timeout,
    scan_shard_for_live_parents,
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


class ScanShardStreamingTest(unittest.TestCase):
    """Pin the streaming-scan contract that fixed the production
    QueryCanceled failure (TA report on PR #75).

    The replacement design must:
      1. Use a SERVER-SIDE cursor (psycopg2 cursor with `name=`) so the
         large result set is paged, not buffered client-side.
      2. NOT issue a `GROUP BY` (the prior bug — single-statement
         hash-aggregate over ~150 M rows was the failure mode).
      3. Aggregate counts per `discovered_from` in Python.
    """

    def _mock_conn_returning_rows(self, rows: list[tuple[str]]):
        """Build a mock connection whose named-cursor iteration yields
        the given (discovered_from,) tuples. Records the SQL executed
        and asserts the cursor was opened with a `name=` (server-side)."""
        cur = MagicMock()
        cur.__iter__.return_value = iter(rows)
        conn = MagicMock()
        # `cursor(name=...)` returns a context manager whose enter is `cur`.
        conn.cursor.return_value.__enter__.return_value = cur
        return conn, cur

    def test_aggregates_per_discovered_from_counts(self):
        rows = [
            ("https://parent.com/a",),
            ("https://parent.com/a",),
            ("https://parent.com/a",),
            ("https://parent.com/b",),
            ("https://other.com/x",),
        ]
        conn, _ = self._mock_conn_returning_rows(rows)
        out = scan_shard_for_live_parents(conn, shard_id=0)
        as_dict = dict(out)
        self.assertEqual(as_dict["https://parent.com/a"], 3)
        self.assertEqual(as_dict["https://parent.com/b"], 1)
        self.assertEqual(as_dict["https://other.com/x"], 1)

    def test_uses_server_side_cursor(self):
        conn, _ = self._mock_conn_returning_rows([])
        scan_shard_for_live_parents(conn, shard_id=42)
        # Server-side cursors require `name=`; without it, psycopg2 buffers
        # the entire 150M-row result client-side and we are back to the
        # OOM/cancel failure mode.
        _, kwargs = conn.cursor.call_args
        self.assertIn("name", kwargs, "scan must use a server-side cursor")
        self.assertIn("42", str(kwargs["name"]), "name should embed shard_id")

    def test_sql_does_not_use_group_by(self):
        # The exact failure on PR #75 was the per-shard hash-aggregate.
        # Stream + aggregate-in-Python is the contract; if a future edit
        # reintroduces a GROUP BY here this test fails fast.
        conn, cur = self._mock_conn_returning_rows([])
        scan_shard_for_live_parents(conn, shard_id=0)
        sql = cur.execute.call_args[0][0]
        self.assertNotIn(
            "GROUP BY",
            sql.upper(),
            "GROUP BY reintroduces the single-statement hash-aggregate "
            "that caused QueryCanceled in production",
        )
        self.assertNotIn(
            "COUNT",
            sql.upper(),
            "Counting belongs in Python now; SQL should only SELECT rows",
        )

    def test_sets_cursor_itersize(self):
        # psycopg2 default itersize is 2000; we want a larger value so
        # round-trips are reasonable. Anything explicit is fine; this test
        # just guards against accidentally dropping the setting.
        conn, cur = self._mock_conn_returning_rows([])
        scan_shard_for_live_parents(conn, shard_id=0)
        self.assertIsNotNone(
            getattr(cur, "itersize", None),
            "itersize must be set on the server-side cursor",
        )

    def test_commits_after_each_shard(self):
        # Long-held read snapshots block VACUUM on a live production
        # table. Each per-shard scan must close its transaction before
        # we move on.
        conn, _ = self._mock_conn_returning_rows([])
        scan_shard_for_live_parents(conn, shard_id=0)
        conn.commit.assert_called()


class DisableStatementTimeoutTest(unittest.TestCase):
    def test_sets_both_timeouts_to_zero(self):
        # Per the TA's production cancel, statement_timeout was the
        # observable knob, but pgbouncer / monitoring tools also cancel
        # idle-in-transaction sessions. We zero both.
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        disable_statement_timeout(conn)

        executed = [c.args[0] for c in cur.execute.call_args_list]
        self.assertEqual(
            executed,
            [
                "SET statement_timeout = 0",
                "SET idle_in_transaction_session_timeout = 0",
            ],
        )
        conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()

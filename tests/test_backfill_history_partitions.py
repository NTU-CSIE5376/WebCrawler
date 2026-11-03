from __future__ import annotations

import unittest
from datetime import datetime, timezone

from scripts.backfill_history_partitions import missing_partition_sql, parse_month

NOV = datetime(2026, 11, 1, tzinfo=timezone.utc)
DEC = datetime(2026, 12, 1, tzinfo=timezone.utc)


class MissingPartitionSqlTest(unittest.TestCase):
    def test_creates_only_missing_months(self):
        existing = {"url_state_history_007_p202610", "url_state_history_007_p202612"}
        self.assertEqual(
            missing_partition_sql("url_state_history_007", existing, [NOV, DEC]),
            ["CREATE TABLE IF NOT EXISTS url_state_history_007_p202611 "
             "PARTITION OF url_state_history_007 FOR VALUES FROM "
             "('2026-11-01T00:00:00+00:00') TO ('2026-12-01T00:00:00+00:00')"],
        )

    def test_nothing_when_all_exist(self):
        existing = {"t_000_p202611", "t_000_p202612"}
        self.assertEqual(missing_partition_sql("t_000", existing, [NOV, DEC]), [])

    def test_december_rolls_over_year(self):
        [sql] = missing_partition_sql("t_000", set(), [DEC])
        self.assertIn("TO ('2027-01-01T00:00:00+00:00')", sql)

    def test_parse_month(self):
        self.assertEqual(parse_month("202611"), NOV)


if __name__ == "__main__":
    unittest.main()

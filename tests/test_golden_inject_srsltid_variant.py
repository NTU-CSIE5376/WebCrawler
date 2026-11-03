"""golden_inject treats a golden URL with and without `srsltid` as the same
page: when crawlerdb only holds the `srsltid` form, that row is marked golden
instead of inserting a second row for the stripped URL."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import golden_inject  # noqa: E402

CLEAN = "https://shop.example.com/p/1"
RAW = CLEAN + "?srsltid=AfmBOoq"


class _FakeCursor:
    """Tracks which URLs exist in url_state_current and what gets written."""

    def __init__(self, existing: set[str]):
        self.existing = set(existing)
        self.upserted: list[str] = []
        self.history: list[str] = []
        self._rows: list[tuple] = []

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("SELECT url FROM"):
            self._rows = [(u,) for u in params[0] if u in self.existing]
        elif "INSERT INTO url_state_current" in sql:
            self.upserted.append(params[0])
            self.existing.add(params[0])
        elif "INSERT INTO url_state_history" in sql:
            self.history.append(params[0])

    def fetchall(self):
        return self._rows


def _inject(existing, url=CLEAN, variant=RAW):
    cur = _FakeCursor(existing)
    result = golden_inject.inject_url(cur, url, 7, 3, 0.8, variant=variant)
    return cur, result


class GoldenInjectVariantTest(unittest.TestCase):
    def test_new_page_inserts_stripped_url(self):
        cur, result = _inject(set())
        self.assertEqual(result, golden_inject.INSERTED)
        self.assertEqual(cur.upserted, [CLEAN])
        self.assertEqual(cur.history, [CLEAN])

    def test_stripped_url_present_is_marked(self):
        cur, result = _inject({CLEAN, RAW})
        self.assertEqual(result, golden_inject.MARKED)
        self.assertEqual(cur.upserted, [CLEAN])
        self.assertEqual(cur.history, [])

    def test_only_srsltid_form_present_marks_it_without_new_row(self):
        cur, result = _inject({RAW})
        self.assertEqual(result, golden_inject.MARKED_VARIANT)
        self.assertEqual(cur.upserted, [RAW])
        self.assertEqual(cur.history, [])
        self.assertNotIn(CLEAN, cur.existing)

    def test_no_variant_behaves_as_before(self):
        cur, result = _inject(set(), variant=None)
        self.assertEqual(result, golden_inject.INSERTED)
        cur, result = _inject({CLEAN}, variant=None)
        self.assertEqual(result, golden_inject.MARKED)


if __name__ == "__main__":
    unittest.main()

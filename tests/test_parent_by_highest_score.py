"""The parent recorded for a URL (discovered_from / parent_page_score /
discovery_source_type) should be the highest-scoring parent observed, not the
first one. A NULL score ranks lowest; ties keep the existing parent."""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import (
    IngestDB,
    _prefer_higher_parent,
)


def _link(url, *, discovered_from, parent_page_score, source_type=1, anchor=None):
    return {
        "url": url,
        "status": "new",
        "shard_id": 3,
        "domain_id": 7,
        "domain_score": 0.1,
        "discovered_from": discovered_from,
        "discovery_source_type": source_type,
        "parent_page_score": parent_page_score,
        "anchor_text": anchor,
    }


class PreferHigherParentTest(unittest.TestCase):
    def test_higher_score_replaces_provenance(self):
        dst = _link("u", discovered_from="lo", parent_page_score=0.2, source_type=1)
        _prefer_higher_parent(dst, _link("u", discovered_from="hi", parent_page_score=0.9, source_type=2))
        self.assertEqual(dst["discovered_from"], "hi")
        self.assertEqual(dst["parent_page_score"], 0.9)
        self.assertEqual(dst["discovery_source_type"], 2)

    def test_lower_score_is_ignored(self):
        dst = _link("u", discovered_from="hi", parent_page_score=0.9)
        _prefer_higher_parent(dst, _link("u", discovered_from="lo", parent_page_score=0.2))
        self.assertEqual(dst["discovered_from"], "hi")
        self.assertEqual(dst["parent_page_score"], 0.9)

    def test_tie_keeps_existing(self):
        dst = _link("u", discovered_from="first", parent_page_score=0.5)
        _prefer_higher_parent(dst, _link("u", discovered_from="second", parent_page_score=0.5))
        self.assertEqual(dst["discovered_from"], "first")

    def test_null_score_ranks_lowest(self):
        # A real-scored parent beats an unknown (NULL) one...
        dst = _link("u", discovered_from="sitemap", parent_page_score=None)
        _prefer_higher_parent(dst, _link("u", discovered_from="page", parent_page_score=0.0))
        self.assertEqual(dst["discovered_from"], "page")
        self.assertEqual(dst["parent_page_score"], 0.0)
        # ...and a NULL never overwrites a real-scored parent.
        dst = _link("u", discovered_from="page", parent_page_score=0.0)
        _prefer_higher_parent(dst, _link("u", discovered_from="sitemap", parent_page_score=None))
        self.assertEqual(dst["discovered_from"], "page")


class AggregateLinksParentTest(unittest.TestCase):
    def test_keeps_highest_scoring_parent_and_sums_inlinks(self):
        recs = [
            {**_link("https://x/p", discovered_from="lo", parent_page_score=0.2),
             "inlink_count_approx": 1},
            {**_link("https://x/p", discovered_from="hi", parent_page_score=0.8),
             "inlink_count_approx": 1},
            {**_link("https://x/p", discovered_from="mid", parent_page_score=0.5),
             "inlink_count_approx": 1},
        ]
        out = IngestDB.aggregate_links(recs)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["discovered_from"], "hi")
        self.assertEqual(out[0]["parent_page_score"], 0.8)
        self.assertEqual(out[0]["inlink_count_approx"], 3)


class _NoFrozenCursor:
    """Cursor stub: _bulk_links only touches it for the frozen-domains check,
    which here returns no frozen domains."""

    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return []


class BulkLinksUpsertSqlTest(unittest.TestCase):
    def test_on_conflict_switches_parent_by_score(self):
        db = IngestDB(Session=None)
        captured = {}

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            if fetch:  # the current-table UPSERT; the other call is the history insert
                captured["sql"] = sql
                return [(r[0], True) for r in rows]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            db._bulk_links(
                cur=_NoFrozenCursor(),
                shard_id=3,
                items=[(0, _link("https://x/a", discovered_from="p", parent_page_score=0.5))],
            )

        sql = captured["sql"]
        self.assertIn("ON CONFLICT (url) DO UPDATE SET", sql)
        # All three provenance columns switch on the same higher-score condition.
        for col in ("discovered_from =", "discovery_source_type =", "parent_page_score ="):
            self.assertIn(col, sql)
        self.assertIn("EXCLUDED.parent_page_score > url_state_current_003.parent_page_score", sql)


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _FakeSess:
    def __init__(self, row):
        self.row = row
        self.captured = None

    def execute(self, stmt, params=None):
        self.captured = (str(stmt), params)
        return _FakeResult(self.row)


class RouterParentUrlScoreTest(unittest.TestCase):
    """The router records the parent page's own url_score (page-level), not the
    parent domain's score."""

    @staticmethod
    def _call(sess, shard_id, src_url):
        from containers.scheduler_ingest.router.service import RouterService
        return RouterService._parent_url_score(None, sess, shard_id, src_url)

    def test_returns_parent_page_url_score_from_its_shard(self):
        sess = _FakeSess(SimpleNamespace(url_score=0.7))
        score = self._call(sess, 7, "https://example.com/parent")
        self.assertEqual(score, 0.7)
        sql, params = sess.captured
        self.assertIn("FROM url_state_current_007", sql)  # zero-padded parent shard
        self.assertEqual(params, {"url": "https://example.com/parent"})

    def test_no_src_url_skips_query(self):
        sess = _FakeSess(SimpleNamespace(url_score=0.7))
        self.assertIsNone(self._call(sess, 7, None))
        self.assertIsNone(sess.captured)

    def test_missing_parent_row_returns_none(self):
        self.assertIsNone(self._call(_FakeSess(None), 7, "u"))

    def test_null_url_score_returns_none(self):
        self.assertIsNone(self._call(_FakeSess(SimpleNamespace(url_score=None)), 7, "u"))


if __name__ == "__main__":
    unittest.main()

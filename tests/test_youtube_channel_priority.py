"""Crawler-assigned priorities (YouTube channel uploads playlists) flow from the
router into url_state_current as url_score > 1 with source = 4, bypassing the
inline ranker, and only ever raise natural / channel rows."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.router.service import _priority_fields
from libs.ipc.new_link_record import SOURCE_YOUTUBE_CHANNEL

CHANNEL = "https://www.youtube.com/playlist?list=UUX6OQ3DkcsbYNE6H8uQQuVA"
VIDEO = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def _link(url, priority=None):
    rec = {
        "url": url,
        "status": "new",
        "shard_id": 64,
        "domain_id": 7,
        "domain_score": 0.1,
        "discovered_from": "https://www.youtube.com/watch?v=xxxxxxxxxxx",
        "discovery_source_type": 1,
        "parent_page_score": 0.5,
        "anchor_text": None,
    }
    if priority is not None:
        rec["url_priority"] = priority
        rec["source"] = SOURCE_YOUTUBE_CHANNEL
    return rec


class RouterPriorityFieldsTest(unittest.TestCase):
    def test_valid_priority_sets_channel_source(self):
        self.assertEqual(
            _priority_fields({"url": CHANNEL, "priority": 1.455}),
            {"url_priority": 1.455, "source": SOURCE_YOUTUBE_CHANNEL},
        )

    def test_missing_or_out_of_range_priority_is_natural(self):
        for link in ({"url": VIDEO}, {"priority": 1.0}, {"priority": 0.9},
                     {"priority": 3.0}, {"priority": "1.5"}, {"priority": True}):
            self.assertEqual(_priority_fields(link), {}, link)


class AggregatePriorityTest(unittest.TestCase):
    def test_keeps_max_priority_regardless_of_order(self):
        recs = [_link(CHANNEL), _link(CHANNEL, 1.2), _link(CHANNEL, 1.8), _link(CHANNEL, 1.000001)]
        (out,) = IngestDB.aggregate_links(recs)
        self.assertEqual(out["url_priority"], 1.8)
        self.assertEqual(out["source"], SOURCE_YOUTUBE_CHANNEL)


class _Cursor:
    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return []


class _Ranker:
    def __init__(self):
        self.calls = []

    def score_many(self, urls):
        self.calls.append(list(urls))
        return [0.3 for _ in urls]


class BulkLinksPriorityTest(unittest.TestCase):
    def _run(self, items):
        ranker = _Ranker()
        db = IngestDB(Session=None, inline_ranker=ranker)
        captured = {}

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            if fetch:
                captured["sql"] = sql
                captured["rows"] = rows
                return [(r[0],) for r in rows]
            captured["history_rows"] = rows
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            db._bulk_links(cur=_Cursor(), shard_id=64, items=list(enumerate(items)))
        return ranker, captured

    def test_priority_rows_bypass_ranker(self):
        ranker, captured = self._run([_link(CHANNEL, 1.455), _link(VIDEO)])
        self.assertEqual(ranker.calls, [[VIDEO]])
        rows = {r[0]: r for r in captured["rows"]}
        # (url, domain_id, domain_score, url_score, url_score_updated_at, ..., source)
        self.assertEqual(rows[CHANNEL][3], 1.455)
        self.assertIsNotNone(rows[CHANNEL][4])
        self.assertEqual(rows[CHANNEL][-1], SOURCE_YOUTUBE_CHANNEL)
        self.assertEqual(rows[VIDEO][3], 0.3)
        self.assertEqual(rows[VIDEO][-1], 0)
        self.assertEqual({r[-1] for r in captured["history_rows"]}, {0, SOURCE_YOUTUBE_CHANNEL})

    def test_on_conflict_only_raises_natural_or_channel_rows(self):
        _, captured = self._run([_link(CHANNEL, 1.455)])
        sql = captured["sql"]
        self.assertIn("anchor_text, source)", sql)
        guard = (
            "EXCLUDED.source = 4 AND url_state_current_064.source IN (0, 4) AND "
            "EXCLUDED.url_score > COALESCE(url_state_current_064.url_score, 0)"
        )
        for col in ("url_score", "url_score_updated_at", "source"):
            self.assertIn(
                f"{col} = CASE WHEN {guard}\n                THEN EXCLUDED.{col} "
                f"ELSE url_state_current_064.{col} END",
                sql,
            )


if __name__ == "__main__":
    unittest.main()

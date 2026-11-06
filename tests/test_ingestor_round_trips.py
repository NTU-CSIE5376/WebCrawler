"""The ingestor runs one statement at a time over a single connection, so every
extra round trip per shard per batch shows up directly in ingest lag. These
tests pin how many lookups each bulk path issues."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB


class _Cursor:
    """Answers SELECT url[, content_hash] from a fixed table of existing rows."""

    def __init__(self, existing=None, frozen=()):
        self.existing = dict(existing or {})
        self.frozen = set(frozen)
        self.sql = []
        self._rows = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if sql.startswith("SELECT url, content_hash FROM"):
            self._rows = [(u, self.existing[u]) for u in params[0] if u in self.existing]
        elif sql.startswith("SELECT url FROM"):
            self._rows = [(u,) for u in params[0] if u in self.existing]
        elif "discovery_frozen" in sql:
            self._rows = [(d,) for d in params[0] if d in self.frozen]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows

    def lookups(self):
        return [s for s in self.sql if s.startswith("SELECT url")]


class _Ranker:
    def __init__(self):
        self.calls = []

    def score_many(self, urls):
        self.calls.append(list(urls))
        return [0.3 for _ in urls]


def _link(url, domain_id=7):
    return {"url": url, "status": "new", "shard_id": 1, "domain_id": domain_id}


def _result(url, status="ok", content_hash="h-new"):
    return {
        "url": url, "status": status, "shard_id": 1, "domain_id": 7,
        "content_hash": content_hash if status == "ok" else None,
        "fail_reason": None if status == "ok" else "HttpError 404",
    }


def _fake_execute_values(captured):
    def fake(cur, sql, rows, page_size, fetch=False, template=None):
        captured.append((sql, rows))
        if fetch:
            if "RETURNING url\n" in sql:
                return [(r[0],) for r in rows]
            return [tuple(r) for r in rows]
        return None
    return fake


class BulkLinksRoundTripTest(unittest.TestCase):
    def _run(self, items, cur, ranker=None):
        db = IngestDB(Session=None, inline_ranker=ranker)
        captured = []
        with patch.object(ingest_db_ops, "execute_values", _fake_execute_values(captured)):
            out = db._bulk_links(cur=cur, shard_id=1, items=list(enumerate(items)))
        return dict(out), captured

    def test_inline_ranker_shares_the_existence_lookup(self):
        old, new = "https://a.example/old", "https://a.example/new"
        cur = _Cursor(existing={old: None})
        ranker = _Ranker()
        result, captured = self._run([_link(old), _link(new)], cur, ranker)
        self.assertEqual(len(cur.lookups()), 1)
        self.assertEqual(ranker.calls, [[new]])
        self.assertEqual(result, {0: False, 1: True})
        history = [rows for sql, rows in captured if "url_state_history" in sql]
        self.assertEqual([r[0] for r in history[0]], [new])

    def test_without_ranker_one_lookup(self):
        cur = _Cursor()
        result, _ = self._run([_link("https://a.example/x")], cur)
        self.assertEqual(len(cur.lookups()), 1)
        self.assertEqual(result, {0: True})

    def test_frozen_domains_are_cached_between_batches(self):
        db = IngestDB(Session=None)
        cur = _Cursor(frozen={9})
        self.assertEqual(db._frozen_domains(cur, {7, 9}), {9})
        self.assertEqual(db._frozen_domains(cur, {7, 9}), {9})
        frozen_queries = [s for s in cur.sql if "discovery_frozen" in s]
        self.assertEqual(len(frozen_queries), 1)
        # A domain not seen yet triggers a lookup for just that id.
        self.assertEqual(db._frozen_domains(cur, {7, 9, 11}), {9})
        self.assertEqual(len([s for s in cur.sql if "discovery_frozen" in s]), 2)

    def test_frozen_cache_expires(self):
        db = IngestDB(Session=None)
        cur = _Cursor(frozen={9})
        with patch.object(ingest_db_ops.time, "monotonic", return_value=1000.0):
            self.assertEqual(db._frozen_domains(cur, {9}), {9})
        cur.frozen = set()
        later = 1000.0 + ingest_db_ops.FROZEN_DOMAIN_CACHE_TTL_SEC
        with patch.object(ingest_db_ops.time, "monotonic", return_value=later):
            self.assertEqual(db._frozen_domains(cur, {9}), set())

    def test_frozen_links_are_dropped(self):
        cur = _Cursor(frozen={9})
        result, captured = self._run(
            [_link("https://f.example/a", domain_id=9), _link("https://a.example/b")], cur,
        )
        self.assertEqual(result, {0: False, 1: True})
        upsert_rows = [rows for sql, rows in captured if "RETURNING url\n" in sql][0]
        self.assertEqual([r[0] for r in upsert_rows], ["https://a.example/b"])


class BulkResultsRoundTripTest(unittest.TestCase):
    def test_one_lookup_for_hash_and_existence(self):
        same = "https://a.example/same"
        changed = "https://a.example/changed"
        fresh = "https://a.example/fresh"
        failed = "https://a.example/failed"
        cur = _Cursor(existing={same: "h-same", changed: "h-old", failed: None})
        db = IngestDB(Session=None)
        captured = []
        items = [
            _result(same, content_hash="h-same"),
            _result(changed),
            _result(fresh),
            _result(failed, status="fail"),
        ]
        with patch.object(ingest_db_ops, "execute_values", _fake_execute_values(captured)):
            out = dict(db._bulk_results_unique(cur, 1, list(enumerate(items))))
        self.assertEqual(len(cur.lookups()), 1)
        self.assertEqual(
            {i: (r.new_link, r.is_upd) for i, r in out.items()},
            {0: (False, False), 1: (False, True), 2: (True, True), 3: (False, False)},
        )


if __name__ == "__main__":
    unittest.main()

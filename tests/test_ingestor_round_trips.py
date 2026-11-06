"""The ingestor runs one statement at a time over a single connection, so every
extra round trip per shard per batch shows up directly in ingest lag. These
tests pin how many lookups each bulk path issues."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.ingestor import service as ingest_service
from containers.scheduler_ingest.ingestor.service import IngestService


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


class FolderBatchingRoundTripTest(unittest.TestCase):
    def test_accumulates_files_and_flushes_each_folder(self):
        db = MagicMock()
        db.aggregate_links.side_effect = IngestDB.aggregate_links
        batches = []

        def process_batch(batch):
            batches.append(batch)
            return [True] * len(batch)

        db.process_batch.side_effect = process_batch
        stats = MagicMock()
        with tempfile.TemporaryDirectory() as tmp, patch.object(ingest_service, "BATCH_SIZE", 4):
            root = Path(tmp)
            svc = IngestService(0, db, stats)
            for folder_id in range(2):
                folder = root / str(folder_id)
                folder.mkdir()
                for file_id in range(2):
                    recs = [
                        _link(f"https://a.example/{folder_id}/{file_id}/{i}")
                        for i in range(3)
                    ]
                    (folder / f"{file_id}.jsonl").write_text(
                        "".join(json.dumps(rec) + "\n" for rec in recs), encoding="utf-8",
                    )
                svc.process_folder(folder)
                # The two files combine into one full batch and a flushed tail;
                # that tail must never carry into the next folder.
                self.assertEqual(
                    [len(batch) for batch in batches], [4, 2] * (folder_id + 1),
                )
                self.assertEqual(stats.write.call_args.kwargs["counters"]["new_links"], 6)
        self.assertEqual(len({rec["url"] for batch in batches for rec in batch}), 12)

    def _process_folder(self, db, recs, batch_size=ingest_db_ops.BATCH_SIZE):
        stats = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "records.jsonl").write_text(
                "".join(json.dumps(rec) + "\n" for rec in recs), encoding="utf-8",
            )
            with patch.object(ingest_service, "BATCH_SIZE", batch_size):
                IngestService(ingestor_id=0, db=db, stats=stats).process_folder(folder)
        return stats.write.call_args.kwargs

    def test_interleaved_shards_fill_batches_and_reduce_database_calls(self):
        cur = _Cursor()
        session = MagicMock()
        session.connection.return_value.connection.cursor.return_value.__enter__.return_value = cur
        session_factory = MagicMock()
        session_factory.begin.return_value.__enter__.return_value = session
        ranker = _Ranker()
        db = IngestDB(Session=session_factory, inline_ranker=ranker)
        batch_size = ingest_db_ops.BATCH_SIZE
        recs = [
            {**_link(f"https://a.example/{sid}/{i}"), "shard_id": sid}
            for i in range(batch_size)
            for sid in range(16)
        ]
        captured = []
        with patch.object(ingest_db_ops, "execute_values", _fake_execute_values(captured)):
            stats = self._process_folder(db, recs)

        # Mixed-shard slicing would issue 256 lookups and scoring calls for
        # these 32,000 links. Full per-shard batches need only 16 of each.
        self.assertEqual(len(cur.lookups()), 16)
        self.assertEqual(len(ranker.calls), 16)
        self.assertTrue(all(len(urls) == batch_size for urls in ranker.calls))
        self.assertEqual(len(captured), 32)  # One current + history insert per shard.
        self.assertEqual(session_factory.begin.call_count, 16)
        scored_urls = [url for urls in ranker.calls for url in urls]
        self.assertCountEqual(scored_urls, [rec["url"] for rec in recs])
        self.assertEqual(stats["counters"]["new_links"], len(recs))

    def test_preserves_discovery_before_results_and_order_within_shards(self):
        db = MagicMock()
        db.aggregate_links.side_effect = IngestDB.aggregate_links
        batches = []

        def process_batch(batch):
            batches.append(batch)
            return [True if rec["status"] == "new" else None for rec in batch]

        db.process_batch.side_effect = process_batch
        shared = "https://a.example/shared"
        links = [
            {**_link(shared), "shard_id": 1},
            {**_link("https://a.example/other"), "shard_id": 2},
            {**_link(shared), "shard_id": 1},
            {**_link("https://a.example/extra-1"), "shard_id": 1},
            {**_link("https://a.example/extra-2"), "shard_id": 1},
        ]
        results = [
            {**_result(shared), "sequence": 0},
            {**_result("https://a.example/other"), "shard_id": 2, "sequence": 1},
            {**_result(shared, status="fail"), "sequence": 2},
            {**_result(shared), "sequence": 3},
        ]
        stats = self._process_folder(db, [*results, *links], batch_size=2)

        self.assertEqual([len(batch) for batch in batches], [2, 1, 1, 2, 1, 1])
        self.assertTrue(all(len({rec["shard_id"] for rec in batch}) == 1 for batch in batches))
        flattened = [rec for batch in batches for rec in batch]
        self.assertTrue(all(rec["status"] == "new" for rec in flattened[:4]))
        self.assertTrue(all(rec["status"] != "new" for rec in flattened[4:]))
        self.assertEqual(flattened[0]["inlink_count_approx"], 2)
        for sid in (1, 2):
            self.assertEqual(
                [rec["sequence"] for rec in flattened[4:] if rec["shard_id"] == sid],
                [rec["sequence"] for rec in results if rec["shard_id"] == sid],
            )
        self.assertEqual(stats["counters"]["new_links"], 4)


if __name__ == "__main__":
    unittest.main()

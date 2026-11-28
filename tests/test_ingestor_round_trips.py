"""The ingestor runs one statement at a time over a single connection, so every
extra round trip per shard per batch shows up directly in ingest lag. These
tests pin how many lookups each bulk path issues."""
from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from collections import Counter
from unittest.mock import MagicMock, patch

from containers.scheduler_ingest.ingestor.libs import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.libs.db_ops import IngestDB
from containers.scheduler_ingest.ingestor.discovery_ingestor.service import DiscoveryIngestor
from containers.scheduler_ingest.ingestor.content_ingestor.service import ContentIngestor
from containers.scheduler_ingest.tests.helpers.parquet_records import write_records
from containers.scheduler_ingest.ingestor.libs.worker import Checkpoint, ShardWriter


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
    def test_accumulates_files_and_flushes_before_advancing_checkpoint(self):
        db, stats = MagicMock(), MagicMock()
        batches = []
        db.process_batch.side_effect = lambda rows: batches.append(rows) or [True]*len(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checkpoint = Checkpoint(root/'discovery.json', root/'legacy.json', root/'content.json')
            svc = DiscoveryIngestor(ShardWriter(db, 0, 16, root/'locks', True), stats,
                                   checkpoint, distinct_url_limit=4)
            for folder_id in range(2):
                folder = root / str(folder_id)
                for file_id in range(2):
                    write_records(folder, [_link(f"https://a.example/{folder_id}/{file_id}/{i}")
                                           for i in range(3)])
                svc.process_folder(folder, '20261128', f'{folder_id*10:04d}')
                # Full batches flush early; discovery tails may span folders.
                if folder_id == 0:
                    self.assertEqual([len(b) for b in batches], [4])
                    self.assertIsNone(checkpoint.progress.state['date'])
            svc.close()
            self.assertEqual([len(b) for b in batches], [4, 4, 4])
            self.assertTrue(checkpoint.progress.seen('20261128', '0010'))
        self.assertEqual(len({r['url'] for b in batches for r in b}), 12)

    def _process_folder(self, db, recs, batch_size=ingest_db_ops.BATCH_SIZE):
        stats = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root/'input'
            write_records(folder, recs)
            checkpoint = Checkpoint(root/'discovery.json', root/'legacy.json', root/'content.json')
            svc = DiscoveryIngestor(ShardWriter(db, 0, 16, root/'locks', True), stats,
                                   checkpoint, distinct_url_limit=batch_size)
            svc.process_folder(folder, '20261128', '0000')
            svc.close()
        counters = Counter()
        for call in stats.write.call_args_list:
            counters.update(call.kwargs['counters'])
        return {'counters': counters}

    def test_interleaved_shards_fill_batches_and_reduce_database_calls(self):
        cur = _Cursor()
        session = MagicMock()
        session.connection.return_value.connection.cursor.return_value.__enter__.return_value = cur
        session_factory = MagicMock()
        session_factory.return_value = session
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
        self.assertEqual(session_factory.call_count, 16)
        self.assertEqual(session.commit.call_count, 16)
        scored_urls = [url for urls in ranker.calls for url in urls]
        self.assertCountEqual(scored_urls, [rec["url"] for rec in recs])
        self.assertEqual(stats["counters"]["new_links"], len(recs))

    def test_content_can_finish_before_discovery_without_reordering_results(self):
        db = MagicMock()
        batches = []
        db.process_batch.side_effect = lambda rows: batches.append(rows) or []
        shared = 'https://a.example/shared'
        links = [_link(shared), _link(shared), _link('https://a.example/other')]
        results = [{**_result(shared, status=status), 'title': str(i)}
                   for i, status in enumerate(['ok', 'fail', 'ok'])]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_records(root/'input', [*results, *links])
            content_checkpoint = Checkpoint(root/'content.json', root/'legacy.json', root/'discovery.json')
            discovery_checkpoint = Checkpoint(root/'discovery.json', root/'legacy.json', root/'content.json')
            content = ContentIngestor(ShardWriter(db, 0, 16, root/'locks', False), MagicMock(),
                                     content_checkpoint, batch_size=2)
            content.process_folder(root/'input', '20261128', '0000')
            self.assertEqual([r['title'] for b in batches for r in b], ['0', '1', '2'])
            self.assertIsNone(discovery_checkpoint.progress.state['date'])
            discovery = DiscoveryIngestor(ShardWriter(db, 0, 16, root/'locks', True), MagicMock(),
                                         discovery_checkpoint)
            discovery.process_folder(root/'input', '20261128', '0000')
            discovery.close()
        self.assertEqual([len(b) for b in batches], [2, 1, 2])
        self.assertEqual(next(r for r in batches[-1] if r['url'] == shared)['inlink_count_approx'], 2)


if __name__ == "__main__":
    unittest.main()

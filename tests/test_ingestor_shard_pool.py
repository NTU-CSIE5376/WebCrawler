"""ShardPool writes a folder's shards in worker processes. These tests run real
spawned workers against a fake DB that logs every batch to disk, and check
that the parallel path matches the in-process path."""
from __future__ import annotations

import functools
import json
import os
import time
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from containers.scheduler_ingest.ingestor import main as ingest_main
from containers.scheduler_ingest.ingestor.db_ops import IngestResult
from containers.scheduler_ingest.ingestor.service import IngestService, merge_counts
from containers.scheduler_ingest.ingestor.shard_pool import ShardPool


class _FakeDB:
    """Links report new for even URLs; results are ok unless status=fail.
    Each batch is appended to <log_dir>/<pid>.jsonl."""

    def __init__(self, log_dir, fail_shard=None, crash_shard=None):
        self.log_dir = Path(log_dir)
        self.fail_shard = fail_shard
        self.crash_shard = crash_shard

    def process_batch(self, batch):
        shard_id = batch[0]["shard_id"]
        with open(self.log_dir / f"{os.getpid()}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps({"shard_id": shard_id, "recs": batch}) + "\n")
        if shard_id == self.crash_shard:
            os._exit(1)
        # Long enough that a just-started worker cannot take every shard.
        time.sleep(0.2)
        if shard_id == self.fail_shard:
            raise RuntimeError("boom")
        out = []
        for rec in batch:
            if rec["status"] == "new":
                out.append(int(rec["url"].rsplit("/", 1)[1]) % 2 == 0)
            else:
                out.append(IngestResult(
                    new_link=False,
                    domain_id=rec["domain_id"],
                    is_ok=rec["status"] == "ok",
                    is_upd=rec["status"] == "ok",
                    fail_reason=None if rec["status"] == "ok" else "http_5xx",
                ))
        return out


def _make_db(log_dir, fail_shard=None, crash_shard=None):
    return _FakeDB(log_dir, fail_shard, crash_shard)


def _records(shards=6):
    recs = []
    for sid in range(shards):
        # Uneven sizes so the pool has big and small shards.
        for i in range(3 + 4 * sid):
            url = f"https://s{sid}.example/{i}"
            recs.append({"url": url, "status": "new", "shard_id": sid, "domain_id": sid})
            if i % 3 == 0:
                recs.append({
                    "url": url, "status": "fail" if i % 2 else "ok",
                    "shard_id": sid, "domain_id": sid, "seq": i,
                })
    return recs


def _read_log(log_dir):
    by_pid = {}
    for f in Path(log_dir).glob("*.jsonl"):
        by_pid[f.stem] = [json.loads(l) for l in f.read_text().splitlines()]
    return by_pid


class ShardPoolTest(unittest.TestCase):
    def _run(self, recs, workers, **db_kw):
        """Process one folder; return (stats kwargs, batch log by pid, shard_done logs)."""
        with tempfile.TemporaryDirectory() as tmp:
            folder, log_dir = Path(tmp, "folder"), Path(tmp, "log")
            folder.mkdir()
            log_dir.mkdir()
            (folder / "r.jsonl").write_text(
                "".join(json.dumps(r) + "\n" for r in recs), encoding="utf-8",
            )
            factory = functools.partial(_make_db, str(log_dir), **db_kw)
            pool = ShardPool(workers, factory, 0) if workers > 1 else None
            db = None if pool else factory()
            stats = MagicMock()
            try:
                with self.assertLogs("ingestor", level="INFO") as logs:
                    IngestService(0, db, stats, shard_pool=pool).process_folder(folder)
            finally:
                if pool:
                    pool.shutdown()
            shard_done = [
                r for r in logs.records if getattr(r, "event", None) == "ingest.shard_done"
            ]
            return stats.write.call_args.kwargs, _read_log(log_dir), shard_done

    def test_parallel_matches_in_process(self):
        recs = _records()
        seq, _, seq_done = self._run(recs, workers=1)
        par, by_pid, par_done = self._run(recs, workers=3)

        self.assertEqual(json.loads(json.dumps(par["counters"])),
                         json.loads(json.dumps(seq["counters"])))
        self.assertEqual(json.loads(json.dumps(par["domains"])),
                         json.loads(json.dumps(seq["domains"])))
        self.assertGreater(par["counters"]["new_links"], 0)
        self.assertGreater(par["counters"]["num_fetch_fail"], 0)
        self.assertNotIn(os.getpid(), map(int, by_pid))  # Parent wrote nothing.

        shard_pids = {}
        for pid, batches in by_pid.items():
            for b in batches:
                shard_pids.setdefault(b["shard_id"], set()).add(pid)
            # Inside each shard a worker writes links before results, and
            # results in their original order.
            for sid in {b["shard_id"] for b in batches}:
                recs_in_shard = [r for b in batches if b["shard_id"] == sid for r in b["recs"]]
                kinds = [r["status"] == "new" for r in recs_in_shard]
                self.assertEqual(kinds, sorted(kinds, reverse=True))
                self.assertEqual(
                    [r["seq"] for r in recs_in_shard if "seq" in r],
                    [r["seq"] for r in recs if r["shard_id"] == sid and "seq" in r],
                )
        self.assertTrue(all(len(p) == 1 for p in shard_pids.values()))
        self.assertGreater(len(by_pid), 1)

        for done in (seq_done, par_done):
            self.assertEqual([r.shard_id for r in done], list(range(6)))
            self.assertEqual(
                [(r.links, r.results) for r in done],
                [(3 + 4 * s, len(range(0, 3 + 4 * s, 3))) for s in range(6)],
            )

    def test_failing_shard_does_not_block_others(self):
        recs = _records()
        out, _, done = self._run(recs, workers=3, fail_shard=2)
        shard2 = sum(1 for r in recs if r["shard_id"] == 2)
        self.assertEqual(out["counters"]["error_count"], shard2)
        healthy = [r for r in recs if r["shard_id"] != 2 and r["status"] != "new"]
        self.assertEqual(
            out["counters"]["num_fetch_ok"] + out["counters"]["num_fetch_fail"], len(healthy),
        )
        self.assertEqual({r.shard_id: r.errors for r in done}[2], shard2)

    def test_worker_crash_propagates(self):
        # The folder must not be marked done when a worker dies mid-write.
        with self.assertRaises(BrokenProcessPool):
            self._run(_records(), workers=2, crash_shard=1)


class MergeCountsTest(unittest.TestCase):
    def test_adds_nested_counters(self):
        dst = {"a": 1, "fail_reasons": {"x": 1}}
        merge_counts(dst, {"a": 2, "b": 3, "fail_reasons": {"x": 1, "y": 4}})
        self.assertEqual(dst, {"a": 3, "b": 3, "fail_reasons": {"x": 2, "y": 4}})


class ShardWorkersConfigTest(unittest.TestCase):
    def test_config_and_env_override(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("INGESTOR_SHARD_WORKERS", None)
            self.assertEqual(ingest_main._shard_workers({}), 1)
            self.assertEqual(ingest_main._shard_workers({"ingestor": {"shard_workers": 4}}), 4)
            os.environ["INGESTOR_SHARD_WORKERS"] = "6"
            self.assertEqual(ingest_main._shard_workers({"ingestor": {"shard_workers": 4}}), 6)
            os.environ["INGESTOR_SHARD_WORKERS"] = "0"
            self.assertEqual(ingest_main._shard_workers({}), 1)


if __name__ == "__main__":
    unittest.main()

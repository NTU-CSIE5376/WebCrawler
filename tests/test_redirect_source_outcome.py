"""A fetch result reported under a different URL than the one the offerer
queued (redirect target, canonicalized form) must still close out the queued
row. Otherwise it stays should_crawl=FALSE with last_fetch_ok NULL forever,
and its domain looks like a black hole in domain_stats_daily (e.g. every
sayweee.com URL redirects to weee.com)."""
from __future__ import annotations

import json
import tempfile
import time
import unittest
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB, IngestResult
from containers.scheduler_ingest.ingestor.service import IngestService
from containers.scheduler_ingest.router import service as router_service
from containers.scheduler_ingest.router.service import RouterConfig, RouterService
from libs.db.sharding.router import ShardRouter


# ---------------------------------------------------------------- router

class _FakeResolver:
    ids = {"sayweee.com": 11, "weee.com": 22, "example.com": 33, "pacificleague.com": 44}

    def __init__(self, sess, cache):
        pass

    def ensure_and_get(self, domain, shard_id):
        return self.ids[domain], False

    def batch_ensure_and_get(self, domains, *, batch_size=1000):
        return {d: (self.ids[d], 0.0) for d in domains}


class _FakeSess:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def begin(self):
        return self


def _router(tmp: str) -> RouterService:
    svc = RouterService.__new__(RouterService)
    svc.cfg = RouterConfig(
        router_id=0,
        crawler_dir_template=str(Path(tmp) / "crawler_{id:02d}"),
        ingestor_dir_template=str(Path(tmp) / "ingestor_{id:02d}"),
        progress_template="",
        stats_dir=str(Path(tmp) / "stats"),
        interval_minutes=10,
        scan_sleep_minutes=1,
        num_shards=256,
        shards_per_ingestor=16,
        domain_overrides={},
        split_subdomains=set(),
        postgres_dsn="",
    )
    svc.sharder = ShardRouter(num_shards=256, shards_per_ingestor=16, domain_overrides={})
    svc.Session = _FakeSess
    svc.stats = SimpleNamespace(write=lambda **kw: None)
    svc._domain_cache = {}
    svc._domain_cache_born = time.monotonic()
    svc._made_dirs = set()
    return svc


def _route(recs: list[dict]) -> list[dict]:
    with tempfile.TemporaryDirectory() as tmp:
        svc = _router(tmp)
        folder = Path(tmp) / "in"
        folder.mkdir()
        (folder / "0000.jsonl").write_text("\n".join(json.dumps(r) for r in recs) + "\n")
        with patch.object(router_service, "DomainResolver", _FakeResolver):
            svc.process_folder(folder)
        out = []
        for p in sorted(Path(tmp).glob("ingestor_*/**/*.jsonl")):
            out.extend(json.loads(line) for line in p.read_text().splitlines())
        return out


class RouterRedirectSourceTest(unittest.TestCase):
    def test_cross_domain_redirect_emits_source_record(self):
        out = _route([{
            "url": "https://www.weee.com/vi/explore/gao-vang",
            "source_url": "https://www.sayweee.com/vi/explore/gao-vang",
            "status": "ok",
            "content": "<html/>",
            "outlinks": [],
            "is_redirect": True,
            "redirect_hop_count": 1,
        }])
        by_url = {r["url"]: r for r in out}
        self.assertEqual(set(by_url), {
            "https://www.weee.com/vi/explore/gao-vang",
            "https://www.sayweee.com/vi/explore/gao-vang",
        })
        final = by_url["https://www.weee.com/vi/explore/gao-vang"]
        self.assertEqual(final["domain_id"], 22)
        self.assertNotIn("redirect_source", final)

        src = by_url["https://www.sayweee.com/vi/explore/gao-vang"]
        sharder = ShardRouter(num_shards=256, shards_per_ingestor=16, domain_overrides={})
        self.assertEqual(src["shard_id"], sharder.domain_to_shard("www.sayweee.com"))
        self.assertEqual(src["domain_id"], 11)
        self.assertEqual(src["status"], "ok")
        self.assertIsNone(src["content"])
        self.assertIsNone(src["content_hash"])
        self.assertEqual(src["outlinks"], [])
        self.assertTrue(src["is_redirect"])
        self.assertTrue(src["redirect_source"])
        self.assertTrue(src["count_domain_stats"])
        self.assertNotIn("_ingestor_id", src)

    def test_same_domain_redirect_not_counted_twice_per_domain(self):
        out = _route([{
            "url": "https://www.example.com/b",
            "source_url": "https://fr.example.com/b",
            "status": "fail",
            "fail_reason": "HttpError 404",
            "content": None,
            "outlinks": [],
            "is_redirect": True,
        }])
        src = next(r for r in out if r.get("redirect_source"))
        self.assertEqual(src["status"], "fail")
        self.assertEqual(src["fail_reason"], "HttpError 404")
        self.assertFalse(src["count_domain_stats"])

    def test_deduped_redirect_emits_only_source_record(self):
        out = _route([{
            "url": "https://pacificleague.com/maintenance/",
            "source_url": "https://pacificleague.com/news/1",
            "status": "ok",
            "content": None,
            "outlinks": [],
            "is_redirect": True,
            "redirect_deduped": True,
        }])
        # pacificleague.com resolves via the fake resolver's default below.
        self.assertEqual([r["url"] for r in out], ["https://pacificleague.com/news/1"])
        self.assertEqual(out[0]["status"], "ok")
        self.assertTrue(out[0]["count_domain_stats"])

    def test_switch_off_emits_only_final_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            svc = _router(tmp)
            svc.cfg = svc.cfg.__class__(**{**svc.cfg.__dict__, "emit_redirect_source": False})
            folder = Path(tmp) / "in"
            folder.mkdir()
            (folder / "0000.jsonl").write_text(json.dumps({
                "url": "https://www.weee.com/a", "source_url": "https://www.sayweee.com/a",
                "status": "ok", "content": "c", "outlinks": [],
            }) + "\n")
            with patch.object(router_service, "DomainResolver", _FakeResolver):
                svc.process_folder(folder)
            out = [json.loads(l) for p in Path(tmp).glob("ingestor_*/**/*.jsonl")
                   for l in p.read_text().splitlines()]
        self.assertEqual([r["url"] for r in out], ["https://www.weee.com/a"])

    def test_no_source_record_without_source_url(self):
        out = _route([{"url": "https://example.com/a", "status": "ok", "content": "c", "outlinks": []}])
        self.assertEqual(len(out), 1)
        self.assertNotIn("redirect_source", out[0])


# ---------------------------------------------------------------- ingestor

class _Cur:
    def __init__(self):
        self.sql = []
        self.calls = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self.calls.append((sql, params))

    def domain_updates(self) -> tuple[set[int], set[int]]:
        """(paused domain_ids, reset domain_ids) from the domain_state UPDATEs."""
        paused, reset = set(), set()
        for sql, params in self.calls:
            if "UPDATE domain_state" not in sql:
                continue
            ids = set(params[0])
            if "crawl_paused_until = NOW()" in sql:
                paused |= ids
            elif "crawl_paused_until = NULL" in sql:
                reset |= ids
        return paused, reset

    def fetchall(self):
        return []


class IngestRedirectSourceTest(unittest.TestCase):
    def _ingest(self, recs):
        db = IngestDB.__new__(IngestDB)
        items = list(enumerate(recs))
        with patch.object(ingest_db_ops, "execute_values", return_value=[]), \
             patch.object(IngestDB, "_existing_urls", return_value={r["url"] for r in recs}):
            return [ir for _, ir in db._bulk_results_unique(_Cur(), 5, items)]

    def _rec(self, url, **kw):
        return {"url": url, "status": "ok", "domain_id": 11, "shard_id": 5, **kw}

    def test_flags_on_ingest_results(self):
        normal, cross, same = self._ingest([
            self._rec("https://a/1"),
            self._rec("https://a/2", redirect_source=True, count_domain_stats=True),
            self._rec("https://a/3", redirect_source=True, count_domain_stats=False),
        ])
        self.assertEqual((normal.count_global, normal.count_domain), (True, True))
        self.assertEqual((cross.count_global, cross.count_domain), (False, True))
        self.assertEqual((same.count_global, same.count_domain), (False, False))
        self.assertTrue(cross.is_ok)

    def test_accumulate_respects_flags(self):
        svc = IngestService.__new__(IngestService)
        counters, domains = defaultdict(int), {}
        svc._accumulate_result(IngestResult(domain_id=22, is_ok=True), counters, domains)
        svc._accumulate_result(
            IngestResult(domain_id=11, is_ok=True, count_global=False, count_domain=True),
            counters, domains,
        )
        svc._accumulate_result(
            IngestResult(domain_id=33, is_ok=False, fail_reason="HttpError 404",
                         count_global=False, count_domain=False),
            counters, domains,
        )
        self.assertEqual(counters["num_fetch_ok"], 1)
        self.assertEqual(counters["num_fetch_fail"], 0)
        self.assertEqual(counters["num_redirect_source"], 2)
        self.assertNotIn("fail_reasons", counters)
        self.assertEqual(domains[22]["num_fetch_ok"], 1)
        self.assertEqual(domains[11]["num_fetch_ok"], 1)
        self.assertNotIn(33, domains)


def _ingest_routed(routed: list[dict]) -> _Cur:
    """Feed router output through IngestDB._bulk_results_unique per shard, as
    the ingestor does, and return the cursor that saw every statement."""
    cur = _Cur()
    db = IngestDB.__new__(IngestDB)
    by_shard: dict[int, list] = defaultdict(list)
    for i, rec in enumerate(routed):
        by_shard[int(rec["shard_id"])].append((i, rec))
    with patch.object(ingest_db_ops, "execute_values", return_value=[]), \
         patch.object(IngestDB, "_existing_urls", return_value=set()):
        for sid, items in by_shard.items():
            db._bulk_results_unique(cur, sid, items)
    return cur


class RedirectSourceDomainPauseTest(unittest.TestCase):
    """A -> B cross-domain redirect: B's failure must pause only B, and the
    synthetic record for A must neither pause nor reset A."""

    SOURCE = "https://www.sayweee.com/vi/explore/gao-vang"  # domain_id 11
    TARGET = "https://www.weee.com/vi/explore/gao-vang"     # domain_id 22

    def _redirect(self, **kw) -> list[dict]:
        return _route([{
            "url": self.TARGET,
            "source_url": self.SOURCE,
            "outlinks": [],
            "is_redirect": True,
            "redirect_hop_count": 1,
            **kw,
        }])

    def test_target_failure_pauses_only_target_domain(self):
        for reason in ("HttpError 403", "HttpError 429", "HttpError 410"):
            with self.subTest(reason=reason):
                routed = self._redirect(status="fail", fail_reason=reason, content=None)
                src = next(r for r in routed if r.get("redirect_source"))
                # The cross-domain statistics are still credited to A.
                self.assertEqual(src["domain_id"], 11)
                self.assertTrue(src["count_domain_stats"])

                paused, reset = _ingest_routed(routed).domain_updates()
                self.assertEqual(paused, {22})
                self.assertEqual(reset, set())

    def test_target_failure_still_counts_for_source_domain_stats(self):
        routed = self._redirect(status="fail", fail_reason="HttpError 403", content=None)
        src = next(r for r in routed if r.get("redirect_source"))
        db = IngestDB.__new__(IngestDB)
        with patch.object(ingest_db_ops, "execute_values", return_value=[]), \
             patch.object(IngestDB, "_existing_urls", return_value=set()):
            ((_, ir),) = db._bulk_results_unique(_Cur(), src["shard_id"], [(0, src)])
        self.assertEqual((ir.domain_id, ir.is_ok, ir.count_global, ir.count_domain),
                         (11, False, False, True))
        self.assertEqual(ir.fail_reason, "HttpError 403")

    def test_successful_source_outcome_does_not_reset_source_pause(self):
        routed = self._redirect(status="ok", content="<html/>")
        paused, reset = _ingest_routed(routed).domain_updates()
        self.assertEqual(paused, set())
        # Only the host that actually answered may clear its own pause.
        self.assertEqual(reset, {22})

    def test_deduped_source_outcome_does_not_reset_source_pause(self):
        routed = _route([{
            "url": self.TARGET, "source_url": self.SOURCE, "status": "ok",
            "content": None, "outlinks": [], "is_redirect": True,
            "redirect_deduped": True,
        }])
        self.assertEqual([r["url"] for r in routed], [self.SOURCE])
        paused, reset = _ingest_routed(routed).domain_updates()
        self.assertEqual((paused, reset), (set(), set()))

    def test_direct_fetch_still_pauses_and_resets(self):
        cur = _ingest_routed(_route([
            {"url": "https://www.sayweee.com/a", "status": "fail",
             "fail_reason": "HttpError 403", "content": None, "outlinks": []},
            {"url": "https://www.weee.com/a", "status": "ok",
             "content": "c", "outlinks": []},
        ]))
        self.assertEqual(cur.domain_updates(), ({11}, {22}))


if __name__ == "__main__":
    unittest.main()

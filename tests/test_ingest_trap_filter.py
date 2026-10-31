"""Ingest-side trap filter: newly discovered links are normalized (tracking /
named session params stripped) and dropped when they hit a high-confidence
trap rule; every changed or dropped link is written to the audit log."""
from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.ingestor.service import IngestService
from containers.scheduler_ingest.ingestor.trap_filter import (
    TrapFilter,
    TrapFilterConfig,
    read_audit_log,
)

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)


def _link(url, **kw):
    return {"url": url, "status": "new", "shard_id": 3, "domain_id": 7,
            "discovered_from": "https://example.com/", **kw}


def _filter(log_dir=None, **kw):
    return TrapFilter(TrapFilterConfig(log_dir=log_dir, **kw), now=lambda: NOW)


class TrapFilterTest(unittest.TestCase):
    def test_clean_link_passes_untouched(self):
        rec = _link("https://example.com/news/2026/10/31/story")
        kept, stats = _filter().apply([rec])
        self.assertEqual(kept, [rec])
        self.assertEqual((stats.dropped, stats.normalized), (0, 0))

    def test_high_confidence_trap_is_dropped(self):
        kept, stats = _filter().apply([
            _link("https://mipt.ru/event/abitu.net/profile/abitu.net/profile/abitu.net/profile/"),
            _link("https://example.com/login?returnUrl=https%3A%2F%2Fexample.com%2Fa%3Fb%3D1"),
        ])
        self.assertEqual(kept, [])
        self.assertEqual(stats.dropped, 2)
        self.assertEqual(stats.by_rule["A_path_repeat"], 1)
        self.assertEqual(stats.by_rule["B_nested_redirect"], 1)

    def test_low_confidence_rules_do_not_drop(self):
        url = "https://example.com/p?a=1&b=2&c=3&d=4&e=5&f=6"   # B_many_params only
        kept, stats = _filter().apply([_link(url)])
        self.assertEqual([r["url"] for r in kept], [url])
        self.assertEqual(stats.dropped, 0)

    def test_tracking_and_session_params_are_stripped(self):
        kept, stats = _filter().apply([
            _link("https://example.com/a?id=5&utm_source=x&srsltid=abc"),
            _link("https://example.com/b;jsessionid=ABC123?x=1"),
        ])
        self.assertEqual([r["url"] for r in kept],
                         ["https://example.com/a?id=5", "https://example.com/b?x=1"])
        self.assertEqual(stats.normalized, 2)
        # Other fields are carried over unchanged.
        self.assertEqual(kept[0]["domain_id"], 7)
        self.assertEqual(kept[0]["discovered_from"], "https://example.com/")

    def test_generic_session_token_kept_by_default(self):
        url = "https://forum.example.com/viewtopic.php?t=1&sid=0123456789abcdef0123456789abcdef"
        kept, _ = _filter().apply([_link(url)])
        self.assertEqual(kept[0]["url"], url)
        kept, _ = _filter(normalize_generic_session_tokens=True).apply([_link(url)])
        self.assertEqual(kept[0]["url"], "https://forum.example.com/viewtopic.php?t=1")

    def test_rules_run_on_normalized_url(self):
        # Trap after stripping: still dropped, and the log records both forms.
        url = "https://example.com/x/x/x/page?utm_source=a"
        with tempfile.TemporaryDirectory() as d:
            kept, stats = _filter(log_dir=d).apply([_link(url)])
            events = list(read_audit_log(Path(d)))
        self.assertEqual(kept, [])
        self.assertEqual(stats.dropped, 1)
        self.assertEqual(events[0]["action"], "drop")
        self.assertEqual(events[0]["url"], url)
        self.assertEqual(events[0]["normalized_url"], "https://example.com/x/x/x/page")
        self.assertEqual(events[0]["rules"], ["A_path_repeat"])

    def test_disabled_steps(self):
        recs = [_link("https://example.com/a?utm_source=x"),
                _link("https://example.com/x/x/x/")]
        kept, stats = _filter(drop=False).apply(recs)
        self.assertEqual(len(kept), 2)
        self.assertEqual(stats.normalized, 1)
        kept, stats = _filter(normalize=False).apply(recs)
        self.assertEqual([r["url"] for r in kept], ["https://example.com/a?utm_source=x"])
        kept, stats = _filter(enabled=False).apply(recs)
        self.assertEqual(kept, recs)

    def test_audit_log_is_daily_gzip_jsonl_and_appends(self):
        with tempfile.TemporaryDirectory() as d:
            f = _filter(log_dir=d)
            f.apply([_link("https://example.com/a?gclid=1")])
            f.apply([_link("https://example.com/x/x/x/"), _link("https://example.com/ok")])
            path = Path(d) / "2026-10-31.jsonl.gz"
            self.assertTrue(path.exists())
            with gzip.open(path, "rt") as fh:
                events = [json.loads(line) for line in fh]
        self.assertEqual([e["action"] for e in events], ["normalize", "drop"])
        self.assertEqual(events[0], {
            "ts": "2026-10-31T12:00:00+00:00", "action": "normalize",
            "url": "https://example.com/a?gclid=1",
            "normalized_url": "https://example.com/a",
            "domain_id": 7, "shard_id": 3,
            "discovered_from": "https://example.com/",
        })
        self.assertNotIn("normalized_url", events[1])

    def test_log_write_failure_does_not_block_ingest(self):
        with tempfile.TemporaryDirectory() as d:
            blocker = Path(d) / "file"
            blocker.write_text("")
            kept, stats = _filter(log_dir=str(blocker / "sub")).apply(
                [_link("https://example.com/x/x/x/")])
        self.assertEqual(kept, [])
        self.assertEqual(stats.dropped, 1)


class _FakeDB:
    def __init__(self):
        self.batches = []

    aggregate_links = staticmethod(IngestDB.aggregate_links)

    def process_batch(self, batch):
        self.batches.append(batch)
        return [True] * len(batch)


class _FakeStats:
    def write(self, **kw):
        self.last = kw


class IngestServiceTrapFilterTest(unittest.TestCase):
    def test_process_folder_filters_links_but_not_results(self):
        recs = [
            _link("https://example.com/a?utm_source=x", inlink_count_approx=1),
            _link("https://example.com/a?gclid=2", inlink_count_approx=1),
            _link("https://example.com/x/x/x/"),
            {"url": "https://example.com/x/x/x/?utm_source=y", "status": "ok",
             "shard_id": 3, "domain_id": 7},
        ]
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d) / "in"
            folder.mkdir()
            (folder / "part.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
            db = _FakeDB()
            svc = IngestService(1, db, _FakeStats(), trap_filter=_filter(log_dir=str(Path(d) / "log")))
            svc.process_folder(folder)
            events = list(read_audit_log(Path(d) / "log"))
        links, results = db.batches
        # Both tracking variants collapse into one row with summed inlinks.
        self.assertEqual([(r["url"], r["inlink_count_approx"]) for r in links],
                         [("https://example.com/a", 2)])
        # Crawl results keep their exact URL, even when it looks like a trap.
        self.assertEqual([r["url"] for r in results],
                         ["https://example.com/x/x/x/?utm_source=y"])
        self.assertEqual(sorted(e["action"] for e in events), ["drop", "normalize", "normalize"])


class GoldenMatchTest(unittest.TestCase):
    def test_golden_with_srsltid_matches_clean_discovered_link(self):
        from scripts.trap_filter_golden_check import golden_index, match_golden
        golden = "https://shop.example.com/p/1?srsltid=AfmBOoq"
        index = golden_index({golden})
        # Crawler discovers the clean URL and drops it: still a golden kill.
        self.assertEqual(match_golden({"url": "https://shop.example.com/p/1"}, index), golden)
        # Discovered with a different srsltid value, then normalized.
        self.assertEqual(match_golden({"url": "https://shop.example.com/p/1?srsltid=zz",
                                       "normalized_url": "https://shop.example.com/p/1"}, index),
                         golden)
        self.assertIsNone(match_golden({"url": "https://shop.example.com/p/2"}, index))


if __name__ == "__main__":
    unittest.main()

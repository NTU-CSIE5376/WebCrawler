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

    def test_only_srsltid_is_stripped_by_default(self):
        kept, stats = _filter().apply([
            _link("https://example.com/a?id=5&srsltid=abc"),
            _link("https://example.com/b;jsessionid=ABC123?x=1"),
            _link("https://example.com/c?PHPSESSID=f00&t=2"),
        ])
        self.assertEqual([r["url"] for r in kept], [
            "https://example.com/a?id=5", "https://example.com/b;jsessionid=ABC123?x=1",
            "https://example.com/c?PHPSESSID=f00&t=2",
        ])
        # Session ids are only shadow-logged.
        self.assertEqual((stats.normalized, stats.would_normalize), (1, 2))
        # Other fields are carried over unchanged.
        self.assertEqual(kept[0]["domain_id"], 7)
        self.assertEqual(kept[0]["discovered_from"], "https://example.com/")

    def test_other_params_are_shadow_logged_only(self):
        urls = [
            "https://example.com/a?id=5&utm_source=x",
            "https://example.com/b?gclid=1",
            "https://shop.example.com/p?oscsid=abc",
            "https://forum.example.com/t.php?t=1&sid=0123456789abcdef0123456789abcdef",
        ]
        with tempfile.TemporaryDirectory() as d:
            kept, stats = _filter(log_dir=d).apply([_link(u) for u in urls])
            events = list(read_audit_log(Path(d)))
        self.assertEqual([r["url"] for r in kept], urls)
        self.assertEqual((stats.normalized, stats.would_normalize), (0, 4))
        self.assertEqual({e["action"] for e in events}, {"would_normalize"})
        self.assertEqual([e["would_normalize_url"] for e in events], [
            "https://example.com/a?id=5", "https://example.com/b",
            "https://shop.example.com/p", "https://forum.example.com/t.php?t=1",
        ])
        self.assertTrue(all("normalized_url" not in e for e in events))

    def test_applied_and_shadow_on_same_link(self):
        url = "https://example.com/a?srsltid=x&utm_source=y&id=1"
        with tempfile.TemporaryDirectory() as d:
            kept, stats = _filter(log_dir=d).apply([_link(url)])
            (event,) = read_audit_log(Path(d))
        self.assertEqual(kept[0]["url"], "https://example.com/a?utm_source=y&id=1")
        self.assertEqual((stats.normalized, stats.would_normalize), (1, 1))
        self.assertEqual(event["action"], "normalize")
        self.assertEqual(event["normalized_url"], "https://example.com/a?utm_source=y&id=1")
        self.assertEqual(event["would_normalize_url"], "https://example.com/a?id=1")

    def test_shadow_can_be_turned_off(self):
        kept, stats = _filter(shadow_normalize=False).apply(
            [_link("https://example.com/a?utm_source=x")])
        self.assertEqual(stats.would_normalize, 0)

    def test_rules_run_on_normalized_url(self):
        # Trap after stripping: still dropped, and the log records both forms.
        url = "https://example.com/x/x/x/page?srsltid=a"
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
        recs = [_link("https://example.com/a?srsltid=x"),
                _link("https://example.com/x/x/x/")]
        kept, stats = _filter(drop=False).apply(recs)
        self.assertEqual(len(kept), 2)
        self.assertEqual(stats.normalized, 1)
        kept, stats = _filter(normalize=False).apply(recs)
        self.assertEqual([r["url"] for r in kept], ["https://example.com/a?srsltid=x"])
        kept, stats = _filter(enabled=False).apply(recs)
        self.assertEqual(kept, recs)

    def test_audit_log_is_daily_gzip_jsonl_and_appends(self):
        with tempfile.TemporaryDirectory() as d:
            f = _filter(log_dir=d)
            f.apply([_link("https://example.com/a?srsltid=1")])
            f.apply([_link("https://example.com/x/x/x/"), _link("https://example.com/ok")])
            path = Path(d) / "2026-10-31.jsonl.gz"
            self.assertTrue(path.exists())
            with gzip.open(path, "rt") as fh:
                events = [json.loads(line) for line in fh]
        self.assertEqual([e["action"] for e in events], ["normalize", "drop"])
        self.assertEqual(events[0], {
            "ts": "2026-10-31T12:00:00+00:00", "action": "normalize",
            "url": "https://example.com/a?srsltid=1",
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
            _link("https://example.com/a?srsltid=x", inlink_count_approx=1),
            _link("https://example.com/a?srsltid=y", inlink_count_approx=1),
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
        # Both stripped variants collapse into one row with summed inlinks.
        self.assertEqual([(r["url"], r["inlink_count_approx"]) for r in links],
                         [("https://example.com/a", 2)])
        # Crawl results keep their exact URL, even when it looks like a trap.
        self.assertEqual([r["url"] for r in results],
                         ["https://example.com/x/x/x/?utm_source=y"])
        self.assertEqual(sorted(e["action"] for e in events), ["drop", "normalize", "normalize"])


    def test_unconvertible_numeric_param_does_not_stall_folder(self):
        # "%C2%B2" decodes to "²": str.isdigit() is true but int() raises.
        # The link must be evaluated (kept, no rule hit) and the rest of the
        # folder ingested, instead of the folder failing on every restart.
        recs = [
            _link("https://example.com/article?page=%C2%B2"),
            _link("https://example.com/list?offset=%E2%91%A0"),
            _link("https://example.com/news/ok"),
            {"url": "https://example.com/done", "status": "ok",
             "shard_id": 3, "domain_id": 7},
        ]
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d) / "in"
            folder.mkdir()
            (folder / "part.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))
            db = _FakeDB()
            svc = IngestService(1, db, _FakeStats(), trap_filter=_filter(log_dir=str(Path(d) / "log")))
            svc.process_folder(folder)
        links, results = db.batches
        self.assertEqual(sorted(r["url"] for r in links), [
            "https://example.com/article?page=%C2%B2",
            "https://example.com/list?offset=%E2%91%A0",
            "https://example.com/news/ok",
        ])
        self.assertEqual([r["url"] for r in results], ["https://example.com/done"])


class TrapRulesNumericTest(unittest.TestCase):
    def test_non_decimal_digits_are_not_numbers(self):
        from libs.frontier.trap_rules import match_rules
        for value in ("%C2%B2", "%E2%91%A0", "1%C2%B2", "%C2%B9%C2%B2%C2%B3%C2%B9"):
            self.assertEqual(match_rules(f"https://e.com/a?page={value}"), [])
            self.assertEqual(match_rules(f"https://e.com/a?start={value}"), [])

    def test_unicode_decimal_digits_still_parsed(self):
        from libs.frontier.trap_rules import match_rules
        # Arabic-Indic "٥٠٠١" = 5001 is a real decimal number.
        self.assertIn("B_big_page", match_rules("https://e.com/a?page=%D9%A5%D9%A0%D9%A0%D9%A1"))
        self.assertIn("B_big_page", match_rules("https://e.com/a?page=5001"))


class SameSrsltidPolicyAsGoldenInjectTest(unittest.TestCase):
    def test_ingest_normalization_matches_libs_urls_strip_srsltid(self):
        # golden_inject (#103) and ingest must turn a URL into the same row.
        from libs.urls import strip_srsltid
        urls = [
            "https://ew.com/story-12174222?srsltid=AfmBOoq",
            "https://a.com/s?term=b27%20supplement&sort=relevance&srsltid=x",
            "https://a.com/s?b=2&SrsLtId=x&a=1",
            "https://a.com/p?srsltid=x#reviews",
            "https://a.com/p?q=srsltid&xsrsltid=1",
            "https://a.com/b;jsessionid=1?utm_source=x&srsltid=y",
            "/goto?url=relative",
        ]
        f = _filter()
        for url in urls:
            kept, _ = f.apply([_link(url)])
            self.assertEqual(kept[0]["url"], strip_srsltid(url), url)


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

    def test_golden_with_utm_matches_shadow_form(self):
        from scripts.trap_filter_golden_check import golden_index, match_golden
        golden = "https://a.example.com/live/1/?utm_source=x"
        index = golden_index({golden})
        ev = {"url": "https://a.example.com/live/1/?utm_source=x&gclid=9",
              "would_normalize_url": "https://a.example.com/live/1/"}
        self.assertEqual(match_golden(ev, index), golden)


if __name__ == "__main__":
    unittest.main()


class LoadTrapFilterConfigTest(unittest.TestCase):
    """Rollout must not depend on the host's ingest.yaml carrying the block."""

    def _load(self, raw):
        from containers.scheduler_ingest.ingestor import main as ingest_main
        with self.assertLogs("ingestor", level="INFO") as logs:
            tf = ingest_main._load_trap_filter(raw, 4)
        events = [r.event for r in logs.records if hasattr(r, "event")]
        return tf, events, logs.records

    def test_config_without_block_enables_reviewed_defaults(self):
        tf, events, records = self._load({"ingestor": {}})
        self.assertIsNotNone(tf)
        self.assertEqual(events, ["ingest.trap_filter_enabled"])
        self.assertEqual(records[0].config_source, "default")
        self.assertEqual(tf.cfg, TrapFilterConfig(log_dir="/data/ipc/trap_filter/ingestor_04"))

    def test_partial_block_keeps_defaults_for_missing_keys(self):
        tf, _, records = self._load({"trap_filter": {"drop": False}})
        self.assertEqual(records[0].config_source, "yaml")
        self.assertEqual(tf.cfg, TrapFilterConfig(
            drop=False, log_dir="/data/ipc/trap_filter/ingestor_04"))

    def test_kill_switch_logs_disabled(self):
        tf, events, records = self._load({"trap_filter": {"enabled": False}})
        self.assertIsNone(tf)
        self.assertEqual(events, ["ingest.trap_filter_disabled"])
        self.assertEqual(records[0].config_source, "yaml")

    def test_shipped_ingest_yaml_runs_filter_with_defaults(self):
        import yaml
        path = Path(__file__).resolve().parents[1] / "containers/scheduler_ingest/config/ingest.yaml"
        tf, _, _ = self._load(yaml.safe_load(path.read_text()))
        self.assertEqual(tf.cfg, TrapFilterConfig(log_dir="/data/ipc/trap_filter/ingestor_04"))

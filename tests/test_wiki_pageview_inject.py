"""Tests for scripts/wiki_pageview_inject.py and the scorer's manual-priority guard.

What we're protecting:

  * Article ranking sums an article's rows across access methods, keeps only
    *.wikipedia projects, drops non-article titles, and survives a sort-order
    break in the dump.
  * Run artifacts (user/automated dumps, top files, priority lists) are
    found and expired by the retention cleanup.
  * The previous run's priority URLs that left this run's list are the ones
    demoted.
  * The upsert stamps url_score_updated_at, re-queues only rows never fetched
    successfully, and never touches golden rows; demotion only resets
    pageview-priority rows.
  * Earlier dumps are deleted once this run's top list exists.
  * The ranker scorer never picks rows whose url_score > 1 on either path.
"""
from __future__ import annotations

import bz2
import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(_SCRIPTS))
_spec = importlib.util.spec_from_file_location(
    "wiki_pageview_inject", _SCRIPTS / "wiki_pageview_inject.py"
)
wpi = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = wpi  # dataclasses resolve types via sys.modules
_spec.loader.exec_module(wpi)

DUMP = b"""de.wikipedia Berlin 10 desktop 30 A30
de.wikipedia Berlin 10 mobile-web 50 A50
en.wikipedia - null desktop 900 A900
en.wikipedia Main_Page 1 desktop 500 A500
en.wikipedia Special:Search null desktop 800 A800
en.wikipedia Taylor_Swift 2 desktop 40 A40
en.wikipedia Taylor_Swift 2 mobile-app 5 A5
en.wikipedia Taylor_Swift 2 mobile-web 60 A60
en.wiktionary cat 3 desktop 9999 A9999
zh.wikipedia \xe5\x8f\xb0\xe7\x81\xa3 4 desktop 70 A70
en.wikipedia "Quoted title" 5 desktop 20 A20
"""


class TopArticlesTest(unittest.TestCase):
    def test_sums_access_methods_and_filters_projects(self):
        ranked = wpi.top_articles(DUMP.splitlines(keepends=True), 10)
        self.assertEqual(
            ranked,
            [
                (500, "en.wikipedia", "Main_Page"),
                (105, "en.wikipedia", "Taylor_Swift"),
                (80, "de.wikipedia", "Berlin"),
                (70, "zh.wikipedia", "台灣"),
                (20, "en.wikipedia", "Quoted title"),
            ],
        )

    def test_keeps_only_top_n(self):
        ranked = wpi.top_articles(DUMP.splitlines(keepends=True), 2)
        self.assertEqual([r[2] for r in ranked], ["Main_Page", "Taylor_Swift"])

    def test_sort_break_partial_sums_are_merged(self):
        lines = [
            b"en.wikipedia A 1 desktop 10 A10\n",
            b"en.wikipedia B 2 desktop 15 A15\n",
            b"en.wikipedia A 1 mobile-web 10 A10\n",
        ]
        self.assertEqual(
            wpi.top_articles(lines, 5),
            [(20, "en.wikipedia", "A"), (15, "en.wikipedia", "B")],
        )

    def test_rejects_non_positive_n(self):
        with self.assertRaises(ValueError):
            wpi.top_articles([], 0)


class ArticleUrlsTest(unittest.TestCase):
    def test_plain_article(self):
        self.assertEqual(
            wpi.article_urls("en.wikipedia", "C,_D"),
            ["https://en.wikipedia.org/wiki/C,_D"],
        )

    def test_zh_expands_variants(self):
        urls = wpi.article_urls("zh.wikipedia", "台灣")
        self.assertEqual(len(urls), 7)
        self.assertIn("https://zh.wikipedia.org/zh-tw/%E5%8F%B0%E7%81%A3", urls)
        self.assertIn("https://zh.wikipedia.org/zh-hk/%E5%8F%B0%E7%81%A3", urls)
        self.assertIn("https://zh.wikipedia.org/zh-sg/%E5%8F%B0%E7%81%A3", urls)
        self.assertIn("https://zh.wikipedia.org/wiki/%E5%8F%B0%E7%81%A3", urls)


class ExtractTopTest(unittest.TestCase):
    def test_writes_url_view_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            dump = Path(tmp) / "pageviews-20260930-user.bz2"
            dump.write_bytes(bz2.compress(DUMP))
            out = Path(tmp) / "top2_pageviews_20260930_user.txt"
            wpi.extract_top(dump, out, 2)
            self.assertEqual(
                out.read_text(encoding="utf-8").splitlines(),
                [
                    "https://en.wikipedia.org/wiki/Main_Page\t500",
                    "https://en.wikipedia.org/wiki/Taylor_Swift\t105",
                ],
            )
            self.assertFalse(Path(f"{out}.part").exists())


class ArtifactTest(unittest.TestCase):
    def test_artifact_dates(self):
        for name in (
            "pageviews-20260930-user.bz2",
            "pageviews-20260930-automated.bz2.part",
            "top100000_pageviews_20260930_user.txt",
            "top1000000_pageviews_20260930.txt",
            "priority_20260930_user.txt",
        ):
            with self.subTest(name=name):
                self.assertEqual(wpi.artifact_date_from_name(name), datetime(2026, 9, 30))
        self.assertIsNone(wpi.artifact_date_from_name("notes.txt"))

    def test_cleanup_expires_old_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in (
                "pageviews-20260920-user.bz2",
                "priority_20260920_user.txt",
                "priority_20260929_user.txt",
                "top100000_pageviews_20260929_user.txt",
            ):
                (root / name).write_text("x")
            wpi.cleanup_old_artifacts(root, 7, now=datetime(2026, 9, 30))
            self.assertEqual(
                sorted(p.name for p in root.iterdir()),
                ["priority_20260929_user.txt", "top100000_pageviews_20260929_user.txt"],
            )

    def test_default_retention_keeps_previous_weekly_priority_list(self):
        # Demotion reads the previous run's list, so cleanup at the start of a
        # weekly run must not delete it, even when the run is a day late.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "priority_20260922_user.txt").write_text("x")
            wpi.cleanup_old_artifacts(
                root, wpi.DEFAULT_RETENTION_DAYS, now=datetime(2026, 9, 30)
            )
            self.assertEqual(
                wpi.load_previous_priority(root, "20260930", "user"), {"x"}
            )


class DeletePreviousDumpsTest(unittest.TestCase):
    def test_keeps_only_current_and_newer_dumps(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in (
                "pageviews-20260929-user.bz2",
                "pageviews-20260929-automated.bz2.part",
                "pageviews-20260930-user.bz2",
                "top1000000_pageviews_20260929_user.txt",
                "priority_20260929_user.txt",
            ):
                (root / name).write_text("x")
            deleted = wpi.delete_previous_dumps(root, "20260930")
            self.assertEqual(
                sorted(p.name for p in deleted),
                ["pageviews-20260929-automated.bz2.part", "pageviews-20260929-user.bz2"],
            )
            self.assertEqual(
                sorted(p.name for p in root.iterdir()),
                [
                    "pageviews-20260930-user.bz2",
                    "priority_20260929_user.txt",
                    "top1000000_pageviews_20260929_user.txt",
                ],
            )


class PriorityListTest(unittest.TestCase):
    def test_demotion_is_previous_minus_current(self):
        self.assertEqual(wpi.demotion_urls({"a", "b", "c"}, ["b", "d"]), ["a", "c"])
        self.assertEqual(wpi.demotion_urls(None, ["b"]), [])

    def test_load_previous_picks_newest_earlier_list_for_agent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wpi.write_priority_list(root / "priority_20260928_user.txt", ["old"])
            wpi.write_priority_list(root / "priority_20260929_user.txt", ["u1", "u2"])
            wpi.write_priority_list(root / "priority_20260929_automated.txt", ["bot"])
            wpi.write_priority_list(root / "priority_20260930_user.txt", ["today"])
            self.assertEqual(
                wpi.load_previous_priority(root, "20260930", "user"), {"u1", "u2"}
            )
            self.assertIsNone(wpi.load_previous_priority(root, "20260928", "user"))


class InjectSqlTest(unittest.TestCase):
    def test_upsert_stamps_and_guards(self):
        sql = " ".join(wpi.current_upsert_sql(0, 1).split())
        self.assertIn("url_score_updated_at = EXCLUDED.url_score_updated_at", sql)
        self.assertIn("url_state_current_000.source IS DISTINCT FROM 1", sql)
        self.assertEqual(wpi.UPSERT_TEMPLATE.count("%s"), 5)
        self.assertIn("NOW()", wpi.UPSERT_TEMPLATE)

    def test_upsert_reschedules_only_never_fetched_rows(self):
        sql = " ".join(wpi.current_upsert_sql(0, 1).split())
        self.assertIn("should_crawl = TRUE", sql)
        self.assertIn("url_state_current_000.last_fetch_ok IS NULL", sql)
        # Not re-queued while likely still in a crawler queue, nor when failing.
        self.assertIn("url_state_current_000.last_scheduled < NOW() - INTERVAL '1 day'", sql)
        self.assertIn("COALESCE(url_state_current_000.num_consecutive_fail, 0) < 3", sql)

    def test_demote_only_resets_pageview_priority_rows(self):
        sql = " ".join(wpi.demote_sql(7, 1).split())
        self.assertIn("UPDATE url_state_current_007", sql)
        self.assertIn("SET url_score = NULL, url_score_updated_at = NULL", sql)
        for guard in ("should_crawl", "url_score > 1", "source IS DISTINCT FROM 1"):
            self.assertIn(guard, sql)

    def test_pageview_score_sits_between_ranker_and_golden(self):
        self.assertGreater(wpi.compute_url_score(1, 1_000_000), 1.0)
        self.assertLessEqual(wpi.compute_url_score(1_000_000, 1_000_000), 2.0)


class _RecordingCursor:
    def __init__(self):
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchall(self):
        return []


class _FakeSession:
    def __init__(self, cursor):
        self._cursor = cursor

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self._cursor


class ScorerManualPriorityTest(unittest.TestCase):
    def _picked_sql(self, steering: bool) -> str:
        try:
            from containers.scheduler_control.scorer.service import (
                GoldenDiscoveryRankerConfig,
                GoldenDiscoveryRankerService,
            )
        except ImportError as exc:  # pragma: no cover - env without deps
            self.skipTest(f"scorer deps unavailable: {exc}")
        cfg = GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=1,
            worker_id=0,
            batch_size=10,
            scan_interval_sec=60,
            max_batches_per_shard=1,
            domain_priority_steering_enabled=steering,
        )
        cursor = _RecordingCursor()
        session = _FakeSession(cursor)
        # sess.connection().connection.cursor(): the second hop is an attribute.
        session.connection = lambda: type("C", (), {"connection": session})()
        svc = GoldenDiscoveryRankerService(cfg=cfg, Session=session, scorer=None)
        self.assertEqual(svc._score_batch(0), 0)
        return " ".join(cursor.sql[0].split())

    def test_legacy_path_skips_manual_priority(self):
        self.assertIn("(url_score IS NULL OR url_score <= 1.0)", self._picked_sql(False))

    def test_steering_path_skips_manual_priority(self):
        self.assertIn("(url_score IS NULL OR url_score <= 1.0)", self._picked_sql(True))


if __name__ == "__main__":
    unittest.main()

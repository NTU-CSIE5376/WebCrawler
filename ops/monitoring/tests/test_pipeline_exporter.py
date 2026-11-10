from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

from ops.monitoring import pipeline_exporter as exporter


class PipelineExporterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = {
            "ipc_root": str(self.root),
            "refresh_seconds": 60,
            "max_scan_entries": 100,
            "stages": [
                {
                    "name": "ingestor",
                    "workers": [0],
                    "interval_minutes": 10,
                    "input_dir_template": "crawl_result/ingestor_{worker:02d}",
                    "progress_template": "progress/ingestor/{worker:02d}.json",
                }
            ],
        }
        self.progress = self.root / "progress/ingestor/00.json"
        self.progress.parent.mkdir(parents=True)
        self.progress.write_text(json.dumps({"date": None, "time": None}))
        self.base = self.root / "crawl_result/ingestor_00"
        self.base.mkdir(parents=True)
        self.now = datetime(2026, 11, 10, 12, 30, tzinfo=UTC).timestamp()

    def collector(self):
        return exporter.Collector(exporter.Config.from_dict(self.raw))

    def bucket(self, clock, date="20261110"):
        path = self.base / date / clock
        path.mkdir(parents=True)
        return path

    def test_ready_lag_subtracts_twice_interval_without_reading_url_payload(self):
        oldest = self.bucket("1200")
        self.bucket("1210")
        self.bucket("1220")
        (oldest / "bad.jsonl").write_text("not json, never opened")
        collector = self.collector()
        real_scandir = exporter.os.scandir

        def dates_and_times_only(path):
            self.assertNotEqual(Path(path), oldest)
            return real_scandir(path)

        with patch.object(exporter.os, "scandir", side_effect=dates_and_times_only):
            collector.refresh(self.now)
        self.assertIn(
            'ready_buckets{stage="ingestor",worker="00"} 2.0', collector.render()
        )
        self.assertIn(
            'oldest_ready_lag_seconds{stage="ingestor",worker="00"} 600.0',
            collector.render(),
        )
        self.assertTrue(collector.healthy())

    def test_seen_buckets_are_excluded_even_when_directories_remain(self):
        self.bucket("1100", date="20261109")
        self.bucket("1150")
        self.bucket("1200")
        self.progress.write_text('{"date":"20261110","time":"1150"}')
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn(
            'ready_buckets{stage="ingestor",worker="00"} 1.0', collector.render()
        )

    def test_missing_progress_is_unknown_not_zero(self):
        self.progress.unlink()
        self.bucket("1200")
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn(
            'progress_present{stage="ingestor",worker="00"} 0.0', collector.render()
        )
        self.assertNotIn("ready_buckets", collector.render())
        self.assertFalse(collector.healthy())

    def test_truncated_progress_removes_last_good_lag_but_preserves_success_timestamp(
        self,
    ):
        self.bucket("1200")
        collector = self.collector()
        collector.refresh(self.now)
        self.progress.write_text('{"date":')
        collector.refresh(self.now + 60)
        text = collector.render()
        self.assertIn('progress_present{stage="ingestor",worker="00"} 1.0', text)
        self.assertIn('scan_success{stage="ingestor",worker="00"} 0.0', text)
        self.assertNotIn("ready_buckets", text)
        self.assertIn(
            f'last_success_timestamp_seconds{{stage="ingestor",worker="00"}} {self.now}',
            text,
        )

    def test_bad_or_oversized_progress_fails_closed(self):
        for content in (
            " " * (exporter.MAX_JSON_BYTES + 1),
            "[]",
            '{"date":"20261110","time":null}',
            '{"date":"20261310","time":"1200"}',
        ):
            with self.subTest(content=content[:100]):
                self.progress.write_text(content)
                collector = self.collector()
                collector.refresh(self.now)
                self.assertNotIn("ready_buckets", collector.render())
                self.assertFalse(collector.healthy())

    def test_missing_input_directory_or_read_error_is_failure(self):
        self.base.rmdir()
        collector = self.collector()
        collector.refresh(self.now)
        self.assertFalse(collector.healthy())
        self.base.mkdir()
        with patch.object(exporter.Path, "open", side_effect=PermissionError("test")):
            collector.refresh(self.now)
        self.assertFalse(collector.healthy())

    def test_scan_budget_exhaustion_drops_partial_result(self):
        for minute in ("1200", "1210", "1220"):
            self.bucket(minute)
        self.raw["max_scan_entries"] = 2
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn(
            'scan_success{stage="ingestor",worker="00"} 0.0', collector.render()
        )
        self.assertNotIn("ready_buckets", collector.render())

    def test_invalid_bucket_name_is_failure(self):
        self.bucket("invalid")
        collector = self.collector()
        collector.refresh(self.now)
        self.assertFalse(collector.healthy())

    def test_symlink_input_progress_and_date_are_rejected(self):
        for target in ("progress", "date", "parent"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as outside:
                if target == "progress":
                    self.progress.unlink()
                    self.progress.symlink_to(Path(outside) / "cursor")
                elif target == "date":
                    (self.base / "20261110").symlink_to(outside)
                else:
                    self.base.rmdir()
                    self.base.symlink_to(outside)
                collector = self.collector()
                collector.refresh(self.now)
                self.assertFalse(collector.healthy())
                self.assertNotIn("ready_buckets", collector.render())
                if target == "progress":
                    self.progress.unlink()
                    self.progress.write_text('{"date":null,"time":null}')
                elif target == "date":
                    (self.base / "20261110").unlink()
                else:
                    self.base.unlink()
                    self.base.mkdir()

    def test_config_rejects_escape_duplicates_and_invalid_bounds(self):
        for key, value in (
            ("ipc_root", "relative"),
            ("refresh_seconds", float("nan")),
            ("max_scan_entries", -1),
            ("max_scan_entries", True),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                exporter.Config.from_dict({**self.raw, key: value})
        for change in (
            {"input_dir_template": "../../etc"},
            {"progress_template": "/etc/passwd"},
            {"workers": [0, 0]},
            {"workers": [True]},
            {"name": 'url"unbounded'},
            {"interval_minutes": float("inf")},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                exporter.Config.from_dict(
                    {**self.raw, "stages": [{**self.raw["stages"][0], **change}]}
                )

    def test_http_uses_cache_and_only_exports_allowlisted_workers(self):
        collector = self.collector()
        collector.refresh(self.now)
        (self.root / "crawl_result/ingestor_01").mkdir()
        server = exporter.ThreadingHTTPServer(
            ("127.0.0.1", 0), exporter.handler_for(collector)
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            address = f"http://127.0.0.1:{server.server_port}"
            with patch.object(
                exporter.os, "scandir", side_effect=AssertionError("must use cache")
            ):
                with urlopen(address + "/metrics", timeout=2) as response:
                    text = response.read().decode()
                    self.assertEqual(response.status, 200)
                    self.assertIn("version=0.0.4", response.headers["Content-Type"])
                with urlopen(address + "/healthz", timeout=2) as response:
                    self.assertEqual(response.status, 200)
                with self.assertRaises(HTTPError) as context:
                    urlopen(address + "/bad", timeout=2)
                self.assertEqual(context.exception.code, 404)
            self.assertNotIn('worker="01"', text)
            for line in text.splitlines():
                if line and not line.startswith("#"):
                    float(line.rsplit(" ", 1)[1])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_stale_cache_or_startup_is_unhealthy(self):
        collector = self.collector()
        self.assertFalse(collector.healthy())
        self.assertNotIn("ready_buckets", collector.render())
        collector.refresh(self.now)
        with patch.object(
            exporter.time, "monotonic", return_value=collector._last_run_monotonic + 181
        ):
            self.assertFalse(collector.healthy())

    def golden(self, *, completed=None, updated=None, started=None):
        self.raw["golden_inject"] = {
            "enabled": True,
            "state_dir": "maintenance/golden_inject",
            "deadline_grace_seconds": 1800,
            "stall_seconds": 900,
        }
        directory = self.root / "maintenance/golden_inject"
        directory.mkdir(parents=True, exist_ok=True)
        state = {
            "started_at": started if started is not None else self.now - 120,
            "complete": completed is not None,
        }
        if completed is not None:
            state["completed_at"] = completed
        if updated is not None:
            state["updated_at"] = updated
        (directory / "checkpoint.json").write_text(json.dumps(state))
        return directory

    def test_golden_deadline_uses_monday_0900_taipei_with_grace(self):
        monday = datetime(2026, 11, 9, 1, 0, tzinfo=UTC).timestamp()
        self.now = monday + 600
        self.golden(completed=monday - 3600, started=monday - 7200)
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn("crawler_golden_schedule_overdue 0.0", collector.render())
        collector.refresh(monday + 1800)
        self.assertIn("crawler_golden_schedule_overdue 1.0", collector.render())

    def test_golden_completed_fresh_or_previous_week_before_schedule_is_healthy(self):
        monday = datetime(2026, 11, 9, 1, 0, tzinfo=UTC).timestamp()
        self.now = monday - 60
        self.golden(completed=monday - 3600, started=monday - 7200)
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn("crawler_golden_schedule_overdue 0.0", collector.render())
        self.now = monday + 3600
        self.golden(completed=monday + 300, started=monday)
        collector.refresh(self.now)
        self.assertIn("crawler_golden_schedule_overdue 0.0", collector.render())
        self.assertIn("crawler_golden_stalled 0.0", collector.render())

    def test_golden_incomplete_run_stalls_using_updated_or_started_timestamp(self):
        self.golden(started=self.now - 1000)
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn("crawler_golden_stalled 1.0", collector.render())
        self.golden(started=self.now - 1000, updated=self.now - 100)
        collector.refresh(self.now)
        self.assertIn("crawler_golden_stalled 0.0", collector.render())

    def test_golden_missing_and_corrupt_checkpoint_do_not_report_healthy_schedule(self):
        directory = self.golden(started=self.now - 1000)
        checkpoint = directory / "checkpoint.json"
        for content in (
            None,
            "{}",
            '{"complete":false,"started_at":NaN}',
            json.dumps({"complete": False, "started_at": self.now + 60}),
        ):
            with self.subTest(content=content):
                if content is None:
                    checkpoint.unlink()
                else:
                    checkpoint.write_text(content)
                collector = self.collector()
                collector.refresh(self.now)
                self.assertIn("crawler_golden_scan_success 0.0", collector.render())
                self.assertNotIn("crawler_golden_schedule_overdue", collector.render())
                self.assertFalse(collector.healthy())

    def test_golden_skipped_count_is_bounded_metadata_and_has_no_url_labels(self):
        directory = self.golden(completed=self.now - 60)
        (directory / "latest-run.json").write_text(
            json.dumps(
                {
                    "skipped_count": 179,
                    "skipped": [{"url": "https://example.org/private"}],
                }
            )
        )
        collector = self.collector()
        collector.refresh(self.now)
        self.assertIn("crawler_golden_skipped_count 179.0", collector.render())
        self.assertNotIn("example.org", collector.render())
        (directory / "latest-run.json").write_text('{"skipped_count":-1}')
        collector.refresh(self.now)
        self.assertIn("crawler_golden_scan_success 0.0", collector.render())

    def test_golden_disabled_has_no_metric_and_config_paths_are_validated(self):
        collector = self.collector()
        collector.refresh(self.now)
        self.assertNotIn("crawler_golden", collector.render())
        for change in (
            {"state_dir": "../../outside"},
            {"enabled": "yes"},
            {"stall_seconds": 0},
            {"deadline_grace_seconds": float("inf")},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                exporter.Config.from_dict(
                    {**self.raw, "golden_inject": {"enabled": True, **change}}
                )


if __name__ == "__main__":
    unittest.main()

"""Golden injection's transaction boundaries, recovery and read-only planning.

All database connections in this module are fakes. Real dependency imports are
kept intact so collection does not alter other tests' YAML or PostgreSQL modules.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2
import tldextract
import yaml

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import golden_inject as inject  # noqa: E402
from golden_inject_batch import BatchResult  # noqa: E402
from libs.db.sharding.router import ShardRouter  # noqa: E402

REAL_CONNECT = inject._connect
OFFLINE_EXTRACTOR = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


class SQLStateError(Exception):
    def __init__(self, code):
        super().__init__(f"database SQLSTATE {code}")
        self.pgcode = code


class FakeConnection:
    def __init__(self, name="db", events=None, commit_error=None):
        self.name = name
        self.events = events if events is not None else []
        self.commit_error = commit_error
        self.closed = False
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0
        self.cursor_value = MagicMock()
        self.cursor_value.__enter__.return_value = self.cursor_value

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.events.append(("commit", self.name))
        self.commit_count += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self):
        self.events.append(("rollback", self.name))
        self.rollback_count += 1

    def close(self):
        self.events.append(("close", self.name))
        self.close_count += 1
        self.closed = True


def batch_result(records, domains=None):
    return BatchResult({"injected": len(records), "marked": 0, "marked_variant": 0},
                       domains or {})


class GoldenInjectCommitIntervalTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state_dir = Path(self.temporary.name) / "state"
        self.enterContext(patch("tldextract.extract", OFFLINE_EXTRACTOR))
        # A forgotten fixture must fail locally instead of reaching script constants.
        self.connect_guard = self.enterContext(patch.object(
            inject, "_connect", side_effect=AssertionError("Unexpected database access")))
        self.enterContext(patch.object(inject, "CRAWLERDB", {
            "host": "test-crawler.invalid", "port": 5433, "dbname": "test_crawler"}))

    def plan(self, count=5, overrides=None, splits=None, urls=None):
        rows = urls if urls is not None else [
            {"id": number, "url": f"https://example.com/{number}"}
            for number in range(1, count + 1)]
        return inject.prepare_plan(rows, [2, 1], overrides or {}, splits or set())

    def run_plan(self, plan, **options):
        with patch.object(inject, "_read", return_value=({}, set())):
            return inject.run_plan(plan, self.state_dir, batch_pause=0, **options)

    def checkpoint(self):
        return json.loads((self.state_dir / "checkpoint.json").read_text())

    def save_checkpoint(self, state):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "checkpoint.json").write_text(json.dumps(state))

    def state(self, plan, cursor=0, complete=False):
        return {"version": inject.PLAN_VERSION, "fingerprint": plan.fingerprint,
                "cursor": cursor, "counts": {"injected": cursor, "marked": 0,
                                             "marked_variant": 0}, "complete": complete}

    def test_real_dependency_imports_are_not_replaced_process_wide(self):
        self.assertTrue(callable(psycopg2.connect))
        self.assertEqual(yaml.safe_load("router: {example.com: 7}"),
                         {"router": {"example.com": 7}})
        self.assertIs(inject.psycopg2, psycopg2)

    def test_commits_interleave_with_bounded_batches_and_have_no_empty_final_commit(self):
        events = []
        connection = FakeConnection(events=events)

        def write(cur, records, cache):
            events.append(("inject", [r.metric_id for r in records]))
            self.assertEqual(self.checkpoint()["cursor"], records[0].metric_id - 1)
            return batch_result(records)

        with patch.object(inject, "_connect", return_value=connection), \
                patch.object(inject, "inject_batch", side_effect=write):
            result = self.run_plan(self.plan(), commit_interval=2, max_retries=0)
        self.assertEqual(events, [("inject", [1, 2]), ("commit", "db"),
                                  ("inject", [3, 4]), ("commit", "db"),
                                  ("inject", [5]), ("commit", "db"), ("close", "db")])
        self.assertEqual(result["processed"], 5)
        self.assertEqual(result["injected"], 5)
        self.assertTrue(self.checkpoint()["complete"])

    def test_acknowledged_first_commit_is_checkpointed_before_later_permanent_failure(self):
        connection = FakeConnection()
        error = SQLStateError("23505")
        calls = []

        def write(cur, records, cache):
            calls.append([r.metric_id for r in records])
            if len(calls) == 2:
                raise error
            return batch_result(records)

        with patch.object(inject, "_connect", return_value=connection), \
                patch.object(inject, "inject_batch", side_effect=write), \
                patch.object(inject, "_retry_delay") as delay:
            with self.assertRaises(SQLStateError) as caught:
                self.run_plan(self.plan(), commit_interval=2, max_retries=3)
        self.assertIs(caught.exception, error)
        self.assertEqual(connection.commit_count, 1)
        self.assertEqual(connection.rollback_count, 1)
        self.assertEqual(self.checkpoint()["cursor"], 2)
        self.assertFalse(self.checkpoint()["complete"])
        delay.assert_not_called()

    def test_transient_commit_failure_reopens_connection_without_caching_uncommitted_domains(self):
        events = []
        dead = FakeConnection("dead", events, psycopg2.OperationalError("connection lost"))
        good = FakeConnection("good", events)
        caches = []
        metric_ids = []
        domain = self.plan(1).records[0].domain
        sid = self.plan(1).records[0].shard_id

        def write(cur, records, cache):
            caches.append(dict(cache))
            metric_ids.append([r.metric_id for r in records])
            return batch_result(records, {domain: (111 if len(caches) == 1 else 222, sid, 0.8)})

        with patch.object(inject, "_connect", side_effect=[dead, good]) as connect, \
                patch.object(inject, "inject_batch", side_effect=write), \
                patch.object(inject, "_retry_delay") as delay:
            summary = self.run_plan(self.plan(3), commit_interval=2, max_retries=1)
        self.assertEqual(metric_ids, [[1, 2], [1, 2], [3]])
        self.assertEqual(caches[:2], [{}, {}])
        self.assertEqual(caches[2], {domain: (222, sid, 0.8)})
        self.assertEqual(summary["injected"], 3)
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(dead.rollback_count, 1)
        self.assertTrue(dead.closed)
        delay.assert_called_once()

    def test_checkpoint_io_failure_after_commit_does_not_retry_database_writes(self):
        connection = FakeConnection()
        real_atomic = inject._atomic_json
        writes = 0

        def checkpoint_write(path, value):
            nonlocal writes
            writes += 1
            if writes == 2:
                raise OSError("state disk is full")
            return real_atomic(path, value)

        with patch.object(inject, "_connect", return_value=connection), \
                patch.object(inject, "inject_batch", side_effect=lambda c, r, cache: batch_result(r)) as write, \
                patch.object(inject, "_atomic_json", side_effect=checkpoint_write), \
                patch.object(inject, "_retry_delay") as delay:
            with self.assertRaisesRegex(OSError, "state disk is full"):
                self.run_plan(self.plan(2), commit_interval=2, max_retries=3)
        self.assertEqual(connection.commit_count, 1)
        self.assertEqual(connection.rollback_count, 0)
        write.assert_called_once()
        delay.assert_not_called()
        self.assertEqual(self.checkpoint()["cursor"], 0)
        self.assertTrue(connection.closed)

    def test_cleanup_failure_does_not_hide_the_original_batch_exception(self):
        connection = FakeConnection()
        error = SQLStateError("23505")
        connection.rollback = MagicMock(side_effect=psycopg2.InterfaceError("closed"))
        connection.close = MagicMock(side_effect=psycopg2.InterfaceError("already closed"))
        with patch.object(inject, "_connect", return_value=connection), \
                patch.object(inject, "inject_batch", side_effect=error):
            with self.assertRaises(SQLStateError) as caught:
                self.run_plan(self.plan(1), max_retries=0)
        self.assertIs(caught.exception, error)
        connection.rollback.assert_called_once()
        connection.close.assert_called_once()

    def test_connection_retry_budget_is_bounded_without_advancing_checkpoint(self):
        with patch.object(inject, "_connect", side_effect=psycopg2.OperationalError("lost")) as connect, \
                patch.object(inject, "_retry_delay") as delay, \
                patch.object(inject, "inject_batch") as write:
            with self.assertRaises(psycopg2.OperationalError):
                self.run_plan(self.plan(1), max_retries=1)
        self.assertEqual(connect.call_count, 2)
        delay.assert_called_once()
        write.assert_not_called()
        self.assertEqual(self.checkpoint()["cursor"], 0)

    def test_routing_change_during_run_is_not_reported_as_complete(self):
        plan = self.plan(1)
        different_shard = (plan.records[0].shard_id + 1) % inject.NUM_SHARDS
        connection = FakeConnection()
        with patch.object(inject, "_connect", return_value=connection), \
                patch.object(inject, "inject_batch", side_effect=lambda c, r, cache: batch_result(r)), \
                patch.object(inject, "_read", return_value=({"example.com": different_shard}, set())):
            with self.assertRaisesRegex(RuntimeError, "Routing changed"):
                inject.run_plan(plan, self.state_dir, max_retries=0, batch_pause=0)
        self.assertEqual(connection.commit_count, 1)
        self.assertEqual(self.checkpoint()["cursor"], 1)
        self.assertFalse(self.checkpoint()["complete"])
        self.assertFalse((self.state_dir / "latest-run.json").exists())

    def test_checkpoint_resumes_only_the_committed_prefix_of_matching_input(self):
        plan = self.plan(5)
        self.save_checkpoint(self.state(plan, cursor=2))
        connection = FakeConnection()
        batches = []

        def write(cur, records, cache):
            batches.append([r.metric_id for r in records])
            return batch_result(records)

        with patch.object(inject, "_connect", return_value=connection), \
                patch.object(inject, "inject_batch", side_effect=write):
            summary = self.run_plan(plan, commit_interval=2, max_retries=0)
        self.assertEqual(batches, [[3, 4], [5]])
        self.assertEqual(summary["processed"], 5)
        self.assertEqual(summary["injected"], 5)

    def test_a_completed_checkpoint_starts_a_fresh_weekly_scan(self):
        plan = self.plan(3)
        self.save_checkpoint(self.state(plan, cursor=3, complete=True))
        self.assertEqual(inject._checkpoint(self.state_dir / "checkpoint.json", plan)["cursor"], 0)

    def test_checkpoint_rejects_invalid_cursor_counts_and_identity(self):
        plan = self.plan(3)
        corruptions = [
            {"cursor": True}, {"cursor": -1}, {"cursor": 1.5},
            {"complete": "false"}, {"fingerprint": None}, {"version": -1},
            {"counts": {"injected": -1, "marked": 0, "marked_variant": 0}},
            {"counts": {"injected": True, "marked": 0, "marked_variant": 0}},
            {"counts": {"injected": 1, "marked": 0, "marked_variant": 0}},
            {"counts": {"injected": 0}},
            {"cursor": 4, "counts": {"injected": 4, "marked": 0, "marked_variant": 0}},
        ]
        for changes in corruptions:
            with self.subTest(changes=changes):
                state = self.state(plan)
                state.update(changes)
                self.save_checkpoint(state)
                with self.assertRaises(ValueError):
                    inject._checkpoint(self.state_dir / "checkpoint.json", plan)

    def test_fingerprint_changes_on_database_target_routing_or_input(self):
        original = self.plan(2)
        with patch.object(inject, "CRAWLERDB", {
                "host": "other.invalid", "port": 5433, "dbname": "test_crawler"}):
            different_target = self.plan(2)
        different_route = self.plan(2, overrides={"example.com": 7})
        different_rows = self.plan(3)
        for changed in (different_target, different_route, different_rows):
            self.assertNotEqual(changed.fingerprint, original.fingerprint)
            self.save_checkpoint(self.state(original, cursor=1))
            fresh = inject._checkpoint(self.state_dir / "checkpoint.json", changed)
            self.assertEqual(fresh["cursor"], 0)
            self.assertEqual(fresh["fingerprint"], changed.fingerprint)
        self.assertEqual(self.plan(urls=list(reversed([
            {"id": 1, "url": "https://example.com/1"},
            {"id": 2, "url": "https://example.com/2"}]))).fingerprint, original.fingerprint)

    def test_plan_matches_shared_full_host_routing_and_preserves_requested_content(self):
        raw = "https://en.wikipedia.org/wiki/Earth?oldid=123&utm_source=x&PHPSESSID=abc&srsltid=track#top"
        rows = [
            {"id": 1, "url": "https://developers.facebook.com/docs/"},
            {"id": 2, "url": "https://www.facebook.com/help/"},
            {"id": 3, "url": "https://shop.bbc.com/p/1"},
            {"id": 4, "url": "https://gov.br/path"},
            {"id": 5, "url": "https://8.8.8.8/path"},
            {"id": 6, "url": raw},
        ]
        splits = {"developers.facebook.com", "shop.bbc.com"}
        overrides = {"developers.facebook.com": 7, "shop.bbc.com": 19, "facebook.com": 32}
        router = ShardRouter(inject.NUM_SHARDS, 16, overrides, splits)
        plan = self.plan(overrides=overrides, splits=splits, urls=rows)
        by_id = {r.metric_id: r for r in plan.records}
        for row in rows:
            record = by_id[row["id"]]
            self.assertEqual((record.domain, record.shard_id), router.route(row["url"])[:2])
        self.assertEqual(by_id[1].shard_id, 7)
        self.assertEqual(by_id[2].shard_id, 32)
        self.assertEqual(by_id[3].shard_id, 19)
        self.assertEqual(by_id[4].domain, "gov.br")
        self.assertEqual(by_id[5].domain, "8.8.8.8")
        self.assertEqual(by_id[6].url,
                         "https://en.wikipedia.org/wiki/Earth?oldid=123&utm_source=x&PHPSESSID=abc#top")
        self.assertEqual(by_id[6].variant, raw)
        self.assertEqual(plan.normalized, 1)

    def test_malformed_urls_are_reported_by_metric_id_without_rewriting_valid_urls(self):
        rows = [{"id": 1, "url": "https://example.com/中文/%2F?q=a%20b&oldid=5"},
                {"id": 2, "url": "ftp://example.com/file"},
                {"id": 3, "url": "https://bad host.example/path"},
                {"id": 4, "url": "https://example.com/path\n"},
                {"id": 5, "url": None},
                {"id": 6, "url": "https://example.com/" + "x" * 2500}]
        plan = self.plan(urls=rows)
        self.assertEqual([r.metric_id for r in plan.records], [1])
        self.assertEqual(plan.records[0].url, rows[0]["url"])
        self.assertEqual({d["metric_url_id"] for d in plan.skipped}, {2, 3, 4, 5, 6})

    def test_transient_classification_does_not_retry_data_or_programming_errors(self):
        for code in ("08006", "40001", "40P01", "55P03", "57014", "57P01", "53300", "YB004"):
            with self.subTest(code=code):
                self.assertTrue(inject._transient(SQLStateError(code)))
        for error in (SQLStateError("23505"), SQLStateError("42601"),
                      SQLStateError("22001"), ValueError("routing mismatch"), OSError("disk")):
            with self.subTest(error=error):
                self.assertFalse(inject._transient(error))
        self.assertTrue(inject._transient(psycopg2.OperationalError("lost")))
        self.assertTrue(inject._transient(psycopg2.InterfaceError("closed")))
        self.assertTrue(inject._transient(inject.ConcurrentDeleteError("target disappeared")))

    def test_read_retries_reopen_read_only_connections_and_close_failed_sessions(self):
        first, second = FakeConnection("first"), FakeConnection("second")
        operation = MagicMock(side_effect=[SQLStateError("40001"), "result"])
        with patch.object(inject, "_connect", side_effect=[first, second]) as connect, \
                patch.object(inject, "_retry_delay") as delay:
            self.assertEqual(inject._read({"host": "test.invalid"}, operation, 1), "result")
        self.assertEqual(connect.call_args_list[0].kwargs, {"read_only": True})
        self.assertEqual(connect.call_args_list[1].kwargs, {"read_only": True})
        self.assertEqual((first.close_count, second.close_count), (1, 1))
        delay.assert_called_once()

    def test_read_only_flag_is_enforced_by_server_connection_options(self):
        with patch.object(inject.psycopg2, "connect") as connect:
            REAL_CONNECT({"host": "test.invalid"}, read_only=True)
        options = connect.call_args.kwargs["options"]
        self.assertIn("default_transaction_read_only=on", options)

    def test_dry_run_reads_sources_only_and_does_not_create_checkpoint_or_write(self):
        metric, crawler = FakeConnection("metric"), FakeConnection("crawler")
        with patch.object(inject, "_connect", side_effect=[metric, crawler]) as connect, \
                patch.object(inject, "fetch_injectable_batch_ids", return_value=[1]), \
                patch.object(inject, "fetch_urls_by_batches", return_value=[
                    {"id": 1, "url": "https://example.com/1"}]), \
                patch.object(inject, "load_sharding_config", return_value=({}, set())), \
                patch.object(inject, "inject_batch") as write, \
                patch.object(inject, "run_plan") as run:
            summary = inject.main(["--dry-run", "--state-dir", str(self.state_dir)])
        self.assertEqual(summary["status"], "dry_run")
        self.assertEqual(summary["valid_rows"], 1)
        self.assertEqual([call.kwargs for call in connect.call_args_list],
                         [{"read_only": True}, {"read_only": True}])
        write.assert_not_called()
        run.assert_not_called()
        self.assertFalse(self.state_dir.exists())
        self.assertEqual((metric.commit_count, crawler.commit_count), (0, 0))
        self.assertTrue(metric.closed and crawler.closed)

    def test_invalid_cli_limits_are_rejected_before_loading_or_connecting(self):
        for option, value in (("--commit-interval", "0"), ("--commit-interval", "-1"),
                              ("--max-retries", "-1"), ("--batch-pause", "-0.1"),
                              ("--batch-pause", "nan"), ("--batch-pause", "inf")):
            with self.subTest(option=option, value=value), patch.object(inject, "load_plan") as load:
                with self.assertRaises(SystemExit) as caught:
                    inject.main([option, value, "--state-dir", str(self.state_dir)])
                self.assertEqual(caught.exception.code, 2)
                load.assert_not_called()
        self.connect_guard.assert_not_called()
        self.assertFalse(self.state_dir.exists())

    def test_second_process_skips_before_reading_or_writing_when_lock_is_held(self):
        code = ("import json,sys;sys.path.insert(0,sys.argv[1]);import golden_inject as g;"
                "g.load_plan=lambda *a: (_ for _ in ()).throw(AssertionError('must not read DB'));"
                "print(json.dumps(g.main(['--state-dir',sys.argv[2]])))")
        with inject.injection_lock(self.state_dir) as acquired:
            self.assertTrue(acquired)
            result = subprocess.run([sys.executable, "-c", code, str(SCRIPTS), str(self.state_dir)],
                                    capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {"status": "skipped_locked"})
        self.assertFalse((self.state_dir / "checkpoint.json").exists())


if __name__ == "__main__":
    unittest.main()

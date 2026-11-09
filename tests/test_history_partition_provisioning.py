"""Missing month partitions on the *_history tables must not wedge ingestion.

On 2026-11-01 no ``_p202611`` partition existed, so every ingest batch failed
("no partition of relation ... found for row") and the accounting pass, which
only provisioned partitions *after* its history-writing rolloff, failed before
it could create them. These tests pin the two fixes: accounting provisions
first, and the ingestor provisions on demand and retries the batch.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from containers.scheduler_control.accounting import service as accounting_service
from containers.scheduler_control.accounting.service import (
    CounterRolloffConfig,
    CounterRolloffService,
)
from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from libs.db.sharding.history_partition import (
    ensure_partitions,
    missing_partition_target,
)

NOV = datetime(2026, 11, 1, tzinfo=timezone.utc)
MISSING_MSG = (
    'no partition of relation "url_state_history_123" found for row\n'
    "DETAIL:  Partition key of the failing row contains (snapshot_at) = "
    "(2026-11-03 01:14:17.513598+00).\n"
)


class MissingPartitionTargetTest(unittest.TestCase):
    def test_parses_parent_and_month(self):
        self.assertEqual(missing_partition_target(MISSING_MSG), ("url_state_history_123", NOV))

    def test_parses_sqlalchemy_wrapped_message(self):
        wrapped = f"(psycopg2.errors.CheckViolation) {MISSING_MSG}\n[SQL: INSERT ...]"
        self.assertEqual(missing_partition_target(wrapped), ("url_state_history_123", NOV))

    def test_ignores_other_errors_and_tables(self):
        self.assertIsNone(missing_partition_target("duplicate key value violates unique constraint"))
        self.assertIsNone(missing_partition_target(MISSING_MSG.replace("url_state_history_123", "users")))


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeSession:
    """Answers the two catalog lookups ensure_partitions makes and records DDL."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "relkind = 'p'" in sql:
            return _Result([(n,) for n in params["names"] if n in self.db.partitioned])
        if "FROM pg_class" in sql:
            return _Result([(n,) for n in params["names"] if n in self.db.existing])
        self.db.ddl.append(sql)
        if self.db.fail_ddl:
            raise RuntimeError("boom")
        return _Result([])

    def commit(self):
        pass

    def rollback(self):
        pass


class _FakeDB:
    def __init__(self, partitioned, existing=(), fail_ddl=False):
        self.partitioned = set(partitioned)
        self.existing = set(existing)
        self.fail_ddl = fail_ddl
        self.ddl = []

    def Session(self):
        return _FakeSession(self)


class EnsurePartitionsTest(unittest.TestCase):
    def test_creates_only_missing_partitions_of_partitioned_parents(self):
        db = _FakeDB(
            partitioned={"url_state_history_000", "url_state_history_001"},
            existing={"url_state_history_000_p202611"},
        )
        created = ensure_partitions(
            db.Session,
            ["url_state_history_000", "url_state_history_001", "content_feature_history_000"],
            [NOV],
        )
        self.assertEqual(created, ["url_state_history_001_p202611"])
        self.assertEqual(len(db.ddl), 1)
        self.assertIn("url_state_history_001_p202611 PARTITION OF url_state_history_001", db.ddl[0])

    def test_failed_ddl_raises_after_trying_every_partition(self):
        db = _FakeDB(partitioned={"url_state_history_000", "url_state_history_001"}, fail_ddl=True)
        with self.assertRaises(RuntimeError):
            ensure_partitions(db.Session, ["url_state_history_000", "url_state_history_001"], [NOV])
        self.assertEqual(len(db.ddl), 2)


def _cfg(**kw):
    base = dict(
        total_shards=2,
        event_retention_days=90,
        batch_size=100,
        history_retention_days=30,
        run_hour_utc=4,
        run_minute_utc=0,
        check_interval_sec=60,
        catch_up_on_start=False,
    )
    base.update(kw)
    return CounterRolloffConfig(**base)


class AccountingProvisionsFirstTest(unittest.TestCase):
    def test_partitions_provisioned_even_when_rolloff_fails(self):
        svc = CounterRolloffService(_cfg(), Session=None)
        calls = []

        def fake_ensure(Session, parents, months):
            calls.append((list(parents), list(months)))
            return []

        def failing_rolloff(shard_id, cutoff_date):
            raise RuntimeError(MISSING_MSG)

        with patch.object(accounting_service, "ensure_partitions", side_effect=fake_ensure), \
             patch.object(svc, "_process_batch", side_effect=failing_rolloff):
            with self.assertRaises(RuntimeError):
                svc.run_once()

        tables = [parents for parents, _ in calls]
        self.assertIn(["url_state_history_000", "url_state_history_001"], tables)
        self.assertIn(["content_feature_history_000", "content_feature_history_001"], tables)
        for _, months in calls:
            self.assertEqual(len(months), 2)  # current + next month

    def test_provisioning_runs_with_retention_disabled(self):
        svc = CounterRolloffService(_cfg(history_retention_days=0), Session=None)
        empty = dict(picked_count=0, processed_count=0, missing_count=0,
                     history_count=0, marked_count=0)
        with patch.object(accounting_service, "ensure_partitions", return_value=[]) as ensure, \
             patch.object(svc, "_process_batch", return_value=empty), \
             patch.object(svc, "_prune_history") as prune:
            svc.run_once()
        self.assertEqual(ensure.call_count, 2)
        prune.assert_not_called()

    def test_provision_error_does_not_block_rolloff(self):
        svc = CounterRolloffService(_cfg(history_retention_days=0), Session=None)
        empty = dict(picked_count=0, processed_count=0, missing_count=0,
                     history_count=0, marked_count=0)
        with patch.object(accounting_service, "ensure_partitions", side_effect=RuntimeError("ddl")), \
             patch.object(svc, "_process_batch", return_value=empty) as rolloff:
            svc.run_once()
        self.assertEqual(rolloff.call_count, 2)


def _rec(shard_id, url="https://example.com/a"):
    return {"url": url, "status": "new", "shard_id": shard_id}


class IngestorSelfHealTest(unittest.TestCase):
    def test_provisions_batch_shards_and_retries(self):
        db = IngestDB(Session=None)
        attempts = []

        def once(recs):
            attempts.append(len(recs))
            if len(attempts) == 1:
                raise RuntimeError(MISSING_MSG)
            return ["ok"] * len(recs)

        recs = [_rec(123), _rec(7, "https://example.com/b")]
        with patch.object(db, "_process_batch_once", side_effect=once), \
             patch.object(ingest_db_ops, "ensure_partitions", return_value=[]) as ensure:
            out = db.process_batch(recs)

        self.assertEqual(out, ["ok", "ok"])
        self.assertEqual(len(attempts), 2)
        _, parents, months = ensure.call_args.args
        self.assertEqual(sorted(parents), ["url_state_history_007", "url_state_history_123"])
        self.assertEqual(list(months), [NOV])

    def test_other_errors_are_reraised_without_provisioning(self):
        db = IngestDB(Session=None)
        with patch.object(db, "_process_batch_once", side_effect=RuntimeError("deadlock")), \
             patch.object(ingest_db_ops, "ensure_partitions") as ensure:
            with self.assertRaisesRegex(RuntimeError, "deadlock"):
                db.process_batch([_rec(1)])
        ensure.assert_not_called()

    def test_retries_even_if_provisioning_itself_fails(self):
        """A concurrent ingestor may win the CREATE race; the retry still runs."""
        db = IngestDB(Session=None)
        calls = []

        def once(recs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError(MISSING_MSG)
            return ["ok"]

        with patch.object(db, "_process_batch_once", side_effect=once), \
             patch.object(ingest_db_ops, "ensure_partitions", side_effect=RuntimeError("dup type")):
            self.assertEqual(db.process_batch([_rec(123)]), ["ok"])


if __name__ == "__main__":
    unittest.main()

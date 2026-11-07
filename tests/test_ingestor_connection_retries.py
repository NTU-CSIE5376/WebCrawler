"""Exercise retry boundaries against real transactions without production writes."""
import json
import sqlite3
from unittest.mock import MagicMock

import psycopg2
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.ingestor.service import IngestService


class Cursor(sqlite3.Cursor):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Connection(sqlite3.Connection):
    def cursor(self, *args, **kwargs):
        kwargs.setdefault("factory", Cursor)
        return super().cursor(*args, **kwargs)


class StateError(psycopg2.OperationalError):
    def __init__(self, code):
        super().__init__("query failed")
        self.code = code

    @property
    def pgcode(self):
        return self.code


@pytest.fixture
def transaction_db(tmp_path, monkeypatch):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'retry.sqlite'}",
        connect_args={"factory": Connection}, pool_size=1, max_overflow=0,
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE tally (value INTEGER NOT NULL)"))
        conn.execute(text("INSERT INTO tally VALUES (0)"))
    sleep = MagicMock()
    monkeypatch.setattr(IngestDB._execute_batch.retry, "sleep", sleep)
    db = IngestDB(sessionmaker(bind=engine, autoflush=False))
    invalidations = []
    event.listen(engine, "invalidate", lambda *args: invalidations.append(args))
    yield db, engine, sleep, invalidations
    engine.dispose()


def value(engine):
    with engine.connect() as conn:
        return conn.scalar(text("SELECT value FROM tally"))


def link(shard=1):
    return {"url": f"https://example.test/{shard}", "status": "new", "shard_id": shard}


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("message", ["connection already closed", "cursor already closed"])
def test_disconnect_retries_entire_transaction_and_discards_connection(transaction_db, wrapped, message):
    db, engine, sleep, invalidations = transaction_db
    connections = []
    calls = []

    def bulk(cur, shard, items):
        connections.append(cur.connection)
        calls.append(shard)
        cur.execute("UPDATE tally SET value=value+1")
        if len(calls) == 2:
            # SQLAlchemy's psycopg2 dialect treats both messages as disconnects.
            error = psycopg2.InterfaceError(message)
            if wrapped:
                error = InterfaceError(None, None, error)
            raise error
        return [(idx, True) for idx, rec in items]

    db._bulk_links = bulk
    assert db.process_batch([link(1), link(2)]) == [True, True]
    assert calls == [1, 2, 1, 2]
    assert connections[0] is connections[1]
    assert connections[2] is connections[3]
    assert connections[0] is not connections[2]
    assert len(invalidations) == 1
    assert value(engine) == 2  # Failed attempt's increments were rolled back.
    sleep.assert_called_once_with(0.2)


def test_checkout_disconnect_gets_a_fresh_session(transaction_db):
    db, engine, sleep, invalidations = transaction_db
    checkouts = []

    def checkout(*args):
        checkouts.append(args)
        if len(checkouts) == 1:
            raise OperationalError(None, None, psycopg2.OperationalError("connection refused"))

    event.listen(engine, "checkout", checkout)
    db._bulk_links = lambda cur, shard, items: [(idx, True) for idx, rec in items]
    assert db.process_batch([link()]) == [True]
    assert len(checkouts) == 2
    sleep.assert_called_once_with(0.2)


def test_exhaustion_is_bounded_and_preserves_original_error(transaction_db):
    db, engine, sleep, invalidations = transaction_db
    error = psycopg2.InterfaceError("connection already closed")
    calls = []

    def bulk(cur, shard, items):
        calls.append(cur.connection)
        cur.execute("UPDATE tally SET value=value+1")
        raise error

    db._bulk_links = bulk
    with pytest.raises(psycopg2.InterfaceError) as caught:
        db.process_batch([link()])
    assert caught.value is error
    assert len(calls) == len(invalidations) == 3
    assert len({id(conn) for conn in calls}) == 3
    assert [call.args[0] for call in sleep.call_args_list] == [0.2, 0.4]
    assert value(engine) == 0


@pytest.mark.parametrize("error", [
    psycopg2.ProgrammingError("syntax error"),
    StateError("57014"),  # Statement timeout.
    StateError("40P01"),  # Deadlock is not a connection failure.
    StateError("40001"),  # Serialization failure.
    ValueError("invalid record"),
])
def test_non_connection_errors_are_not_retried(transaction_db, error):
    db, engine, sleep, invalidations = transaction_db
    bulk = MagicMock(side_effect=error)
    db._bulk_links = bulk
    with pytest.raises(type(error)) as caught:
        db.process_batch([link()])
    assert caught.value is error
    bulk.assert_called_once()
    sleep.assert_not_called()
    assert not invalidations


def test_lost_commit_acknowledgment_never_replays_additive_writes(transaction_db, monkeypatch):
    db, engine, sleep, invalidations = transaction_db
    original_commit = engine.dialect.do_commit
    calls = []

    def bulk(cur, shard, items):
        calls.append(shard)
        cur.execute("UPDATE tally SET value=value+1")
        return [(idx, True) for idx, rec in items]

    def lost_ack(connection):
        original_commit(connection)
        raise psycopg2.OperationalError("server closed the connection unexpectedly")

    db._bulk_links = bulk
    monkeypatch.setattr(engine.dialect, "do_commit", lost_ack)
    with pytest.raises((OperationalError, psycopg2.OperationalError)):
        db.process_batch([link()])
    assert value(engine) == 1  # COMMIT succeeded before its acknowledgment was lost.
    assert calls == [1]
    sleep.assert_not_called()


def test_unknown_transaction_status_is_not_retried(transaction_db):
    db, engine, sleep, invalidations = transaction_db
    db._bulk_links = MagicMock(side_effect=StateError("08007"))
    with pytest.raises(StateError):
        db.process_batch([link()])
    db._bulk_links.assert_called_once()
    sleep.assert_not_called()


def test_service_counts_recovered_records_only_once(transaction_db, tmp_path):
    db, engine, sleep, invalidations = transaction_db
    attempts = []

    def bulk(cur, shard, items):
        cur.execute("UPDATE tally SET value=value+1")
        attempts.append(shard)
        if len(attempts) == 1:
            raise psycopg2.InterfaceError("connection already closed")
        return [(idx, True) for idx, rec in items]

    db._bulk_links = bulk
    folder = tmp_path / "folder"
    folder.mkdir()
    (folder / "input.jsonl").write_text(json.dumps(link()) + "\n")
    stats = MagicMock()
    IngestService(0, db, stats).process_folder(folder)
    stats.write.assert_called_once()
    counters = stats.write.call_args.kwargs["counters"]
    assert counters["new_links"] == 1
    assert counters.get("ingest_error", 0) == 0
    assert value(engine) == 1


def test_decorator_preserves_method_metadata():
    assert IngestDB._execute_batch.__name__ == "_execute_batch"
    assert IngestDB._execute_batch.__wrapped__.__name__ == "_execute_batch"


@pytest.mark.parametrize("disconnect_after_provisioning", [False, True])
def test_missing_partition_recovery_preserves_transaction_retry_boundary(
    transaction_db, monkeypatch, disconnect_after_provisioning,
):
    from datetime import datetime, timezone
    from containers.scheduler_ingest.ingestor import db_ops

    db, engine, sleep, invalidations = transaction_db
    attempts = []
    provisioned = []
    missing = (
        'no partition of relation "url_state_history_001" found for row\n'
        'DETAIL: Partition key of the failing row contains (snapshot_at) = '
        '(2026-11-03 01:14:17.513598+00).'
    )

    def bulk(cur, shard, items):
        attempts.append(shard)
        cur.execute("UPDATE tally SET value=value+1")
        if len(attempts) == 1:
            raise psycopg2.errors.CheckViolation(missing)
        if disconnect_after_provisioning and len(attempts) == 2:
            raise psycopg2.InterfaceError("connection already closed")
        return [(idx, True) for idx, _ in items]

    def provision(Session, parents, months):
        # The missing-partition attempt must roll back before provisioning.
        assert value(engine) == 0
        provisioned.append((parents, months))
        return parents

    monkeypatch.setattr(db, "_bulk_links", bulk)
    monkeypatch.setattr(db_ops, "ensure_partitions", provision)

    assert db.process_batch([link()]) == [True]
    assert value(engine) == 1  # Neither failed attempt increments the counter.
    assert provisioned == [(
        ["url_state_history_001"],
        [datetime(2026, 11, 1, tzinfo=timezone.utc)],
    )]
    assert len(attempts) == 2 + int(disconnect_after_provisioning)
    assert len(invalidations) == int(disconnect_after_provisioning)
    assert sleep.call_count == int(disconnect_after_provisioning)

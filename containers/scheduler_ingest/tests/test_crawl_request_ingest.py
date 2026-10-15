"""Database integration tests. CI supplies CRAWL_REQUEST_TEST_DSN."""
from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import pytest
from sqlalchemy import Boolean, Column, DateTime, DefaultClause, Integer, MetaData, SmallInteger, String, create_engine, text
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from libs.db.models.domain.state import DomainState
from libs.db.sharding.table_factory import url_state_current_table, url_state_history_table, url_event_counter_table
from containers.scheduler_control.offerer.selection.golden_discovery_ranker_v1_strategy import GoldenDiscoveryRankerV1Strategy
from scripts.migrate_add_last_crawl_request_at import migrate


@pytest.fixture
def database():
    dsn = os.environ.get("CRAWL_REQUEST_TEST_DSN")
    if not dsn:
        pytest.skip("Set CRAWL_REQUEST_TEST_DSN to run PostgreSQL integration tests")
    schema = "crawl_request_test_" + uuid4().hex
    admin = create_engine(dsn)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(dsn, connect_args={"options": f"-csearch_path={schema}"})
    try:
        metadata = MetaData(schema=schema)
        domain = DomainState.__table__.to_metadata(metadata)
        domain.append_column(Column("discovery_frozen", Boolean, server_default=text("false")))
        domain.append_column(Column("domain_fail_count", Integer, server_default=text("0")))
        domain.append_column(Column("crawl_paused_until", DateTime(timezone=True)))
        for shard in (3, 4):
            for model in (url_state_current_table(shard), url_state_history_table(shard), url_event_counter_table(shard)):
                table = model.__table__.to_metadata(metadata)
                # Production schema has server defaults and migration columns
                # that are not all declared on the legacy logical ORM models.
                for col in table.columns:
                    if col.default is not None and col.default.is_scalar:
                        col.server_default = DefaultClause(text(str(col.default.arg).lower()))
                if "first_seen" in table.c:
                    table.append_column(Column("source", SmallInteger, server_default=text("0")))
                    table.append_column(Column("discovered_from", String))
        metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO domain_state (domain_id,domain,shard_id,discovery_frozen) VALUES (7,'example.com',3,true)"))
        yield IngestDB(sessionmaker(bind=engine)), engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()


def request_record(url="https://example.com/a", at=None, shard=3):
    return {"url": url, "status": "crawl_request", "shard_id": shard,
            "domain_id": 7, "domain_score": 0.9, "inlink_count_approx": 0,
            "requested_at": (at or datetime.now(timezone.utc)).isoformat()}


def state(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT * FROM url_state_current_003 WHERE url='https://example.com/a'")).mappings().one()


def claim(engine, at):
    with engine.begin() as conn:
        conn.execute(text("UPDATE url_state_current_003 SET should_crawl=false,last_scheduled=:at"), {"at": at})


def test_api_requests_requeue_existing_urls_and_bypass_discovery_freeze(database):
    db, engine = database
    at = datetime.now(timezone.utc)
    assert db.process_batch([request_record(at=at)]) == [True]
    assert state(engine)["should_crawl"] is True
    claim(engine, at + timedelta(seconds=1))
    assert db.process_batch([request_record(at=at + timedelta(seconds=2))]) == [False]
    assert state(engine)["should_crawl"] is True
    assert state(engine)["last_crawl_request_at"] == at + timedelta(seconds=2)


def test_replayed_request_does_not_schedule_another_crawl(database):
    db, engine = database
    at = datetime.now(timezone.utc)
    record = request_record(at=at)
    db.process_batch([record])
    claim(engine, at + timedelta(seconds=1))
    db.process_batch([record])
    assert state(engine)["should_crawl"] is False


@pytest.mark.parametrize("status", ["ok", "fail"])
def test_delayed_result_cannot_clear_a_newer_unscheduled_request(database, status):
    db, engine = database
    at = datetime.now(timezone.utc)
    db.process_batch([request_record(at=at)])
    claim(engine, at + timedelta(seconds=1))
    db.process_batch([request_record(at=at + timedelta(seconds=3))])
    old_result = {"url": "https://example.com/a", "status": status, "domain_id": 7,
                  "shard_id": 3, "fetched_at": (at + timedelta(seconds=2)).isoformat()}
    db.process_batch([old_result])
    assert state(engine)["should_crawl"] is True
    claim(engine, at + timedelta(seconds=4))
    db.process_batch([{**old_result, "fetched_at": (at + timedelta(seconds=5)).isoformat()}])
    assert state(engine)["should_crawl"] is False


def test_request_wins_over_result_in_same_batch(database):
    db, engine = database
    at = datetime.now(timezone.utc)
    result = {"url": "https://example.com/a", "status": "ok", "domain_id": 7,
              "shard_id": 3, "fetched_at": at.isoformat()}
    db.process_batch([request_record(at=at + timedelta(seconds=1)), result])
    assert state(engine)["should_crawl"] is True


def test_normal_discovery_does_not_force_a_recrawl(database):
    db, engine = database
    at = datetime.now(timezone.utc)
    db.process_batch([request_record(at=at)])
    claim(engine, at + timedelta(seconds=1))
    with engine.begin() as conn:
        conn.execute(text("UPDATE domain_state SET discovery_frozen=false"))
    discovery = {**request_record(at=at + timedelta(seconds=2)), "status": "new"}
    db.process_batch([discovery])
    assert state(engine)["should_crawl"] is False


def test_invalid_request_rolls_back_all_shards(database):
    db, engine = database
    with pytest.raises(ValueError):
        db.process_batch([request_record(), request_record(url="https://example.com/b", shard=4) | {"requested_at": "bad"}])
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM url_state_current_003")) == 0
        assert conn.scalar(text("SELECT COUNT(*) FROM url_state_current_004")) == 0


def test_offer_claim_uses_actual_time_not_transaction_start(database):
    db, engine = database
    db.process_batch([request_record(at=datetime(2020, 1, 1, tzinfo=timezone.utc))])
    session = sessionmaker(bind=engine)()
    try:
        transaction_start = session.scalar(text("SELECT CURRENT_TIMESTAMP"))
        with engine.connect() as conn:
            requested_at = conn.scalar(text("SELECT clock_timestamp()"))
        assert requested_at > transaction_start
        db.process_batch([request_record(at=requested_at)])
        strategy = GoldenDiscoveryRankerV1Strategy(lambda: session)
        assert strategy.claim_domain_urls(3, 7, 100) == ["https://example.com/a"]
        assert state(engine)["last_scheduled"] >= requested_at
        db.process_batch([request_record(at=requested_at)])
        assert state(engine)["should_crawl"] is False
    finally:
        session.close()


def test_migration_adds_nullable_marker_and_is_idempotent(database):
    _, engine = database
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE url_state_current_000 (url TEXT PRIMARY KEY)"))
    raw = engine.raw_connection()
    try:
        migrate(raw.driver_connection, num_shards=1)
        migrate(raw.driver_connection, num_shards=1)
    finally:
        raw.close()
    with engine.connect() as conn:
        assert conn.execute(text("SELECT last_crawl_request_at FROM url_state_current_000")).fetchall() == []

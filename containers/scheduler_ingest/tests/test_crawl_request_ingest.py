"""Database integration tests. CI supplies CRAWL_REQUEST_TEST_DSN."""
from datetime import datetime, timezone
import json
import os
from uuid import uuid4

import pytest
from sqlalchemy import Boolean, Column, DateTime, DefaultClause, Integer, MetaData, SmallInteger, String, create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from containers.scheduler_ingest.api.dispatcher import SubmissionDispatcher
from libs.ipc.new_link_record import build_new_link_record
from libs.db.models.domain.state import DomainState
from libs.db.sharding.table_factory import url_state_current_table, url_state_history_table, url_event_counter_table


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
            conn.execute(text("INSERT INTO domain_state (domain_id,domain,shard_id,discovery_frozen) VALUES (7,'example.com',3,false)"))
        yield IngestDB(sessionmaker(bind=engine)), engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()



def request_record(url="https://example.com/a", shard=3):
    return build_new_link_record(
        url=url, shard_id=shard, domain_id=7, domain_score=0.9,
        discovered_from=None, discovery_source_type=0,
        inlink_count_approx=0, inlink_count_external=0,
    )


def dispatch(engine, tmp_path, records):
    pending = tmp_path / "pending"
    pending.mkdir(exist_ok=True)
    batch = pending / "batch.json"
    batch.write_text(json.dumps(records))
    dispatcher = SubmissionDispatcher({
        "api": {"crawl_request": {"pending_dir": str(pending)}},
        "router": {"shards_per_ingestor": 16, "interval_minutes": 10,
                   "ingestor_dir_template": str(tmp_path / "ingestor_{id:02d}")},
    }, session_factory=sessionmaker(bind=engine))
    dispatcher.process_file(batch)
    return [json.loads(line) for path in tmp_path.rglob("*.jsonl")
            for line in path.read_text().splitlines()]


def state(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT * FROM url_state_current_003 WHERE url='https://example.com/a'")).mappings().one()


def test_new_requests_use_original_ingestor(database, tmp_path):
    db, engine = database
    records = dispatch(engine, tmp_path, [request_record()])
    assert records[0]["status"] == "new"
    assert db.process_batch(records) == [True]
    assert state(engine)["should_crawl"] is True


def test_known_requests_only_reset_crawl_flag_even_when_discovery_frozen(database, tmp_path):
    db, engine = database
    db.process_batch([request_record()])
    with engine.begin() as conn:
        conn.execute(text("UPDATE url_state_current_003 SET should_crawl=false,title='Keep me',num_fetch_ok_90d=5"))
        conn.execute(text("UPDATE domain_state SET discovery_frozen=true"))
    records = dispatch(engine, tmp_path, [request_record()])
    assert state(engine)["should_crawl"] is True
    assert db.process_batch(records) == [False]
    assert state(engine)["title"] == "Keep me"
    assert state(engine)["num_fetch_ok_90d"] == 5


@pytest.mark.parametrize("status", ["ok", "fail"])
def test_existing_fetch_result_handling_is_preserved(database, tmp_path, status):
    db, engine = database
    db.process_batch([request_record()])
    dispatch(engine, tmp_path, [request_record()])
    assert state(engine)["should_crawl"] is True
    db.process_batch([{"url": "https://example.com/a", "status": status,
                      "domain_id": 7, "shard_id": 3,
                      "fetched_at": datetime.now(timezone.utc).isoformat()}])
    assert state(engine)["should_crawl"] is False


def test_dispatch_failure_rolls_back_requeues_and_retains_batch(database, tmp_path):
    db, engine = database
    db.process_batch([request_record()])
    with engine.begin() as conn:
        conn.execute(text("UPDATE url_state_current_003 SET should_crawl=false"))
        conn.execute(text("DROP TABLE url_state_current_004"))
    with pytest.raises(ProgrammingError, match="url_state_current_004"):
        dispatch(engine, tmp_path, [request_record(), request_record(shard=4)])
    assert state(engine)["should_crawl"] is False
    assert (tmp_path / "pending" / "batch.json").exists()
    assert not list(tmp_path.rglob("*.jsonl"))


def test_repeated_request_can_requeue_again(database, tmp_path):
    db, engine = database
    db.process_batch([request_record()])
    for _ in range(2):
        with engine.begin() as conn:
            conn.execute(text("UPDATE url_state_current_003 SET should_crawl=false"))
        dispatch(engine, tmp_path, [request_record()])
        assert state(engine)["should_crawl"] is True

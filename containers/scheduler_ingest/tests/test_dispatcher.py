from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest.mock import Mock
import json
import subprocess
import sys

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.api import dispatcher as dispatcher_module
from containers.scheduler_ingest.api.dispatcher import SubmissionDispatcher
from containers.scheduler_ingest.api.queue_lock import batch_claim
from libs.ipc.jsonio import read_jsonl
from libs.ipc.new_link_record import build_new_link_record


@pytest.fixture
def setup_dispatcher(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
    with engine.begin() as conn:
        for shard in (3, 4):
            conn.execute(text(f"CREATE TABLE url_state_current_{shard:03d} (url TEXT PRIMARY KEY, should_crawl BOOLEAN)"))
            for index in range(4):
                conn.execute(text(f"INSERT INTO url_state_current_{shard:03d} VALUES (:url, false)"),
                             {"url": f"https://example.com/{shard}/{index}"})
    config = {
        "api": {"crawl_request": {"pending_dir": str(tmp_path / "pending"), "dispatcher_workers": 2}},
        "router": {"num_shards": 256, "shards_per_ingestor": 16, "interval_minutes": 10,
                   "ingestor_dir_template": str(tmp_path / "ingestor_{id:02d}")},
    }
    dispatcher = SubmissionDispatcher(config, session_factory=sessionmaker(bind=engine))
    yield dispatcher, config, engine, tmp_path
    engine.dispose()


def batch(dispatcher, name, *items):
    dispatcher.pending_dir.mkdir(exist_ok=True)
    path = dispatcher.pending_dir / f"{name}.json"
    path.write_text(json.dumps([build_new_link_record(
        url=f"https://example.com/{shard}/{index}", shard_id=shard,
            domain_id=1, domain_score=0, discovered_from=None, discovery_source_type=0,
    ) for shard, index in items]))
    return path


def test_run_once_coalesces_requests_by_shard_and_preserves_batches(setup_dispatcher, monkeypatch):
    dispatcher, _, engine, root = setup_dispatcher
    first = batch(dispatcher, "a", (3, 0), (4, 0))
    second = batch(dispatcher, "b", (3, 1), (4, 1))
    update = Mock(wraps=dispatcher._update_shard)
    monkeypatch.setattr(dispatcher, "_update_shard", update)
    dispatcher.run_once()
    assert Counter(call.args[0] for call in update.call_args_list) == {3: 1, 4: 1}
    assert not first.exists() and not second.exists()
    # Both shards belong to one ingestor: one file per request, not per shard.
    files = list(root.glob("ingestor_*/*/*/*.jsonl"))
    assert len(files) == 2
    assert sorted(len(read_jsonl(path)) for path in files) == [2, 2]
    with engine.connect() as conn:
        for shard in (3, 4):
            assert conn.scalar(text(f"SELECT COUNT(*) FROM url_state_current_{shard:03d} WHERE should_crawl")) == 2


@pytest.mark.parametrize("max_urls,max_batches,expected", [(2, 32, 3), (100, 2, 2), (3, 32, 3), (1, 32, 3)])
def test_coalescing_respects_limits_and_handles_oversized_requests(setup_dispatcher, monkeypatch, max_urls, max_batches, expected):
    dispatcher, _, _, _ = setup_dispatcher
    dispatcher.max_urls, dispatcher.max_batches = max_urls, max_batches
    for index in range(3):
        batch(dispatcher, str(index), (3, 0), (3, 1))
    update = Mock(wraps=dispatcher._update_shard)
    monkeypatch.setattr(dispatcher, "_update_shard", update)
    dispatcher.run_once()
    assert update.call_count == expected
    assert not list(dispatcher.pending_dir.glob("*.json"))


def test_run_once_skips_owned_batch_and_processes_another(setup_dispatcher):
    dispatcher, _, _, _ = setup_dispatcher
    busy = batch(dispatcher, "a", (3, 0))
    available = batch(dispatcher, "b", (4, 0))
    with batch_claim(busy) as owned:
        assert owned
        dispatcher.run_once()
        assert busy.exists()
        assert not available.exists()
    dispatcher.run_once()
    assert not busy.exists()


@pytest.mark.parametrize("payload", [{}, [{"shard_id": 999, "url": "https://example.com"}], [{"shard_id": 3}]])
def test_invalid_batch_does_not_block_valid_requests(setup_dispatcher, payload):
    dispatcher, _, _, _ = setup_dispatcher
    bad = batch(dispatcher, "a", (3, 0))
    bad.write_text(json.dumps(payload))
    good = batch(dispatcher, "b", (4, 0))
    dispatcher.run_once()
    assert bad.exists() and not good.exists()


def test_publication_failure_keeps_only_failed_batch_for_retry(setup_dispatcher, monkeypatch):
    dispatcher, _, _, root = setup_dispatcher
    bad = batch(dispatcher, "a", (3, 0))
    good = batch(dispatcher, "b", (4, 0))
    original = dispatcher._publish_batch

    def publish(path, records):
        if path == bad:
            raise OSError("disk full")
        return original(path, records)

    monkeypatch.setattr(dispatcher, "_publish_batch", publish)
    dispatcher.run_once()
    assert bad.exists() and not good.exists()
    monkeypatch.setattr(dispatcher, "_publish_batch", original)
    dispatcher.run_once()
    assert not bad.exists()
    assert len(list(root.glob("ingestor_*/*/*/*.jsonl"))) == 2


def test_different_shards_execute_in_parallel(setup_dispatcher):
    dispatcher, _, engine, _ = setup_dispatcher
    path = batch(dispatcher, "a", (3, 0), (4, 0))
    reached_update = Barrier(2)

    @event.listens_for(engine, "before_cursor_execute")
    def synchronize(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE url_state_current_"):
            reached_update.wait(timeout=5)

    dispatcher.process_file(path)
    assert not path.exists()


def test_same_shard_serializes_across_dispatcher_instances(setup_dispatcher, monkeypatch):
    first, config, engine, _ = setup_dispatcher
    second = SubmissionDispatcher(config, session_factory=sessionmaker(bind=engine))
    a, b = batch(first, "a", (3, 0)), batch(first, "b", (3, 1))
    entered, attempted, release = Event(), Event(), Event()
    updates = []
    original_lock = dispatcher_module.queue_lock

    @event.listens_for(engine, "before_cursor_execute")
    def block_first(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE url_state_current_"):
            updates.append(statement)
            if len(updates) == 1:
                entered.set()
                assert release.wait(timeout=5)

    def observe_lock(directory):
        if entered.is_set():
            attempted.set()
        return original_lock(directory)

    monkeypatch.setattr(dispatcher_module, "queue_lock", observe_lock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(first.process_file, a)
        try:
            assert entered.wait(timeout=5)
            two = pool.submit(second.process_file, b)
            assert attempted.wait(timeout=5)
            assert len(updates) == 1
        finally:
            release.set()
        one.result(timeout=10)
        two.result(timeout=10)
    assert len(updates) == 2
    assert not a.exists() and not b.exists()


def test_shard_lock_held_by_another_process_blocks_only_that_shard(setup_dispatcher, monkeypatch):
    dispatcher, _, _, _ = setup_dispatcher
    path = batch(dispatcher, "a", (3, 0))
    attempted = Event()
    original_lock = dispatcher_module.queue_lock

    def observe_lock(directory):
        attempted.set()
        return original_lock(directory)

    monkeypatch.setattr(dispatcher_module, "queue_lock", observe_lock)
    code = """
from pathlib import Path
import sys
from containers.scheduler_ingest.api.queue_lock import queue_lock
with queue_lock(Path(sys.argv[1])):
    print('locked', flush=True)
    sys.stdin.read()
"""
    process = subprocess.Popen([sys.executable, "-c", code, str(dispatcher.shard_locks / "003")],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "locked"
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(dispatcher.process_file, path)
            try:
                assert attempted.wait(timeout=5)
                assert not future.done()
                # A different shard is not held by the child process.
                other = batch(dispatcher, "b", (4, 0))
                dispatcher.process_file(other)
                assert not other.exists() and path.exists()
            finally:
                process.kill()
                process.wait(timeout=5)
            future.result(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    assert not path.exists()


@pytest.mark.parametrize("setting", ["dispatcher_workers", "dispatcher_max_urls", "dispatcher_max_batches"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_invalid_dispatcher_settings_fail_at_startup(setup_dispatcher, setting, value):
    _, config, engine, _ = setup_dispatcher
    config["api"]["crawl_request"][setting] = value
    with pytest.raises(ValueError, match=setting):
        SubmissionDispatcher(config, session_factory=sessionmaker(bind=engine))

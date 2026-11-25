"""Bounded process submissions, write acknowledgements, failures, and drain."""
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
import os
import pickle
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from twisted.internet import reactor
from twisted.internet.defer import succeed
from twisted.internet.task import Clock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crawler import pipelines, json_writer


@pytest.fixture
def controlled_writer(tmp_path, monkeypatch, request):
    pool = Mock()
    monkeypatch.setattr(json_writer, "ProcessPoolExecutor", Mock(return_value=pool))
    jobs, notifications = [], []

    def submit(fn, payloads):
        job = Future()
        job.payloads = payloads
        job.set_running_or_notify_cancel()
        jobs.append(job)
        return job

    pool.submit.side_effect = submit
    monkeypatch.setattr(reactor, "callFromThread",
                        lambda fn, *a, **kw: notifications.append(lambda: fn(*a, **kw)))
    clock = Clock()
    monkeypatch.setattr(reactor, "callLater", clock.callLater)
    monkeypatch.setattr(json_writer, "deferToThread", lambda fn, **kw: (fn(**kw), succeed(None))[1])
    options = {"batch_size": 1, **getattr(request, "param", {})}
    writer = pipelines.JsonPipeline(str(tmp_path), 10, writer_processes=1, on_broken=Mock(), **options)
    writer.stats = Mock()
    spider = SimpleNamespace(crawler_id=0)
    writer.open_spider(spider)
    writer._test_clock = clock
    return writer, spider, jobs, notifications, pool


def finish(jobs, notifications, index, error=None):
    if error is None:
        jobs[index].set_result([None] * len(jobs[index].payloads))
    else:
        jobs[index].set_exception(error)
    notifications.pop(0)()


def test_submission_is_bounded_and_errors_reach_the_caller(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    items = [{"url": f"https://example.test/{i}"} for i in range(3)]
    results = [writer.process_item(item, spider) for item in items]
    received, errors = [], []
    for result in results:
        result.addCallbacks(received.append, lambda f: errors.append(f.type))
    assert len(jobs) == 2 and received == []
    assert getattr(spider, "items_written", 0) == 0
    finish(jobs, notifications, 0)
    assert received == items[:1] and len(jobs) == 3
    assert spider.items_written == 1
    finish(jobs, notifications, 1, OSError("write failed"))
    assert errors == [OSError]
    assert spider.items_written == 1
    finish(jobs, notifications, 2)
    assert received == [items[0], items[2]] and writer._pending == 0
    assert spider.items_written == 2
    assert writer.close_spider(spider).called
    pool.shutdown.assert_called_once_with(wait=True)


def test_cancellation_keeps_actual_writes_bounded_and_shutdown_drains(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    results = [writer.process_item({"url": f"https://example.test/{i}"}, spider) for i in range(3)]
    results[0].addErrback(lambda failure: None)
    results[0].cancel()
    assert len(jobs) == 2
    closed = writer.close_spider(spider)
    assert not closed.called
    pool.shutdown.assert_not_called()
    with pytest.raises(RuntimeError, match="closing"):
        writer.process_item({}, spider)
    finish(jobs, notifications, 0)
    assert len(jobs) == 3
    assert not results[1].called and not closed.called
    finish(jobs, notifications, 1)
    assert not closed.called
    finish(jobs, notifications, 2)
    assert results[2].called and closed.called and writer._pending == 0
    pool.shutdown.assert_called_once_with(wait=True)


def test_broken_pool_notifies_once_and_fails_waiting_writes(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    failures = []
    for i in range(3):
        writer.process_item({"url": str(i)}, spider).addErrback(lambda f: failures.append(f.type))
    finish(jobs, notifications, 0, BrokenProcessPool("worker died"))
    finish(jobs, notifications, 1, BrokenProcessPool("worker died"))
    assert failures == [BrokenProcessPool] * 3
    writer._on_broken.assert_called_once()
    assert len(jobs) == 2 and writer._pending == 0
    assert writer.close_spider(spider).called


def test_writer_failure_stops_refilling_and_closes_spider():
    from crawler.spiders.spider import HtmlSpider
    spider = HtmlSpider()
    spider.manual_bulk_crawls = SimpleNamespace(closed=False, poller=Mock(running=True))
    spider.crawler = SimpleNamespace(stats=Mock(), engine=Mock())
    spider.crawler.engine.close_spider.return_value = succeed(None)
    spider._writer_pool_broken()
    spider._writer_pool_broken()
    assert spider.manual_bulk_crawls.closed
    assert spider._reserve_urls("after_failure") == []
    spider.manual_bulk_crawls.poller.stop.assert_called_once()
    spider.crawler.engine.close_spider.assert_called_once_with(spider, reason="json_writer_pool_broken")


def test_writer_rejects_multiple_processes_that_could_reorder_appends(tmp_path):
    with pytest.raises(ValueError, match="0 or 1"):
        pipelines.JsonPipeline(str(tmp_path), 10, writer_processes=2)


def test_process_writer_preserves_output_and_recovers_from_child_death(tmp_path):
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "helpers/json_writer_runner.py"), str(tmp_path)],
        cwd=root / "containers/crawler",
        env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("controlled_writer", [{"batch_size": 8}], indirect=True)
def test_eight_records_flush_without_waiting_for_timer(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    results = [writer.process_item({"url": str(i)}, spider) for i in range(8)]
    assert len(jobs) == 1 and len(jobs[0].payloads) == 8
    assert not any(d.called for d in results)
    finish(jobs, notifications, 0)
    assert all(d.called for d in results) and spider.items_written == 8
    assert writer.close_spider(spider).called
    assert not writer._test_clock.getDelayedCalls()


@pytest.mark.parametrize("controlled_writer", [{"batch_size": 8}], indirect=True)
def test_partial_batch_flushes_after_fifty_ms_and_on_shutdown(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    writer.process_item({"url": "first"}, spider)
    writer._test_clock.advance(0.049)
    assert not jobs
    writer._test_clock.advance(0.002)
    assert len(jobs) == 1 and len(jobs[0].payloads) == 1
    writer.process_item({"url": "second"}, spider)
    closed = writer.close_spider(spider)
    assert len(jobs) == 2 and not closed.called
    finish(jobs, notifications, 0)
    finish(jobs, notifications, 1)
    assert closed.called and spider.items_written == 2


@pytest.mark.parametrize("controlled_writer", [{"batch_size": 8, "batch_bytes": 4096}], indirect=True)
def test_byte_cap_and_two_batch_bound_preserve_order_under_backpressure(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    items = [{"url": str(i), "content": "x" * 1000} for i in range(30)]
    for item in items:
        writer.process_item(item, spider)
    assert len(jobs) == 2
    assert sum(entry[3] is not None for entry in writer._pool._waiting) <= 1
    closed = writer.close_spider(spider)
    index = 0
    while writer._pending:
        assert len(jobs) - index <= 2
        assert writer._pool._inflight <= 2
        assert sum(map(len, jobs[index].payloads)) <= 4096
        finish(jobs, notifications, index)
        index += 1
    assert closed.called
    written = [pickle.loads(payload)[1]["url"] for job in jobs for payload in job.payloads]
    assert written == [item["url"] for item in items]


@pytest.mark.parametrize("controlled_writer", [{"batch_size": 8, "batch_bytes": 4096}], indirect=True)
def test_oversized_record_runs_alone_without_dropping_or_reordering(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    for item in [{"url": "small"}, {"url": "large", "content": "x" * 8000}, {"url": "last"}]:
        writer.process_item(item, spider)
    assert len(jobs) == 1
    closed = writer.close_spider(spider)
    finish(jobs, notifications, 0)
    assert len(jobs) == 2 and len(jobs[1].payloads) == 1
    assert len(jobs[1].payloads[0]) > 4096
    assert writer._pool._inflight == 1
    finish(jobs, notifications, 1)
    assert len(jobs) == 3
    finish(jobs, notifications, 2)
    assert closed.called and spider.items_written == 3
    assert [pickle.loads(p)[1]["url"] for j in jobs for p in j.payloads] == ["small", "large", "last"]


@pytest.mark.parametrize("controlled_writer", [{"batch_size": 8}], indirect=True)
def test_partial_batch_errors_are_acknowledged_per_record(controlled_writer):
    writer, spider, jobs, notifications, pool = controlled_writer
    successes, errors = [], []
    for i in range(8):
        writer.process_item({"url": str(i)}, spider).addCallbacks(successes.append, lambda f: errors.append(f.type))
    jobs[0].set_result([None, OSError("disk error"), *([None] * 6)])
    notifications.pop(0)()
    assert len(successes) == 7 and errors == [OSError]
    assert spider.items_written == 7
    writer._on_broken.assert_not_called()
    assert writer.close_spider(spider).called


def test_worker_preserves_successful_records_when_one_path_fails(tmp_path):
    output = tmp_path / "records.jsonl"
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory")
    entries = [(output, {"n": 1}), (blocked / "records.jsonl", {"n": 2}), (output, {"n": 3})]
    outcomes = json_writer.append_records([pickle.dumps((str(p), r)) for p, r in entries])
    assert outcomes[0] is None and isinstance(outcomes[1], OSError) and outcomes[2] is None
    import json
    assert [json.loads(line)["n"] for line in output.read_text().splitlines()] == [1, 3]


@pytest.mark.parametrize("options", [{"batch_size": 0}, {"batch_bytes": 0}, {"flush_interval": float("nan")}])
def test_invalid_batch_limits_are_rejected(options):
    with pytest.raises(ValueError):
        json_writer.JsonWriterPool(**options)


def test_completed_future_does_not_retain_record_batch_in_a_cycle(controlled_writer):
    import weakref
    writer, spider, jobs, notifications, pool = controlled_writer
    writer.process_item({"url": "first", "content": "x" * 10000}, spider)
    finish(jobs, notifications, 0)
    reference = weakref.ref(jobs.pop())
    assert reference() is None
    assert writer.close_spider(spider).called

"""Process results must return to the reactor without leaking admission slots."""
import json
import os
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest
from scrapy import Request
from scrapy.http import HtmlResponse
from twisted.internet import reactor
from twisted.internet.defer import CancelledError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import crawler.spiders.parser as module  # noqa: E402
from crawler.spiders.parser import HtmlParserPool  # noqa: E402


@pytest.fixture
def controlled_pool(monkeypatch):
    executor = Mock()
    monkeypatch.setattr(module, "ProcessPoolExecutor", Mock(return_value=executor))
    pool = HtmlParserPool(2, on_broken=Mock())
    futures, notifications = [], []

    def submit(*args):
        future = Future()
        future.set_running_or_notify_cancel()
        futures.append(future)
        return future

    pool._executor.submit.side_effect = submit
    monkeypatch.setattr(reactor, "callFromThread", notifications.append)
    response = HtmlResponse("https://example.com", body=b"<html/>", encoding="utf-8",
                            request=Request("https://example.com"))
    return pool, response, futures, notifications


def submit(pool, response):
    result = pool._slots.acquire()
    result.addCallback(lambda _: pool._submit(response))
    return result


def test_admission_is_bounded_and_completion_runs_on_reactor(controlled_pool):
    pool, response, futures, notifications = controlled_pool
    results = [submit(pool, response) for _ in range(6)]
    assert len(futures) == 4
    assert pool.waiting == 2
    received = []
    results[0].addCallback(received.append)

    futures[0].set_result({"title": "done"})
    assert received == []
    assert len(futures) == 4
    notifications.pop(0)()

    assert received == [{"title": "done"}]
    assert len(futures) == 5
    assert pool.waiting == 1


def test_cancelling_running_task_holds_slot_until_child_finishes(controlled_pool):
    pool, response, futures, notifications = controlled_pool
    result = submit(pool, response)
    errors = []
    result.addErrback(lambda failure: errors.append(failure.type))
    result.cancel()
    assert errors == [CancelledError]
    assert pool.inflight == 1
    assert not futures[0].cancelled()

    futures[0].set_result({})
    notifications.pop(0)()
    assert pool.inflight == 0


def test_cancelling_waiter_does_not_submit_another_job(controlled_pool):
    pool, response, futures, notifications = controlled_pool
    results = [submit(pool, response) for _ in range(5)]
    results[-1].addErrback(lambda failure: None)
    results[-1].cancel()
    assert not pool.waiting
    futures[0].set_result({})
    notifications.pop(0)()
    assert len(futures) == 4
    assert pool.inflight == 3


def test_child_exception_reaches_callback_and_releases_slot(controlled_pool):
    pool, response, futures, notifications = controlled_pool
    result = submit(pool, response)
    errors = []
    result.addErrback(lambda failure: errors.append(failure.value))
    futures[0].set_exception(ValueError("bad HTML task"))
    notifications.pop(0)()
    assert str(errors[0]) == "bad HTML task"
    assert pool.inflight == 0
    pool._on_broken.assert_not_called()


def test_broken_pool_rejects_waiters_and_notifies_shutdown_once(controlled_pool):
    pool, response, futures, notifications = controlled_pool
    errors = []
    for _ in range(7):
        submit(pool, response).addErrback(lambda failure: errors.append(failure.type))
    assert pool.waiting == 3
    for future in futures:
        future.set_exception(BrokenProcessPool("child died"))
    while notifications:
        notifications.pop(0)()
    # Waiters and subsequent submissions never reach the broken executor.
    submit(pool, response).addErrback(lambda failure: errors.append(failure.type))
    assert errors == [BrokenProcessPool] * 8
    assert len(futures) == 4
    assert pool.inflight == pool.waiting == 0
    pool._on_broken.assert_called_once_with()


def test_broken_pool_submission_requests_shutdown(controlled_pool):
    pool, response, _, _ = controlled_pool
    pool._executor.submit.side_effect = BrokenProcessPool("already broken")
    errors = []
    submit(pool, response).addErrback(lambda failure: errors.append(failure.type))
    assert errors == [BrokenProcessPool]
    assert pool.inflight == 0
    pool._on_broken.assert_called_once_with()


def test_cancelled_task_still_reports_broken_pool(controlled_pool):
    pool, response, futures, notifications = controlled_pool
    result = submit(pool, response)
    result.addErrback(lambda failure: None)
    result.cancel()
    futures[0].set_exception(BrokenProcessPool("child died after cancellation"))
    notifications.pop(0)()
    assert pool.inflight == 0
    pool._on_broken.assert_called_once_with()


def test_submit_failure_releases_slot(controlled_pool):
    pool, response, _, _ = controlled_pool
    pool._executor.submit.side_effect = RuntimeError("pool unavailable")
    errors = []
    submit(pool, response).addErrback(lambda failure: errors.append(failure.value))
    assert str(errors[0]) == "pool unavailable"
    assert pool.inflight == 0


def test_closing_pool_rejects_new_tasks_and_releases_slot(controlled_pool):
    pool, response, _, _ = controlled_pool
    pool._closing = True
    errors = []
    submit(pool, response).addErrback(lambda failure: errors.append(failure.value))
    assert str(errors[0]) == "HTML parser pool is closing"
    pool._executor.submit.assert_not_called()
    assert pool.inflight == 0


def test_two_real_processes_match_inline_and_close_cleanly(tmp_path):
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "helpers/html_parser_runner.py"), str(tmp_path)],
        cwd=root / "containers/crawler",
        env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"},
        capture_output=True, text=True, timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["same_output"]
    assert report["processes"] == 2
    assert report["pages"] == 12
    assert report["reactor_ticks"] > 0
    assert report["children_after_close"] == 0


def test_killed_parser_child_stops_crawler_and_restart_produces_output(tmp_path):
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / "helpers/html_parser_failure_runner.py"),
         str(tmp_path)],
        cwd=root / "containers/crawler",
        env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"},
        capture_output=True, text=True, timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["failed_reason"] == "parser_pool_broken"
    events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    interrupted = [event for event in events if event.get("event") == "parser.interrupted"]
    assert len(interrupted) == 6
    assert len({event["url"] for event in interrupted}) == 6
    assert all(event["domain_id"] == 7 for event in interrupted)
    assert len([event for event in events if event.get("event") == "parser.pool_broken"]) == 1
    assert report["failure_count"] == 1
    assert report["interrupted"] == 6
    assert report["inflight"] == report["waiting"] == report["pending"] == 0
    assert report["domain_pending"] == {}
    assert report["children_after_close"] == 0
    assert report["restarted_reason"] == "test_complete"
    assert report["restarted_output"] == report["original_output"]


def test_supervisor_restarts_crawlers_after_clean_failure_shutdown():
    import configparser

    config = configparser.ConfigParser(interpolation=None)
    config.read(Path(__file__).resolve().parents[1] / "supervisord.conf")
    for name in ("program:crawler_default", "program:crawler_canary"):
        assert config[name].getboolean("autorestart")

import asyncio
import hashlib
import json
import os
import sys
import threading
import time
from collections import deque
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from scrapy import Request
from scrapy.dupefilters import BaseDupeFilter
from scrapy.http.request import NO_CALLBACK

from ops.production import crawler_checkpoint as transition


class Queue:
    def __init__(self, requests=()):
        self.requests = deque(requests)
        self.df = BaseDupeFilter()

    def __len__(self):
        return len(self.requests)

    def next_request(self):
        return self.requests.popleft() if self.requests else None

    def enqueue_request(self, request):
        self.requests.append(request)
        return True


class Scheduler:
    def __init__(self, requests):
        self.normal = Queue(requests)
        self.manual = Queue()

    def next_request(self):
        return self.normal.next_request()


class Downloader:
    def __init__(self, active):
        self.active = set(active)
        self.slots = {}
        self.middleware = SimpleNamespace(middlewares=[])
        self.kicked = []

    def _process_queue(self, slot):
        self.kicked.append(slot)


class Consumer:
    def __init__(self):
        self.inflight = set()

    def refill(self):
        return "refilled"


class Spider:
    crawler_id = 2

    def __init__(self):
        self.manual_bulk_crawls = Consumer()
        self._pending_requests = 117
        self._domain_pending = {7: 117}

    def _reserve_urls(self, *args, **kwargs):
        return [(7, "https://example.test/new")]


@pytest.fixture
def env(monkeypatch):
    module = ModuleType("config_transition_fixture")
    module.__file__ = transition.SOURCE_FILE
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(Spider, "__module__", module.__name__)
    monkeypatch.setattr(transition, "EXPECTED_PID", os.getpid())
    monkeypatch.setattr(transition, "EXPECTED_WORKER", 2)
    monkeypatch.setattr(transition, "NONCE", "local-proof-controller-nonce-12345")
    requests = [Request(f"https://example.test/{index}", meta={"source_url": f"https://example.test/{index}", "_track_domain_id": 7}) for index in range(3)]
    scheduler = Scheduler(requests[:2])
    downloader = Downloader(requests[2:])
    slot = SimpleNamespace(scheduler=scheduler, inprogress=set(requests[2:]), nextcall=SimpleNamespace(schedule=lambda: None))
    scraper = SimpleNamespace(active=set(), queue=deque(), itemproc_size=0, active_size=0)
    engine = SimpleNamespace(_slot=slot, _start=None, downloader=downloader, scraper=SimpleNamespace(slot=scraper))
    spider = Spider()
    return SimpleNamespace(spider=spider, engine=engine), requests


def test_freeze_stops_only_intake_dispatch_and_restores_exact_queues(env):
    async def run():
        crawler, requests = env
        scheduler = crawler.engine._slot.scheduler
        original_normal = list(scheduler.normal.requests)
        first = transition.operation(crawler, "freeze")
        assert first["frozen"]
        assert crawler.spider._reserve_urls(force=True) == []
        assert crawler.spider.manual_bulk_crawls.refill() is None
        assert scheduler.next_request() is None
        assert list(scheduler.normal.requests) == original_normal
        transition.operation(crawler, "resume")
        assert crawler.spider._reserve_urls(force=True)
        assert crawler.spider.manual_bulk_crawls.refill() == "refilled"
        assert scheduler.next_request() is requests[0]
        assert "_reserve_urls" not in crawler.spider.__dict__
    asyncio.run(run())


def test_checkpoint_fsyncs_all_normal_work_and_keeps_original_objects(env, tmp_path):
    async def run():
        crawler, requests = env
        transition.operation(crawler, "freeze")
        saved = await transition.checkpoint(crawler, str(tmp_path))
        raw = await asyncio.to_thread(Path(saved["path"]).read_bytes)
        assert hashlib.sha256(raw).hexdigest() == saved["sha256"]
        payload = json.loads(raw)
        assert saved["request_count"] == 3
        assert {row["request_id"] for row in payload["requests"]} == {id(request) for request in requests}
        assert len(crawler.engine._slot.scheduler.normal) == 0
        assert crawler.spider._pending_requests == 117
        assert transition.ready_to_stop(crawler)["ready"]
        transition.operation(crawler, "resume")
        assert list(crawler.engine._slot.scheduler.normal.requests) == requests[:2]
        assert crawler.engine._slot.inprogress == {requests[2]}
    asyncio.run(run())


def test_api_and_internal_robots_recover_through_original_durable_tasks(env, tmp_path):
    async def run():
        crawler, _requests = env
        api = Request("https://example.test/api", meta={"manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 0})
        robots = Request("https://example.test/robots.txt", meta={"dont_obey_robotstxt": True}, callback=NO_CALLBACK)
        crawler.engine._slot.inprogress.update((api, robots))
        crawler.engine.downloader.active.update((api, robots))
        transition.operation(crawler, "freeze")
        saved = await transition.checkpoint(crawler, str(tmp_path))
        payload = json.loads(await asyncio.to_thread(Path(saved["path"]).read_bytes))
        assert len(payload["requests"]) == 3
        assert all(row["request_id"] not in {id(api), id(robots)} for row in payload["requests"])
        transition.operation(crawler, "resume")
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["transfers", "scraper", "items", "start", "engine_extra", "dispatched_slot"])
def test_checkpoint_refuses_any_unfinished_result_or_unaccounted_transfer(env, tmp_path, kind):
    async def run():
        crawler, requests = env
        transition.operation(crawler, "freeze")
        engine = crawler.engine
        if kind in {"transfers", "dispatched_slot"}:
            engine.downloader.slots["example.test"] = SimpleNamespace(
                active={requests[2]}, queue=deque(), transferring={requests[2]} if kind == "transfers" else set())
        elif kind == "scraper":
            engine.scraper.slot.active.add(requests[2])
        elif kind == "items":
            engine.scraper.slot.itemproc_size = 1
        elif kind == "start":
            engine._start = object()
        elif kind == "engine_extra":
            engine._slot.inprogress.add(requests[0])
        with pytest.raises(RuntimeError):
            await transition.checkpoint(crawler, str(tmp_path))
        transition.operation(crawler, "resume")
    asyncio.run(run())


def test_writer_runs_off_event_loop_and_cancellation_does_not_drop_work(env, tmp_path, monkeypatch):
    async def run():
        crawler, requests = env
        transition.operation(crawler, "freeze")
        original = transition._write_checkpoint
        entered = threading.Event()

        def writer(*args):
            assert threading.current_thread() is not threading.main_thread()
            entered.set()
            time.sleep(0.08)
            return original(*args)

        monkeypatch.setattr(transition, "_write_checkpoint", writer)
        task = asyncio.create_task(transition.checkpoint(crawler, str(tmp_path)))
        beats = 0
        while not entered.is_set():
            if task.done():
                await task
            await asyncio.sleep(0.002)
        for _ in range(3):
            await asyncio.sleep(0.005)
            beats += 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert beats == 3 and not getattr(crawler.spider, transition.STATE_ATTRIBUTE).get("checkpoint")
        transition.operation(crawler, "resume")
        assert list(crawler.engine._slot.scheduler.normal.requests) == requests[:2]
    asyncio.run(run())


def test_auto_resume_after_controller_disappears_restores_held_requests(env, tmp_path, monkeypatch):
    async def run():
        crawler, requests = env
        transition.operation(crawler, "freeze")
        await transition.checkpoint(crawler, str(tmp_path))
        state = getattr(crawler.spider, transition.STATE_ATTRIBUTE)
        state["timer"].cancel()
        asyncio.get_running_loop().call_later(0.01, transition._restore, crawler, state, "auto_resumed")
        await asyncio.sleep(0.02)
        assert state["phase"] == "auto_resumed"
        assert list(crawler.engine._slot.scheduler.normal.requests) == requests[:2]
        assert crawler.spider._reserve_urls(force=True)
    asyncio.run(run())


def test_stale_checkpoint_cannot_authorize_stop_after_new_normal_work(env, tmp_path):
    async def run():
        crawler, _ = env
        transition.operation(crawler, "freeze")
        await transition.checkpoint(crawler, str(tmp_path))
        crawler.engine._slot.scheduler.normal.enqueue_request(Request("https://example.test/new", meta={"source_url": "https://example.test/new", "_track_domain_id": 7}))
        with pytest.raises(RuntimeError):
            transition.ready_to_stop(crawler)
        transition.operation(crawler, "resume")
    asyncio.run(run())


def test_guard_rejects_wrong_worker_and_unmerged_parser(env, monkeypatch):
    async def run():
        crawler, _ = env
        monkeypatch.setattr(transition, "EXPECTED_WORKER", 3)
        with pytest.raises(RuntimeError):
            transition.operation(crawler, "freeze")
        monkeypatch.setattr(transition, "EXPECTED_WORKER", 2)
        crawler.spider.html_parser_pool = object()
        with pytest.raises(RuntimeError):
            transition.operation(crawler, "freeze")
    asyncio.run(run())


def test_corrupt_request_metadata_keeps_held_objects_for_rollback(env, tmp_path):
    async def run():
        crawler, requests = env
        del requests[0].meta["source_url"]
        transition.operation(crawler, "freeze")
        with pytest.raises(RuntimeError):
            await transition.checkpoint(crawler, str(tmp_path))
        transition.operation(crawler, "resume")
        assert list(crawler.engine._slot.scheduler.normal.requests) == requests[:2]
    asyncio.run(run())


def test_operator_conflict_does_not_overwrite_their_method_or_discard_held_requests(env, tmp_path):
    async def run():
        crawler, requests = env
        transition.operation(crawler, "freeze")
        await transition.checkpoint(crawler, str(tmp_path))
        replacement = lambda *args: None
        crawler.engine.downloader._process_queue = replacement
        transition.operation(crawler, "resume")
        state = getattr(crawler.spider, transition.STATE_ATTRIBUTE)
        assert state["phase"] == "restore_conflict" and state["held"] == requests[:2]
        assert crawler.engine.downloader._process_queue is replacement
    asyncio.run(run())

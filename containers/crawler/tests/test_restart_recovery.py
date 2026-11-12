import asyncio
import hashlib
import json
from collections import deque
from types import SimpleNamespace

import pytest
from scrapy import Request
from scrapy.http import TextResponse
from twisted.internet.task import Clock
from twisted.python.failure import Failure

from containers.crawler.crawler import restart_recovery as recovery


class Signals:
    def __init__(self):
        self.connections = []

    def connect(self, receiver, signal):
        self.connections.append((receiver, signal))

    def disconnect(self, receiver, signal):
        self.connections = [entry for entry in self.connections if entry != (receiver, signal)]


class RetryAfterMiddleware:
    def __init__(self):
        self.clock = Clock()
        self.cooldown_until = {}


class Downloader:
    def __init__(self):
        self.slots = {}
        self.kicked = []
        self.middleware = SimpleNamespace(middlewares=[RetryAfterMiddleware()])

    def _get_slot(self, request):
        key = request.meta["download_slot"]
        if key not in self.slots:
            self.slots[key] = SimpleNamespace(latercall=None, queue=deque())
        return key, self.slots[key]

    def _process_queue(self, slot):
        self.kicked.append(slot)


class Spider:
    crawler_id = 2

    def __init__(self):
        self._pending_requests = 0
        self._max_pending_requests = 0
        self._domain_pending = {}

    def parse(self, response):
        return []

    def errback(self, failure):
        return []

    def _build_request(self, url, domain):
        self._domain_pending[domain] = self._domain_pending.get(domain, 0) + 1
        return Request(url, callback=self.parse, errback=self.errback,
                       meta={"source_url": url, "_track_domain_id": domain})


class Engine:
    running = False

    def __init__(self):
        self.downloader = Downloader()
        self.requests = []
        self.paused = False
        self.stopped = False

    def crawl(self, request):
        self.requests.append(request)

    def pause(self):
        self.paused = True

    async def stop_async(self):
        self.stopped = True


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(recovery, "time", SimpleNamespace(time=lambda: 1000.0))
    root = tmp_path / "repo"
    paths = ["containers/crawler/crawler/spiders/spider.py",
             "containers/crawler/crawler/restart_recovery.py",
             "containers/crawler/config/site_policies.yml"]
    hashes = {}
    for relative in paths:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative)
        hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(recovery, "REPOSITORY_ROOT", root)
    payload = {
        "schema": 2, "pid": 2_000_000_000, "crawler_id": 2,
        "requests": [
            {"request_id": 11, "source_url": "https://example.test/a", "current_url": "https://example.test/a", "domain_id": 7, "method": "GET"},
            {"request_id": 12, "source_url": "https://example.test/b", "current_url": "https://example.test/b", "domain_id": 7, "method": "GET"},
        ],
        "retry_after_deadlines_epoch": {"example.test": 1020.0},
    }
    directory = tmp_path / "runtime"
    directory.mkdir()
    checkpoint = directory / "release1" / "worker-02.json"
    checkpoint.parent.mkdir()
    raw = canonical(payload)
    checkpoint.write_bytes(raw)
    manifest = {
        "schema": 1, "release_id": "release1", "source_sha256": hashes,
        "workers": {"2": {"path": str(checkpoint), "sha256": hashlib.sha256(raw).hexdigest(), "old_pid": payload["pid"]}},
        "cooldown_deadlines_epoch": {"example.test": 1030.0},
    }
    manifest_path = directory / "active.json"
    manifest_path.write_bytes(canonical(manifest))
    spider = Spider()
    crawler = SimpleNamespace(spider=spider, settings={"CRAWLER_RESTART_MANIFEST": str(manifest_path)}, signals=Signals(), engine=Engine())
    return crawler, spider, manifest_path, manifest, checkpoint, payload


def prepare(env):
    crawler, spider, *_ = env
    return recovery.RestartRecovery.prepare(crawler, spider)


def update_payload(env):
    _, _, manifest_path, manifest, checkpoint, payload = env
    raw = canonical(payload)
    checkpoint.write_bytes(raw)
    manifest["workers"]["2"]["sha256"] = hashlib.sha256(raw).hexdigest()
    manifest_path.write_bytes(canonical(manifest))


def test_no_manifest_is_noop_before_normal_start(tmp_path):
    crawler = SimpleNamespace(settings={"CRAWLER_RESTART_MANIFEST": str(tmp_path / "absent.json")})
    assert recovery.RestartRecovery.prepare(crawler, Spider()) is None


def test_validation_does_not_read_or_dispatch_normal_work(env):
    instance = prepare(env)
    crawler, spider, *_ = env
    assert instance.pending == {0, 1}
    assert spider._pending_requests == 0 and not spider._domain_pending
    assert not crawler.engine.requests
    assert len(crawler.signals.connections) == 3


@pytest.mark.parametrize("kind", ["checksum", "source", "worker", "pid", "url", "domain", "post", "duplicate", "deadline", "schema"])
def test_invalid_manifest_or_checkpoint_fails_before_dispatch(env, kind):
    crawler, spider, path, manifest, checkpoint, payload = env
    if kind == "checksum":
        checkpoint.write_bytes(b"wrong")
    elif kind == "source":
        manifest["source_sha256"][next(iter(manifest["source_sha256"]))] = "0" * 64
        path.write_bytes(canonical(manifest))
    else:
        if kind == "worker":
            payload["crawler_id"] = 3
        elif kind == "pid":
            payload["pid"] = 0
        elif kind == "url":
            payload["requests"][1]["source_url"] = "http://[invalid"
        elif kind == "domain":
            payload["requests"][1]["domain_id"] = False
        elif kind == "post":
            payload["requests"][1]["method"] = "POST"
        elif kind == "duplicate":
            payload["requests"][1]["request_id"] = 11
        elif kind == "deadline":
            payload["retry_after_deadlines_epoch"]["example.test"] = float("nan")
        elif kind == "schema":
            payload["schema"] = 1
        update_payload(env)
    with pytest.raises(recovery.RecoveryError):
        prepare(env)
    assert not crawler.engine.requests and not spider._domain_pending


def test_manifest_and_checkpoint_symlinks_rejected(env):
    _, _, path, _manifest, checkpoint, _ = env
    target = path.with_name("manifest-real.json")
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(recovery.RecoveryError):
        prepare(env)
    path.unlink()
    target.rename(path)
    target = checkpoint.with_name("checkpoint-real.json")
    checkpoint.rename(target)
    checkpoint.symlink_to(target)
    with pytest.raises(recovery.RecoveryError):
        prepare(env)


def test_engine_start_restores_cooldowns_before_enqueue_and_is_idempotent(env):
    async def run():
        crawler, spider, *_ = env
        instance = prepare(env)
        original = crawler.engine.crawl

        def guarded(request):
            slot = crawler.engine.downloader.slots["example.test"]
            assert slot.latercall is not None
            original(request)

        crawler.engine.crawl = guarded
        await instance.engine_started()
        assert len(crawler.engine.requests) == 2
        assert spider._pending_requests == 2 and spider._domain_pending == {7: 2}
        assert all(request.dont_filter for request in crawler.engine.requests)
        await instance.engine_started()
        assert len(crawler.engine.requests) == 2
        middleware = crawler.engine.downloader.middleware.middlewares[0]
        middleware.clock.advance(29)
        assert not crawler.engine.downloader.kicked
        middleware.clock.advance(1)
        assert len(crawler.engine.downloader.kicked) == 1
        assert not middleware.cooldown_until
    asyncio.run(run())


def test_idle_slot_gc_cannot_bypass_saved_cooldown(env):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)
        await instance.engine_started()
        downloader = crawler.engine.downloader
        middleware = downloader.middleware.middlewares[0]
        old_slot = downloader.slots.pop("example.test")
        old_slot.latercall.cancel()  # Scrapy's idle slot GC calls Slot.close().
        middleware.clock.advance(5)
        request = Request("https://example.test/new", meta={"download_slot": "example.test"})
        _, new_slot = downloader._get_slot(request)
        instance.request_reached_downloader(request)
        assert new_slot is not old_slot and new_slot.latercall is not None
        middleware.clock.advance(24)
        assert not downloader.kicked
        middleware.clock.advance(1)
        assert downloader.kicked == [new_slot]
        assert not any(receiver == instance.request_reached_downloader for receiver, _ in crawler.signals.connections)
    asyncio.run(run())


def test_a_later_429_timer_is_not_replaced_on_same_slot(env):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)
        await instance.engine_started()
        downloader = crawler.engine.downloader
        middleware = downloader.middleware.middlewares[0]
        slot = downloader.slots["example.test"]
        slot.latercall.cancel()
        middleware.cooldown_until["example.test"] = 80
        replacement = middleware.clock.callLater(80, lambda: None)
        slot.latercall = replacement
        instance.request_reached_downloader(Request("https://example.test/new", meta={"download_slot": "example.test"}))
        assert slot.latercall is replacement
    asyncio.run(run())


def test_done_only_after_every_successful_pipeline_item_and_failure_item(env, monkeypatch):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)
        writes = []
        original = recovery._write_done

        def record(path, receipt):
            writes.append(receipt)
            original(path, receipt)

        monkeypatch.setattr(recovery, "_write_done", record)
        await instance.engine_started()
        first, second = crawler.engine.requests
        await instance.item_scraped({}, TextResponse(first.url, request=first))
        assert not writes and not instance.done.exists()
        # Scrapy sends the original Failure through item_scraped when an
        # errback yielded the result item, after its pipeline completed.
        failure = Failure(RuntimeError("fetch failed"))
        failure.request = second
        await instance.item_scraped({}, failure)
        assert len(writes) == 1 and instance.completed
        await instance.item_scraped({}, failure)
        assert len(writes) == 1
        receipt = json.loads(instance.done.read_text())
        assert receipt["completed_count"] == 2
    asyncio.run(run())


def test_partial_completion_replays_whole_checkpoint_after_crash(env):
    async def run():
        crawler, _spider, *_ = env
        first = prepare(env)
        await first.engine_started()
        request = crawler.engine.requests[0]
        await first.item_scraped({}, TextResponse(request.url, request=request))
        assert not first.done.exists()
        crawler.engine = Engine()
        crawler.spider = Spider()
        restarted = recovery.RestartRecovery.prepare(crawler, crawler.spider)
        await restarted.engine_started()
        assert len(crawler.engine.requests) == 2  # at-least-once, no lost task
    asyncio.run(run())


def test_pending_checkpoint_after_handoff_accepts_new_compatible_source(env, monkeypatch):
    async def run():
        crawler, _, path, manifest, _, payload = env
        first = prepare(env)
        await first.engine_started()
        assert first.handoff.exists() and not first.done.exists()
        manifest["source_sha256"][next(iter(manifest["source_sha256"]))] = "0" * 64
        path.write_bytes(canonical(manifest))
        # PID reuse after the validated original handoff is no longer evidence
        # that the original worker is alive.
        import os
        monkeypatch.setattr(recovery, "os", SimpleNamespace(**{**vars(os), "getpid": lambda: payload["pid"]}))
        crawler.engine = Engine()
        crawler.spider = Spider()
        restarted = recovery.RestartRecovery.prepare(crawler, crawler.spider)
        await restarted.engine_started()
        assert len(crawler.engine.requests) == 2 and not restarted.completed
    asyncio.run(run())


def test_corrupt_handoff_fails_closed_without_dispatch(env):
    instance = prepare(env)
    instance.handoff.write_text('{}')
    with pytest.raises(recovery.RecoveryError):
        prepare(env)
    assert not env[0].engine.requests


def test_recreated_container_can_reuse_numeric_pid_without_false_overlap(env, monkeypatch):
    crawler, _, _, _, _, payload = env
    import os
    payload["pid"] = os.getpid()
    env[3]["workers"]["2"]["old_pid"] = payload["pid"]
    payload["process_identity"] = {"boot_id": "same-boot", "pid_namespace": "old-namespace", "start_ticks": 100}
    update_payload(env)
    monkeypatch.setattr(recovery, "_namespace_identity", lambda: {"boot_id": "same-boot", "pid_namespace": "new-namespace"})
    instance = prepare(env)
    assert instance is not None and not crawler.engine.requests


def test_same_original_process_is_rejected_but_later_pid_reuse_is_allowed(env, monkeypatch):
    _, _, _, _, _, payload = env
    payload["process_identity"] = {"boot_id": "boot", "pid_namespace": "namespace", "start_ticks": 100}
    update_payload(env)
    monkeypatch.setattr(recovery, "_namespace_identity", lambda: {"boot_id": "boot", "pid_namespace": "namespace"})
    monkeypatch.setattr(recovery, "_start_ticks", lambda pid: 100)
    with pytest.raises(recovery.RecoveryError):
        prepare(env)
    monkeypatch.setattr(recovery, "_start_ticks", lambda pid: 200)
    assert prepare(env) is not None


def test_handoff_failure_stops_before_dispatch_and_preserves_pending_checkpoint(env, monkeypatch):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)

        def fail(*args):
            raise OSError("handoff disk unavailable")

        monkeypatch.setattr(recovery, "_write_handoff", fail)
        with pytest.raises(OSError):
            await instance.engine_started()
        assert crawler.engine.paused and crawler.engine.stopped
        assert not instance.done.exists() and not instance.handoff.exists()
    asyncio.run(run())


def test_completed_checkpoint_skips_replay_but_retains_future_cooldowns(env):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)
        await instance.engine_started()
        for request in crawler.engine.requests:
            await instance.item_scraped({}, TextResponse(request.url, request=request))
        crawler.engine = Engine()
        crawler.spider = Spider()
        restarted = recovery.RestartRecovery.prepare(crawler, crawler.spider)
        await restarted.engine_started()
        assert not crawler.engine.requests
        assert crawler.engine.downloader.slots["example.test"].latercall is not None
    asyncio.run(run())


def test_completed_and_expired_runtime_history_does_not_block_new_source(env, monkeypatch):
    async def run():
        crawler, _, manifest_path, manifest, _, _ = env
        instance = prepare(env)
        await instance.engine_started()
        for request in crawler.engine.requests:
            await instance.item_scraped({}, TextResponse(request.url, request=request))
        # A later reviewed release can change its source after all checkpoint
        # tasks finished; an obsolete completed manifest must remain harmless.
        manifest["source_sha256"][next(iter(manifest["source_sha256"]))] = "0" * 64
        manifest_path.write_bytes(canonical(manifest))
        monkeypatch.setattr(recovery, "time", SimpleNamespace(time=lambda: 2000.0))
        crawler.engine = Engine()
        crawler.spider = Spider()
        assert recovery.RestartRecovery.prepare(crawler, crawler.spider) is None
    asyncio.run(run())


def test_redirect_copy_keeps_completion_identity(env):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)
        await instance.engine_started()
        original = crawler.engine.requests[0]
        redirected = original.replace(url="https://other.test/final")
        await instance.item_scraped({}, TextResponse(redirected.url, request=redirected))
        assert instance.pending == {1}
    asyncio.run(run())


def test_unrelated_and_unpersisted_items_do_not_complete_checkpoint(env):
    async def run():
        instance = prepare(env)
        await instance.engine_started()
        await instance.item_scraped({}, TextResponse("https://other.test/", request=Request("https://other.test/")))
        await instance.item_scraped({}, None)
        assert instance.pending == {0, 1} and not instance.done.exists()
    asyncio.run(run())


def test_zero_normal_tasks_complete_once_but_seed_cooldowns(env):
    async def run():
        crawler, _, _, _, _, payload = env
        payload["requests"] = []
        update_payload(env)
        instance = prepare(env)
        await instance.engine_started()
        assert instance.completed and instance.done.exists()
        assert not crawler.engine.requests
        assert crawler.engine.downloader.slots["example.test"].latercall is not None
    asyncio.run(run())


def test_restore_failure_stops_engine_before_any_request_can_dispatch(env):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)
        crawler.engine.downloader.middleware.middlewares = []
        with pytest.raises(recovery.RecoveryError):
            await instance.engine_started()
        assert crawler.engine.paused and crawler.engine.stopped
        assert not crawler.engine.requests
    asyncio.run(run())


def test_bad_done_receipt_is_not_silently_accepted(env):
    instance = prepare(env)
    instance.done.write_text('{}')
    with pytest.raises(recovery.RecoveryError):
        prepare(env)


def test_fsync_failure_never_marks_checkpoint_complete(env, monkeypatch):
    async def run():
        crawler, _, *_ = env
        instance = prepare(env)

        def fail(*args):
            raise OSError("disk unavailable")

        monkeypatch.setattr(recovery, "_write_done", fail)
        await instance.engine_started()
        first, second = crawler.engine.requests
        await instance.item_scraped({}, TextResponse(first.url, request=first))
        with pytest.raises(OSError):
            await instance.item_scraped({}, TextResponse(second.url, request=second))
        assert not instance.completed and not instance.done.exists()
    asyncio.run(run())

"""Bounded operations helper; invoke directly on the worker's asyncio thread.

The parent controller owns source/config deployment and process management.
Checkpoint file I/O runs in a thread over an immutable JSON payload. No method
waits synchronously for the reactor, another thread, or a network response.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import sys
import threading
import time
from pathlib import Path

from scrapy.http.request import NO_CALLBACK

EXPECTED_PID = None
EXPECTED_WORKER = None
NONCE = None
SOURCE_FILE = "/app/containers/crawler/crawler/spiders/spider.py"
TIMEOUT_SECONDS = 1200
MAX_NORMAL_REQUESTS = 100_000
STATE_ATTRIBUTE = "_approved_config_transition"


def _guard(crawler):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Operation must execute directly on the main event loop")
    if type(EXPECTED_PID) is not int or os.getpid() != EXPECTED_PID:
        raise RuntimeError("Expected PID mismatch")
    if type(EXPECTED_WORKER) is not int or not 0 <= EXPECTED_WORKER < 16:
        raise RuntimeError("Invalid expected worker")
    if not isinstance(NONCE, str) or len(NONCE) < 24:
        raise RuntimeError("Missing controller nonce")
    spider = crawler.spider
    if type(spider.crawler_id) is not int or spider.crawler_id != EXPECTED_WORKER:
        raise RuntimeError("Worker ID mismatch")
    source = getattr(sys.modules.get(type(spider).__module__), "__file__", None)
    if source != SOURCE_FILE or hasattr(spider, "html_parser_pool"):
        raise RuntimeError("Expected merged ordinary crawler source")
    state = getattr(spider, STATE_ATTRIBUTE, None)
    if state is not None and state["nonce"] != NONCE:
        raise RuntimeError("Another controller owns this worker")
    return spider, state


def _parts(crawler):
    engine = crawler.engine
    slot = getattr(engine, "_slot", None) or getattr(engine, "slot", None)
    return engine, slot, slot.scheduler, engine.downloader, engine.scraper.slot


def _save(owner, name, replacement):
    return {
        "owner": owner, "name": name,
        "present": name in owner.__dict__,
        "value": owner.__dict__.get(name),
        "bound": getattr(owner, name), "replacement": replacement,
    }


def _restore(crawler, state, phase):
    spider = crawler.spider
    current = getattr(spider, STATE_ATTRIBUTE, None)
    if current is not state or state["phase"] != "frozen":
        return
    # Restoring a scheduler owned by someone else could lose our held work.
    if any(item["owner"].__dict__.get(item["name"]) is not item["replacement"]
           for item in state["methods"]) or _parts(crawler)[2].normal is not state["normal"]:
        state["phase"] = "restore_conflict"
        return
    normal = state["normal"]
    for restored, request in enumerate(state["held"]):
        # Existing production normal work uses BaseDupeFilter (no filtering).
        # Re-enqueue without touching normal ownership counters a second time.
        if not normal.enqueue_request(request):
            del state["held"][:restored]
            state["phase"] = "restore_conflict"
            return
    state["held"].clear()
    for item in state["methods"]:
        if item["present"]:
            setattr(item["owner"], item["name"], item["value"])
        else:
            item["owner"].__dict__.pop(item["name"], None)
    if state["timer"] is not None:
        state["timer"].cancel()
    state["phase"] = phase
    downloader = crawler.engine.downloader
    for slot in downloader.slots.values():
        if slot.queue:
            downloader._process_queue(slot)
    _engine, slot, *_ = _parts(crawler)
    slot.nextcall.schedule()


def _snapshot(crawler, state):
    engine, slot, scheduler, downloader, scraper = _parts(crawler)
    if state and state["phase"] == "frozen" and (
            tuple(map(id, (engine, slot, scheduler, downloader, scraper, scheduler.normal))) != state["parts"]):
        raise RuntimeError("Pipeline objects changed during the transition")
    values = {
        "scheduler_normal": len(scheduler.normal),
        "scheduler_manual": len(scheduler.manual),
        "engine_inprogress": len(slot.inprogress),
        "downloader_active": len(downloader.active),
        "transferring": sum(len(item.transferring) for item in downloader.slots.values()),
        "scraper_active": len(scraper.active),
        "scraper_queue": len(scraper.queue),
        "scraper_itemproc": scraper.itemproc_size,
        "scraper_active_size": scraper.active_size,
    }
    if any(type(value) not in (int, float) or value < 0 for value in values.values()):
        raise RuntimeError("Invalid pipeline counter")
    frozen = bool(state and state["phase"] == "frozen" and all(
        item["owner"].__dict__.get(item["name"]) is item["replacement"]
        for item in state["methods"]
    ))
    return {
        "pid": os.getpid(), "worker_id": crawler.spider.crawler_id,
        "phase": state["phase"] if state else "not_frozen", "frozen": frozen,
        "counts": values, "start_iterator_active": engine._start is not None,
        "held_normal": len(state["held"]) if state else 0,
        "owned_pending": crawler.spider._pending_requests,
        "owned_domain_pending": sum(crawler.spider._domain_pending.values()),
        "remaining_seconds": max(0, state["deadline"] - state["loop"].time())
            if frozen else 0,
        "checkpoint": state.get("checkpoint") if state else None,
    }


def operation(crawler, action):
    if action not in {"freeze", "status", "resume"}:
        raise ValueError("Expected freeze, status or resume")
    spider, state = _guard(crawler)
    if action == "freeze" and (state is None or state["phase"] != "frozen"):
        if state is not None and state["phase"] == "restore_conflict":
            raise RuntimeError("Previous restore conflict needs review")
        engine, slot, scheduler, downloader, scraper = _parts(crawler)
        if engine._start is not None:
            raise RuntimeError("Initial start iterator must finish first")
        if len(scheduler.normal) > MAX_NORMAL_REQUESTS:
            raise RuntimeError("Normal queue exceeds checkpoint bound")
        if type(scheduler.normal.df).__name__ != "BaseDupeFilter":
            raise RuntimeError("Queue restoration requires the no-op normal dupefilter")
        loop = asyncio.get_running_loop()
        no_work = lambda *args, **kwargs: None
        methods = [
            _save(spider, "_reserve_urls", lambda *args, **kwargs: []),
            _save(spider.manual_bulk_crawls, "refill", no_work),
            _save(scheduler, "next_request", no_work),
            _save(downloader, "_process_queue", no_work),
        ]
        state = {
            "nonce": NONCE, "phase": "frozen", "methods": methods,
            "normal": scheduler.normal, "held": [], "loop": loop,
            "parts": tuple(map(id, (engine, slot, scheduler, downloader, scraper, scheduler.normal))),
            "deadline": loop.time() + TIMEOUT_SECONDS, "timer": None,
        }
        setattr(spider, STATE_ATTRIBUTE, state)
        try:
            for item in methods:
                setattr(item["owner"], item["name"], item["replacement"])
            state["timer"] = loop.call_later(TIMEOUT_SECONDS, _restore, crawler, state, "auto_resumed")
        except BaseException:
            # Installation is synchronous; restore all entries still ours.
            for item in methods:
                if item["owner"].__dict__.get(item["name"]) is item["replacement"]:
                    if item["present"]:
                        setattr(item["owner"], item["name"], item["value"])
                    else:
                        item["owner"].__dict__.pop(item["name"], None)
            state["phase"] = "freeze_failed_resumed"
            raise
    elif action == "resume" and state is not None:
        _restore(crawler, state, "resumed")
    return _snapshot(crawler, state)


def _row(request, location):
    if "manual_bulk_crawl_job_id" in request.meta:
        return None
    if request.callback is NO_CALLBACK and request.meta.get("dont_obey_robotstxt") is True:
        return None  # The saved original task regenerates this internal request.
    if request.method != "GET" or request.body:
        raise RuntimeError("Only original normal GET tasks can be checkpointed")
    domain_id = request.meta.get("_track_domain_id")
    source_url = request.meta.get("source_url")
    if type(domain_id) is not int or domain_id <= 0:
        raise RuntimeError("Normal request lacks a valid domain ID")
    if not isinstance(source_url, str) or not source_url.startswith(("http://", "https://")):
        raise RuntimeError("Normal request lacks its original source URL")
    return {"request_id": id(request), "source_url": source_url, "domain_id": domain_id,
            "current_url": request.url, "method": request.method, "location": location}


def _quiet(crawler, state):
    status = _snapshot(crawler, state)
    if not status["frozen"] or status["remaining_seconds"] < 120:
        raise RuntimeError("Frozen controller lease is missing or expiring")
    counts = status["counts"]
    for name in ("transferring", "scraper_active", "scraper_queue", "scraper_itemproc", "scraper_active_size"):
        if counts[name]:
            raise RuntimeError("Current transfers and item processing must finish")
    engine, slot, _scheduler, downloader, _scraper = _parts(crawler)
    if engine._start is not None:
        raise RuntimeError("Start iterator is active")
    if {id(request) for request in slot.inprogress} != {id(request) for request in downloader.active}:
        raise RuntimeError("An engine request is still processing its result")
    for item in downloader.slots.values():
        # Detect scheduled _wait_for_download coroutines that have not yet
        # entered the transferring set; they were already removed from queue.
        if {id(request) for request in item.active} != {id(request) for request, _ in item.queue}:
            raise RuntimeError("A slot has dispatched work outside its waiting queue")
    return status


def _capture(crawler, state):
    _quiet(crawler, state)
    _engine, slot, scheduler, downloader, _scraper = _parts(crawler)
    if len(state["held"]) + len(scheduler.normal) > MAX_NORMAL_REQUESTS:
        raise RuntimeError("Normal queue exceeds checkpoint bound")
    # Keep each popped object in memory before attempting the next pop. A
    # failure or controller timeout re-enqueues these same Request objects.
    while len(scheduler.normal):
        request = scheduler.normal.next_request()
        if request is None:
            raise RuntimeError("Nonempty normal scheduler could not produce a request")
        state["held"].append(request)
    rows = []
    ids = set()
    for location, requests in (("scheduler", state["held"]), ("engine", sorted(slot.inprogress, key=id))):
        for request in requests:
            row = _row(request, location)
            if row is None:
                continue
            if row["request_id"] in ids:
                raise RuntimeError("A normal Request exists in multiple locations")
            ids.add(row["request_id"])
            rows.append(row)
    if len(rows) > MAX_NORMAL_REQUESTS:
        raise RuntimeError("Checkpoint exceeds request bound")
    # Preserve queue order for held requests; engine order has no ordering
    # guarantee and should not be interpreted as an original scheduling rank.
    deadlines = {}
    epoch = time.time()
    for middleware in downloader.middleware.middlewares:
        if type(middleware).__name__ == "RetryAfterMiddleware":
            clock_now = middleware.clock.seconds()
            for key, deadline in middleware.cooldown_until.items():
                remaining = max(0, deadline - clock_now)
                if not math.isfinite(remaining):
                    raise RuntimeError("Invalid cooldown deadline")
                deadlines[str(key)] = max(deadlines.get(str(key), epoch), epoch + remaining)
    return {
        "schema": 2, "pid": os.getpid(), "crawler_id": crawler.spider.crawler_id,
        "captured_at_epoch": epoch, "requests": rows,
        "retry_after_deadlines_epoch": deadlines,
        "owned_pending": crawler.spider._pending_requests,
        "owned_domain_pending": sum(crawler.spider._domain_pending.values()),
        "api_inflight": len(crawler.spider.manual_bulk_crawls.inflight),
        "api_scheduler": len(scheduler.manual),
    }


def _write_checkpoint(directory, payload):
    directory = Path(directory)
    if not directory.is_absolute() or ".." in directory.parts:
        raise RuntimeError("An absolute checkpoint directory is required")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink():
        raise RuntimeError("Checkpoint directory must not be a symlink")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    digest = hashlib.sha256(raw).hexdigest()
    path = directory / f"worker-{payload['crawler_id']:02d}-{payload['pid']}-{digest}.json"
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(raw)
            file.flush()
            os.fsync(file.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    parent_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    return {"path": str(path), "sha256": digest, "request_count": len(payload["requests"]),
            "max_retry_after_epoch": max(payload["retry_after_deadlines_epoch"].values(), default=0),
            "old_pid": payload["pid"], "worker_id": payload["crawler_id"]}


async def checkpoint(crawler, directory):
    _spider, state = _guard(crawler)
    if state is None or state["phase"] != "frozen":
        raise RuntimeError("Freeze the worker first")
    if state.get("checkpoint"):
        raise RuntimeError("A checkpoint already exists; validate it instead of replacing it")
    payload = _capture(crawler, state)
    # The original objects remain held in memory. The automatic lease expiry
    # restores them even if async file writing outlives cancellation.
    result = await asyncio.to_thread(_write_checkpoint, directory, payload)
    if state["phase"] != "frozen":
        raise RuntimeError("Controller lease expired during checkpoint; do not stop")
    # Revalidate live objects after the thread has completed. Quota waiters may
    # move to downloader slot queues, but their original task identities remain.
    latest = _capture(crawler, state)
    if latest["requests"] != payload["requests"]:
        raise RuntimeError("Normal work changed during checkpoint; do not stop")
    state["checkpoint"] = result
    state["captured_rows"] = payload["requests"]
    return result


def ready_to_stop(crawler):
    _spider, state = _guard(crawler)
    if state is None or not state.get("checkpoint"):
        raise RuntimeError("No durable checkpoint")
    latest = _capture(crawler, state)
    if latest["requests"] != state["captured_rows"]:
        raise RuntimeError("Checkpoint no longer matches live work")
    return {"ready": True, "snapshot": _snapshot(crawler, state)}

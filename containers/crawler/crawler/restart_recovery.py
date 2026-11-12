"""Restore a deployment checkpoint before the crawler dispatches any request.

The manifest and checkpoints are runtime work data, never site-policy overrides.
Without an active manifest, ordinary startup is unchanged. Completion is recorded
only after every checkpoint task produced its single normal result item and the
item pipeline accepted it. A crash before then can repeat completed fetches on
the next startup: this is intentionally at-least-once recovery.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import re
import time
from pathlib import Path

import scrapy
from scrapy import signals

logger = logging.getLogger("crawler")
DEFAULT_MANIFEST = "/data/ipc/crawler_restart/active.json"
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MAX_REQUESTS = 100_000
RELEASE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\Z")
SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
META_CHECKPOINT = "_restart_checkpoint_sha256"
META_INDEX = "_restart_checkpoint_index"


class RecoveryError(RuntimeError):
    """The deployment checkpoint is unsafe to restore automatically."""


def _object(path):
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise RecoveryError("Cannot read restart metadata") from error
    if not isinstance(value, dict):
        raise RecoveryError("Restart metadata must be an object")
    return value


def _descendant(base, value):
    if not isinstance(value, str):
        raise RecoveryError("Invalid checkpoint path")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise RecoveryError("Checkpoint path must be absolute")
    try:
        path.resolve().relative_to(base.resolve())
    except ValueError as error:
        raise RecoveryError("Checkpoint path leaves the restart directory") from error
    current = path
    while current != base:
        if current.is_symlink():
            raise RecoveryError("Restart metadata cannot use symlinks")
        current = current.parent
        if current == current.parent:
            raise RecoveryError("Checkpoint path is outside the restart directory")
    return path


def _deadlines(value):
    if not isinstance(value, dict):
        raise RecoveryError("Cooldown deadlines must be an object")
    result = {}
    for key, deadline in value.items():
        if (not isinstance(key, str) or not key or len(key) > 300
                or type(deadline) not in (float, int) or not math.isfinite(deadline)
                or deadline < 0):
            raise RecoveryError("Invalid cooldown deadline")
        result[key] = deadline
    return result


def _write_receipt(path, receipt):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(raw)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(path)
        parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _write_done(path, receipt):
    _write_receipt(path, receipt)


def _write_handoff(path, receipt):
    _write_receipt(path, receipt)


class RestartRecovery:
    @classmethod
    def prepare(cls, crawler, spider):
        """Validate in spider construction, before normal queue files are read."""
        manifest_path = Path(crawler.settings.get("CRAWLER_RESTART_MANIFEST", DEFAULT_MANIFEST))
        if not manifest_path.exists():
            return None
        if manifest_path.is_symlink() or not manifest_path.is_absolute():
            raise RecoveryError("Invalid active restart manifest")
        manifest = _object(manifest_path)
        if manifest.get("schema") != 1:
            raise RecoveryError("Unknown restart manifest schema")
        release = manifest.get("release_id")
        if not isinstance(release, str) or not RELEASE_RE.fullmatch(release):
            raise RecoveryError("Invalid restart release ID")
        workers = manifest.get("workers")
        if not isinstance(workers, dict):
            raise RecoveryError("Missing restart workers")
        entry = workers.get(str(spider.crawler_id))
        if entry is None:
            # A deliberately scoped rolling restart can leave unrelated workers alone.
            return None
        if not isinstance(entry, dict):
            raise RecoveryError("Invalid restart worker entry")
        hashes = manifest.get("source_sha256")
        required = {
            "containers/crawler/crawler/spiders/spider.py",
            "containers/crawler/crawler/restart_recovery.py",
            "containers/crawler/config/site_policies.yml",
        }
        if not isinstance(hashes, dict) or not required <= hashes.keys():
            raise RecoveryError("Missing restart source hashes")
        source_paths = []
        for relative, digest in hashes.items():
            if not isinstance(relative, str) or not relative.startswith("containers/crawler/"):
                raise RecoveryError("Invalid restart source path")
            path = _descendant(REPOSITORY_ROOT, str(REPOSITORY_ROOT / relative))
            if not isinstance(digest, str) or not SHA_RE.fullmatch(digest):
                raise RecoveryError("Invalid restart source hash")
            source_paths.append((path, digest))
        checkpoint_path = _descendant(manifest_path.parent, entry.get("path"))
        try:
            raw = checkpoint_path.read_bytes()
        except OSError as error:
            raise RecoveryError("Cannot read worker checkpoint") from error
        checkpoint_hash = entry.get("sha256")
        if (not isinstance(checkpoint_hash, str) or not SHA_RE.fullmatch(checkpoint_hash)
                or hashlib.sha256(raw).hexdigest() != checkpoint_hash):
            raise RecoveryError("Worker checkpoint checksum mismatch")
        try:
            payload = json.loads(raw)
        except ValueError as error:
            raise RecoveryError("Invalid worker checkpoint JSON") from error
        if (not isinstance(payload, dict) or payload.get("schema") != 2
                or payload.get("crawler_id") != spider.crawler_id
                or type(payload.get("pid")) is not int or payload["pid"] <= 0
                or payload["pid"] != entry.get("old_pid")):
            raise RecoveryError("Worker checkpoint identity mismatch")
        rows = payload.get("requests")
        if not isinstance(rows, list) or len(rows) > MAX_REQUESTS:
            raise RecoveryError("Invalid checkpoint request count")
        seen = set()
        for row in rows:
            if (not isinstance(row, dict) or row.get("method") != "GET"
                    or type(row.get("domain_id")) is not int or row["domain_id"] <= 0
                    or type(row.get("request_id")) is not int
                    or row["request_id"] in seen
                    or not isinstance(row.get("source_url"), str)
                    or not row["source_url"].startswith(("http://", "https://"))):
                raise RecoveryError("Invalid normal checkpoint request")
            seen.add(row["request_id"])
        deadlines = _deadlines(manifest.get("cooldown_deadlines_epoch", {}))
        for key, deadline in _deadlines(payload.get("retry_after_deadlines_epoch", {})).items():
            deadlines[key] = max(deadlines.get(key, 0), deadline)
        done = _descendant(manifest_path.parent, str(manifest_path.parent / release / f"worker-{spider.crawler_id:02d}.done.json"))
        handoff = done.with_name(f"worker-{spider.crawler_id:02d}.handoff.json")
        _descendant(manifest_path.parent, str(handoff))
        handed_off = False
        if handoff.exists():
            receipt = _object(handoff)
            if (receipt.get("phase") != "handed_off" or receipt.get("checkpoint_sha256") != checkpoint_hash
                    or receipt.get("release_id") != release or receipt.get("old_pid") != payload["pid"]
                    or receipt.get("worker_id") != spider.crawler_id
                    or receipt.get("request_count") != len(rows)):
                raise RecoveryError("Invalid recovery handoff receipt")
            handed_off = True
        completed = False
        if done.exists():
            receipt = _object(done)
            if (receipt.get("phase") != "completed" or receipt.get("checkpoint_sha256") != checkpoint_hash
                    or receipt.get("release_id") != release
                    or receipt.get("worker_id") != spider.crawler_id
                    or receipt.get("completed_count") != len(rows)):
                raise RecoveryError("Invalid recovery completion receipt")
            completed = True
        if not completed:
            # An unfinished checkpoint must target exactly this release and
            # must never overlap its original worker. Once fully completed,
            # its old source pin must not block unrelated later deployments.
            if not handed_off:
                if payload["pid"] == os.getpid() or (Path("/proc") / str(payload["pid"])).exists():
                    raise RecoveryError("The old worker must stop before replay")
                for path, digest in source_paths:
                    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                        raise RecoveryError("Restart source does not match the pinned release")
            try:
                inspect.signature(spider._build_request).bind("https://restart.invalid/", 1)
            except (AttributeError, TypeError, ValueError) as error:
                raise RecoveryError("Normal request recovery interface is incompatible") from error
            for row in rows:
                try:
                    scrapy.Request(row["source_url"], callback=spider.parse, errback=spider.errback)
                except (ValueError, TypeError) as error:
                    raise RecoveryError("Invalid checkpoint URL") from error
        elif not any(deadline > time.time() for deadline in deadlines.values()):
            return None  # A completed, expired manifest is inert runtime history.
        recovery = cls(crawler, spider, rows, deadlines, release, checkpoint_hash, done, completed,
                       handoff, handed_off, payload["pid"], hashes)
        crawler.signals.connect(recovery.engine_started, signal=signals.engine_started)
        crawler.signals.connect(recovery.request_reached_downloader, signal=signals.request_reached_downloader)
        if not completed:
            crawler.signals.connect(recovery.item_scraped, signal=signals.item_scraped)
        return recovery

    def __init__(self, crawler, spider, rows, deadlines, release, digest, done, completed,
                 handoff, handed_off, old_pid, source_hashes):
        self.crawler, self.spider = crawler, spider
        self.rows, self.deadlines = rows, deadlines
        self.release, self.digest, self.done = release, digest, done
        self.pending = set() if completed else set(range(len(rows)))
        self.completed = completed
        self.handoff, self.handed_off = handoff, handed_off
        self.old_pid, self.source_hashes = old_pid, source_hashes
        self.started = False
        self.writing_done = False
        self.submitted = 0
        self.cooldown_middleware = None
        self.cooldown_slots = {}
        self.cooldown_guard_timer = None

    def _stop_cooldown_guard(self):
        self.crawler.signals.disconnect(self.request_reached_downloader, signal=signals.request_reached_downloader)
        self.cooldown_slots.clear()

    def _arm_slot(self, key, slot, deadline):
        downloader = self.crawler.engine.downloader
        middleware = self.cooldown_middleware
        clock = middleware.clock
        middleware.cooldown_until[key] = deadline
        if slot.latercall is not None:
            slot.latercall.cancel()

        def resume():
            if middleware.cooldown_until.get(key, 0) > deadline:
                return
            middleware.cooldown_until.pop(key, None)
            slot.latercall = None
            downloader._process_queue(slot)

        slot.latercall = clock.callLater(max(0, deadline - clock.seconds()), resume)
        self.cooldown_slots[key] = slot

    def _seed_cooldowns(self):
        downloader = self.crawler.engine.downloader
        middleware = next((item for item in downloader.middleware.middlewares
                           if type(item).__name__ == "RetryAfterMiddleware"), None)
        future = {key: deadline for key, deadline in self.deadlines.items() if deadline > time.time()}
        if not future:
            self._stop_cooldown_guard()
            return 0
        if middleware is None:
            raise RecoveryError("RetryAfterMiddleware is required to restore cooldowns")
        self.cooldown_middleware = middleware
        # Both installed Scrapy 2.19 and 2.20 support this method; 2.19 still
        # requires the deprecated spider argument, while 2.20 makes it optional.
        needs_spider = inspect.signature(downloader._get_slot).parameters.get("spider")
        for key, epoch_deadline in future.items():
            request = scrapy.Request("https://restart.invalid/", meta={"download_slot": key})
            if needs_spider is not None and needs_spider.default is inspect.Parameter.empty:
                slot_key, slot = downloader._get_slot(request, self.spider)
            else:
                slot_key, slot = downloader._get_slot(request)
            if slot_key != key:
                raise RecoveryError("Restored downloader slot does not match")
            remaining = max(0, epoch_deadline - time.time())
            clock = middleware.clock
            deadline = max(middleware.cooldown_until.get(key, 0), clock.seconds() + remaining)
            self._arm_slot(key, slot, deadline)
        self.cooldown_guard_timer = middleware.clock.callLater(
            max(0, max(future.values()) - time.time()), self._stop_cooldown_guard,
        )
        return len(future)

    def request_reached_downloader(self, request, **kwargs):
        """Re-arm a saved deadline if Scrapy garbage-collected its idle slot.

        This signal runs after _get_slot and before _process_queue. There is
        no network/file/database operation here, and the guard is removed when
        the saved cooldowns expire.
        """
        key = request.meta.get("download_slot")
        if key not in self.deadlines or self.cooldown_middleware is None:
            return
        middleware = self.cooldown_middleware
        deadline = middleware.cooldown_until.get(key, 0)
        if deadline <= middleware.clock.seconds():
            return
        slot = self.crawler.engine.downloader.slots[key]
        if self.cooldown_slots.get(key) is not slot:
            self._arm_slot(key, slot, deadline)

    async def engine_started(self):
        if self.started:
            return
        self.started = True
        engine = self.crawler.engine
        if engine.running:
            raise RecoveryError("Recovery must run before engine request dispatch")
        try:
            restored_slots = self._seed_cooldowns()
            for index in sorted(self.pending):
                row = self.rows[index]
                previous_domain_pending = self.spider._domain_pending.get(row["domain_id"], 0)
                self.spider._pending_requests += 1
                request = self.spider._build_request(row["source_url"], row["domain_id"])
                if (not isinstance(request, scrapy.Request)
                        or request.meta.get("source_url") != row["source_url"]
                        or request.meta.get("_track_domain_id") != row["domain_id"]
                        or self.spider._domain_pending.get(row["domain_id"]) != previous_domain_pending + 1):
                    raise RecoveryError("Normal request recovery interface changed")
                request.dont_filter = True
                request.meta[META_CHECKPOINT] = self.digest
                request.meta[META_INDEX] = index
                engine.crawl(request)
                self.submitted += 1
            self.spider._max_pending_requests = max(self.spider._max_pending_requests, self.spider._pending_requests)
            if not self.handed_off and not self.completed:
                await asyncio.to_thread(_write_handoff, self.handoff, {
                    "schema": 1, "phase": "handed_off", "release_id": self.release,
                    "worker_id": self.spider.crawler_id, "checkpoint_sha256": self.digest,
                    "old_pid": self.old_pid, "request_count": len(self.rows),
                    "source_sha256": self.source_hashes, "pid": os.getpid(),
                    "handed_off_at_epoch": time.time(),
                })
                self.handed_off = True
            if not self.pending and not self.completed:
                await self._complete()
            logger.info("crawler.restart_recovered", extra={
                "event": "crawler.restart_recovered", "checkpoint_requests": len(self.rows),
                "submitted_requests": self.submitted, "restored_cooldown_slots": restored_slots,
                "checkpoint_complete": self.completed,
            })
        except BaseException:
            # Signal errors are normally logged and ignored by Scrapy. Stop
            # the engine before it becomes running, leaving the durable data
            # available for the next startup instead of releasing traffic.
            engine.pause()
            await engine.stop_async()
            raise

    async def item_scraped(self, item, response, **kwargs):
        if self.completed:
            return
        request = getattr(response, "request", None)
        if request is None:
            return
        meta = request.meta
        index = meta.get(META_INDEX)
        if meta.get(META_CHECKPOINT) != self.digest or type(index) is not int or index not in self.pending:
            return
        self.pending.remove(index)
        if not self.pending:
            await self._complete()

    async def _complete(self):
        if self.completed or self.writing_done:
            return
        self.writing_done = True
        receipt = {
            "schema": 1, "phase": "completed", "release_id": self.release,
            "worker_id": self.spider.crawler_id, "checkpoint_sha256": self.digest,
            "completed_count": len(self.rows), "completed_at_epoch": time.time(),
            "pid": os.getpid(),
        }
        try:
            await asyncio.to_thread(_write_done, self.done, receipt)
            self.completed = True
            self.crawler.signals.disconnect(self.item_scraped, signal=signals.item_scraped)
        finally:
            self.writing_done = False

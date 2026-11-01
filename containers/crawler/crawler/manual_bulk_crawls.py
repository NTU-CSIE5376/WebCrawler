"""Feed crawl requests from a per-crawler SQLite frontier into the shared downloader.

Threads: the reactor only schedules in memory. One frontier thread owns the
SQLite connection (ingest, lease, requeue, close, delete). One segment thread
writes gzip segments and status snapshots. Results reach the reactor through
call_from_thread.
"""
from __future__ import annotations

import gzip
import heapq
import json
import logging
import math
import os
import shutil
import sqlite3
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from random import random

import scrapy
from libs.ipc.crawl_requests import (
    GB,
    PART_SUFFIX,
    SEGMENT_NAME,
    CrawlRequestPaths,
    atomic_write_json,
    parse_segment_name,
    read_json,
    segment_name,
    terminal_record,
    utc_now,
)
from libs.urls import host_of
from pqdict import pqdict
from scrapy.exceptions import IgnoreRequest
from scrapy.http import TextResponse
from twisted.internet.error import (
    ConnectError,
    ConnectionLost,
    ConnectionRefusedError,
    DNSLookupError,
    TCPTimedOutError,
    TimeoutError,
)
from twisted.internet.task import LoopingCall
from twisted.web._newclient import ResponseNeverReceived

from .fair_scheduler import MANUAL

logger = logging.getLogger("crawler")

HTML_TYPES = {b"text/html", b"application/xhtml+xml"}
HOUR_S = 3600


def slot_key(url: str) -> str:
    """The manual queue key; the single place to align with the downloader slot policy."""
    return host_of(url)


def failure_error(failure) -> str:
    if failure.check(DNSLookupError):
        return "dns"
    # TCPTimedOutError is a ConnectError, so timeouts are checked first.
    if failure.check(TimeoutError, TCPTimedOutError):
        return "timeout"
    if failure.check(ConnectionRefusedError, ConnectionLost, ResponseNeverReceived, ConnectError):
        return "connection"
    if failure.check(IgnoreRequest) and "robots.txt" in str(failure.value):
        return "robots"
    return "retries_exhausted"


@dataclass(slots=True)
class _Row:
    rowid: int
    item_id: str
    url: str
    priority: int
    requeues: int = 0


@dataclass
class _Slot:
    outstanding: int = 0
    latency: float = 1.0  # Bootstrap estimate until a response supplies download_latency.
    jitter: float = field(default_factory=random)  # Fixed [0, 1) seconds per slot.
    blocked_until: float = 0.0
    blocks: int = 0  # Consecutive 429/503 responses.
    limits: tuple | None = None  # Live (concurrency, delay) used by the last refresh.
    requests: set = field(default_factory=set)


@dataclass
class _Work:
    """One (request, slot): queued rows in SQLite, leased rows in memory."""
    queued: int = 0
    buffer: deque = field(default_factory=deque)
    outstanding: int = 0
    leasing: bool = False
    added: int = 0  # Rows ever added to `queued`; corrects counts after a short lease.
    done: deque = field(default_factory=deque)

    @property
    def remaining(self) -> int:
        return self.queued + len(self.buffer) + self.outstanding


@dataclass
class _Request:
    id: str
    passes: int = 0
    meta: dict | None = None
    expires_ts: float = math.inf
    cancelled: bool = False
    closing: bool = False
    unacked_bytes: int = 0
    slots: set = field(default_factory=set)
    ready: pqdict = field(default_factory=lambda: pqdict(reverse=True))


def _setting(settings, name, default, minimum, kind=int):
    value = settings.get(name, default)
    if (isinstance(value, bool) or not isinstance(value, (int, float) if kind is float else int)
            or value < minimum):
        raise ValueError(f"{name} must be {'a number' if kind is float else 'an integer'} of at least {minimum}")
    return value


class Frontier:
    """This crawler's SQLite frontier. Every method runs on the single frontier thread."""

    def __init__(self, paths: CrawlRequestPaths, crawler_id: int):
        self.paths = paths
        self.crawler_id = crawler_id
        self.db = None

    def open(self):
        path = self.paths.frontier(self.crawler_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        with self.db:
            # state: 0 queued, 1 leased (buffered, downloading, or record awaiting
            # its segment), 2 closing (cancelled/expired record awaiting its segment).
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS url(rowid INTEGER PRIMARY KEY, request TEXT NOT NULL,"
                " item_id TEXT NOT NULL, url TEXT NOT NULL, slot TEXT NOT NULL,"
                " priority INTEGER NOT NULL, state INTEGER NOT NULL DEFAULT 0,"
                " requeues INTEGER NOT NULL DEFAULT 0, UNIQUE(request, item_id))")
            self.db.execute("CREATE INDEX IF NOT EXISTS url_next ON url(request, slot, state, priority, rowid)")
            # Highest closed segment seq per request, so acked names are never reused.
            self.db.execute("CREATE TABLE IF NOT EXISTS segment_seq(request TEXT PRIMARY KEY, seq INTEGER NOT NULL)")
            # Single owner: anything leased or closing before a crash is redone.
            self.db.execute("UPDATE url SET state=0 WHERE state<>0")
        counts = {(request, slot): count for request, slot, count in self.db.execute(
            "SELECT request, slot, count(*) FROM url GROUP BY request, slot")}
        seqs = dict(self.db.execute("SELECT request, seq FROM segment_seq"))
        for directory in self.paths.root.glob(f"segments/*/crawler_{self.crawler_id:02d}"):
            request_id = directory.parent.name
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        if entry.name.endswith(PART_SUFFIX):
                            # Its rows were reset to queued above and will be redone.
                            os.unlink(entry.path)
                        elif (parsed := parse_segment_name(entry.name)) is not None:
                            seqs[request_id] = max(seqs.get(request_id, 0), parsed[1])
            except FileNotFoundError:
                continue  # Removed by GC meanwhile.
        return counts, seqs, self.check({request for request, _ in counts})

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def check(self, request_ids):
        """Request id -> (meta or None when removed, cancelled)."""
        states = {}
        for request_id in request_ids:
            try:
                meta = read_json(self.paths.meta(request_id))
            except FileNotFoundError:
                meta = None
            states[request_id] = (meta, self.paths.cancelled(request_id).exists())
        return states

    def ingest(self, limit: int):
        """Move up to `limit` inbox files into SQLite; return per-(request, slot) additions."""
        inbox = self.paths.inbox(self.crawler_id)
        files = []
        try:
            with os.scandir(inbox) as entries:
                for entry in entries:
                    if entry.name.endswith(".json") and not entry.name.startswith("."):
                        files.append((entry.stat().st_mtime, entry.path))
        except FileNotFoundError:
            return {}, {}
        deltas, states = defaultdict(int), {}
        for _, path in sorted(files)[:limit]:
            try:
                payload = read_json(Path(path))
                request_id = payload["request_id"]
                items = payload["items"]
            except FileNotFoundError:
                continue
            except (ValueError, KeyError, TypeError):
                logger.exception("manual_bulk_crawl.inbox_invalid", extra={"path": path})
                os.replace(path, path + ".bad")
                continue
            if request_id not in states:
                states.update(self.check([request_id]))
            meta, cancelled = states[request_id]
            if meta is not None and not cancelled:
                with self.db:
                    for item in items:
                        slot = slot_key(item["url"])
                        cursor = self.db.execute(
                            "INSERT OR IGNORE INTO url(request, item_id, url, slot, priority) VALUES (?, ?, ?, ?, ?)",
                            (request_id, item["id"], item["url"], slot, item["priority"]))
                        # Ignored: the same id is still queued, leased or awaiting its segment.
                        if cursor.rowcount:
                            deltas[request_id, slot] += 1
            os.unlink(path)
        return dict(deltas), states

    def lease(self, request_id: str, slot: str, limit: int):
        with self.db:
            rows = self.db.execute(
                "SELECT rowid, item_id, url, priority, requeues FROM url"
                " WHERE request=? AND slot=? AND state=0 ORDER BY priority, rowid LIMIT ?",
                (request_id, slot, limit)).fetchall()
            self.db.executemany("UPDATE url SET state=1 WHERE rowid=?", [(row[0],) for row in rows])
        return rows

    def requeue(self, rowid: int):
        with self.db:
            self.db.execute("UPDATE url SET state=0, requeues=requeues+1 WHERE rowid=?", (rowid,))

    def close_rows(self, request_id: str, limit: int):
        """Mark up to `limit` queued rows closing; their records are written next."""
        with self.db:
            rows = self.db.execute(
                "SELECT rowid, item_id, url, priority, slot FROM url WHERE request=? AND state=0 LIMIT ?",
                (request_id, limit)).fetchall()
            self.db.executemany("UPDATE url SET state=2 WHERE rowid=?", [(row[0],) for row in rows])
        return rows

    def delete_rows(self, request_id: str, seq: int, rowids):
        """Forget rows whose records are in a closed segment."""
        with self.db:
            self.db.executemany("DELETE FROM url WHERE rowid=?", [(rowid,) for rowid in rowids])
            self.db.execute(
                "INSERT INTO segment_seq(request, seq) VALUES (?, ?)"
                " ON CONFLICT(request) DO UPDATE SET seq=max(seq, excluded.seq)", (request_id, seq))

    def drop_request(self, request_id: str):
        with self.db:
            self.db.execute("DELETE FROM url WHERE request=?", (request_id,))
            self.db.execute("DELETE FROM segment_seq WHERE request=?", (request_id,))




@dataclass
class _Part:
    path: object
    raw: object
    gz: object
    seq: int
    opened_at: float
    rowids: list = field(default_factory=list)


class SegmentWriter:
    """Gzip JSON-lines segments of one crawler. Every method runs on the segment thread."""

    def __init__(self, paths: CrawlRequestPaths, crawler_id: int, *, max_bytes: int, max_age_s: float,
                 level: int, disk_usage):
        self.paths = paths
        self.crawler_id = crawler_id
        self.max_bytes = max_bytes
        self.max_age_s = max_age_s
        self.level = level
        self.disk_usage = disk_usage
        self.parts: dict[str, _Part] = {}
        self.seqs: dict[str, int] = {}

    def start(self, seqs):
        self.seqs.update(seqs)

    def _open(self, request_id: str, now: float) -> _Part | None:
        if not self.paths.meta(request_id).exists():
            return None  # Removed by GC; do not recreate its directory.
        seq = self.seqs.get(request_id, 0) + 1
        self.seqs[request_id] = seq
        directory = self.paths.segments(request_id, self.crawler_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (segment_name(self.crawler_id, seq) + PART_SUFFIX)
        raw = path.open("wb")
        part = _Part(path, raw, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=self.level, mtime=0),
                     seq, now)
        self.parts[request_id] = part
        return part

    def write(self, request_id: str, records, now: float):
        """Append (rowid, record) pairs; return segments closed by the size limit."""
        part = self.parts.get(request_id) or self._open(request_id, now)
        if part is None:
            return []
        for rowid, record in records:
            part.gz.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8", "replace"))
            part.rowids.append(rowid)
        return [self._close(request_id)] if part.raw.tell() >= self.max_bytes else []

    def _close(self, request_id: str):
        part = self.parts.pop(request_id)
        part.gz.close()
        part.raw.flush()
        os.fsync(part.raw.fileno())
        size = part.raw.tell()
        part.raw.close()
        try:
            os.replace(part.path, part.path.with_name(part.path.name.removesuffix(PART_SUFFIX)))
        except FileNotFoundError:
            pass  # GC removed the request meanwhile.
        return request_id, part.seq, part.rowids, size

    def rotate(self, now: float):
        return [self._close(request_id) for request_id, part in list(self.parts.items())
                if now - part.opened_at >= self.max_age_s]

    def close_all(self):
        return [self._close(request_id) for request_id in list(self.parts)]

    def discard(self, request_id: str):
        part = self.parts.pop(request_id, None)
        if part is not None:
            part.gz.close()
            part.raw.close()
            part.path.unlink(missing_ok=True)

    def measure(self, request_ids):
        """Unacked bytes per request in this crawler (closed + open), and free disk."""
        unacked = {}
        for request_id in request_ids:
            total = 0
            try:
                with os.scandir(self.paths.segments(request_id, self.crawler_id)) as entries:
                    for entry in entries:
                        if SEGMENT_NAME.fullmatch(entry.name):
                            try:
                                total += entry.stat().st_size
                            except FileNotFoundError:
                                pass  # Acked meanwhile.
            except FileNotFoundError:
                pass
            if request_id in self.parts:
                total += self.parts[request_id].raw.tell()
            unacked[request_id] = total
        return unacked, self.disk_usage(self.paths.root).free

    def write_status(self, snapshot):
        atomic_write_json(self.paths.status(self.crawler_id), snapshot)


class ManualBulkCrawlConsumer:
    def __init__(self, spider, *, executor=None, segment_executor=None, call_from_thread=None,
                 clock=None, disk_usage=None):
        self.spider = spider
        settings = spider.crawler.settings
        self.crawler_id = spider.crawler_id
        self.paths = CrawlRequestPaths(settings["MANUAL_BULK_CRAWL_ROOT"])
        self.concurrency = settings.getint("CONCURRENT_REQUESTS", 64)
        self.host_headroom = _setting(settings, "MANUAL_BULK_CRAWL_HOST_HEADROOM", 1, 0)
        self.poll_s = _setting(settings, "MANUAL_BULK_CRAWL_POLL_S", 1, 0.01, float)
        self.ingest_files = _setting(settings, "MANUAL_BULK_CRAWL_INGEST_FILES", 8, 1)
        self.close_chunk = _setting(settings, "MANUAL_BULK_CRAWL_CLOSE_CHUNK", 10000, 1)
        self.status_s = _setting(settings, "MANUAL_BULK_CRAWL_STATUS_S", 60, 0.01, float)
        self.top_hosts = _setting(settings, "MANUAL_BULK_CRAWL_TOP_HOSTS", 10, 1)
        self.shards = _setting(settings, "MANUAL_BULK_CRAWL_SHARDS", 16, 1)
        self.min_free_bytes = _setting(settings, "MANUAL_BULK_CRAWL_MIN_FREE_GB", 300, 0, float) * GB
        self.max_requeues = _setting(settings, "MANUAL_BULK_CRAWL_MAX_REQUEUES", 5, 0)
        self.block_base_s = _setting(settings, "MANUAL_BULK_CRAWL_BLOCK_BASE_S", 30, 0, float)
        self.max_block_s = _setting(settings, "MANUAL_BULK_CRAWL_MAX_BLOCK_S", 3600, 0, float)
        if clock is None or call_from_thread is None:
            from twisted.internet import reactor
            clock = clock or reactor
            call_from_thread = call_from_thread or reactor.callFromThread
        self.clock = clock
        self.call_from_thread = call_from_thread
        self.executor = executor or ThreadPoolExecutor(1, thread_name_prefix="manual-frontier")
        self.segment_executor = segment_executor or ThreadPoolExecutor(1, thread_name_prefix="manual-segments")
        self.frontier = Frontier(self.paths, self.crawler_id)
        self.writer = SegmentWriter(
            self.paths, self.crawler_id,
            max_bytes=_setting(settings, "MANUAL_BULK_CRAWL_SEGMENT_BYTES", 64 * 1024 * 1024, 1),
            max_age_s=_setting(settings, "MANUAL_BULK_CRAWL_SEGMENT_S", 60, 0, float),
            level=_setting(settings, "MANUAL_BULK_CRAWL_GZIP_LEVEL", 6, 0),
            disk_usage=disk_usage or shutil.disk_usage)
        self.slots: dict[str, _Slot] = {}
        self.works: dict[tuple[str, str], _Work] = {}
        self.requests: dict[str, _Request] = {}
        self.inflight: dict[tuple[str, int], tuple[str, _Row]] = {}
        self.floor_paused = False
        self.started = self.closed = self.filling = False
        self.busy: set[str] = set()  # Periodic jobs still running in a thread.
        self.next_status = 0.0
        self.poller = LoopingCall(self.poll)
        self.poller.clock = clock

    # Thread plumbing

    def _run(self, executor, fn, *args, then=None, always=None, skip_empty=False):
        if self.closed:
            return
        future = executor.submit(fn, *args)

        def done(future):
            if skip_empty and always is None and future.exception() is None and not future.result():
                return  # Spare the reactor a wakeup for "nothing happened".
            self.call_from_thread(self._deliver, future, then, always, fn.__name__)

        future.add_done_callback(done)

    def _deliver(self, future, then, always, name):
        if always is not None:
            always()
        try:
            result = future.result()
        except Exception:
            logger.exception(f"manual_bulk_crawl.{name}_failed")
            return
        if then is not None and not self.closed:
            then(result)

    def _periodic(self, name, executor, fn, *args, then):
        if name in self.busy:
            return
        self.busy.add(name)
        self._run(executor, fn, *args, then=then, always=lambda: self.busy.discard(name))

    # Lifecycle

    def start(self):
        self._run(self.executor, self.frontier.open, then=self._opened)

    def _opened(self, result):
        counts, seqs, states = result
        self._run(self.segment_executor, self.writer.start, seqs)
        for (request_id, slot), count in counts.items():
            self._add(request_id, slot, count)
        self._apply_states(states)
        self.started = True
        self.poller.start(self.poll_s, now=True)

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.poller.running:
            self.poller.stop()
        try:
            # Close open segments so their records are published and their rows forgotten.
            for request_id, seq, rowids, _ in self.segment_executor.submit(self.writer.close_all).result():
                if rowids:
                    self.executor.submit(self.frontier.delete_rows, request_id, seq, rowids).result()
            self.executor.submit(self.frontier.close).result()
        except Exception:
            logger.exception("manual_bulk_crawl.close_failed")
        finally:
            self.segment_executor.shutdown(wait=True)
            self.executor.shutdown(wait=True)
        self.inflight.clear()
        self.works.clear()
        self.slots.clear()
        self.requests.clear()

    def poll(self):
        if self.closed or not self.started:
            return
        now = self.clock.seconds()
        self._periodic("measure", self.segment_executor, self.writer.measure, list(self.requests),
                       then=self._measured)
        self._periodic("check", self.executor, self.frontier.check, list(self.requests), then=self._apply_states)
        self._periodic("ingest", self.executor, self.frontier.ingest, self.ingest_files, then=self._ingested)
        self._run(self.segment_executor, self.writer.rotate, now, then=self._segments_closed, skip_empty=True)
        for request in list(self.requests.values()):
            reason = self._paused(request)
            if reason in ("cancelled", "expired"):
                self._close_request(request, reason)
        self._refresh_changed_slots()
        if now >= self.next_status:
            self.next_status = now + self.status_s
            self._run(self.segment_executor, self.writer.write_status, self.snapshot())
        self.refill()

    # Frontier results

    def _ingested(self, result):
        deltas, states = result
        for (request_id, slot), count in deltas.items():
            self._add(request_id, slot, count)
        self._apply_states({request_id: state for request_id, state in states.items()
                            if request_id in self.requests})
        self.refill()

    def _add(self, request_id, slot, count):
        request = self.requests.get(request_id)
        if request is None:
            # A new request joins at the current minimum pass: no banked turns.
            start = min((other.passes for other in self.requests.values()), default=0)
            request = self.requests[request_id] = _Request(request_id, passes=start)
        work = self.works.get((request_id, slot))
        if work is None:
            work = self.works[request_id, slot] = _Work()
            request.slots.add(slot)
            self.slots.setdefault(slot, _Slot()).requests.add(request_id)
        work.queued += count
        work.added += count
        self._refresh(request_id, slot)

    def _apply_states(self, states):
        for request_id, (meta, cancelled) in states.items():
            request = self.requests.get(request_id)
            if request is None:
                continue
            if meta is None:
                self._drop(request)
                continue
            request.meta = meta
            request.expires_ts = datetime.fromisoformat(meta["expires_at"]).timestamp()
            request.cancelled = cancelled
        self.refill()

    def _drop(self, request):
        """The request was removed by GC: forget its rows and records."""
        self._run(self.executor, self.frontier.drop_request, request.id)
        self._run(self.segment_executor, self.writer.discard, request.id)
        for slot_key in request.slots:
            del self.works[request.id, slot_key]
            slot = self.slots[slot_key]
            slot.requests.discard(request.id)
            self._forget_idle_slot(slot_key)
        del self.requests[request.id]

    def _forget_idle_slot(self, slot_key):
        slot = self.slots.get(slot_key)
        # A blocked slot is kept so new URLs for the host still wait out the block.
        if (slot is not None and not slot.requests and not slot.outstanding
                and slot.blocked_until <= self.clock.seconds()):
            del self.slots[slot_key]

    def _measured(self, result):
        unacked, free = result
        for request_id, size in unacked.items():
            if request_id in self.requests:
                self.requests[request_id].unacked_bytes = size
        floor = free < self.min_free_bytes
        if floor != self.floor_paused:
            logger.warning("manual_bulk_crawl.floor", extra={"paused": floor, "free_bytes": free})
            self.floor_paused = floor
        self.refill()

    def _segments_closed(self, closed):
        for request_id, seq, rowids, _ in closed:
            if rowids:
                self._run(self.executor, self.frontier.delete_rows, request_id, seq, rowids)

    def _emit(self, request_id, records):
        """Hand (rowid, record) pairs to the segment thread."""
        if records:
            self._run(self.segment_executor, self.writer.write, request_id, records, self.clock.seconds(),
                      then=self._segments_closed, skip_empty=True)

    # Cancel and expiry

    def _paused(self, request) -> str | None:
        if request.meta is None:
            return "unknown"
        if request.cancelled:
            return "cancelled"
        if utc_now().timestamp() >= request.expires_ts:
            return "expired"
        if request.unacked_bytes > request.meta["result_quota_bytes"] / self.shards:
            return "quota"
        return None

    def _close_request(self, request, reason):
        records = []
        for slot_key in list(request.slots):
            work = self.works[request.id, slot_key]
            while work.buffer:
                row = work.buffer.popleft()
                records.append((row.rowid, terminal_record(row.item_id, row.url, row.priority, error=reason)))
            request.ready.pop(slot_key, None)
        self._emit(request.id, records)
        if not request.closing and any(self.works[request.id, slot].queued for slot in request.slots):
            request.closing = True
            self._run(self.executor, self.frontier.close_rows, request.id, self.close_chunk,
                      then=lambda rows: self._rows_closed(request.id, reason, rows),
                      always=lambda: setattr(request, "closing", False))
        for slot_key in list(request.slots):
            self._refresh(request.id, slot_key)

    def _rows_closed(self, request_id, reason, rows):
        request = self.requests.get(request_id)
        if request is None:
            return
        records = []
        for rowid, item_id, url, priority, slot in rows:
            work = self.works.get((request_id, slot))
            if work is not None:
                work.queued = max(0, work.queued - 1)
            records.append((rowid, terminal_record(item_id, url, priority, error=reason)))
        if len(rows) < self.close_chunk:
            # Every queued row is closed; no requeue happens for a closing request.
            for slot in request.slots:
                self.works[request_id, slot].queued = 0
        self._emit(request_id, records)
        for slot in list(request.slots):
            self._refresh(request_id, slot)

    # Scheduling

    def _domain_limits(self, host, slot):
        settings = self.spider.crawler.settings
        downloader = getattr(self.spider.crawler.engine, "downloader", None)
        live = getattr(downloader, "slots", {}).get(host)
        if live is not None:
            concurrency, delay = live.concurrency, live.delay
            slot.limits = (concurrency, delay)
        else:
            overrides = settings.getdict("DOWNLOAD_SLOTS").get(host, {})
            concurrency = overrides.get("concurrency", settings.getint("CONCURRENT_REQUESTS_PER_IP")
                                        or settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN"))
            delay = overrides.get("delay", settings.getfloat("AUTOTHROTTLE_START_DELAY")
                                  if settings.getbool("AUTOTHROTTLE_ENABLED")
                                  else settings.getfloat("DOWNLOAD_DELAY"))
            slot.limits = None
        # Inverse QPS: delay-bound or latency/concurrency-bound, whichever is slower.
        seconds_per_url = max(delay, slot.latency / max(1, concurrency))
        if delay > 0:
            # A slot starts one request per delay, so beyond about latency / delay in
            # transfer, more outstanding requests only wait in the slot and hold capacity.
            concurrency = min(concurrency, math.ceil(slot.latency / delay - 1e-9) + self.host_headroom)
        return concurrency, seconds_per_url

    def _refresh(self, request_id, slot_key):
        """Recompute one (request, slot) entry of the request's LPT queue."""
        request = self.requests.get(request_id)
        work = self.works.get((request_id, slot_key))
        if request is None or work is None:
            return
        slot = self.slots[slot_key]
        if not work.remaining and not work.leasing:
            request.ready.pop(slot_key, None)
            del self.works[request_id, slot_key]
            request.slots.discard(slot_key)
            slot.requests.discard(request_id)
            self._forget_idle_slot(slot_key)
            if not request.slots and not request.closing:
                del self.requests[request_id]
            return
        limit, seconds_per_url = self._domain_limits(slot_key, slot)
        if (slot.outstanding < limit and slot.blocked_until <= self.clock.seconds()
                and (work.buffer or (work.queued and not work.leasing))):
            # Longest remaining service time first, with fixed sub-second jitter.
            request.ready[slot_key] = work.remaining * seconds_per_url + slot.jitter
        else:
            request.ready.pop(slot_key, None)

    def _refresh_slot(self, slot_key):
        slot = self.slots.get(slot_key)
        for request_id in list(slot.requests) if slot else ():
            self._refresh(request_id, slot_key)

    def _refresh_changed_slots(self):
        """Pick up live delay/concurrency changes (AutoThrottle) without scanning every slot."""
        downloader = getattr(self.spider.crawler.engine, "downloader", None)
        for key, live in list(getattr(downloader, "slots", {}).items()):
            slot = self.slots.get(key)
            if slot is not None and slot.limits != (live.concurrency, live.delay):
                self._refresh_slot(key)

    def _lease(self, request_id, slot_key):
        work = self.works[request_id, slot_key]
        limit = max(1, 2 * self._domain_limits(slot_key, self.slots[slot_key])[0])
        work.leasing = True
        mark = work.added

        def leased(rows):
            self._leased(request_id, slot_key, limit, mark, rows)

        def failed():
            current = self.works.get((request_id, slot_key))
            if current is not None:
                current.leasing = False

        self._run(self.executor, self.frontier.lease, request_id, slot_key, limit, then=leased, always=failed)

    def _leased(self, request_id, slot_key, limit, mark, rows):
        work = self.works.get((request_id, slot_key))
        if work is None:
            return  # Dropped meanwhile; its rows are gone with the request.
        work.buffer.extend(_Row(*row) for row in rows)
        work.queued = max(0, work.queued - len(rows))
        if len(rows) < limit:
            # No queued row was left at lease time; only later additions remain.
            work.queued = min(work.queued, work.added - mark)
        request = self.requests[request_id]
        reason = self._paused(request)
        if reason in ("cancelled", "expired"):
            self._close_request(request, reason)
        self._refresh(request_id, slot_key)
        self.refill()

    def _next_request(self):
        best = None
        for request in self.requests.values():
            if request.ready and self._paused(request) is None and (best is None or request.passes < best.passes):
                best = request
        return best

    def refill(self):
        if self.closed or self.filling or not self.started or self.floor_paused:
            return
        self.filling = True
        try:
            while len(self.inflight) < self.concurrency:
                request = self._next_request()
                if request is None:
                    break
                slot_key = request.ready.top()
                work = self.works[request.id, slot_key]
                if not work.buffer:
                    request.ready.pop(slot_key)
                    self._lease(request.id, slot_key)
                    continue
                if not self._dispatch(request, slot_key, work):
                    break
        finally:
            self.filling = False

    def _dispatch(self, request, slot_key, work) -> bool:
        row = work.buffer.popleft()
        slot = self.slots[slot_key]
        try:
            self.spider.crawler.engine.crawl(scrapy.Request(
                row.url, callback=self.parse, errback=self.fetch_failed, dont_filter=True,
                meta={MANUAL: (request.id, row.rowid)},
            ))
        except Exception:
            work.buffer.appendleft(row)
            logger.exception("manual_bulk_crawl.schedule_failed")
            return False
        slot.outstanding += 1
        work.outstanding += 1
        self.inflight[request.id, row.rowid] = (slot_key, row)
        # Stride with equal weights; a request without ready work must not bank turns.
        for other in self.requests.values():
            if other is not request and other.passes < request.passes and (
                    not other.ready or self._paused(other) is not None):
                other.passes = request.passes
        request.passes += 1
        self._refresh_slot(slot_key)
        if (len(work.buffer) < self._domain_limits(slot_key, slot)[0] and work.queued
                and not work.leasing):
            self._lease(request.id, slot_key)
        return True

    # Download callbacks

    def parse(self, response):
        self._finish(response.meta.get(MANUAL), response=response)

    def fetch_failed(self, failure):
        response = getattr(failure.value, "response", None)
        logger.warning("request.fail", extra={
            "event": "request.fail", "url": failure.request.url, "domain": host_of(failure.request.url),
            "fail_reason": f"HttpError {response.status}" if response is not None else failure.type.__name__,
            "status": response.status if response is not None else None, "source": "manual",
        })
        self._finish(failure.request.meta.get(MANUAL), response=response,
                     failure=failure if response is None else None)

    def _finish(self, key, *, response=None, failure=None):
        entry = self.inflight.pop(tuple(key), None) if key else None
        if entry is None:
            return  # Stale callback after shutdown or after the request was removed.
        request_id = key[0]
        slot_key, row = entry
        slot = self.slots.get(slot_key)
        work = self.works.get((request_id, slot_key))
        if slot is not None:
            slot.outstanding -= 1
            latency = response.meta.get("download_latency") if response is not None else None
            if latency is not None and latency > 0:
                slot.latency = 0.8 * slot.latency + 0.2 * latency
        if work is None:
            # The request was removed while this URL was downloading.
            self._forget_idle_slot(slot_key)
            self._refresh_slot(slot_key)
            self.refill()
            return
        work.outstanding -= 1
        request = self.requests[request_id]
        now = self.clock.seconds()
        if response is not None and response.status in (429, 503):
            self._block(slot_key, slot, response, now)
            if row.requeues < self.max_requeues and self._paused(request) not in ("cancelled", "expired"):
                work.queued += 1
                work.added += 1
                self._run(self.executor, self.frontier.requeue, row.rowid)
                self._refresh_slot(slot_key)
                self.refill()
                return
        elif response is not None:
            slot.blocks = 0
        self._emit(request_id, [(row.rowid, self._record(row, response, failure))])
        work.done.append(now)
        self._refresh_slot(slot_key)
        self.refill()

    def _record(self, row, response, failure):
        if response is None:
            return terminal_record(row.item_id, row.url, row.priority, error=failure_error(failure))
        html = None
        if 200 <= response.status < 300:
            content_type = response.headers.get(b"Content-Type", b"").split(b";", 1)[0].strip().lower()
            if content_type in HTML_TYPES:
                html = (response.text if isinstance(response, TextResponse)
                        else response.body.decode("utf-8", "replace"))
                error = None
            else:
                error = "non_html"
        else:
            error = f"http_{response.status}"
        return terminal_record(row.item_id, row.url, row.priority, final_url=response.url,
                               status_code=response.status, error=error, fetched_at=utc_now(), html=html)

    def _block(self, slot_key, slot, response, now):
        delay = None
        raw = response.headers.get(b"Retry-After")
        if raw:
            text = raw.decode("latin-1").strip()
            try:
                delay = float(int(text)) if text.isdigit() else (
                    parsedate_to_datetime(text) - utc_now()).total_seconds()
            except (TypeError, ValueError, IndexError, OverflowError):
                delay = None
        if delay is None:
            delay = self.block_base_s * 2 ** slot.blocks
        delay = min(max(delay, 0.0), self.max_block_s)
        slot.blocks += 1
        slot.blocked_until = max(slot.blocked_until, now + delay)
        self.clock.callLater(delay, self._unblock, slot_key)

    def _unblock(self, slot_key):
        if not self.closed:
            self._refresh_slot(slot_key)
            self._forget_idle_slot(slot_key)
            self.refill()

    # Observation

    def snapshot(self) -> dict:
        cutoff = self.clock.seconds() - HOUR_S
        requests = {}
        for request in self.requests.values():
            queued = leased = 0
            hosts = []
            for slot_key in request.slots:
                work = self.works[request.id, slot_key]
                while work.done and work.done[0] < cutoff:
                    work.done.popleft()
                queued += work.queued
                leased += len(work.buffer) + work.outstanding
                hosts.append((work.remaining, slot_key, len(work.done)))
            requests[request.id] = {
                "queued": queued, "leased": leased, "paused": self._paused(request),
                "unacked_bytes": request.unacked_bytes,
                "top_hosts": [{"host": host, "remaining": remaining, "done_1h": done}
                              for remaining, host, done in heapq.nlargest(self.top_hosts, hosts)],
            }
        return {"crawler_id": self.crawler_id, "updated_at": utc_now().isoformat(),
                "floor_paused": self.floor_paused, "requests": requests}

    def heartbeat(self) -> dict:
        now = self.clock.seconds()
        quota = any(self._paused(request) == "quota" for request in self.requests.values())
        paused = "floor" if self.floor_paused else ("quota" if quota else None)
        return {
            "manual_queued": sum(work.queued + len(work.buffer) for work in self.works.values()),
            "manual_outstanding": len(self.inflight),
            "manual_unacked_bytes": sum(request.unacked_bytes for request in self.requests.values()),
            "manual_blocked_slots": sum(slot.blocked_until > now for slot in self.slots.values()),
            "manual_paused": paused,
        }



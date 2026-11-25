"""Feed bounded API work into the existing downloader and domain slots."""
import logging
import math
import re
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from time import monotonic
from random import random

import scrapy
from scrapy import signals
from pqdict import pqdict
from twisted.internet.task import LoopingCall

from libs.ipc.manual_bulk_crawl import (
    JobUnavailable, ManualBulkCrawlBatch, iter_batches, is_expired,
)
from libs.urls import host_of
from libs.ipc.processing_feedback import ProcessingFeedback
from libs.ipc.manual_bulk_crawl import write_json
from .site_policies import SitePolicies
from .dispatch_defaults import MANUAL_SHARE_PERCENT

logger = logging.getLogger("crawler")


@dataclass
class _ActiveJob:
    batch: ManualBulkCrawlBatch
    pending: set[int]
    inflight: set[int] = field(default_factory=set)
    failed: bool = False
    domain_id: int | None = None


@dataclass
class _DomainQueue:
    pending: deque[tuple[str, int]] = field(default_factory=deque)
    ingest_pending: deque[tuple[str, int]] = field(default_factory=deque)
    outstanding: int = 0
    ingest_outstanding: int = 0
    ready_mode: str = "fetch"
    passes: dict = field(default_factory=lambda: {"fetch": 0, "ingest": 0})
    jitter: float = field(default_factory=random)
    latency: float = 1.0  # Bootstrap estimate until a response supplies download_latency.


def _int_setting(settings, name, default, minimum):
    value = settings[name] if name in settings else default
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer of at least {minimum}")
    return value


class ManualBulkCrawlConsumer:
    def __init__(self, spider):
        self.spider = spider
        self.policies = SitePolicies.load()
        settings = spider.crawler.settings
        self.pending_dir = Path(settings["MANUAL_BULK_CRAWL_QUEUE_DIR"])
        self.concurrency = settings.getint("CONCURRENT_REQUESTS", 64)
        self.max_active_jobs = _int_setting(settings, "MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS", 64, 1)
        self.host_headroom = _int_setting(settings, "MANUAL_BULK_CRAWL_HOST_HEADROOM", 1, 0)
        self.jobs: dict[str, _ActiveJob] = {}
        self.processing_feedback = ProcessingFeedback()
        self.admitted_at = {}
        self.pending_by_mode = {"fetch": 0, "ingest": 0}
        self.outstanding_by_mode = {"fetch": 0, "ingest": 0}
        self.mode_passes = {"fetch": 0, "ingest": 0}
        policy = settings.get("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", "weighted")
        self.share = 50 if policy == "balanced" else settings.getint("MANUAL_BULK_CRAWL_SHARE_PERCENT", MANUAL_SHARE_PERCENT)
        self.reserve_sources = policy in ("weighted", "balanced")
        self.inflight: set[tuple[str, int]] = set()
        self.saves = 0
        self.save_seconds = 0.0
        self.domains: dict[str, _DomainQueue] = {}  # Keyed by resolved downloader slot.
        self.inflight_domains: dict[tuple[str, int], str] = {}
        self.ready = pqdict(reverse=True)
        self.next_job_scan = 0.0
        self.finished = set()
        self.retry_after = {}
        self.closed = False
        self.filling = False
        self.poller = LoopingCall(self.poll)
        signal_manager = getattr(spider.crawler, "signals", None)
        if signal_manager is not None:
            signal_manager.connect(self.item_saved, signal=signals.item_scraped)
            signal_manager.connect(self.item_failed, signal=signals.item_error)
            signal_manager.connect(self.item_failed, signal=signals.item_dropped)

    def start(self):
        self.poller.start(1, now=False)

    def close(self):
        self.closed = True
        if self.poller.running:
            self.poller.stop()
        for job in self.jobs.values():
            job.batch.close()
        self.jobs.clear()
        self.admitted_at.clear()
        self.inflight.clear()
        self.inflight_domains.clear()
        self.domains.clear()
        self.ready.clear()

    def poll(self):
        if self.closed:
            return
        # Forget finished entries once this crawler has removed its queue marker.
        try:
            self.finished.intersection_update(path.stem for path in (self.pending_dir / f"crawler_{self.spider.crawler_id:02d}").glob("*.json"))
        except OSError:
            self.spider.logger.exception("manual_bulk_crawl.queue_failed")
        for job_id, job in list(self.jobs.items()):
            if not job.batch.path.exists() or is_expired(job.batch.payload):
                job.failed = True
                self._discard_pending(job_id, job)
                self.release_if_drained(job_id, job)
        # Re-read live slot delays, including AutoThrottle changes.
        self._refresh_ready()
        self.next_job_scan = 0.0
        self.refill()
        try:
            feedback = self.pending_dir / "feedback"
            feedback.mkdir(parents=True, exist_ok=True)
            write_json(feedback / f"crawler_{self.spider.crawler_id:02d}.json",
                       self.processing_feedback.snapshot())
        except OSError:
            logger.exception("crawler.processing_feedback_error",
                             extra={"event":"crawler.processing_feedback_error"})

    def _domain_limits(self, slot_key, domain):
        settings = self.spider.crawler.settings
        downloader = getattr(self.spider.crawler.engine, "downloader", None)
        slot = getattr(downloader, "slots", {}).get(slot_key)
        if slot is not None:
            concurrency, delay = slot.concurrency, slot.delay
        else:
            overrides = settings.getdict("DOWNLOAD_SLOTS").get(slot_key, {})
            concurrency = overrides.get("concurrency", settings.getint("CONCURRENT_REQUESTS_PER_IP")
                                        or settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN"))
            delay = overrides.get("delay", settings.getfloat("AUTOTHROTTLE_START_DELAY")
                                  if settings.getbool("AUTOTHROTTLE_ENABLED")
                                  else settings.getfloat("DOWNLOAD_DELAY"))
        # Inverse QPS: delay-bound or latency/concurrency-bound, whichever is slower.
        seconds_per_url = max(delay, domain.latency / max(1, concurrency))
        if delay > 0:
            # A slot starts one request per delay, so beyond about latency / delay in
            # transfer, more outstanding requests only wait in the slot and hold capacity.
            concurrency = min(concurrency, math.ceil(domain.latency / delay - 1e-9) + self.host_headroom)
        return concurrency, seconds_per_url

    @staticmethod
    def _queue(domain, mode):
        return domain.ingest_pending if mode == "ingest" else domain.pending

    def _source_limit(self, total, mode, other_has_work):
        if self.reserve_sources and total > 1 and other_has_work:
            fetch = max(1, min(total - 1, total * self.share // 100))
            return fetch if mode == "fetch" else total - fetch
        return total

    def _mode_available(self, domain, mode, limit):
        if not self._queue(domain, mode) or domain.outstanding >= limit:
            return False
        other = "ingest" if mode == "fetch" else "fetch"
        other_work = self.pending_by_mode[other] or self.outstanding_by_mode[other]
        if self.outstanding_by_mode[mode] >= self._source_limit(self.concurrency, mode, other_work):
            return False
        if self.concurrency == 1 and self.reserve_sources and other_work and self.pending_by_mode[other]:
            preferred = min(self.mode_passes, key=lambda m: (self.mode_passes[m], m))
            if mode != preferred:
                return False
        own = domain.ingest_outstanding if mode == "ingest" else domain.outstanding - domain.ingest_outstanding
        other_active = domain.outstanding - own
        if own >= self._source_limit(limit, mode, bool(self._queue(domain, other) or other_active)):
            return False
        if limit == 1 and self.reserve_sources and self._queue(domain, other):
            preferred = min(domain.passes, key=lambda m: (domain.passes[m], m))
            if mode != preferred:
                return False
        return True

    def _refresh_ready(self, slot_keys: Iterable[str] | None = None) -> None:
        """Preserve source shares and the existing remaining-service-time heuristic."""
        for slot_key in list(self.domains) if slot_keys is None else slot_keys:
            domain = self.domains[slot_key]
            if not domain.pending and not domain.ingest_pending:
                self.ready.pop(slot_key, None)
                if not domain.outstanding:
                    del self.domains[slot_key]
                continue
            limit, seconds_per_url = self._domain_limits(slot_key, domain)
            eligible = [mode for mode in ("fetch", "ingest") if self._mode_available(domain, mode, limit)]
            if not eligible:
                self.ready.pop(slot_key, None)
                continue
            mode = min(eligible, key=lambda m: (domain.passes[m], m))
            domain.ready_mode = mode
            remaining = len(domain.pending) + len(domain.ingest_pending) + domain.outstanding
            self.ready[slot_key] = remaining * seconds_per_url + domain.jitter

    def _discard_pending(self, job_id, job):
        if not job.pending:
            return
        self.pending_by_mode[job.batch.processing_mode] -= len(job.pending)
        job.pending.clear()
        touched = set()
        for slot_key, domain in self.domains.items():
            for mode in ("fetch", "ingest"):
                previous = self._queue(domain, mode)
                pending = deque(entry for entry in previous if entry[0] != job_id)
                if len(pending) != len(previous):
                    if mode == "ingest":
                        domain.ingest_pending = pending
                    else:
                        domain.pending = pending
                    touched.add(slot_key)
        self._refresh_ready(touched)

    def _load_jobs(self):
        if monotonic() < self.next_job_scan or len(self.jobs) >= self.max_active_jobs:
            return
        self.next_job_scan = monotonic() + 1
        touched = set()
        try:
            batches = iter_batches(
                self.pending_dir, self.spider.crawler_id,
                exclude_job_ids=self.finished | self.jobs.keys() | self.retry_after.keys(),
            )
            for batch in islice(batches, self.max_active_jobs - len(self.jobs)):
                job_id = batch.path.stem
                match = re.fullmatch(r"offerer/\d+/domain/(\d+)(?:/[^/]+)?", batch.payload.get("client_reference") or "")
                domain_id = int(match[1]) if match and batch.processing_mode == "ingest" else None
                self.jobs[job_id] = _ActiveJob(batch=batch, pending=set(batch.remaining), domain_id=domain_id)
                self.pending_by_mode[batch.processing_mode] += len(batch.remaining)
                for index in sorted(batch.remaining):
                    host = host_of(batch.urls[index])
                    slot_key = self.policies.resolve(host).request_limits.download_slot or host
                    domain = self.domains.get(slot_key)
                    if domain is None:
                        domain = self.domains[slot_key] = _DomainQueue()
                    self._queue(domain, batch.processing_mode).append((job_id, index))
                    touched.add(slot_key)
        except Exception:
            self.spider.logger.exception("manual_bulk_crawl.queue_failed")
        finally:
            self._refresh_ready(touched)

    def refill(self):
        if self.closed or self.filling:
            return
        self.filling = True
        try:
            self.retry_after = {job_id: deadline for job_id, deadline in self.retry_after.items()
                                if deadline > monotonic()}
            self._load_jobs()
            while len(self.inflight) < self.concurrency and self.ready:
                slot_key, _ = self.ready.popitem()
                domain = self.domains[slot_key]
                mode = domain.ready_mode
                limit, _ = self._domain_limits(slot_key, domain)
                if not self._mode_available(domain, mode, limit):
                    self._refresh_ready([slot_key])
                    continue
                job_id, index = self._queue(domain, mode)[0]
                job = self.jobs[job_id]
                if not job.batch.path.exists() or is_expired(job.batch.payload):
                    job.failed = True
                    self._discard_pending(job_id, job)
                    self.release_if_drained(job_id, job)
                    continue
                self._queue(domain, mode).popleft()
                self.pending_by_mode[mode] -= 1
                self.outstanding_by_mode[mode] += 1
                other = "fetch" if mode == "ingest" else "ingest"
                for passes in (domain.passes, self.mode_passes):
                    passes[other] = max(passes[other], passes[mode])
                    passes[mode] += self.share if mode == "ingest" else 100 - self.share
                job.pending.remove(index)
                domain.outstanding += 1
                domain.ingest_outstanding += int(mode == "ingest")
                self.inflight_domains[job_id, index] = slot_key
                job.inflight.add(index)
                self.inflight.add((job_id, index))
                if job.domain_id is not None:
                    self.admitted_at[job_id,index] = monotonic()
                    active = sum(len(j.inflight) for j in self.jobs.values() if j.domain_id == job.domain_id)
                    self.processing_feedback.admitted(job.domain_id, active)
                self._refresh_ready([slot_key])
                try:
                    self.spider.crawler.engine.crawl(scrapy.Request(
                        job.batch.urls[index],
                        callback=self.parse_ingest if job.batch.processing_mode == "ingest" else self.parse,
                        errback=self.fetch_failed,
                        dont_filter=True,
                        meta={"manual_bulk_crawl_job_id": job_id, "manual_bulk_crawl_index": index,
                              "crawl_job_mode": job.batch.processing_mode},
                    ))
                except Exception:
                    job.failed = True
                    job.inflight.discard(index)
                    self.inflight.discard((job_id, index))
                    self._release_domain(job_id, index)
                    self._discard_pending(job_id, job)
                    self.spider.logger.exception("manual_bulk_crawl.schedule_failed")
                    self.release_if_drained(job_id, job)
        finally:
            self.filling = False

    def parse(self, response):
        content_type = response.headers.get(b"Content-Type", b"").split(b";", 1)[0].strip().lower()
        html = content_type in {b"text/html", b"application/xhtml+xml"}
        error = None if 200 <= response.status < 300 else f"HTTP {response.status}"
        if not html:
            error = error or "NonHTML content-type"
        self.save(response.meta["manual_bulk_crawl_job_id"], response.meta["manual_bulk_crawl_index"],
                  download_latency=response.meta.get("download_latency"),
                  final_url=response.url, status_code=response.status,
                  body=response.body if html else None, error=error)

    async def parse_ingest(self, response):
        """Reuse normal parsing; the item-saved signal acknowledges persistence."""
        try:
            items = await self.spider.parse(response)
            for item in items:
                item["ingest_job"] = self._item_context(response.request, response)
            return items
        except Exception:
            self._output_failed(response.meta["manual_bulk_crawl_job_id"],
                                response.meta["manual_bulk_crawl_index"])
            raise

    @staticmethod
    def _item_context(request, response=None):
        return dict(job_id=request.meta["manual_bulk_crawl_job_id"],
                    index=request.meta["manual_bulk_crawl_index"],
                    final_url=response.url if response is not None else request.url,
                    status_code=response.status if response is not None else None,
                    download_latency=request.meta.get("download_latency"))

    def item_saved(self, item, response=None, spider=None):
        context = item.get("ingest_job")
        if context:
            self.save(context["job_id"], context["index"],
                      download_latency=context["download_latency"],
                      final_url=context["final_url"], status_code=context["status_code"],
                      error=item.get("fail_reason"))

    def item_failed(self, item, response=None, spider=None, **kwargs):
        context = item.get("ingest_job")
        if context:
            self._output_failed(context["job_id"], context["index"])

    def _output_failed(self, job_id, index):
        """Leave failed output unacknowledged so the existing job retry resumes it."""
        job = self.jobs.get(job_id)
        if job is None or index not in job.inflight:
            return
        job.failed = True
        job.inflight.discard(index)
        self.inflight.discard((job_id, index))
        self._release_domain(job_id, index)
        self._discard_pending(job_id, job)
        self.release_if_drained(job_id, job)
        self.refill()

    def fetch_failed(self, failure):
        job = self.jobs.get(failure.request.meta["manual_bulk_crawl_job_id"])
        if job is not None and job.batch.processing_mode == "ingest":
            # HTTP/network failures produce the same failure records as ordinary
            # crawling and are acknowledged only after those records are written.
            try:
                items = list(self.spider.errback(failure))
            except Exception:
                self._output_failed(failure.request.meta["manual_bulk_crawl_job_id"],
                                    failure.request.meta["manual_bulk_crawl_index"])
                raise
            for item in items:
                item["ingest_job"] = self._item_context(failure.request, getattr(failure.value, "response", None))
            return items
        response = getattr(failure.value, "response", None)
        logger.warning("request.fail", extra={
            "event": "request.fail", "url": failure.request.url, "domain": host_of(failure.request.url),
            "fail_reason": f"HttpError {response.status}" if response is not None else failure.type.__name__,
            "status": response.status if response is not None else None, "source": "manual",
        })
        if response is not None:
            return self.parse(response)
        meta = failure.request.meta
        self.save(meta["manual_bulk_crawl_job_id"], meta["manual_bulk_crawl_index"],
                  final_url=failure.request.url, status_code=None, error=failure.type.__name__)

    def _release_domain(self, job_id, index, latency=None):
        self.admitted_at.pop((job_id,index), None)
        slot_key = self.inflight_domains.pop((job_id, index))
        domain = self.domains[slot_key]
        domain.outstanding -= 1
        mode = self.jobs[job_id].batch.processing_mode
        domain.ingest_outstanding -= int(mode == "ingest")
        self.outstanding_by_mode[mode] -= 1
        if latency is not None and latency > 0:
            domain.latency = 0.8 * domain.latency + 0.2 * latency
        self._refresh_ready()

    def save(self, job_id, index, *, download_latency=None, **result):
        job = self.jobs.get(job_id)
        if job is None or index not in job.inflight:
            return
        try:
            if not job.failed:
                started = monotonic()
                job.batch.save(index, **result)
                admitted = self.admitted_at.get((job_id,index))
                if admitted is not None and job.domain_id is not None:
                    self.processing_feedback.completed(job.domain_id, monotonic()-admitted)
                # Saves run on the reactor thread; the heartbeat reports their total time.
                self.saves += 1
                self.save_seconds += monotonic() - started
        except JobUnavailable:
            job.failed = True
        except Exception:
            job.failed = True
            self.spider.logger.exception("manual_bulk_crawl.storage_failed")
        finally:
            job.inflight.discard(index)
            self.inflight.discard((job_id, index))
            self._release_domain(job_id, index, download_latency)
            if job.failed:
                self._discard_pending(job_id, job)
            self.release_if_drained(job_id, job)
            # Refill immediately, even while other URLs in this job are slow.
            self.refill()

    def release_if_drained(self, job_id, job):
        if job.inflight or (job.pending and not job.failed):
            return
        job.batch.close()
        del self.jobs[job_id]
        self.next_job_scan = 0.0
        if job.failed:
            # Drain this job's callbacks before retrying its partition. Retry on
            # a later poll rather than spinning on a broken queue or disk.
            self.retry_after[job_id] = monotonic() + 1
        else:
            self.finished.add(job_id)

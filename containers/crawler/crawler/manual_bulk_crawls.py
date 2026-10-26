"""Feed bounded API work into the existing downloader and domain slots."""
from collections import deque
from dataclasses import dataclass, field
import heapq
from itertools import islice
from pathlib import Path
from time import monotonic

import scrapy
from scrapy.utils.httpobj import urlparse_cached
from twisted.internet.task import LoopingCall

from libs.ipc.manual_bulk_crawl import (
    JobUnavailable, ManualBulkCrawlBatch, iter_batches, is_expired,
)


@dataclass
class _ActiveJob:
    batch: ManualBulkCrawlBatch
    pending: set[int]
    inflight: set[int] = field(default_factory=set)
    failed: bool = False


@dataclass
class _DomainQueue:
    pending: deque[tuple[str, int]] = field(default_factory=deque)
    outstanding: int = 0
    latency: float = 1.0  # Bootstrap estimate until a response supplies download_latency.
    last_selected: int = 0
    version: int = 0


class ManualBulkCrawlConsumer:
    def __init__(self, spider):
        self.spider = spider
        settings = spider.crawler.settings
        self.pending_dir = Path(settings["MANUAL_BULK_CRAWL_QUEUE_DIR"])
        self.concurrency = settings.getint("CONCURRENT_REQUESTS", 64)
        self.max_active_jobs = (settings["MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS"]
                                if "MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS" in settings else 64)
        if (isinstance(self.max_active_jobs, bool) or not isinstance(self.max_active_jobs, int)
                or self.max_active_jobs < 1):
            raise ValueError("MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS must be a positive integer")
        self.jobs: dict[str, _ActiveJob] = {}
        self.inflight: set[tuple[str, int]] = set()
        self.domains: dict[str, _DomainQueue] = {}
        self.inflight_domains: dict[tuple[str, int], str] = {}
        self.ready = []
        self.ready_revision = 0
        self.dispatch_count = 0
        self.next_job_scan = 0.0
        self.finished = set()
        self.retry_after = {}
        self.closed = False
        self.filling = False
        self.poller = LoopingCall(self.poll)

    def start(self):
        self.poller.start(1, now=False)

    def close(self):
        self.closed = True
        if self.poller.running:
            self.poller.stop()
        for job in self.jobs.values():
            job.batch.close()
        self.jobs.clear()
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
        self._rebuild_ready()
        self.next_job_scan = 0.0
        self.refill()

    def _slot_key(self, url):
        request = scrapy.Request(url)
        downloader = getattr(self.spider.crawler.engine, "downloader", None)
        return (downloader.get_slot_key(request) if downloader is not None
                else urlparse_cached(request).hostname)

    def _domain_limits(self, key, domain):
        settings = self.spider.crawler.settings
        downloader = getattr(self.spider.crawler.engine, "downloader", None)
        slot = getattr(downloader, "slots", {}).get(key)
        if slot is not None:
            concurrency, delay = slot.concurrency, slot.delay
        else:
            overrides = settings.getdict("DOWNLOAD_SLOTS").get(key, {})
            concurrency = overrides.get("concurrency", settings.getint("CONCURRENT_REQUESTS_PER_IP")
                                        or settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN"))
            delay = overrides.get("delay", settings.getfloat("AUTOTHROTTLE_START_DELAY")
                                  if settings.getbool("AUTOTHROTTLE_ENABLED")
                                  else settings.getfloat("DOWNLOAD_DELAY"))
        # Inverse QPS: delay-bound or latency/concurrency-bound, whichever is slower.
        seconds_per_url = max(delay, domain.latency / max(1, concurrency))
        return concurrency, seconds_per_url

    def _offer(self, key):
        domain = self.domains[key]
        self.ready_revision += 1
        domain.version = self.ready_revision
        if not domain.pending:
            if not domain.outstanding:
                del self.domains[key]
            return
        limit, seconds_per_url = self._domain_limits(key, domain)
        if domain.outstanding >= limit:
            return
        remaining = len(domain.pending) + domain.outstanding
        # Prioritize the longest remaining service time among domains below
        # their prefetch cap. Equal scores use least-recent dispatch order.
        heapq.heappush(self.ready, (
            -remaining * seconds_per_url, domain.last_selected, key, domain.version,
        ))

    def _rebuild_ready(self):
        self.ready.clear()
        for key in list(self.domains):
            self._offer(key)

    def _discard_pending(self, job_id, job):
        if not job.pending:
            return
        job.pending.clear()
        for domain in self.domains.values():
            domain.pending = deque(entry for entry in domain.pending if entry[0] != job_id)
        self._rebuild_ready()

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
                self.jobs[job_id] = _ActiveJob(batch=batch, pending=set(batch.remaining))
                for index in sorted(batch.remaining):
                    key = self._slot_key(batch.urls[index])
                    self.domains.setdefault(key, _DomainQueue()).pending.append((job_id, index))
                    touched.add(key)
        except Exception:
            self.spider.logger.exception("manual_bulk_crawl.queue_failed")
        finally:
            for key in touched:
                self._offer(key)

    def refill(self):
        if self.closed or self.filling:
            return
        self.filling = True
        try:
            self.retry_after = {job_id: deadline for job_id, deadline in self.retry_after.items()
                                if deadline > monotonic()}
            self._load_jobs()
            while len(self.inflight) < self.concurrency and self.ready:
                _, _, key, version = heapq.heappop(self.ready)
                domain = self.domains.get(key)
                if domain is None or version != domain.version:
                    continue
                job_id, index = domain.pending[0]
                job = self.jobs[job_id]
                if not job.batch.path.exists() or is_expired(job.batch.payload):
                    job.failed = True
                    self._discard_pending(job_id, job)
                    self.release_if_drained(job_id, job)
                    continue
                domain.pending.popleft()
                job.pending.remove(index)
                domain.outstanding += 1
                self.dispatch_count += 1
                domain.last_selected = self.dispatch_count
                self.inflight_domains[job_id, index] = key
                job.inflight.add(index)
                self.inflight.add((job_id, index))
                self._offer(key)
                try:
                    self.spider.crawler.engine.crawl(scrapy.Request(
                        job.batch.urls[index], callback=self.parse, errback=self.fetch_failed,
                        dont_filter=True,
                        meta={"manual_bulk_crawl_job_id": job_id, "manual_bulk_crawl_index": index,
                              "manual_bulk_crawl_scheduling_policy": job.batch.payload.get("scheduling_policy", "balanced")},
                    ))
                except Exception:
                    job.failed = True
                    job.inflight.discard(index)
                    self.inflight.discard((job_id, index))
                    self._release_domain(job_id, index)
                    self._discard_pending(job_id, job)
                    self.spider.logger.exception("manual_bulk_crawl.schedule_failed")
                    self.release_if_drained(job_id, job)
            # Lazy heap invalidation stays bounded even across many tiny jobs.
            if len(self.ready) > 2 * (len(self.domains) + self.concurrency):
                self._rebuild_ready()
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

    def fetch_failed(self, failure):
        response = getattr(failure.value, "response", None)
        if response is not None:
            return self.parse(response)
        meta = failure.request.meta
        self.save(meta["manual_bulk_crawl_job_id"], meta["manual_bulk_crawl_index"],
                  final_url=failure.request.url, status_code=None, error=failure.type.__name__)

    def _release_domain(self, job_id, index, latency=None):
        key = self.inflight_domains.pop((job_id, index))
        domain = self.domains[key]
        domain.outstanding -= 1
        if latency is not None and latency > 0:
            domain.latency = 0.8 * domain.latency + 0.2 * latency
        self._offer(key)

    def save(self, job_id, index, *, download_latency=None, **result):
        job = self.jobs.get(job_id)
        if job is None or index not in job.inflight:
            return
        try:
            if not job.failed:
                job.batch.save(index, **result)
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

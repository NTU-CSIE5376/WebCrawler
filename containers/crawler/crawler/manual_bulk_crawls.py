"""Feed bounded API work into the existing downloader and domain slots."""
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic

import scrapy
from twisted.internet.task import LoopingCall

from libs.ipc.manual_bulk_crawl import (
    DEFAULT_MAX_CONCURRENCY_RATIO, JobUnavailable, ManualBulkCrawlBatch, claim_next, is_expired,
)


@dataclass
class _ActiveJob:
    batch: ManualBulkCrawlBatch
    pending: deque[int]
    inflight: set[int] = field(default_factory=set)
    failed: bool = False


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
        self.finished = set()
        self.retry_after = {}
        self.closed = False
        self.filling = False
        self.poller = LoopingCall(self.poll)

    @property
    def request_limit(self):
        # One budget across all jobs, not one allowance per job. A lower limit
        # on a newly claimed job drains existing requests before more admission.
        ratio = min((job.batch.payload.get("max_concurrency_ratio", DEFAULT_MAX_CONCURRENCY_RATIO)
                     for job in self.jobs.values()), default=DEFAULT_MAX_CONCURRENCY_RATIO)
        return int(self.concurrency * ratio)

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

    def poll(self):
        if self.closed:
            return
        # Keep only completed portions whose jobs are still waiting on other
        # crawlers. Avoid reloading their URL metadata on every completion.
        try:
            self.finished.intersection_update(path.stem for path in self.pending_dir.glob("*.json"))
        except OSError:
            self.spider.logger.exception("manual_bulk_crawl.queue_failed")
        for job_id, job in list(self.jobs.items()):
            if not job.batch.path.exists() or is_expired(job.batch.payload):
                job.pending.clear()
                job.failed = True
                self.release_if_drained(job_id, job)
        self.refill()

    def refill(self):
        if self.closed or self.filling:
            return
        self.filling = True
        try:
            self.retry_after = {job_id: deadline for job_id, deadline in self.retry_after.items()
                                if deadline > monotonic()}
            while not self.jobs or len(self.inflight) < self.request_limit:
                ready = next(((job_id, job) for job_id, job in self.jobs.items()
                              if job.pending and not job.failed
                              and job.batch.path.exists() and not is_expired(job.batch.payload)), None)
                if ready is None:
                    if len(self.jobs) >= self.max_active_jobs:
                        break
                    try:
                        batch = claim_next(
                            self.pending_dir, self.spider.crawler_id,
                            exclude_job_ids=self.finished | self.jobs.keys() | self.retry_after.keys(),
                        )
                    except Exception:
                        self.spider.logger.exception("manual_bulk_crawl.queue_failed")
                        break
                    if batch is None:
                        break
                    job_id = batch.path.stem
                    job = _ActiveJob(batch=batch, pending=deque(sorted(batch.remaining)))
                    self.jobs[job_id] = job
                else:
                    job_id, job = ready
                if len(self.inflight) >= self.request_limit:
                    break
                index = job.pending.popleft()
                job.inflight.add(index)
                self.inflight.add((job_id, index))
                try:
                    self.spider.crawler.engine.crawl(scrapy.Request(
                        job.batch.urls[index], callback=self.parse, errback=self.fetch_failed,
                        dont_filter=True,
                        meta={"manual_bulk_crawl_job_id": job_id, "manual_bulk_crawl_index": index},
                    ))
                except Exception:
                    job.failed = True
                    job.inflight.discard(index)
                    self.inflight.discard((job_id, index))
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
                  final_url=response.url, status_code=response.status,
                  body=response.body if html else None, error=error)

    def fetch_failed(self, failure):
        response = getattr(failure.value, "response", None)
        if response is not None:
            return self.parse(response)
        meta = failure.request.meta
        self.save(meta["manual_bulk_crawl_job_id"], meta["manual_bulk_crawl_index"],
                  final_url=failure.request.url, status_code=None, error=failure.type.__name__)

    def save(self, job_id, index, **result):
        job = self.jobs.get(job_id)
        if job is None or index not in job.inflight:
            return
        try:
            if not job.failed:
                job.batch.save(index, **result)
        except JobUnavailable:
            job.failed = True
            job.pending.clear()
        except Exception:
            job.failed = True
            self.spider.logger.exception("manual_bulk_crawl.storage_failed")
        finally:
            job.inflight.discard(index)
            self.inflight.discard((job_id, index))
            self.release_if_drained(job_id, job)
            # Refill immediately, even while other URLs in this job are slow.
            self.refill()

    def release_if_drained(self, job_id, job):
        if job.inflight or (job.pending and not job.failed):
            return
        job.batch.close()
        del self.jobs[job_id]
        if job.failed:
            # Drain this job's callbacks before releasing ownership. Retry on
            # a later poll rather than spinning on a broken queue or disk.
            self.retry_after[job_id] = monotonic() + 1
        else:
            self.finished.add(job_id)

"""Share download dispatch between normal crawling and manual API work."""
from pathlib import Path

from scrapy.core.scheduler import BaseScheduler, Scheduler
from scrapy.dupefilters import BaseDupeFilter


class FairScheduler(BaseScheduler):
    """Choose between two source queues using one crawler-wide dispatch policy.

    Each source retains Scrapy's downloader-aware domain queues and persistence.
    Choosing a source before memory/disk storage also keeps an unserializable
    manual callback from unfairly overtaking normal requests on disk.
    """
    def __init__(self, normal, manual, scheduling_policy="manual_first"):
        self.normal = normal
        self.manual = manual
        if scheduling_policy not in ("balanced", "manual_first", "normal_first"):
            raise ValueError("MANUAL_BULK_CRAWL_SCHEDULING_POLICY must be balanced, manual_first or normal_first")
        self.scheduling_policy = scheduling_policy
        self.next_source = 0

    @classmethod
    def from_crawler(cls, crawler):
        normal = Scheduler.from_crawler(crawler)
        jobdir = crawler.settings.get("JOBDIR")
        manual = Scheduler(
            # Manual URLs intentionally bypass cross-request deduplication.
            dupefilter=BaseDupeFilter(),
            jobdir=str(Path(jobdir) / "manual") if jobdir else None,
            dqclass=normal.dqclass, mqclass=normal.mqclass,
            logunser=normal.logunser, stats=crawler.stats,
            pqclass=normal.pqclass, crawler=crawler,
        )
        return cls(normal, manual,
                   crawler.settings.get("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", "manual_first"))

    def open(self, spider):
        self.manual.open(spider)
        return self.normal.open(spider)

    def close(self, reason):
        self.manual.close(reason)
        return self.normal.close(reason)

    def enqueue_request(self, request):
        queue = self.manual if "manual_bulk_crawl_job_id" in request.meta else self.normal
        return queue.enqueue_request(request)

    def _take(self, queue, source):
        request = queue.next_request() if queue else None
        if request is not None:
            queue.stats.inc_value(f"scheduler/dequeued/{source}")
        return request

    def next_request(self):
        queues = (self.normal, self.manual)
        if self.scheduling_policy == "balanced":
            first = self.next_source
        else:
            first = 1 if self.scheduling_policy == "manual_first" else 0
        for index in (first, 1 - first):
            request = self._take(queues[index], "manual" if index else "normal")
            if request is not None:
                if self.scheduling_policy == "balanced":
                    # Give the other source the next turn; skip empty queues.
                    self.next_source = 1 - index
                return request
        return None

    def has_pending_requests(self):
        return bool(len(self))

    def __len__(self):
        return len(self.normal) + len(self.manual)

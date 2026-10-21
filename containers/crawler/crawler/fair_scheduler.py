"""Share download dispatch between normal crawling and manual API work."""
from pathlib import Path

from scrapy.core.scheduler import BaseScheduler, Scheduler
from scrapy.dupefilters import BaseDupeFilter


class FairScheduler(BaseScheduler):
    """Round-robin over two standard Scrapy schedulers.

    Each source retains Scrapy's downloader-aware domain queues and persistence.
    Choosing a source before memory/disk storage also keeps an unserializable
    manual callback from unfairly overtaking normal requests on disk.
    """
    def __init__(self, normal, manual):
        self.normal = normal
        self.manual = manual
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
        return cls(normal, manual)

    def open(self, spider):
        self.manual.open(spider)
        return self.normal.open(spider)

    def close(self, reason):
        self.manual.close(reason)
        return self.normal.close(reason)

    def enqueue_request(self, request):
        queue = self.manual if "manual_bulk_crawl_job_id" in request.meta else self.normal
        return queue.enqueue_request(request)

    def next_request(self):
        queues = (self.normal, self.manual)
        for offset in range(2):
            index = (self.next_source + offset) % 2
            queue = queues[index]
            if not queue:
                continue
            request = queue.next_request()
            if request is not None:
                # Each successful dispatch gives the other source the next
                # turn. An empty source is skipped without limiting capacity.
                self.next_source = 1 - index
                source = "manual" if index else "normal"
                queue.stats.inc_value(f"scheduler/dequeued/{source}")
                return request
        return None

    def has_pending_requests(self):
        return bool(len(self))

    def __len__(self):
        return len(self.normal) + len(self.manual)

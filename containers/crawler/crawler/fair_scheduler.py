"""Share download dispatch between normal crawling and manual API work."""
from pathlib import Path

from scrapy.core.scheduler import BaseScheduler, Scheduler
from scrapy.dupefilters import BaseDupeFilter
from libs.ipc.manual_bulk_crawl import DEFAULT_SCHEDULING_POLICY, validate_scheduling_policy


class FairScheduler(BaseScheduler):
    """Choose normal/manual dispatch according to the policy of each manual job.

    Each source retains Scrapy's downloader-aware domain queues and persistence.
    Choosing a source before memory/disk storage also keeps an unserializable
    manual callback from unfairly overtaking normal requests on disk.
    """
    def __init__(self, normal, manual, manual_first, normal_first):
        self.normal = normal
        self.manual = manual
        self.manual_queues = {"balanced": manual, "manual_first": manual_first,
                              "normal_first": normal_first}
        self.next_source = 0

    @classmethod
    def from_crawler(cls, crawler):
        normal = Scheduler.from_crawler(crawler)
        jobdir = crawler.settings.get("JOBDIR")
        def manual_queue(directory):
            return Scheduler(
                # Manual URLs intentionally bypass cross-request deduplication.
                dupefilter=BaseDupeFilter(),
                jobdir=str(Path(jobdir) / directory) if jobdir else None,
                dqclass=normal.dqclass, mqclass=normal.mqclass,
                logunser=normal.logunser, stats=crawler.stats,
                pqclass=normal.pqclass, crawler=crawler,
            )
        return cls(normal, manual_queue("manual"), manual_queue("manual_first"), manual_queue("normal_first"))

    def open(self, spider):
        for queue in self.manual_queues.values():
            queue.open(spider)
        return self.normal.open(spider)

    def close(self, reason):
        for queue in self.manual_queues.values():
            queue.close(reason)
        return self.normal.close(reason)

    def enqueue_request(self, request):
        queue = self.normal
        if "manual_bulk_crawl_job_id" in request.meta:
            policy = validate_scheduling_policy(request.meta.get("manual_bulk_crawl_scheduling_policy", DEFAULT_SCHEDULING_POLICY))
            queue = self.manual_queues[policy]
        return queue.enqueue_request(request)

    def _take(self, queue, source):
        request = queue.next_request() if queue else None
        if request is not None:
            queue.stats.inc_value(f"scheduler/dequeued/{source}")
        return request

    def next_request(self):
        request = self._take(self.manual_queues["manual_first"], "manual")
        if request is not None:
            return request
        queues = (self.normal, self.manual)
        for offset in range(2):
            index = (self.next_source + offset) % 2
            queue = queues[index]
            if not queue:
                continue
            source = "manual" if index else "normal"
            request = self._take(queue, source)
            if request is not None:
                # Each successful dispatch gives the other source the next
                # turn. An empty source is skipped without limiting capacity.
                self.next_source = 1 - index
                return request
        return self._take(self.manual_queues["normal_first"], "manual")

    def has_pending_requests(self):
        return bool(len(self))

    def __len__(self):
        return len(self.normal) + sum(len(queue) for queue in self.manual_queues.values())

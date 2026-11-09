"""Measure manual-job feeding and durable saves with an immediate fake downloader.

Run with the crawler's Python environment from the repository root:
PYTHONPATH=. containers/crawler/.venv/bin/python scripts/benchmark_manual_bulk_crawl_feeder.py

This isolates local queue/storage overhead; it does not measure HTTP throughput.
Jobs assigned to another crawler model a shared queue with unrelated work.
"""
import argparse
from collections import deque
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import Mock

from scrapy.http import HtmlResponse
from scrapy.settings import Settings

from containers.crawler.crawler import manual_bulk_crawls as feeder
from libs.ipc.manual_bulk_crawl import prepare_job, job_status


def measure(urls, unrelated_jobs, unrelated_urls, body_bytes):
    with TemporaryDirectory(prefix="manual-bulk-feeder-") as directory:
        root = Path(directory)
        pending = root / "pending"
        pending.mkdir()
        for number in range(unrelated_jobs + 1):
            own = number == 0
            count = urls if own else unrelated_urls
            path = pending / f"{number:032x}.json"
            payload = {
                "version": 1,
                "urls": [f"https://example.com/{number}/{i}" for i in range(count)],
                "crawler_ids": [0 if own else 1] * count,
                "result_dir": str(root / "results"),
            }
            path.write_text(json.dumps(payload))
            prepare_job(path, payload)
        requests = deque()
        spider = SimpleNamespace(crawler_id=0, logger=Mock(), crawler=SimpleNamespace(
            settings=Settings({"MANUAL_BULK_CRAWL_QUEUE_DIR": str(pending),
                               "CONCURRENT_REQUESTS": 256,
                               "CONCURRENT_REQUESTS_PER_DOMAIN": 256}),
            engine=SimpleNamespace(crawl=requests.append),
        ))
        consumer = feeder.ManualBulkCrawlConsumer(spider)
        original_batches = feeder.iter_batches
        scans = 0
        scan_seconds = 0.0

        def timed_batches(*args, **kwargs):
            nonlocal scans, scan_seconds
            scans += 1
            started = perf_counter()
            try:
                yield from original_batches(*args, **kwargs)
            finally:
                scan_seconds += perf_counter() - started

        feeder.iter_batches = timed_batches
        body = b"<html>" + b"x" * body_bytes + b"</html>"
        started = perf_counter()
        try:
            consumer.poll()
            completed = 0
            while requests:
                request = requests.popleft()
                consumer.parse(HtmlResponse(request.url, request=request, body=body,
                                            headers={"Content-Type": "text/html"}))
                completed += 1
            elapsed = perf_counter() - started
            job_id = f"{0:032x}"
            payload = json.loads((pending / f"{job_id}.json").read_text())
            status = job_status(job_id, payload, root / "results" / job_id)
            assert completed == status["succeeded_urls"] == urls
            assert not consumer.jobs and not consumer.inflight
            spider.logger.exception.assert_not_called()
            return {"seconds": elapsed, "urls_per_second": completed / elapsed,
                    "queue_scans": scans, "queue_scan_seconds": scan_seconds}
        finally:
            consumer.close()
            feeder.iter_batches = original_batches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--urls", type=int, default=1000)
    parser.add_argument("--unrelated-jobs", type=int, default=100)
    parser.add_argument("--unrelated-urls", type=int, default=1000)
    parser.add_argument("--body-bytes", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if min(args.urls, args.repeats) < 1 or min(args.unrelated_jobs, args.unrelated_urls, args.body_bytes) < 0:
        parser.error("URL count and repeats must be positive; other counts must be nonnegative")
    for repeat in range(args.repeats):
        print(json.dumps({"repeat": repeat + 1, **vars(args),
                          **measure(args.urls, args.unrelated_jobs, args.unrelated_urls, args.body_bytes)}),
              flush=True)


if __name__ == "__main__":
    main()

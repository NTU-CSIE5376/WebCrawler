"""Real scheduler checkpoint proof against an isolated local HTTP server."""
import asyncio
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from scrapy.utils.reactor import install_reactor

install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

import tldextract
from crawler.spiders.spider import HtmlSpider
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from ops.production import crawler_checkpoint as operation


def main():
    root = Path(sys.argv[1])
    root.mkdir(parents=True, exist_ok=True)
    visits = []
    count = 200

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            visits.append(self.path)
            body = b"<html><title>scheduler checkpoint</title></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    settings = Settings()
    settings.setmodule("crawler.settings", priority="project")
    for key, value in {
        "CRAWLER_RESTART_MANIFEST": str(root / "no-active-manifest.json"),
        "URL_QUEUE_TEMPLATE": str(root / "queue" / "crawler_{id:02d}"),
        "RESULT_DIR_TEMPLATE": str(root / "results" / "crawler_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "manual"),
        "REMOTE_CONTROL_ENABLED": False, "ROBOTSTXT_OBEY": False,
        "MEMUSAGE_ENABLED": False, "AUTOTHROTTLE_ENABLED": False,
        "DOWNLOAD_DELAY": 0, "RANDOMIZE_DOWNLOAD_DELAY": False,
        "CONCURRENT_REQUESTS": 4, "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "LOG_LEVEL": "ERROR",
    }.items():
        settings.set(key, value, priority="cmdline")
    tldextract.extract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
    process = CrawlerProcess(settings)
    crawler = process.create_crawler(HtmlSpider)
    operation.EXPECTED_PID = os.getpid()
    operation.EXPECTED_WORKER = 2
    operation.NONCE = "real-scheduler-checkpoint-proof-123456"
    operation.SOURCE_FILE = sys.modules[HtmlSpider.__module__].__file__
    counters = {"items": 0, "checkpoint": None, "error": None}

    async def exercise():
        try:
            assert crawler.engine._start is None
            spider = crawler.spider
            spider._pending_requests += count
            spider._schedule_reserved_urls([(7, f"{base}/{index}") for index in range(count)])
            frozen = operation.operation(crawler, "freeze")
            assert frozen["counts"]["scheduler_normal"] == count
            assert not visits
            checkpoint = await operation.checkpoint(crawler, str(root / "checkpoints"))
            assert checkpoint["request_count"] == count and not visits
            assert operation.ready_to_stop(crawler)["ready"]
            assert len(crawler.engine._slot.scheduler.normal) == 0
            operation.operation(crawler, "resume")
            counters["checkpoint"] = checkpoint
        except Exception as error:  # noqa: BLE001 -- fail the isolated proof and stop its own engine
            counters["error"] = repr(error)
            await crawler.engine.stop_async()

    def started():
        asyncio.get_running_loop().call_later(0.05, lambda: asyncio.create_task(exercise()))

    def item_scraped(**kwargs):
        counters["items"] += 1
        if counters["items"] == count:
            asyncio.create_task(crawler.engine.stop_async())

    crawler.signals.connect(started, signal=signals.engine_started)
    crawler.signals.connect(item_scraped, signal=signals.item_scraped)
    failures = []
    deferred = process.crawl(crawler, crawler_id=2)
    deferred.addErrback(lambda failure: failures.append(failure.getErrorMessage()))
    process.start()
    server.shutdown()
    server.server_close()
    if failures or counters["error"]:
        raise RuntimeError(failures or counters["error"])
    records = []
    for path in (root / "results").rglob("*.jsonl"):
        records.extend(json.loads(line) for line in path.read_text().splitlines() if line)
    assert counters["checkpoint"] and len(records) == count and len(visits) == count
    assert crawler.spider._pending_requests == 0
    print(json.dumps({"proof_crawler_checkpoint": True, "checkpointed": count,
                      "normal_results": len(records), "http_requests": len(visits)}))


if __name__ == "__main__":
    main()

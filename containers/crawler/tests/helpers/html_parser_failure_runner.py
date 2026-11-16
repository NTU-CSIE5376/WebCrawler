"""Kill a real parser child and exercise Scrapy closure and a fresh crawler."""
import asyncio
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sys
from unittest.mock import Mock

from html_parser_runner import offline_suffix_list


async def run(root):
    from concurrent.futures import ProcessPoolExecutor

    from scrapy import signals
    from scrapy.crawler import CrawlerRunner
    from scrapy.settings import Settings
    from scrapy.utils.defer import maybe_deferred_to_future
    from twisted.internet import reactor
    from twisted.web.resource import Resource
    from twisted.web.server import Site

    import crawler.spiders.parser as parser_module
    from crawler.spiders.spider import HtmlSpider

    def create_pool(**kwargs):
        kwargs["initializer"] = offline_suffix_list
        return ProcessPoolExecutor(**kwargs)

    parser_module.ProcessPoolExecutor = create_pool

    class Page(Resource):
        isLeaf = True

        def render_GET(self, request):
            request.setHeader(b"Content-Type", b"text/html; charset=utf-8")
            return b'<html><title>Recovered</title><a href="https://example.com/">link</a></html>'

    listener = reactor.listenTCP(0, Site(Page()), interface="127.0.0.1")
    base_url = f"http://127.0.0.1:{listener.getHost().port}"

    class TrialSpider(HtmlSpider):
        def __init__(self, fail, **kwargs):
            super().__init__(**kwargs)
            self.fail = fail
            self.check = None
            self.checks = 0

        async def start(self):
            self._pending_requests += 1
            yield self._build_request(base_url + "/warm", 7)

        async def parse(self, response):
            items = await super().parse(response)
            if self.fail and response.url.endswith("/warm"):
                # Freeze the single worker to guarantee both submitted and
                # admission-waiting work exist at the moment it is killed.
                self.child, = self.html_parser_pool._executor._processes.values()
                os.kill(self.child.pid, signal.SIGSTOP)
                self.queue.pop_domain_batches = Mock(
                    side_effect=AssertionError("Reserved work after parser failure"),
                )
                self._pending_requests += 6
                for index in range(6):
                    self.crawler.engine.crawl(self._build_request(base_url + f"/{index}", 7))
                self.check = reactor.callLater(.01, self.kill_when_admitted)
            return items

        def kill_when_admitted(self):
            pool = self.html_parser_pool
            if pool.inflight == 2 and pool.waiting == 4:
                os.kill(self.child.pid, signal.SIGKILL)
                return
            self.checks += 1
            if self.checks >= 500:
                # Make a failed test drain rather than leave a stopped child.
                os.kill(self.child.pid, signal.SIGKILL)
                raise AssertionError("Responses did not reach parser admission")
            self.check = reactor.callLater(.01, self.kill_when_admitted)

    settings = Settings()
    settings.setmodule("crawler.settings")
    settings.setdict({
        "PARSER_PROCESSES": 1,
        "URL_QUEUE_TEMPLATE": str(root / "queue_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "manual"),
        "OBSLOG_HEARTBEAT_SEC": 0,
        "IPC_TOP_UP_INTERVAL_SEC": 0,
        "ROBOTSTXT_OBEY": False,
        "DOWNLOADER_MIDDLEWARES": {},
        "SPIDER_MIDDLEWARES": {},
        "AUTOTHROTTLE_ENABLED": False,
        "DOWNLOAD_DELAY": 0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
        "HTTPPROXY_ENABLED": False,
        "CLOSESPIDER_TIMEOUT": 10,
        "LOG_ENABLED": False,
    }, priority="cmdline")
    runner = CrawlerRunner(settings)
    report = {}
    try:
        for fail, name in ((True, "failed"), (False, "restarted")):
            crawler = runner.create_crawler(TrialSpider)
            output = root / name
            crawler.settings.set("RESULT_DIR_TEMPLATE", str(output), priority="cmdline")

            def scraped(item, spider):
                if not spider.fail:
                    crawler.engine.close_spider(spider, reason="test_complete")

            crawler.signals.connect(scraped, signal=signals.item_scraped, weak=False)
            await maybe_deferred_to_future(runner.crawl(crawler, fail=fail))
            pool = crawler.spider.html_parser_pool
            assert not multiprocessing.active_children()
            assert pool.inflight == pool.waiting == 0
            records = [json.loads(line) for path in output.rglob("*.jsonl")
                       for line in path.read_text().splitlines()]
            assert len(records) == 1, records
            # The actual JsonPipeline must have persisted the parsed result.
            records[0].pop("fetched_at")
            report[f"{name}_reason"] = crawler.stats.get_value("finish_reason")
            if fail:
                crawler.spider.queue.pop_domain_batches.assert_not_called()
                report.update(
                    failure_count=crawler.stats.get_value("parser/pool_broken"),
                    interrupted=crawler.stats.get_value("parser/interrupted"),
                    inflight=pool.inflight, waiting=pool.waiting,
                    pending=crawler.spider._pending_requests,
                    domain_pending=crawler.spider._domain_pending,
                    children_after_close=len(multiprocessing.active_children()),
                    original_output=records,
                )
            else:
                report["restarted_output"] = records
    finally:
        await maybe_deferred_to_future(runner.stop())
        await maybe_deferred_to_future(listener.stopListening())
    (root / "report.json").write_text(json.dumps(report))


if __name__ == "__main__":
    from scrapy.utils.reactor import install_reactor
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    from twisted.internet import task
    from twisted.internet.defer import Deferred
    task.react(lambda reactor: Deferred.fromFuture(asyncio.ensure_future(run(Path(sys.argv[1])))))

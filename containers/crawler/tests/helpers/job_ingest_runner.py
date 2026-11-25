"""Exercise both API job modes with the real parser, process writer, and signals."""
import json
from pathlib import Path
import sys

from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.reactor import install_reactor

install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
from twisted.internet.task import LoopingCall
import tldextract
from crawler.spiders.spider import HtmlSpider


class JobSpider(HtmlSpider):
    name = "job_processing_test"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.watch, signal=signals.spider_opened)
        crawler.signals.connect(spider.stop_watch, signal=signals.spider_closed)
        return spider

    def watch(self):
        # Ingest jobs must not decrement ordinary crawl inventory.
        self._pending_requests = 7
        self.timer = LoopingCall(self.check_done)
        self.timer.start(0.05, now=False)

    def check_done(self):
        if self.manual_bulk_crawls.finished and not self.manual_bulk_crawls.jobs:
            self.crawler.engine.close_spider(self, reason="jobs_completed")

    def stop_watch(self):
        if self.timer.running:
            self.timer.stop()


def main():
    root = Path(sys.argv[1])
    tldextract.extract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
    settings = Settings()
    settings.setmodule("crawler.settings")
    settings.update({
        "URL_QUEUE_TEMPLATE": str(root / "queue_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "pending"),
        "RESULT_DIR_TEMPLATE": str(root / "output_{id:02d}"),
        "SHARED_REQUEST_BUDGET_DIR": str(root / "budgets"),
        "PARSER_PROCESSES": 0, "JSON_WRITER_PROCESSES": 1,
        "IPC_JOB_INGEST_ENABLED": True,
        "ROBOTSTXT_OBEY": False, "HTTPPROXY_ENABLED": False,
        "AUTOTHROTTLE_ENABLED": False, "DOWNLOAD_DELAY": 0,
        "OBSLOG_HEARTBEAT_SEC": 0, "IPC_TOP_UP_INTERVAL_SEC": 0,
        "CLOSESPIDER_TIMEOUT": 15,
    })
    process = CrawlerProcess(settings)
    crawler = process.create_crawler(JobSpider)
    process.crawl(crawler)
    process.start()
    (root / "observation.json").write_text(json.dumps({
        "finish_reason": crawler.stats.get_value("finish_reason"),
        "normal_pending": crawler.spider._pending_requests,
        "item_count": crawler.stats.get_value("item_scraped_count", 0),
    }))


if __name__ == "__main__":
    main()

"""Run the shared-slot integration scenario in its own Scrapy reactor."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from scrapy import Request, Spider, signals
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DontCloseSpider
from scrapy.resolver import dnscache
from scrapy.settings import Settings
from scrapy.utils.reactor import install_reactor

# Install the subprocess's reactor before importing crawler modules.
install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

from crawler.manual_bulk_crawls import ManualBulkCrawlConsumer
from crawler.site_policies import SitePolicies
from libs.config.loader import load_yaml


class ObservedConsumer(ManualBulkCrawlConsumer):
    """Record scheduling decisions while delegating to the real consumer."""

    def _domain_limits(self, slot_key, domain):
        result = super()._domain_limits(slot_key, domain)
        slot = self.spider.crawler.engine.downloader.slots.get(slot_key)
        if slot_key == "wikipedia" and slot is not None:
            self.spider.observation["samples"].append({
                "delay": slot.delay,
                "concurrency": result[0],
                "seconds_per_url": result[1],
                "outstanding": domain.outstanding,
                "pending": len(domain.pending),
            })
        return result


class SharedSlotSpider(Spider):
    name = "manual_shared_slot_test"
    crawler_id = 0

    def __init__(self, port, observation, **kwargs):
        super().__init__(**kwargs)
        self.port = port
        self.observation = observation

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.consumer = ObservedConsumer(spider)
        crawler.signals.connect(spider.scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(spider.downloaded, signal=signals.response_downloaded)
        crawler.signals.connect(spider.idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.closed_consumer, signal=signals.spider_closed)
        return spider

    async def start(self):
        yield Request(f"http://en.wikipedia.org:{self.port}/wiki/warmup", callback=self.begin_manual)

    def begin_manual(self, response):
        self.observation["warmup_latency"] = response.meta["download_latency"]
        self.observation["warmup_delay"] = self.crawler.engine.downloader.slots["wikipedia"].delay
        self.consumer._load_jobs()
        self.observation["initial_pending"] = {
            key: len(queue.pending) for key, queue in self.consumer.domains.items()
        }
        self.consumer.start()
        self.consumer.poll()

    def scheduled(self, request):
        if "manual_bulk_crawl_job_id" not in request.meta:
            return
        if urlsplit(request.url).hostname == "other.example.org":
            wiki = self.consumer.domains.get("wikipedia")
            self.observation["other_admissions"].append({
                "outstanding": wiki.outstanding if wiki else 0,
                "pending": len(wiki.pending) if wiki else 0,
            })

    def downloaded(self, response, request):
        self.observation["downloads"].append({
            "host": urlsplit(request.url).hostname,
            "slot": request.meta["download_slot"],
        })

    def idle(self):
        if self.consumer.jobs:
            raise DontCloseSpider

    def closed_consumer(self):
        self.consumer.close()


def configure_policies():
    document = load_yaml("config/site_policies.yml")
    document["default"].update(crawler_mode="fixed", request_limits={
        "max_qps": 100, "max_concurrent_requests": 8,
    })
    document["domains"]["wikipedia.org"]["request_limits"].update(
        max_qps=100, max_concurrent_requests=2, target_concurrent_requests=0.2)
    policies = SitePolicies(document)
    SitePolicies.load = classmethod(lambda cls: policies)
    return policies


def build_settings(root, policies):
    settings = Settings()
    settings.setmodule("crawler.settings")
    settings.update({
        "CONCURRENT_REQUESTS": 3,
        "AUTOTHROTTLE_START_DELAY": 0.01,
        "AUTOTHROTTLE_MAX_DELAY": 1,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.2,
        "DOWNLOAD_SLOTS": policies.download_slots(start_delay=0.01),
        "RANDOMIZE_DOWNLOAD_DELAY": False,
        "ROBOTSTXT_OBEY": False,
        "HTTPPROXY_ENABLED": False,
        "DNS_RESOLVER": "scrapy.resolver.CachingThreadedResolver",
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "pending"),
        "SHARED_REQUEST_BUDGET_DIR": str(root / "request_budgets"),
        "ITEM_PIPELINES": {},
        "LOG_LEVEL": "ERROR",
        "CLOSESPIDER_TIMEOUT": 15,
    })
    return settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("port", type=int)
    args = parser.parse_args()

    policies = configure_policies()
    for host in ("en.wikipedia.org", "zh.wikipedia.org", "other.example.org"):
        dnscache[host] = "127.0.0.1"
    observation = {"samples": [], "other_admissions": [], "downloads": []}
    process = CrawlerProcess(build_settings(args.root, policies))
    crawler = process.create_crawler(SharedSlotSpider)
    process.crawl(crawler, port=args.port, observation=observation)
    process.start()
    observation["finish_reason"] = crawler.stats.get_value("finish_reason")
    (args.root / "observation.json").write_text(json.dumps(observation))


if __name__ == "__main__":
    main()

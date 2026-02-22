from __future__ import annotations

import tldextract
from w3lib.url import canonicalize_url
from datetime import datetime

import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider, IgnoreRequest
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError

from crawler.items import PageItem
from crawler.queue_consumer import QueueConsumer

ACCEPTED_CONTENT_TYPES = ["text/html", "application/xhtml+xml"]

class HtmlSpider(scrapy.Spider):
    name = "html_spider"

    def __init__(self, crawler_id: int = 0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crawler_id = int(crawler_id)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)

        qtpl = crawler.settings["URL_QUEUE_TEMPLATE"]
        spider.queue = QueueConsumer(queue_dir=qtpl.format(id=spider.crawler_id))
        spider.link_extractor = LinkExtractor(canonicalize=True)

        crawler.signals.connect(spider.on_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.req_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(spider.req_start, signal=signals.request_reached_downloader)
        crawler.signals.connect(spider.req_end, signal=signals.response_received)

        return spider

    async def start(self):
        urls = self.queue.pop_batch()
        t = datetime.now()
        print(f"[crawler-{self.crawler_id:02d}] Get {len(urls)} new requests, time={t}", flush=True)

        for u in urls:
            yield scrapy.Request(
                url=u,
                callback=self.parse,
                errback=self.errback
            )

    def on_idle(self):
        urls = self.queue.pop_batch()
        t = datetime.now()
        print(f"[crawler-{self.crawler_id:02d}] Get {len(urls)} new requests, time={t}", flush=True)

        for u in urls:
            self.crawler.engine.crawl(
                scrapy.Request(
                    url=u,
                    callback=self.parse,
                    errback=self.errback
                )
            )

        raise DontCloseSpider

    def _extract_domain(self, url):
        extracted = tldextract.extract(url)
        domain = ".".join([p for p in [extracted.domain, extracted.suffix] if p])
        return domain


    def parse(self, response):
        url = canonicalize_url(response.url)
        domain = self._extract_domain(url)

        ctype = response.headers.get("Content-Type", b"").decode().lower()
        if not any(t in ctype for t in ACCEPTED_CONTENT_TYPES):
            yield PageItem(
                url=url,
                domain=domain,
                fail_reason="NonHTML content-type",
                content=None,
                outlinks=[],
            )
            return

        outlinks = []

        for link in self.link_extractor.extract_links(response):
            if not link.nofollow:
                u = canonicalize_url(link.url)
                outlinks.append({
                    "url": u,
                    "domain": self._extract_domain(u),
                    "anchor": (link.text or "").strip()[:200]
                })

        yield PageItem(
            url=url,
            domain=domain,
            fail_reason=None,
            content=response.text,
            outlinks=outlinks,
        )

    def errback(self, failure):
        url = canonicalize_url(failure.request.url)
        domain = self._extract_domain(url)

        item = PageItem(
            url=url,
            domain=domain,
            fail_reason=failure.type.__name__,
            content=None,
            outlinks=[],
        )

        if failure.check(HttpError):
            item["fail_reason"] = f"HttpError {failure.value.response.status}"
        elif failure.check(IgnoreRequest):
            item["fail_reason"] = f"IgnoreRequest {failure.getErrorMessage()}"
            if "exceeded DOWNLOAD_MAXSIZE" in item["fail_reason"]:
                item["fail_reason"] = f"IgnoreRequest exceeded DOWNLOAD_MAXSIZE"

        yield item

    def req_scheduled(self, request):
        t = datetime.now()
        print(f"[crawler-{self.crawler_id:02d}] Request scheduled: time={t}, url={request.url}", flush=True)
    def req_start(self, request):
        t = datetime.now()
        print(f"[crawler-{self.crawler_id:02d}] Download started: time={t}, url={request.url}", flush=True)
        request.meta["t_down_start"] = t
    def req_end(self, response, request):
        t = datetime.now()
        print(f"[crawler-{self.crawler_id:02d}] Download ended: time={t}, url={request.url}, latency={t - request.meta.get("t_down_start", t)}", flush=True)


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
        self._inflight = 0
        self._max_inflight = 0
        self._max_transferring = 0
        self._max_slot_queue = 0
        self._pending_requests = 0
        self._max_pending_requests = 0
        self._domain_pending: dict[int, int] = {}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)

        qtpl = crawler.settings["URL_QUEUE_TEMPLATE"]
        spider.queue = QueueConsumer(queue_dir=qtpl.format(id=spider.crawler_id))
        spider.link_extractor = LinkExtractor(canonicalize=True)

        spider.domain_low_watermark = max(
            0, crawler.settings.getint("IPC_DOMAIN_LOW_WATERMARK", 10)
        )

        crawler.signals.connect(spider.on_idle, signal=signals.spider_idle)
        crawler.signals.connect(spider.req_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(spider.req_start, signal=signals.request_reached_downloader)
        crawler.signals.connect(spider.req_end, signal=signals.response_received)

        return spider

    def _set_inflight_stats(self):
        stats = getattr(self.crawler, "stats", None)
        if stats is None:
            return
        stats.set_value("inflight/current", self._inflight, spider=self)
        stats.set_value("inflight/max", self._max_inflight, spider=self)
        stats.set_value("pending/current", self._pending_requests, spider=self)
        stats.set_value("pending/max", self._max_pending_requests, spider=self)
        stats.set_value("active_domains/current", len(self._domain_pending), spider=self)
        runtime = self._downloader_runtime()
        stats.set_value("transferring/current", runtime["transferring"], spider=self)
        stats.set_value("transferring/max", self._max_transferring, spider=self)
        stats.set_value("slot_queue/current", runtime["slot_queue"], spider=self)
        stats.set_value("slot_queue/max", self._max_slot_queue, spider=self)

    def _downloader_runtime(self) -> dict[str, int]:
        downloader = getattr(getattr(self.crawler, "engine", None), "downloader", None)
        slots = getattr(downloader, "slots", {}) or {}

        transferring = 0
        slot_queue = 0
        slot_active = 0

        for slot in slots.values():
            transferring += len(getattr(slot, "transferring", ()) or ())
            slot_queue += len(getattr(slot, "queue", ()) or ())
            slot_active += len(getattr(slot, "active", ()) or ())

        self._max_transferring = max(self._max_transferring, transferring)
        self._max_slot_queue = max(self._max_slot_queue, slot_queue)

        return {
            "transferring": transferring,
            "slot_queue": slot_queue,
            "slot_active": slot_active,
            "slots": len(slots),
        }

    def _runtime_suffix(self) -> str:
        runtime = self._downloader_runtime()
        return (
            f"active_domains={len(self._domain_pending)}, "
            f"pending={self._pending_requests}, pending_max={self._max_pending_requests}, "
            f"inflight={self._inflight}, inflight_max={self._max_inflight}, "
            f"transferring={runtime['transferring']}, transferring_max={self._max_transferring}, "
            f"slot_queue={runtime['slot_queue']}, slot_queue_max={self._max_slot_queue}, "
            f"slot_active={runtime['slot_active']}, slots={runtime['slots']}"
        )

    def _log(self, message: str):
        print(f"[crawler-{self.crawler_id:02d}] {message}, {self._runtime_suffix()}", flush=True)

    def _build_request(self, url: str, domain_id: int) -> scrapy.Request:
        source_url, fetch_url = split_bench_url(url)
        self._domain_pending[domain_id] = self._domain_pending.get(domain_id, 0) + 1
        return scrapy.Request(
            url=fetch_url,
            callback=self.parse,
            errback=self.errback,
            meta={"source_url": source_url, "_track_domain_id": domain_id},
        )

    def _reserve_urls(self, reason: str, force: bool = False) -> list[tuple[int, str]]:
        needs_domains = len(self._domain_pending) < self.domain_low_watermark
        if not force and not needs_domains:
            return []

        pending_before = self._pending_requests
        slots = self.domain_low_watermark - len(self._domain_pending)
        if slots <= 0:
            slots = 1 if force else 0
        if slots <= 0:
            return []

        batch = self.queue.pop_domain_batches(
            limit=slots,
            exclude_domain_ids=set(self._domain_pending.keys()),
        )

        reserved: list[tuple[int, str]] = []
        for domain_id, urls in batch.items():
            for url in urls:
                reserved.append((domain_id, url))
            self._pending_requests += len(urls)

        if reserved:
            self._max_pending_requests = max(self._max_pending_requests, self._pending_requests)
            self._set_inflight_stats()
            self._log(
                "Top-up loaded "
                f"{len(reserved)} requests in {len(batch)} domain files, reason={reason}, "
                f"pending_before={pending_before}, pending_after={self._pending_requests}, "
                f"new_domains={len(batch)}"
            )
        elif force or needs_domains:
            self._set_inflight_stats()
            self._log(
                f"Top-up found no batch, reason={reason}, "
                f"pending_before={pending_before}, pending_after={self._pending_requests}"
            )

        return reserved

    def _schedule_reserved_urls(self, entries: list[tuple[int, str]]) -> int:
        for domain_id, url in entries:
            self.crawler.engine.crawl(self._build_request(url, domain_id))
        return len(entries)

    def _maybe_top_up(self, reason: str, force: bool = False) -> int:
        entries = self._reserve_urls(reason=reason, force=force)
        return self._schedule_reserved_urls(entries)

    def _finish_owned_request(self, reason: str, domain_id: int = 0) -> None:
        self._pending_requests = max(0, self._pending_requests - 1)
        if domain_id and domain_id in self._domain_pending:
            self._domain_pending[domain_id] -= 1
            if self._domain_pending[domain_id] <= 0:
                del self._domain_pending[domain_id]
        self._set_inflight_stats()
        if len(self._domain_pending) < self.domain_low_watermark:
            self._maybe_top_up(reason=f"{reason}_low_watermark")

    def spider_opened(self, spider=None):
        self._set_inflight_stats()

    async def start(self):
        for domain_id, url in self._reserve_urls(reason="start", force=True):
            yield self._build_request(url, domain_id)

    def on_idle(self):
        self._maybe_top_up(reason="idle", force=True)
        raise DontCloseSpider

    def _extract_domain(self, url):
        extracted = tldextract.extract(url)
        domain = ".".join([p for p in [extracted.domain, extracted.suffix] if p])
        return domain


    def parse(self, response):
        url = canonicalize_url(response.url)
        source_url = response.meta.get("source_url", response.url)
        track_domain_id = response.meta.get("_track_domain_id", 0)
        fetched_url = canonicalize_url(response.url)
        domain = self._extract_domain(fetched_url)

        ctype = response.headers.get("Content-Type", b"").decode().lower()
        if not any(t in ctype for t in ACCEPTED_CONTENT_TYPES):
            self._finish_owned_request(reason="non_html", domain_id=track_domain_id)
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

        self._finish_owned_request(reason="parse", domain_id=track_domain_id)
        yield PageItem(
            url=url,
            domain=domain,
            fail_reason=None,
            content=response.text,
            outlinks=outlinks,
        )

    def errback(self, failure):
        url = canonicalize_url(failure.request.url)
        source_url = failure.request.meta.get("source_url", failure.request.url)
        track_domain_id = failure.request.meta.get("_track_domain_id", 0)
        fetched_url = canonicalize_url(failure.request.url)
        domain = self._extract_domain(fetched_url)

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

        self._finish_owned_request(reason="errback", domain_id=track_domain_id)
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


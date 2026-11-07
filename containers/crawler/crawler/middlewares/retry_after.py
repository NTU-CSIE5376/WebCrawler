"""Pause a downloader slot immediately on 429, before queued transfers start."""
import logging
from datetime import timezone
from email.utils import parsedate_to_datetime

from scrapy import signals


logger = logging.getLogger("crawler")


class RetryAfterMiddleware:
    COOLDOWN_SECONDS = 10

    def __init__(self, crawler, clock):
        self.crawler = crawler
        self.clock = clock
        self.cooldown_until = {}

    @classmethod
    def from_crawler(cls, crawler):
        from twisted.internet import reactor

        instance = cls(crawler, reactor)
        crawler.signals.connect(instance.response_downloaded, signal=signals.response_downloaded)
        return instance

    def _cooldown_seconds(self, response):
        value = response.headers.get("Retry-After", b"").decode("ascii", errors="replace").strip()
        try:
            seconds = int(value)
        except ValueError:
            try:
                deadline = parsedate_to_datetime(value)
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                seconds = deadline.timestamp() - self.clock.seconds()
            except (ValueError, TypeError, OverflowError):
                seconds = 0
        return max(self.COOLDOWN_SECONDS, seconds)

    def response_downloaded(self, response, request, spider=None):
        if response.status != 429:
            return
        downloader = self.crawler.engine.downloader
        slot_key = request.meta["download_slot"]
        slot = downloader.slots[slot_key]
        now = self.clock.seconds()
        deadline = max(self.cooldown_until.get(slot_key, 0), now + self._cooldown_seconds(response))
        self.cooldown_until[slot_key] = deadline
        # This signal precedes the downloader draining its queue. Reuse the
        # slot timer so queued requests and retries wait together, and Scrapy's
        # slot shutdown cancels the timer. This private API stays here.
        if slot.latercall is not None:
            slot.latercall.cancel()

        def resume():
            self.cooldown_until.pop(slot_key, None)
            slot.latercall = None
            downloader._process_queue(slot)

        slot.latercall = self.clock.callLater(deadline - now, resume)
        self.crawler.stats.inc_value("retry_after/cooldowns")
        logger.warning("crawler.rate_limited", extra={
            "event": "crawler.rate_limited", "url": request.url,
            "download_slot": slot_key, "cooldown_seconds": deadline - now,
        })

"""Wikipedia website rules, selected declaratively in site_policies.yml."""
import logging
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, quote, unquote, urlsplit

from scrapy.exceptions import IgnoreRequest
from scrapy.http.request import NO_CALLBACK


logger = logging.getLogger("crawler")


class WikipediaMiddleware:
    COOLDOWN_SECONDS = 10

    @classmethod
    def from_crawler(cls, crawler):
        from twisted.internet import reactor

        instance = cls()
        instance.crawler = crawler
        instance.clock = reactor
        instance.cooldown_until = {}
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

    def response_downloaded(self, response, request, policy):
        if response.status != 429:
            return
        downloader = self.crawler.engine.downloader
        slot_key = request.meta["download_slot"]
        slot = downloader.slots[slot_key]
        now = self.clock.seconds()
        deadline = max(self.cooldown_until.get(slot_key, 0), now + self._cooldown_seconds(response))
        self.cooldown_until[slot_key] = deadline
        # response_downloaded fires before the next queued transfer. Reuse the
        # slot's timer to pause queued requests and retries without blocking
        # other slots; downloader shutdown also cancels this timer. Scrapy has
        # no public per-slot pause API, so keep the private queue call here.
        if slot.latercall is not None:
            slot.latercall.cancel()

        def resume():
            self.cooldown_until.pop(slot_key, None)
            slot.latercall = None
            downloader._process_queue(slot)

        slot.latercall = self.clock.callLater(deadline - now, resume)
        self.crawler.stats.inc_value("wikipedia/cooldowns")
        logger.warning("wikipedia.rate_limited", extra={
            "event": "wikipedia.rate_limited", "url": request.url,
            "download_slot": slot_key, "cooldown_seconds": deadline - now,
        })

    def process_request(self, request, policy):
        url = urlsplit(request.url)
        robots = url.path == "/robots.txt" and request.callback is NO_CALLBACK
        if request.method not in {"GET", "HEAD"}:
            raise IgnoreRequest("Wikipedia crawling requires GET or HEAD")
        path = url.path
        if path == "/w/index.php":
            titles = parse_qs(url.query).get("title", [])
            if len(titles) != 1 or not titles[0].strip():
                raise IgnoreRequest("Wikipedia URL cannot be normalized to /wiki/Title without a title")
            title = titles[0]
            path = "/wiki/" + quote(title.strip().replace(" ", "_"), safe="/:")
        elif not (robots or (path.startswith("/wiki/") and path != "/wiki/")):
            raise IgnoreRequest("Wikipedia URL cannot be normalized to /wiki/Title")
        elif not robots:
            title = unquote(path.removeprefix("/wiki/"))
            normalized_title = title.strip().replace(" ", "_")
            if not normalized_title:
                raise IgnoreRequest("Wikipedia URL cannot be normalized to /wiki/Title without a title")
            if normalized_title != title:
                path = "/wiki/" + quote(normalized_title, safe="/:")
        normalized_url = url._replace(path=path, query="", fragment="").geturl()
        if normalized_url != request.url:
            logger.info(
                "wikipedia.url_normalized",
                extra={"event": "wikipedia.url_normalized", "original_url": request.url,
                       "normalized_url": normalized_url},
            )
            return request.replace(url=normalized_url)
        for name, values in policy.headers.items():
            request.headers[name] = values
        request.headers["Accept-Encoding"] = "gzip"
        request.headers["Cookie"] = []
        request.meta["dont_merge_cookies"] = True
        if policy.download_timeout_sec is not None:
            request.meta["download_timeout"] = policy.download_timeout_sec

"""Skip re-fetching a redirect target this crawler fetched recently.

Many queued URLs can redirect to the same page: a site in maintenance sends
every path to /maintenance/, a regional host sends each catalogue URL to the
same www page. Scrapy's dupefilter is disabled here, so each of those
redirects fetched the target again (one sample minute: 116 fetches of
pacificleague.com/maintenance/).

RecentFetches remembers canonical URLs that were fetched successfully. The
middleware drops a *redirected* request whose target is in there with
DuplicateRedirectTarget; the spider's errback then reports the queued URL as
resolved (ok, no content), so the URL is recorded but the content is not
stored twice. First-hop requests are never dropped: an offered URL is always
fetched.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import Callable

from scrapy.exceptions import IgnoreRequest
from w3lib.url import canonicalize_url


class DuplicateRedirectTarget(IgnoreRequest):
    """A redirect led to a URL fetched successfully within the TTL."""


class RecentFetches:
    def __init__(
        self,
        ttl_sec: float,
        max_entries: int,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.ttl_sec = ttl_sec
        self.max_entries = max_entries
        self.clock = clock
        self._seen: OrderedDict[str, float] = OrderedDict()

    def add(self, url: str) -> None:
        self._seen[url] = self.clock()
        self._seen.move_to_end(url)
        while len(self._seen) > self.max_entries:
            self._seen.popitem(last=False)

    def seen(self, url: str) -> bool:
        at = self._seen.get(url)
        if at is None:
            return False
        if self.clock() - at > self.ttl_sec:
            del self._seen[url]
            return False
        return True

    def __len__(self) -> int:
        return len(self._seen)


class RedirectTargetDedupMiddleware:
    """Downloader middleware; the spider owns the RecentFetches instance
    (`spider.recent_fetches`) and fills it on successful parses."""

    def __init__(self, stats=None):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(stats=crawler.stats)

    def process_request(self, request, spider=None):
        recent = getattr(spider, "recent_fetches", None)
        if recent is None:
            return None
        if not request.meta.get("redirect_urls"):
            return None
        if "manual_bulk_crawl_job_id" in request.meta:
            return None
        target = canonicalize_url(request.url)
        if recent.seen(target):
            if self.stats is not None:
                self.stats.inc_value("redirect_dedup/skipped")
            raise DuplicateRedirectTarget(target)
        return None

"""Bounded HTML extraction in child processes, with results on the reactor."""
from __future__ import annotations

import multiprocessing
import os
import signal
import sys
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse

from scrapy.http import TextResponse
from scrapy.link import Link
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.defer import maybe_deferred_to_future
from twisted.internet.defer import Deferred, DeferredSemaphore, succeed
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from crawler.middlewares.youtube import normalize_youtube_url
from libs.urls import MAX_URL_LEN, domain_of


class CanonicalLinkExtractor(LinkExtractor):
    """Canonicalize once and use tuple suffix matching for default filters."""

    def __init__(self, **kwargs):
        super().__init__(canonicalize=True, **kwargs)
        # Scrapy adds the leading dots; preserve its exact suffix strings.
        self._denied_suffixes = tuple(self.deny_extensions)

    def _link_allowed(self, link: Link, schemes: set[str] | None = None) -> bool:
        if (self.allow_res or self.deny_res or self.allow_domains
                or self.deny_domains or self.restrict_text):
            if schemes is None:
                return super()._link_allowed(link)
            return super()._link_allowed(link, schemes)
        parsed_url = urlparse(link.url)
        if schemes is None:
            # Before Scrapy 2.20, the callback used a fixed scheme whitelist.
            if link.url.split("://", 1)[0] not in {"http", "https", "file", "ftp"}:
                return False
        elif schemes and parsed_url.scheme not in schemes:
            # Scrapy 2.20 passes response/configured schemes; empty allows all.
            return False
        return not parsed_url.path.lower().endswith(self._denied_suffixes)


class HtmlParser:
    """Synchronous HTML extraction with an extractor owned by this instance."""

    def __init__(self):
        self.link_extractor = CanonicalLinkExtractor()

    def parse(self, response: TextResponse) -> dict:
        outlinks = []
        seen_urls: set[str] = set()
        for link in self.link_extractor.extract_links(response):
            if link.nofollow:
                continue
            # The extractor already canonicalized link.url.
            url = normalize_youtube_url(link.url)
            if len(url) > MAX_URL_LEN or url in seen_urls:
                continue
            seen_urls.add(url)
            outlinks.append({
                "url": url,
                "domain": domain_of(url),
                "anchor": (link.text or "").strip()[:200],
            })
        return {
            "outlinks": outlinks,
            "title": (response.xpath("//title/text()").get() or "").strip()[:500] or None,
            "hreflang_count": len(response.xpath(
                "//link[contains(concat(' ', normalize-space(@rel), ' '), ' alternate ') and @hreflang]"
            )),
            "has_json_ld": bool(response.xpath('//script[@type="application/ld+json"]')),
        }


# Set once by the executor initializer in each child process. The parent uses
# its own HtmlParser for inline parsing; parser instances never cross IPC.
_worker_parser: HtmlParser


def _init_parser(parent_pid: int):
    global _worker_parser
    if sys.platform == "linux":
        import ctypes

        # Ask the kernel to kill this worker if its creating parent dies.
        libc = ctypes.CDLL(None, use_errno=True)
        prctl = libc.prctl
        prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                          ctypes.c_ulong, ctypes.c_ulong]
        prctl.restype = ctypes.c_int
        if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        # Parent death before prctl() is not delivered retrospectively.
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGKILL)
    # The crawler handles terminal interrupts and drains its children.
    # Supervisor sends TERM to the parent and KILL to the group on timeout.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _worker_parser = HtmlParser()


def _parse_html(url, body, encoding, response_class):
    """Return extracted fields without sending the HTML back to the parent."""
    response = response_class(url, body=body, encoding=encoding)
    return _worker_parser.parse(response)


class HtmlParserPool:
    """Run HtmlParser in bounded child processes, or locally when disabled."""

    def __init__(self, processes: int = 2):
        if processes < 0:
            raise ValueError("PARSER_PROCESSES must be non-negative")
        self._parser = HtmlParser() if processes == 0 else None
        self._slots = DeferredSemaphore(processes * 2) if processes else None
        self._closing = False
        self._shutdown: Deferred | None = None
        # Never fork a running reactor or inherit its sockets and threads.
        self._executor = (
            ProcessPoolExecutor(
                max_workers=processes,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_init_parser,
                initargs=(os.getpid(),),
            ) if processes else None
        )

    @property
    def inflight(self) -> int:
        return self._slots.limit - self._slots.tokens if self._slots else 0

    @property
    def waiting(self) -> int:
        return len(self._slots.waiting) if self._slots else 0

    async def parse(self, response: TextResponse) -> dict:
        if self._closing:
            raise RuntimeError("HTML parser pool is closing")
        if self._executor is None:
            return self._parser.parse(response)
        result = self._slots.acquire()
        result.addCallback(lambda _: self._submit(response))
        return await maybe_deferred_to_future(result)

    def _submit(self, response: TextResponse) -> Deferred:
        from twisted.internet import reactor

        try:
            if self._closing:
                raise RuntimeError("HTML parser pool is closing")
            future = self._executor.submit(
                _parse_html, response.url, response.body, response.encoding, type(response),
            )
        except Exception:
            self._slots.release()
            raise

        result = Deferred(lambda _: future.cancel())

        def completed():
            try:
                fields = future.result()
            except BaseException:
                if not result.called:
                    result.errback(Failure())
            else:
                if not result.called:
                    result.callback(fields)
            finally:
                # Cancellation must not free a slot while the child still runs.
                self._slots.release()

        future.add_done_callback(lambda _: reactor.callFromThread(completed))
        return result

    def close(self) -> Deferred:
        if self._shutdown is None:
            self._closing = True
            # Scrapy waits for this Deferred; joining never blocks the reactor.
            if self._executor is None:
                self._shutdown = succeed(None)
            else:
                self._shutdown = deferToThread(
                    self._executor.shutdown, wait=True, cancel_futures=True,
                )
        return self._shutdown

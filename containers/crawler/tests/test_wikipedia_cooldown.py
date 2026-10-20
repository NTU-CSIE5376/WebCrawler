"""Wikipedia backoff pauses the shared download slot, including queued work."""
from datetime import datetime, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.http import Response
from scrapy.settings import Settings
from scrapy.statscollectors import MemoryStatsCollector
from twisted.internet.task import Clock

from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
from containers.crawler.crawler.middlewares.wikipedia import WikipediaMiddleware
from containers.crawler.crawler.site_policies import SitePolicies


@pytest.fixture
def cooldown(monkeypatch):
    policies = SitePolicies({"domains": {"wikipedia.org": {
        "middleware": f"{WikipediaMiddleware.__module__}.WikipediaMiddleware",
        "request_limits": {"download_slot": "wikipedia"},
    }}})
    monkeypatch.setattr(SitePolicies, "load", classmethod(lambda cls: policies))
    crawler = Crawler(Spider, Settings())
    crawler.stats = MemoryStatsCollector(crawler)
    slot = SimpleNamespace(latercall=None)
    crawler.engine = SimpleNamespace(downloader=SimpleNamespace(
        slots={"wikipedia": slot}, _process_queue=Mock()))
    dispatcher = SiteMiddlewareDispatcher.from_crawler(crawler)
    middleware = dispatcher.handlers[policies.resolve("en.wikipedia.org").middleware]
    middleware.clock = Clock()
    return crawler, dispatcher, middleware, slot


def received(crawler, status=429, retry_after=None, host="en.wikipedia.org"):
    request = Request(f"https://{host}/wiki/Test", meta={"download_slot": "wikipedia"})
    response = Response(request.url, status=status,
                        headers={} if retry_after is None else {"Retry-After": retry_after},
                        request=request)
    results = crawler.signals.send_catch_log(signal=signals.response_downloaded,
        request=request, response=response, spider=None)
    assert all(result is None for _, result in results)
    return request, response


@pytest.mark.parametrize("header,seconds", [
    (None, 10), ("", 10), ("invalid", 10), ("-1", 10), ("0", 10),
    ("3", 10), ("11", 11), ("120", 120),
    (format_datetime(datetime.fromtimestamp(30, timezone.utc), usegmt=True), 30),
    (format_datetime(datetime.fromtimestamp(0, timezone.utc), usegmt=True), 10),
])
def test_cooldown_honors_retry_after_and_resumes_once(cooldown, header, seconds, caplog):
    crawler, dispatcher, middleware, slot = cooldown
    # Downloader and spider registrations must share one handler/signal receiver.
    assert SiteMiddlewareDispatcher.from_crawler(crawler) is dispatcher
    with caplog.at_level("WARNING", logger="crawler"):
        received(crawler, retry_after=header)
    assert crawler.stats.get_value("wikipedia/cooldowns") == 1
    assert slot.latercall is not None
    middleware.clock.advance(seconds - 0.01)
    crawler.engine.downloader._process_queue.assert_not_called()
    middleware.clock.advance(0.02)
    crawler.engine.downloader._process_queue.assert_called_once_with(slot)
    assert slot.latercall is None
    assert middleware.cooldown_until == {}
    record = next(r for r in caplog.records if getattr(r, "event", None) == "wikipedia.rate_limited")
    assert record.cooldown_seconds == seconds


def test_more_429s_extend_shared_slot_without_shortening_longer_wait(cooldown):
    crawler, _, middleware, slot = cooldown
    received(crawler, retry_after="30")
    middleware.clock.advance(2)
    received(crawler, host="fr.wikipedia.org")
    middleware.clock.advance(27)
    crawler.engine.downloader._process_queue.assert_not_called()
    received(crawler)
    assert len(middleware.clock.getDelayedCalls()) == 1
    middleware.clock.advance(9)
    crawler.engine.downloader._process_queue.assert_not_called()
    middleware.clock.advance(1)
    crawler.engine.downloader._process_queue.assert_called_once_with(slot)


def test_cooldown_replaces_pacing_timer_and_shutdown_can_cancel_it(cooldown):
    crawler, _, middleware, slot = cooldown
    old_timer = middleware.clock.callLater(0.1, pytest.fail, "old pacing timer fired")
    slot.latercall = old_timer
    received(crawler)
    assert not old_timer.active()
    # Scrapy Slot.close() uses exactly this cancellation on shutdown.
    slot.latercall.cancel()
    middleware.clock.advance(20)
    crawler.engine.downloader._process_queue.assert_not_called()


@pytest.mark.parametrize("status,host", [(200, "en.wikipedia.org"), (503, "en.wikipedia.org"),
                                          (429, "example.org")])
def test_other_responses_do_not_pause(cooldown, status, host):
    crawler, _, middleware, slot = cooldown
    received(crawler, status=status, host=host)
    assert slot.latercall is None
    assert middleware.clock.getDelayedCalls() == []


def test_existing_scrapy_retry_remains_bounded_and_preserves_manual_job(cooldown):
    crawler, dispatcher, _, _ = cooldown
    request = Request("https://en.wikipedia.org/wiki/Test", meta={
        "manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 7})
    dispatcher.process_request(request)
    retry_crawler = Crawler(Spider, Settings({"RETRY_TIMES": 1}))
    retry_crawler.spider = Spider("test")
    retry_crawler.spider.crawler = retry_crawler
    retry_crawler.stats = MemoryStatsCollector(retry_crawler)
    retry = RetryMiddleware.from_crawler(retry_crawler)
    response = Response(request.url, status=429, request=request)
    retried = retry.process_response(request, response)
    assert isinstance(retried, Request)
    assert retried.meta["manual_bulk_crawl_job_id"] == "job"
    assert retried.meta["manual_bulk_crawl_index"] == 7
    assert retried.meta["retry_times"] == 1
    assert retried.dont_filter
    dispatcher.process_request(retried)
    assert retried.meta["download_slot"] == "wikipedia"
    exhausted = Response(retried.url, status=429, request=retried)
    assert retry.process_response(retried, exhausted) is exhausted


def test_live_cooldown_pauses_queued_wikipedia_but_not_another_domain(tmp_path):
    import json
    import os
    from pathlib import Path
    import subprocess
    import sys
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append((self.headers['Host'], self.path, time.monotonic()))
            attempts = sum(path == '/wiki/limited' for _, path, _ in hits)
            limited = self.path == '/wiki/limited' and attempts == 1
            self.send_response(429 if limited else 200)
            if limited:
                self.send_header('Retry-After', '1')
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'OK')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = Path(__file__).resolve().parents[3]
    script = '''
import json,sys
from scrapy import Spider,Request,signals
from scrapy.crawler import CrawlerProcess
from scrapy.resolver import dnscache
from scrapy.utils.reactor import install_reactor
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')
from crawler.site_policies import SitePolicies
from crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
policies=SitePolicies({'domains':{'wikipedia.org':{
    'middleware':'crawler.middlewares.wikipedia.WikipediaMiddleware',
    'request_limits':{'download_slot':'wikipedia'}}}})
SitePolicies.load=classmethod(lambda cls: policies)
for host in ['en.wikipedia.org','fr.wikipedia.org','example.org']:
    dnscache[host]='127.0.0.1'
port=sys.argv[1]
class TestSpider(Spider):
    name='cooldown_test'
    async def start(self):
        yield Request(f'http://en.wikipedia.org:{port}/wiki/limited',priority=10,
            meta={'manual_bulk_crawl_job_id':'job','manual_bulk_crawl_index':7})
        yield Request(f'http://fr.wikipedia.org:{port}/wiki/queued')
    def parse(self,response):
        if response.url.endswith('/wiki/limited'):
            assert response.meta['manual_bulk_crawl_job_id']=='job'
            assert response.meta['manual_bulk_crawl_index']==7
        self.crawler.stats.inc_value('test/success')
process=CrawlerProcess({'LOG_LEVEL':'ERROR','ROBOTSTXT_OBEY':False,
    'HTTPPROXY_ENABLED':False,'DNS_RESOLVER':'scrapy.resolver.CachingThreadedResolver',
    'DOWNLOADER_MIDDLEWARES':{'crawler.middlewares.site_dispatcher.SiteMiddlewareDispatcher':80},
    'SPIDER_MIDDLEWARES':{'crawler.middlewares.site_dispatcher.SiteMiddlewareDispatcher':500},
    'DOWNLOAD_SLOTS':{'wikipedia':{'concurrency':1,'delay':0}},
    'RETRY_TIMES':1,'CLOSESPIDER_TIMEOUT':20})
crawler=process.create_crawler(TestSpider)
def response_downloaded(response,request,**kwargs):
    if response.status==429:
        crawler.engine.crawl(Request(f'http://example.org:{port}/other'))
crawler.signals.connect(response_downloaded,signal=signals.response_downloaded)
process.crawl(crawler)
process.start()
assert crawler.stats.get_value('test/success')==3, crawler.stats.get_stats()
assert crawler.stats.get_value('retry/count')==1
assert crawler.stats.get_value('wikipedia/cooldowns')==1
'''
    try:
        result = subprocess.run([sys.executable, '-c', script, str(server.server_port)],
            cwd=root/'containers/crawler', env={**os.environ,
            'PYTHONPATH': f'{root}:{root / "containers/crawler"}'},
            capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stdout + result.stderr
        assert len(hits) == 4, hits
        first = next(t for _, path, t in hits if path == '/wiki/limited')
        wiki_after = [t for _, path, t in hits[1:] if path.startswith('/wiki/')]
        assert len(wiki_after) == 2
        assert all(t - first >= 9.9 for t in wiki_after), hits
        other = next(t for _, path, t in hits if path == '/other')
        assert 0 <= other - first < 5, hits
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

from pathlib import Path

import pytest
from scrapy import Request, Spider
from scrapy.crawler import Crawler
from scrapy.downloadermiddlewares.redirect import RedirectMiddleware
from scrapy.http import Response
from scrapy.settings import Settings

from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
from containers.crawler.crawler.middlewares.yahoo import YahooMiddleware
from containers.crawler.crawler.site_policies import SitePolicies


@pytest.fixture
def dispatcher():
    return SiteMiddlewareDispatcher(SitePolicies({"domains": {"yahoo.com": {
        "middleware": f"{YahooMiddleware.__module__}.YahooMiddleware",
    }}}))


@pytest.mark.parametrize("url", [
    "https://www.yahoo.com/news/article.html?guccounter=1&key=one&key=two#section",
    "https://www.yahoo.com/robots.txt",
    "https://finance.yahoo.com/quote/TEST?p=TEST",
])
@pytest.mark.parametrize("manual", [False, True])
def test_public_requests_preserve_urls_and_context(dispatcher, url, manual):
    def callback(response):
        pass

    meta = {"manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 3} if manual else {}
    request = Request(url, callback=callback, cb_kwargs={"value": 1}, priority=7, meta=meta,
                      headers={"Cookie": "private=session", "Sec-Fetch-Site": "none", "Accept-Encoding": "gzip"})
    original_url = request.url
    assert dispatcher.process_request(request) is None
    assert request.url == original_url
    assert request.callback is callback
    assert request.cb_kwargs == {"value": 1}
    assert request.priority == 7
    assert request.headers["User-Agent"].startswith(b"WebCrawlerBot/")
    assert request.headers.getlist("Cookie") == []
    assert request.headers.getlist("Sec-Fetch-Site") == []
    assert request.headers["Accept-Encoding"] == b"gzip"
    assert request.meta["dont_merge_cookies"] is True
    assert "dont_obey_robotstxt" not in request.meta
    assert "dont_retry" not in request.meta
    for key, value in meta.items():
        assert request.meta[key] == value


def test_publisher_redirect_restores_original_headers(dispatcher):
    request = Request("https://www.yahoo.com/news/article.html", headers={
        "User-Agent": "OriginalCrawler", "Sec-Fetch-Site": "none",
    })
    dispatcher.process_request(request)
    crawler = Crawler(Spider, Settings())
    crawler.spider = Spider("test")
    redirected = RedirectMiddleware.from_crawler(crawler).process_response(
        request, Response(request.url, status=302, headers={"Location": "https://publisher.example/article"}),
    )
    assert dispatcher.process_request(redirected) is None
    assert redirected.headers["User-Agent"] == b"OriginalCrawler"
    assert redirected.headers["Sec-Fetch-Site"] == b"none"
    assert "dont_merge_cookies" not in redirected.meta


def test_host_headers_and_timeout_remain_configurable():
    dispatcher = SiteMiddlewareDispatcher(SitePolicies({
        "domains": {"yahoo.com": {"middleware": f"{YahooMiddleware.__module__}.YahooMiddleware"}},
        "hosts": {"www.yahoo.com": {"headers": {"User-Agent": "ConfiguredBot", "Accept-Language": "en"},
                                     "download_timeout_sec": 8}},
    }))
    request = Request("https://www.yahoo.com/news/article.html")
    dispatcher.process_request(request)
    assert request.headers["User-Agent"] == b"ConfiguredBot"
    assert request.headers["Accept-Language"] == b"en"
    assert request.meta["download_timeout"] == 8


def test_bundled_limits_apply_domain_pacing_and_share_domain_budget(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(root))
    monkeypatch.chdir(root)
    policies = SitePolicies.load()
    dispatcher = SiteMiddlewareDispatcher(policies)
    request = Request("https://www.yahoo.com/news/article.html")
    dispatcher.process_request(request)
    assert request.meta["download_slot"] == "yahoo.com"
    assert request.headers["User-Agent"].startswith(b"WebCrawlerBot/")
    for host in ("www.yahoo.com", "finance.yahoo.com", "sports.yahoo.com", "weather.yahoo.com"):
        assert policies.shared_budgets(host) == (("domain:yahoo.com", 16),)
        limits = policies.resolve(host).request_limits
        assert limits.download_slot == "yahoo.com"
        assert limits.target_concurrent_requests == 8
        assert limits.max_concurrent_requests == 16
        assert limits.max_qps == 16
        request = Request(f"https://{host}/")
        dispatcher.process_request(request)
        assert request.meta["download_slot"] == "yahoo.com"
    assert policies.resolve("example.com").request_limits.target_concurrent_requests is None
    assert policies.resolve("example.com").request_limits.max_qps == 4
    assert policies.shared_budgets("example.com") == ()

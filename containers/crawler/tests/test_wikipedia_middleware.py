import pytest
from scrapy import Request
from scrapy.exceptions import IgnoreRequest
from scrapy.http.request import NO_CALLBACK

from containers.crawler.crawler.middlewares import wikipedia
from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
from containers.crawler.crawler.site_policies import SitePolicies, RequestLimits


def limits():
    return RequestLimits(download_slot="wikipedia", max_concurrent_requests=10, max_qps=18)


def policy():
    return SitePolicies({"domains": {"wikipedia.org": {
        "middleware": f"{wikipedia.__name__}.WikipediaMiddleware",
        "request_limits": limits().model_dump(),
        "headers": {"User-Agent": "WebCrawlerBot/1.0 (+https://example.com/bot)", "Sec-Fetch-Site": None},
    }}})


def test_rejects_writes_and_applies_request_identity():
    dispatcher = SiteMiddlewareDispatcher(policy())
    with pytest.raises(IgnoreRequest):
        dispatcher.process_request(Request("https://en.wikipedia.org/wiki/Title", method="POST"))
    for request in [Request("https://en.wikipedia.org/wiki/Title"),
                    Request("https://fr.wikipedia.org/wiki/Titre", method="HEAD"),
                    Request("https://en.wikipedia.org/robots.txt", callback=NO_CALLBACK)]:
        dispatcher.process_request(request)
        assert request.headers["User-Agent"].startswith(b"WebCrawlerBot/")
        assert request.headers["Accept-Encoding"] == b"gzip"
        assert request.headers.getlist("Cookie") == []
        assert request.meta["dont_merge_cookies"] is True


def test_configured_limits_do_not_override_scrapy_politeness():
    dispatcher = SiteMiddlewareDispatcher(policy())
    request = Request("https://en.wikipedia.org/wiki/Title")
    dispatcher.process_request(request)
    assert request.meta["download_slot"] == "wikipedia"
    assert "autothrottle_dont_adjust_delay" not in request.meta
    assert dispatcher.policies.resolve("en.wikipedia.org").request_limits.max_qps == 18


@pytest.mark.parametrize("path", [
    "/", "/wiki/", "/wiki/%20", "/robots.txt",
    "/wiki/Carburetor%20", "/wiki/D.%20V.%20V.%20Danayya", "/wiki/Fortune's%20Fool",
    "/wiki/Title?curid=123&utm_source=test#History", "/wiki/Title#History",
    "/wiki/Title;part?oldid=123#History", "/wiki/Caf%C3%A9%20?utm_source=test",
    "/wiki/Title?diff=prev&oldid=123&action=raw",
    "/wiki/Title?key=one&key=two&empty=&flag&encoded=%2F%23%25+value",
    "/w/index.php?curid=123", "/w/index.php?oldid=123", "/w/index.php?title=",
    "/w/index.php?title=First&title=Second", "/w/index.php?title=Main+Page&oldid=123",
    "/w/index.php?title=Caf%C3%A9", "/w/index.php?title=Category%3AScience%2FHistory",
    "/w/index.php?title=What%3F%23%25",
])
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_preserves_requested_url_and_request_context(path, method):
    dispatcher = SiteMiddlewareDispatcher(policy())
    def callback(response):
        pass
    def errback(failure):
        pass
    request = Request("https://en.wikipedia.org" + path, method=method,
                       callback=callback, errback=errback, priority=5,
                       cb_kwargs={"example": "value"},
                       meta={"manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 7})
    original_url = request.url
    assert dispatcher.process_request(request) is None
    assert request.url == original_url
    assert request.method == method
    assert request.callback is callback and request.errback is errback
    assert request.cb_kwargs == {"example": "value"}
    assert request.priority == 5
    assert request.meta["manual_bulk_crawl_job_id"] == "job"
    assert request.meta["manual_bulk_crawl_index"] == 7
    assert request.headers["User-Agent"].startswith(b"WebCrawlerBot/")
    assert request.headers["Accept-Encoding"] == b"gzip"

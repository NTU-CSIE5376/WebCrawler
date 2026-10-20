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


@pytest.mark.parametrize("url", [
    "https://en.wikipedia.org/w/index.php?curid=123",
    "https://en.wikipedia.org/w/index.php?oldid=123",
    "https://en.wikipedia.org/w/index.php?title=",
    "https://en.wikipedia.org/w/index.php?title=First&title=Second",
    "https://en.wikipedia.org/", "https://en.wikipedia.org/wiki/",
    "https://en.wikipedia.org/robots.txt",
    "https://en.wikipedia.org/wiki/%20",
])
def test_rejects_noncanonical_article_urls(url):
    with pytest.raises(IgnoreRequest, match="/wiki/Title"):
        SiteMiddlewareDispatcher(policy()).process_request(Request(url))


def test_rejects_writes_but_allows_internal_robots_and_current_articles():
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


@pytest.mark.parametrize("source,target", [
    ("/wiki/Title?oldid=123", "/wiki/Title"),
    ("/wiki/Carburetor%20", "/wiki/Carburetor"),
    ("/wiki/D.%20V.%20V.%20Danayya", "/wiki/D._V._V._Danayya"),
    ("/wiki/Fortune's%20Fool", "/wiki/Fortune%27s_Fool"),
    ("/wiki/Title?curid=123&utm_source=test#History", "/wiki/Title"),
    ("/wiki/Title#History", "/wiki/Title"),
    ("/wiki/Title;part?oldid=123", "/wiki/Title;part"),
    ("/wiki/Caf%C3%A9?utm_source=test", "/wiki/Caf%C3%A9"),
    ("/w/index.php?title=Main+Page&oldid=123", "/wiki/Main_Page"),
    ("/w/index.php?title=Caf%C3%A9", "/wiki/Caf%C3%A9"),
    ("/w/index.php?title=Category%3AScience%2FHistory", "/wiki/Category:Science/History"),
    ("/w/index.php?title=What%3F%23%25", "/wiki/What%3F%23%25"),
])
@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_normalizes_to_current_article_and_logs_once(source, target, method, caplog):
    dispatcher = SiteMiddlewareDispatcher(policy())
    def callback(response):
        pass
    def errback(failure):
        pass
    original = Request("https://en.wikipedia.org" + source, method=method,
                       callback=callback, errback=errback, priority=5,
                       cb_kwargs={"example": "value"},
                       meta={"manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 7})
    with caplog.at_level("INFO", logger="crawler"):
        normalized = dispatcher.process_request(original)
        assert isinstance(normalized, Request)
        assert normalized.url == "https://en.wikipedia.org" + target
        assert normalized.method == method
        assert normalized.callback is callback and normalized.errback is errback
        assert normalized.cb_kwargs == original.cb_kwargs
        assert normalized.priority == original.priority
        assert normalized.meta["manual_bulk_crawl_job_id"] == "job"
        assert normalized.meta["manual_bulk_crawl_index"] == 7
        assert dispatcher.process_request(normalized) is None
    records = [record for record in caplog.records if getattr(record, "event", None) == "wikipedia.url_normalized"]
    assert len(records) == 1
    assert records[0].original_url == original.url
    assert records[0].normalized_url == normalized.url
    assert normalized.headers["User-Agent"].startswith(b"WebCrawlerBot/")
    assert normalized.headers["Accept-Encoding"] == b"gzip"


def test_canonical_request_does_not_log_normalization(caplog):
    with caplog.at_level("INFO", logger="crawler"):
        SiteMiddlewareDispatcher(policy()).process_request(Request("https://en.wikipedia.org/wiki/Title"))
    assert not any(getattr(record, "event", None) == "wikipedia.url_normalized" for record in caplog.records)

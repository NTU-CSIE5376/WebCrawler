import asyncio

import pytest
from scrapy import Request
from scrapy.http import HtmlResponse

from containers.crawler.crawler.items import PageItem
from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
from containers.crawler.crawler.middlewares.youtube import YouTubeMiddleware
from containers.crawler.crawler.site_policies import SitePolicies


@pytest.fixture
def middleware():
    return SiteMiddlewareDispatcher(SitePolicies({"domains": {"youtube.com": {
        "middleware": f"{YouTubeMiddleware.__module__}.YouTubeMiddleware",
    }}, "hosts": {"disabled.youtube.com": {"middleware": None}}}))


def run_output(middleware, response, items, *, asynchronous=False):
    async def source():
        for item in items:
            yield item

    async def collect():
        result = source() if asynchronous else iter(items)
        return [item async for item in middleware.process_spider_output(response, result)]

    return asyncio.run(collect())


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("body", [
    '<script>var ytInitialData = {"items":[{"videoId":"abcdefghijk"},{"videoId":"lmnopqrstuv"},{"videoId":"abcdefghijk"}]};</script>',
    '<script>ytInitialPlayerResponse = {"videoDetails":{"videoId":"lmnopqrstuv"}};</script>',
    '<a href="/watch?v=abcdefghijk">existing</a><a href="/shorts/lmnopqrstuv">short</a>',
])
def test_enriches_items_preserving_existing_links_and_requests(middleware, body, asynchronous):
    response = HtmlResponse("https://www.youtube.com/watch?v=abcdefghijk", body=body, encoding="utf-8")
    existing = {"url": "https://www.youtube.com/watch?v=abcdefghijk", "domain": "youtube.com", "anchor": "original"}
    page = PageItem(content=body, outlinks=[existing])
    request = Request("https://example.com/")
    items = run_output(middleware, response, [page, request], asynchronous=asynchronous)
    assert items == [page, request]
    assert page["outlinks"] == [existing, {
        "url": "https://www.youtube.com/watch?v=lmnopqrstuv", "domain": "youtube.com", "anchor": "",
    }]


@pytest.mark.parametrize("hostname", ["example.com", "youtube.com.example.com", "notyoutube.com", "disabled.youtube.com"])
def test_unmatched_and_disabled_hosts_do_not_extract(middleware, hostname):
    response = HtmlResponse(f"https://{hostname}/", body=b'/watch?v=abcdefghijk', encoding="utf-8")
    page = PageItem(content=response.text, outlinks=[])
    assert run_output(middleware, response, [page]) == [page]
    assert page["outlinks"] == []


def test_manual_results_and_non_html_items_are_unchanged(middleware):
    response = HtmlResponse("https://youtube.com/", body=b'/watch?v=abcdefghijk', encoding="utf-8")
    page = PageItem(content=None, outlinks=[], fail_reason="NonHTML content-type")
    # Manual crawl callbacks do not emit PageItems.
    assert run_output(middleware, response, []) == []
    assert run_output(middleware, response, [page]) == [page]
    assert page["outlinks"] == []


def test_request_hook_is_optional(middleware):
    request = Request("https://youtube.com/")
    assert middleware.process_request(request) is None
    assert request.headers == {}

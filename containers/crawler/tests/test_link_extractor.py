"""Keep stock Scrapy link semantics while optimizing the normal crawl path."""
from html import escape
import asyncio
import inspect
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest
from scrapy import Request
from scrapy.http import HtmlResponse
from scrapy.linkextractors import IGNORED_EXTENSIONS, LinkExtractor
from scrapy.utils.test import get_crawler
import tldextract

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crawler.spiders.parser import CanonicalLinkExtractor, HtmlParser  # noqa: E402
from crawler.spiders.spider import HtmlSpider  # noqa: E402


def page(body, url="https://example.com/root/index.html"):
    return HtmlResponse(url, body=body.encode(), encoding="utf-8",
                        headers={"Content-Type": "text/html"}, request=Request(url))


URLS = [
    "/page?b=2&a=1#one", "/page?a=1&b=2#two", "/blank?b=&a=1",
    "/file.PDF?download=1", "/file.pdf#anchor", "/file.pdf/child",
    "/page?name=file.pdf", "/file.p%64f", "/file%2Epdf", "/file.tar.gz",
    "/caf%C3%A9?q=%252F&b=2&a=1", "/café?q=hello world", "/file.custom",
    "https://other.example/page", "ftp://example.com/page", "file:///tmp/page",
    "mailto:test@example.com", "javascript:void(0)", "data:text/plain,hello",
    *[f"/download.{ext.upper()}?query=1" for ext in IGNORED_EXTENSIONS],
]
HTML = ('<html><head><base href="https://example.com/base/"></head><body>'
        '<div class="links">'
        + "".join(f'<a href="{escape(url, quote=True)}">link {n}</a>'
                  for n, url in enumerate(URLS))
        + '<a href="/nofollow" rel="nofollow">nofollow</a>'
        '<area href="/area" alt="area"><a href="/page?b=2&amp;a=1">duplicate</a>'
        '</div><a href="/outside">outside</a></body></html>')


@pytest.mark.parametrize("options", [
    {}, {"deny_extensions": []}, {"deny_extensions": ["custom", "tar.gz"]},
    {"deny_extensions": ["PDF"]}, {"allow": r"/page"}, {"deny": r"/page"},
    {"allow_domains": ["example.com"]}, {"deny_domains": ["other.example"]},
    {"restrict_text": r"link [0-9]$"}, {"restrict_css": ".links"},
    {"restrict_xpaths": '//div[@class="links"]'}, {"unique": False},
])
@pytest.mark.parametrize("url", [
    "https://example.com/root/index.html", "ftp://example.com/page", "file:///tmp/page",
])
def test_links_match_stock_scrapy_including_order_and_metadata(options, url):
    expected = LinkExtractor(canonicalize=True, **options).extract_links(page(HTML, url))
    actual = CanonicalLinkExtractor(**options).extract_links(page(HTML, url))
    assert actual == expected


@pytest.mark.skipif("schemes" not in inspect.signature(LinkExtractor).parameters,
                    reason="Configurable schemes require Scrapy 2.20")
@pytest.mark.parametrize("schemes", [[], ["https"], ["ftp", "file"], ["mailto"]])
@pytest.mark.parametrize("filters", [{}, {"allow": r"example.com"}])
def test_configured_schemes_match_stock_scrapy(schemes, filters):
    options = {"schemes": schemes, **filters}
    expected = LinkExtractor(canonicalize=True, **options).extract_links(page(HTML))
    actual = CanonicalLinkExtractor(**options).extract_links(page(HTML))
    assert actual == expected


def test_reusing_html_parser_keeps_page_results_independent(monkeypatch):
    monkeypatch.setattr(tldextract, "extract", tldextract.TLDExtract(
        suffix_list_urls=(), cache_dir=None))
    parser = HtmlParser()
    first = parser.parse(page('<title>First</title><a href="/shared">first</a>'))
    second = parser.parse(page('<title>Second</title><a href="/shared">second</a>'))

    assert first["title"] == "First"
    assert second["title"] == "Second"
    assert first["outlinks"] == [
        {"url": "https://example.com/shared", "domain": "example.com", "anchor": "first"},
    ]
    assert second["outlinks"] == [
        {"url": "https://example.com/shared", "domain": "example.com", "anchor": "second"},
    ]


def test_spider_preserves_canonical_urls_youtube_dedup_and_metadata(tmp_path, monkeypatch):
    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(tldextract, "extract", tldextract.TLDExtract(
        suffix_list_urls=(), cache_dir=None))
    crawler = get_crawler(HtmlSpider, {
        "PARSER_PROCESSES": 0,
        "URL_QUEUE_TEMPLATE": str(tmp_path / "queue_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(tmp_path / "manual"),
    })
    spider = HtmlSpider.from_crawler(crawler, crawler_id=0)
    monkeypatch.setattr(spider, "_finish_owned_request", Mock())
    response = page('''<html><head><title> A title </title>
        <link rel="alternate" hreflang="en" href="/en">
        <script type="application/ld+json">{}</script></head><body>
        <a href="/page?b=2&amp;a=1#one"> page </a>
        <a href="/page?a=1&amp;b=2#two">duplicate</a>
        <a href="/image.JPG?x=1">image</a>
        <a href="/nofollow" rel="nofollow">ignored</a>
        <a href="https://youtu.be/abcdefghijk?t=8">video</a>
        <a href="https://www.youtube.com/watch?v=abcdefghijk&amp;t=5">same video</a>
        </body></html>''', url="https://example.com/start?z=2&a=1")

    item, = asyncio.run(spider.parse(response))

    assert item["url"] == "https://example.com/start?a=1&z=2"
    assert item["outlinks"] == [
        {"url": "https://example.com/page?a=1&b=2", "domain": "example.com", "anchor": "page"},
        {"url": "https://www.youtube.com/watch?v=abcdefghijk", "domain": "youtube.com", "anchor": "video"},
    ]
    assert item["title"] == "A title"
    assert item["hreflang_count"] == 1
    assert item["has_json_ld"] is True
    assert item["content"] == response.text

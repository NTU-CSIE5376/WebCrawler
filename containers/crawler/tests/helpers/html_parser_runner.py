"""Exercise actual spawned workers with Scrapy's default asyncio reactor."""
import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sys
from unittest.mock import Mock


def offline_suffix_list(parent_pid):
    from crawler.spiders.parser import _init_parser
    _init_parser(parent_pid)
    assert "crawler.spiders.spider" not in sys.modules
    import tldextract
    tldextract.extract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


async def run(root):
    from scrapy import Request
    from scrapy.http import HtmlResponse, XmlResponse
    from scrapy.utils.defer import maybe_deferred_to_future
    from scrapy.utils.test import get_crawler
    import crawler.spiders.spider as module
    import crawler.spiders.parser as parser_module

    import tldextract
    tldextract.extract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
    # The test must not fetch the public suffix list in either parent or child.
    def create_pool(**kwargs):
        kwargs["initializer"] = offline_suffix_list
        return ProcessPoolExecutor(**kwargs)

    parser_module.ProcessPoolExecutor = create_pool
    crawler = get_crawler(module.HtmlSpider, {
        "URL_QUEUE_TEMPLATE": str(root / "queue_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "manual"),
        "PARSER_PROCESSES": 2,
        "OBSLOG_HEARTBEAT_SEC": 0,
        "IPC_TOP_UP_INTERVAL_SEC": 0,
    })
    spider = module.HtmlSpider.from_crawler(crawler)
    crawler.engine = Mock()
    crawler.engine.downloader.slots = {}
    spider._finish_owned_request = Mock()
    spider.manual_bulk_crawls = Mock()
    responses = []
    for index in range(12):
        url = f"https://example.com/page?b={index}&a=1"
        html = (f'<html><head><title>Café {index}</title>'
                '<base href="https://other.example/base/">'
                '<link rel="alternate" hreflang="en" href="/en">'
                '<script type="application/ld+json">{}</script></head><body>'
                '<a href="https://youtu.be/abcdefghijk?t=8">video</a>'
                '<a href="https://www.youtube.com/watch?v=abcdefghijk&amp;t=5">duplicate</a>'
                '<a href="/nofollow" rel="nofollow">skip</a><a href="/file.PDF">skip</a>'
                + ''.join(f'<a href="child?b={n}&amp;a=1#frag">café {n}</a>' for n in range(120))
                + '</body></html>')
        encoding = "cp1252" if index % 2 else "utf-8"
        response_class = HtmlResponse
        content_type = "text/html"
        if index == 11:
            response_class = XmlResponse
            content_type = "application/xhtml+xml"
            html = ('<html xmlns="http://www.w3.org/1999/xhtml"><head><title>XHTML</title>'
                    '</head><body><a href="https://example.com/child">child</a></body></html>')
        responses.append(response_class(
            url, body=html.encode(encoding), encoding=encoding,
            headers={"Content-Type": f"{content_type}; charset={encoding}",
                     "Last-Modified": "Wed, 21 Oct 2015 07:28:00 GMT", "ETag": '"test"'},
            request=Request(url, meta={"redirect_times": index % 3, "_track_domain_id": 7}),
        ))
    # Inline rollback path uses the same public callback as the pool path.
    inline_crawler = get_crawler(module.HtmlSpider, {
        "URL_QUEUE_TEMPLATE": str(root / "queue_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "manual"),
        "PARSER_PROCESSES": 0,
    })
    inline_spider = module.HtmlSpider.from_crawler(inline_crawler)
    inline_spider._finish_owned_request = Mock()
    expected = [list(map(dict, await inline_spider.parse(response))) for response in responses]
    await maybe_deferred_to_future(inline_spider.html_parser_pool.close())
    spider._finish_owned_request.reset_mock()
    spider.spider_opened()
    pool = spider.html_parser_pool
    ticks = 0
    running = True

    async def heartbeat():
        nonlocal ticks
        while running:
            ticks += 1
            await asyncio.sleep(.01)

    ticker = asyncio.create_task(heartbeat())
    try:
        actual = await asyncio.gather(*(spider.parse(response) for response in responses))
        actual = [list(map(dict, items)) for items in actual]
        assert spider._finish_owned_request.call_count == len(responses)
        assert pool.inflight == 0
        processes = len(pool._executor._processes)

        # Terminal interrupts go to the whole group. The crawler handles them
        # while children finish and wait for the pool's shutdown sentinel.
        for process in pool._executor._processes.values():
            os.kill(process.pid, signal.SIGINT)
        assert list(map(dict, await spider.parse(responses[0]))) == expected[0]

        # Non-HTML responses must bypass the pool.
        before = pool._executor.submit
        pool._executor.submit = Mock(side_effect=AssertionError("non-HTML reached parser"))
        response = HtmlResponse("https://example.com/file", body=b"binary",
                                headers={"Content-Type": "image/png"},
                                request=Request("https://example.com/file"))
        item, = await spider.parse(response)
        assert item["fail_reason"] == "NonHTML content-type"
        pool._executor.submit.assert_not_called()
        pool._executor.submit = before

        # A submission failure must release domain ownership exactly once.
        spider._finish_owned_request = Mock()
        spider._pending_requests = 1
        spider._domain_pending = {7: 1}
        original_finish = module.HtmlSpider._finish_owned_request.__get__(spider)
        spider._finish_owned_request.side_effect = original_finish
        spider._set_inflight_stats = Mock()
        spider._maybe_top_up = Mock()
        pool._executor.submit = Mock(side_effect=RuntimeError("test pool failure"))
        try:
            await spider.parse(responses[0])
        except RuntimeError as exc:
            assert str(exc) == "test pool failure"
        else:
            raise AssertionError("Submission failure was swallowed")
        assert spider._finish_owned_request.call_count == 1
        assert not spider._domain_pending
        assert pool.inflight == 0
    finally:
        running = False
        await ticker
        await maybe_deferred_to_future(spider.spider_closed())

    (root / "report.json").write_text(json.dumps({
        "same_output": actual == expected,
        "pages": len(actual), "processes": processes, "reactor_ticks": ticks,
        "children_after_close": len(multiprocessing.active_children()),
        "pending_after_failure": spider._pending_requests,
    }))


if __name__ == "__main__":
    from scrapy.utils.reactor import install_reactor
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    from twisted.internet import task
    from twisted.internet.defer import Deferred
    task.react(lambda reactor: Deferred.fromFuture(asyncio.ensure_future(run(Path(sys.argv[1])))))

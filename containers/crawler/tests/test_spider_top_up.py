"""Queue intake must not depend on owned requests finishing or the engine idling."""
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import scrapy
from scrapy.utils.test import get_crawler

# The spider imports `crawler.*` relative to the scrapy project root, as in the container.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from crawler.spiders.spider import HtmlSpider  # noqa: E402


@pytest.fixture
def spider(tmp_path):
    queue_dir = tmp_path / "url_queue" / "crawler_00"
    queue_dir.mkdir(parents=True)
    crawler = get_crawler(HtmlSpider, {
        "URL_QUEUE_TEMPLATE": str(tmp_path / "url_queue" / "crawler_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(tmp_path / "manual"),
    })
    spider = HtmlSpider.from_crawler(crawler, crawler_id=0)
    crawler.engine = Mock()
    crawler.engine.downloader.slots = {}
    spider.crawler = crawler

    def enqueue(domain_id, urls):
        (queue_dir / f"domain_{domain_id}.json").write_text(json.dumps({"urls": urls}))

    return spider, enqueue


def scheduled(spider):
    return [c.args[0] for c in spider.crawler.engine.crawl.call_args_list]


def test_periodic_top_up_reads_queue_while_engine_is_busy_with_other_work(spider):
    # Owned work drained while the queue was empty; manual crawls keep the
    # engine busy, so neither a completion nor spider_idle will ever fire.
    spider, enqueue = spider
    enqueue(7, ["https://a.example/1", "https://a.example/2"])

    spider._periodic_top_up()

    assert [r.url for r in scheduled(spider)] == ["https://a.example/1", "https://a.example/2"]
    assert spider._domain_pending == {7: 2}


def test_periodic_top_up_respects_low_watermark(spider):
    spider, enqueue = spider
    spider.domain_low_watermark = 1
    spider._domain_pending = {1: 5}
    enqueue(7, ["https://a.example/1"])

    spider._periodic_top_up()

    assert scheduled(spider) == []


def test_dropped_owned_request_releases_its_domain(spider):
    spider, enqueue = spider
    enqueue(7, ["https://a.example/1"])
    spider._periodic_top_up()
    (request,) = scheduled(spider)

    spider.req_dropped(request)

    assert spider._domain_pending == {}
    assert spider._pending_requests == 0


def test_dropped_request_not_owned_by_queue_is_ignored(spider):
    spider, _ = spider
    spider._domain_pending = {7: 1}
    spider._pending_requests = 1

    spider.req_dropped(scrapy.Request("https://manual.example/", meta={"manual_bulk_crawl_job_id": "j"}))

    assert spider._domain_pending == {7: 1}
    assert spider._pending_requests == 1

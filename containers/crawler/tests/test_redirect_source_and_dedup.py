"""Crawl results keep the queued URL (source_url) next to the final URL, and
a redirect to a target fetched recently is not fetched again."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from scrapy.http import HtmlResponse, Request
from twisted.python.failure import Failure

# The spider imports its siblings as the top-level `crawler` package, as it
# does inside the container (cwd = containers/crawler).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from containers.crawler.crawler.pipelines import JsonPipeline  # noqa: E402
from containers.crawler.crawler.redirect_dedup import (  # noqa: E402
    DuplicateRedirectTarget,
    RecentFetches,
    RedirectTargetDedupMiddleware,
)
from crawler.spiders.spider import HtmlSpider  # noqa: E402
from crawler import redirect_dedup as spider_redirect_dedup  # noqa: E402


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ---------------------------------------------------------------- RecentFetches

def test_recent_fetches_ttl():
    clock = _Clock()
    r = RecentFetches(ttl_sec=60, max_entries=10, clock=clock)
    r.add("https://a/x")
    assert r.seen("https://a/x")
    clock.t = 61
    assert not r.seen("https://a/x")
    assert len(r) == 0


def test_recent_fetches_evicts_oldest():
    r = RecentFetches(ttl_sec=60, max_entries=2, clock=_Clock())
    r.add("https://a/1")
    r.add("https://a/2")
    r.add("https://a/1")  # refresh: 2 is now the oldest
    r.add("https://a/3")
    assert r.seen("https://a/1")
    assert not r.seen("https://a/2")
    assert r.seen("https://a/3")


# ---------------------------------------------------------------- middleware

def _spider_with(recent):
    return SimpleNamespace(recent_fetches=recent)


def test_middleware_drops_redirect_to_recent_target():
    recent = RecentFetches(ttl_sec=60, max_entries=10, clock=_Clock())
    recent.add("https://pacificleague.com/maintenance/")
    mw = RedirectTargetDedupMiddleware()
    req = Request(
        "https://pacificleague.com/maintenance/",
        meta={"redirect_urls": ["https://pacificleague.com/news/1"]},
    )
    with pytest.raises(DuplicateRedirectTarget):
        mw.process_request(req, _spider_with(recent))


def test_middleware_never_drops_first_hop():
    recent = RecentFetches(ttl_sec=60, max_entries=10, clock=_Clock())
    recent.add("https://a.com/x")
    mw = RedirectTargetDedupMiddleware()
    assert mw.process_request(Request("https://a.com/x"), _spider_with(recent)) is None


def test_middleware_passes_unseen_target_and_manual_jobs():
    recent = RecentFetches(ttl_sec=60, max_entries=10, clock=_Clock())
    recent.add("https://a.com/x")
    mw = RedirectTargetDedupMiddleware()
    unseen = Request("https://a.com/y", meta={"redirect_urls": ["https://a.com/z"]})
    manual = Request(
        "https://a.com/x",
        meta={"redirect_urls": ["https://a.com/z"], "manual_bulk_crawl_job_id": "j"},
    )
    assert mw.process_request(unseen, _spider_with(recent)) is None
    assert mw.process_request(manual, _spider_with(recent)) is None


def test_middleware_disabled_without_cache():
    mw = RedirectTargetDedupMiddleware()
    req = Request("https://a.com/x", meta={"redirect_urls": ["https://a.com/z"]})
    assert mw.process_request(req, _spider_with(None)) is None


# ---------------------------------------------------------------- spider

def _spider(recent=None):
    spider = HtmlSpider.__new__(HtmlSpider)
    spider.recent_fetches = recent
    spider.finished = []
    spider._finish_owned_request = lambda reason, domain_id=0: spider.finished.append(reason)
    from scrapy.linkextractors import LinkExtractor
    spider.link_extractor = LinkExtractor(canonicalize=True)
    return spider


def _response(url, source_url, redirect_times=1):
    req = Request(url, meta={
        "source_url": source_url,
        "_track_domain_id": 7,
        "redirect_times": redirect_times,
    })
    return HtmlResponse(
        url=url,
        request=req,
        body=b"<html><head><title>t</title></head><body></body></html>",
        headers={"Content-Type": "text/html"},
        encoding="utf-8",
    )


def test_parse_reports_source_url_and_remembers_target():
    recent = RecentFetches(ttl_sec=60, max_entries=10, clock=_Clock())
    spider = _spider(recent)
    resp = _response(
        "https://www.weee.com/vi/explore/gao-vang",
        "https://www.sayweee.com/vi/explore/gao-vang",
    )
    (item,) = list(spider.parse(resp))
    assert item["url"] == "https://www.weee.com/vi/explore/gao-vang"
    assert item["source_url"] == "https://www.sayweee.com/vi/explore/gao-vang"
    assert item["is_redirect"] is True
    assert recent.seen("https://www.weee.com/vi/explore/gao-vang")


def test_errback_reports_deduped_redirect_as_resolved():
    spider = _spider(RecentFetches(ttl_sec=60, max_entries=10, clock=_Clock()))
    req = Request(
        "https://pacificleague.com/maintenance/",
        meta={
            "source_url": "https://pacificleague.com/news/1",
            "_track_domain_id": 7,
            "redirect_times": 1,
            "redirect_urls": ["https://pacificleague.com/news/1"],
        },
    )
    exc = spider_redirect_dedup.DuplicateRedirectTarget("https://pacificleague.com/maintenance/")
    failure = Failure(exc)
    failure.request = req
    (item,) = list(spider.errback(failure))
    assert item["source_url"] == "https://pacificleague.com/news/1"
    assert item["redirect_deduped"] is True
    assert item["fail_reason"] is None
    assert item["content"] is None
    assert spider.finished == ["redirect_dedup"]


# ---------------------------------------------------------------- pipeline

def _write(tmp_path, item):
    p = JsonPipeline(result_dir_template=str(tmp_path / "{id}"), interval_minutes=10)
    p.base_dir = tmp_path
    p.process_item(item, spider=None)
    (path,) = list(tmp_path.rglob("*.jsonl"))
    return json.loads(path.read_text().splitlines()[0])


def test_pipeline_writes_source_url_when_it_differs(tmp_path):
    rec = _write(tmp_path, {
        "url": "https://www.weee.com/vi/explore/gao-vang",
        "source_url": "https://www.sayweee.com/vi/explore/gao-vang",
        "content": "<html/>",
    })
    assert rec["source_url"] == "https://www.sayweee.com/vi/explore/gao-vang"
    assert rec["status"] == "ok"
    assert "redirect_deduped" not in rec


def test_pipeline_omits_equal_source_url(tmp_path):
    rec = _write(tmp_path, {"url": "https://a.com/x", "source_url": "https://a.com/x", "content": "c"})
    assert "source_url" not in rec


def test_pipeline_deduped_redirect_is_ok_without_content(tmp_path):
    rec = _write(tmp_path, {
        "url": "https://pacificleague.com/maintenance/",
        "source_url": "https://pacificleague.com/news/1",
        "content": None,
        "redirect_deduped": True,
    })
    assert rec["status"] == "ok"
    assert rec["redirect_deduped"] is True
    assert rec["content"] is None

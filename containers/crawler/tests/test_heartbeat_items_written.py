"""spider.heartbeat reports how many results JsonPipeline wrote since the
previous heartbeat, so Grafana can chart crawler output cheaply."""
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

# spider.py imports `crawler.*` the way the container runs it (cwd containers/crawler).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crawler.pipelines import JsonPipeline  # noqa: E402
from crawler.spiders.spider import HtmlSpider  # noqa: E402


def _pipeline(tmp_path, spider):
    pipe = JsonPipeline(str(tmp_path / "crawler_{id:02d}"), interval_minutes=10)
    pipe.open_spider(spider)
    return pipe


def test_pipeline_counts_written_and_ok_items(tmp_path):
    spider = SimpleNamespace(crawler_id=3)
    pipe = _pipeline(tmp_path, spider)
    pipe.process_item({"url": "https://a.example/", "content": "<html/>"}, spider)
    pipe.process_item({"url": "https://a.example/x", "fail_reason": "HttpError 404"}, spider)
    assert (spider.items_written, spider.items_ok) == (2, 1)
    lines = [json.loads(l) for f in tmp_path.rglob("*.jsonl") for l in f.read_text().splitlines()]
    assert len(lines) == 2


def test_heartbeat_reports_and_resets_counts(tmp_path, caplog, monkeypatch):
    spider = HtmlSpider.__new__(HtmlSpider)
    spider.crawler_id = 3
    spider._domain_pending = {}
    spider._pending_requests = spider._max_pending_requests = 0
    spider._max_slot_active = spider._max_transferring = spider._max_slot_queue = 0
    spider.items_written = spider.items_ok = 0
    spider.crawler = SimpleNamespace(
        engine=SimpleNamespace(downloader=SimpleNamespace(active=())),
        stats=SimpleNamespace(get_value=lambda key, default=None: default),
    )
    spider.manual_bulk_crawls = SimpleNamespace(inflight={}, saves=0, save_seconds=0.0)
    monkeypatch.setattr(spider, "_downloader_runtime",
                        lambda: {"slot_active": 0, "transferring": 0, "slot_queue": 0, "slots": 0})
    monkeypatch.setattr(spider, "_scraper_runtime",
                        lambda: {"active_size_ratio": 0.0, "itemproc_size": 0})
    monkeypatch.setattr(spider, "_scheduler_runtime",
                        lambda: {"engine_slot_inprogress": 0, "scheduler_size": 0})
    monkeypatch.setattr(spider, "_count_domains_at_limit", lambda: 0)

    pipe = _pipeline(tmp_path, spider)
    for i in range(3):
        pipe.process_item({"url": f"https://a.example/{i}", "content": "x"}, spider)

    with caplog.at_level(logging.INFO, logger="crawler"):
        spider._emit_heartbeat()
        spider._emit_heartbeat()
    beats = [r for r in caplog.records if getattr(r, "event", None) == "spider.heartbeat"]
    assert [(b.items_written, b.items_ok) for b in beats] == [(3, 3), (0, 0)]

"""Keep download capacity busy across jobs while preserving per-job ownership."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from scrapy import Spider
from scrapy.core.downloader import Downloader
from scrapy.http import HtmlResponse
from scrapy.settings import Settings
from scrapy.utils.test import get_crawler
from twisted.internet.task import Clock

from containers.crawler.crawler.manual_bulk_crawls import ManualBulkCrawlConsumer
from libs.ipc import manual_bulk_crawl as storage
from libs.ipc.manual_bulk_crawl import next_batch, prepare_job, job_status


@pytest.fixture
def queue(tmp_path):
    pending = tmp_path / "pending"
    pending.mkdir()

    def enqueue(job_id, count=0, *, urls=None):
        urls = urls if urls is not None else [f"https://example.com/{job_id}/{i}" for i in range(count)]
        path = pending / f"{job_id}.json"
        path.write_text(json.dumps({"version": 1,
                                   "urls": urls,
                                   "crawler_ids": [0] * len(urls),
                                   "result_dir": str(tmp_path / "results")}))
        prepare_job(path, json.loads(path.read_text()))
        return path

    def consumer(concurrency=2, *, domain_concurrency=None, slot_settings=None, **limits):
        requests = []
        spider = SimpleNamespace(crawler_id=0, logger=Mock(), crawler=SimpleNamespace(
            settings=Settings({"CONCURRENT_REQUESTS": concurrency,
                               "CONCURRENT_REQUESTS_PER_DOMAIN": domain_concurrency or concurrency,
                               "DOWNLOAD_SLOTS": slot_settings or {},
                               "MANUAL_BULK_CRAWL_QUEUE_DIR": str(pending),
                               **{f"MANUAL_BULK_CRAWL_{name.upper()}": value for name, value in limits.items()}}),
            engine=SimpleNamespace(crawl=requests.append),
        ))
        return ManualBulkCrawlConsumer(spider), requests

    return enqueue, consumer, tmp_path


def complete(consumer, request, body=b"<html>saved</html>"):
    consumer.parse(HtmlResponse(request.url, request=request, body=body,
                                headers={"Content-Type": "text/html"}))


@pytest.mark.parametrize("concurrency", [256, 10, 1])
def test_manual_alone_can_fill_all_download_capacity(queue, concurrency):
    enqueue, factory, _ = queue
    enqueue("a", 1)
    enqueue("b", 300)
    consumer, requests = factory(concurrency=concurrency)
    try:
        consumer.poll()
        assert len(requests) == concurrency
        assert len(consumer.inflight) == concurrency
        complete(consumer, requests[0])
        assert len(requests) == concurrency + 1
        assert len(consumer.inflight) == concurrency
    finally:
        consumer.close()


def test_next_job_finishes_while_earlier_job_still_has_a_slow_url(queue):
    enqueue, factory, root = queue
    first = enqueue("a", 2)
    second = enqueue("b", 1)
    consumer, requests = factory(concurrency=2)
    try:
        consumer.poll()
        slow, fast = requests
        assert len(requests) == 2
        complete(consumer, fast)
        # URL a/0 is still outstanding. Its completion must not gate job b.
        assert requests[-1].meta["manual_bulk_crawl_job_id"] == "b"
        assert len(consumer.inflight) == 2
        complete(consumer, requests[-1], body=b"job b")
        assert not (second.parent / "crawler_00" / second.name).exists()
        assert first.exists()
        assert (root / "results/b/crawler_00/00000.html").read_bytes() == b"job b"
        assert json.loads((root / "results/a/crawler_00/progress.json").read_text())["completed_at"] is None
        complete(consumer, slow, body=b"job a")
        assert not (first.parent / "crawler_00" / first.name).exists()
        assert (root / "results/a/crawler_00/00000.html").read_bytes() == b"job a"
        assert not consumer.inflight
    finally:
        consumer.close()


def test_large_job_keeps_a_bounded_backlog_and_refills_on_completion(queue):
    enqueue, factory, root = queue
    enqueue("a", 6)
    consumer, requests = factory(concurrency=2)
    try:
        consumer.poll()
        assert len(requests) == 2
        # Leave the first URL slow while the rest repeatedly refill its sibling slot.
        for position in range(1, 6):
            complete(consumer, requests[position])
            assert len(consumer.inflight) <= 2
        assert len(requests) == 6
        assert json.loads((root / "results/a/crawler_00/progress.json").read_text())["completed_at"] is None
        complete(consumer, requests[0])
        status = job_status("a", json.loads((root / "pending/a.json").read_text()), root / "results/a")
        assert status["completed_urls"] == 6
        assert len({(request.meta["manual_bulk_crawl_job_id"], request.meta["manual_bulk_crawl_index"])
                    for request in requests}) == 6
    finally:
        consumer.close()


def test_active_job_limit_releases_capacity_without_waiting_for_other_jobs(queue):
    enqueue, factory, _ = queue
    for name in ("a", "b", "c"):
        enqueue(name, 1)
    consumer, requests = factory(concurrency=10, max_active_jobs=2)
    try:
        consumer.poll()
        assert len(requests) == 2
        assert len(consumer.jobs) == 2
        complete(consumer, requests[1])
        assert requests[-1].meta["manual_bulk_crawl_job_id"] == "c"
        assert len(consumer.jobs) == 2
        assert "a" in consumer.jobs
    finally:
        consumer.close()


def test_storage_failure_retains_ownership_until_that_jobs_callbacks_drain(queue, monkeypatch):
    enqueue, factory, root = queue
    path = enqueue("a", 4)
    consumer, requests = factory(concurrency=2)
    try:
        consumer.poll()
        monkeypatch.setattr(consumer.jobs["a"].batch, "save", Mock(side_effect=OSError("disk full")))
        complete(consumer, requests[0])
        assert "a" in consumer.jobs
        assert len(requests) == 2, "Unscheduled URLs must wait for the failed attempt to drain"
        complete(consumer, requests[1])
        assert not consumer.jobs
        retry = next_batch(path.parent)
        try:
            assert retry.remaining == {0, 1, 2, 3}
        finally:
            retry.close()
    finally:
        consumer.close()


def test_failed_job_does_not_block_results_from_another_job(queue, monkeypatch):
    enqueue, factory, root = queue
    enqueue("a", 2)
    enqueue("b", 1)
    consumer, requests = factory(concurrency=3)
    try:
        consumer.poll()
        monkeypatch.setattr(consumer.jobs["a"].batch, "save", Mock(side_effect=OSError("disk full")))
        complete(consumer, requests[0])
        complete(consumer, requests[2], body=b"unrelated job")
        assert (root / "results/b/crawler_00/00000.html").read_bytes() == b"unrelated job"
        assert "a" in consumer.jobs
        assert len(consumer.inflight) == 1
    finally:
        consumer.close()


def test_publication_retry_resumes_saved_results_without_refetching(queue, monkeypatch):
    enqueue, factory, root = queue
    path = enqueue("a", 1)
    consumer, requests = factory(concurrency=2)
    original = storage.os.replace

    def fail_publication(source, target):
        if target == root / "results/a/crawler_00/progress.json":
            raise OSError("publication interrupted")
        return original(source, target)

    try:
        consumer.poll()
        with monkeypatch.context() as patch:
            patch.setattr(storage.os, "replace", fail_publication)
            complete(consumer, requests[0])
        assert path.exists()
        consumer.retry_after.clear()
        consumer.poll()
        assert not (path.parent / "crawler_00" / path.name).exists()
        assert len(requests) == 1
        assert (root / "results/a/crawler_00/00000.html").read_bytes() == b"<html>saved</html>"
    finally:
        consumer.close()


def test_shutdown_releases_every_job_and_ignores_stale_callbacks(queue):
    enqueue, factory, root = queue
    first = enqueue("a", 1)
    enqueue("b", 1)
    consumer, requests = factory(concurrency=2)
    consumer.poll()
    consumer.close()
    complete(consumer, requests[0])
    consumer.poll()
    assert len(requests) == 2
    assert not consumer.jobs
    retry_a = next_batch(first.parent)
    retry_b = next_batch(first.parent, exclude_job_ids={retry_a.path.stem})
    try:
        assert {retry_a.path.stem, retry_b.path.stem} == {"a", "b"}
    finally:
        retry_a.close()
        retry_b.close()


def test_scheduling_failure_does_not_release_an_inflight_request_claim(queue):
    enqueue, factory, root = queue
    path = enqueue("a", 2)
    consumer, requests = factory(concurrency=2)

    def schedule(request):
        if requests:
            raise RuntimeError("scheduler unavailable")
        requests.append(request)

    consumer.spider.crawler.engine.crawl = schedule
    try:
        consumer.poll()
        assert len(requests) == 1
        assert "a" in consumer.jobs
        complete(consumer, requests[0])
        retry = next_batch(path.parent)
        assert retry is not None
        retry.close()
    finally:
        consumer.close()


def test_temporary_queue_scan_error_does_not_stop_processing(queue, monkeypatch):
    enqueue, factory, _ = queue
    enqueue("a", 1)
    consumer, requests = factory()
    original = Path.glob
    failed = False

    def fail_once(path, pattern, *args, **kwargs):
        nonlocal failed
        if path == consumer.pending_dir / "crawler_00" and not failed:
            failed = True
            raise OSError("temporarily unavailable")
        return original(path, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", fail_once)
    try:
        consumer.refill()
        assert failed, "The worker partition scan must exercise the injected error"
        assert not requests
        assert not consumer.jobs
        consumer.spider.logger.exception.assert_called_once_with("manual_bulk_crawl.queue_failed")
        consumer.poll()
        assert len(requests) == 1
        complete(consumer, requests[0])
        assert not consumer.jobs
    finally:
        consumer.close()


@pytest.mark.parametrize("name", ["max_active_jobs"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, None])
def test_invalid_prefetch_configuration_fails_at_startup(queue, name, value):
    _, factory, _ = queue
    with pytest.raises(ValueError, match=f"MANUAL_BULK_CRAWL_{name.upper()}"):
        factory(**{name: value})


@pytest.mark.parametrize("value", [-1, True, 1.5, None])
def test_invalid_host_headroom_fails_at_startup(queue, value):
    _, factory, _ = queue
    with pytest.raises(ValueError, match="MANUAL_BULK_CRAWL_HOST_HEADROOM"):
        factory(host_headroom=value)


@pytest.mark.parametrize("value", [-1, -0.5, True, None, "5"])
def test_invalid_host_interval_fails_at_startup(queue, value):
    _, factory, _ = queue
    with pytest.raises(ValueError, match="MANUAL_BULK_CRAWL_HOST_INTERVAL"):
        factory(host_interval=value)


def paced(consumer, monkeypatch):
    """One fake clock for the consumer's monotonic() and its timers."""
    import containers.crawler.crawler.manual_bulk_crawls as consumer_module

    clock = Clock()
    clock.advance(1000)
    monkeypatch.setattr(consumer_module, "monotonic", clock.seconds)
    consumer.poller.clock = clock
    return clock


def test_host_interval_spaces_one_hosts_requests_without_holding_others(queue, monkeypatch):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://paced.example/{i}" for i in range(3)])
    enqueue("b", urls=["https://other.example/0"])
    consumer, requests = factory(concurrency=8, host_interval=5.0)
    clock = paced(consumer, monkeypatch)
    try:
        consumer.poll()
        assert sorted(r.url for r in requests) == ["https://other.example/0", "https://paced.example/0"]
        complete(consumer, requests[0])
        clock.advance(4.9)
        assert len(requests) == 2, "A completed request does not shorten the interval"
        clock.advance(0.1)
        assert requests[-1].url == "https://paced.example/1"
        complete(consumer, requests[-1])
        clock.advance(5.0)
        assert requests[-1].url == "https://paced.example/2"
        assert len(requests) == 4
    finally:
        consumer.close()


def test_late_timer_shortens_the_next_gap_instead_of_lowering_the_rate(queue, monkeypatch):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://paced.example/{i}" for i in range(3)])
    consumer, requests = factory(concurrency=8, host_interval=5.0)
    clock = paced(consumer, monkeypatch)
    try:
        consumer.poll()
        clock.advance(6.0)  # The timer for t = 1005 runs at t = 1006.
        assert len(requests) == 2, "Pacing does not wait for the previous response"
        clock.advance(3.9)
        assert len(requests) == 2
        clock.advance(0.1)
        assert len(requests) == 3, "The third request keeps the grid: t = 1010"
    finally:
        consumer.close()


def test_host_interval_restarts_after_an_idle_spell(queue, monkeypatch):
    enqueue, factory, _ = queue
    enqueue("a", urls=["https://paced.example/a"])
    consumer, requests = factory(concurrency=8, host_interval=5.0)
    clock = paced(consumer, monkeypatch)
    try:
        consumer.poll()
        complete(consumer, requests[0])
        clock.advance(20.0)
        enqueue("b", urls=[f"https://paced.example/b{i}" for i in range(2)])
        consumer.poll()
        assert len(requests) == 2
        clock.advance(4.9)
        assert len(requests) == 2, "No catching up on the idle time"
        clock.advance(0.1)
        assert len(requests) == 3
    finally:
        consumer.close()


def test_host_interval_holds_across_jobs_after_the_backlog_empties(queue, monkeypatch):
    enqueue, factory, _ = queue
    enqueue("a", urls=["https://paced.example/a"])
    consumer, requests = factory(concurrency=8, host_interval=5.0)
    clock = paced(consumer, monkeypatch)
    try:
        consumer.poll()
        complete(consumer, requests[0])
        clock.advance(1.0)
        enqueue("b", urls=["https://paced.example/b"])
        consumer.poll()
        assert len(requests) == 1
        clock.advance(4.0)
        assert requests[-1].url == "https://paced.example/b"
    finally:
        consumer.close()


def test_close_cancels_pending_host_interval_timers(queue, monkeypatch):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://paced.example/{i}" for i in range(2)])
    consumer, requests = factory(concurrency=8, host_interval=5.0)
    clock = paced(consumer, monkeypatch)
    consumer.poll()
    complete(consumer, requests[0])
    assert len(requests) == 1
    assert clock.getDelayedCalls()
    consumer.close()
    assert not clock.getDelayedCalls()


def slots(consumer, **hosts):
    """Live downloader slots: host -> (concurrency, delay)."""
    consumer.spider.crawler.engine.downloader = SimpleNamespace(slots={
        host: SimpleNamespace(concurrency=concurrency, delay=delay)
        for host, (concurrency, delay) in hosts.items()})


@pytest.mark.parametrize("headroom", [0, 1, 2])
@pytest.mark.parametrize("latency,delay,in_transfer", [(2.5, 1.0, 3), (3.5, 1.75, 2), (1.0, 4.0, 1)])
def test_host_cap_follows_the_slot_delay(queue, headroom, latency, delay, in_transfer):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://slow.example/{i}" for i in range(30)])
    consumer, requests = factory(concurrency=32, host_headroom=headroom)
    slots(consumer, **{"slow.example": (16, delay)})
    try:
        consumer.poll()
        consumer.domains["slow.example"].latency = latency
        consumer.poll()
        assert len(requests) == in_transfer + headroom
    finally:
        consumer.close()


def test_manual_and_normal_requests_share_the_hostname_slot(queue, monkeypatch):
    # One slot per hostname means one AutoThrottle delay, so manual work cannot add a second rate.
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from crawler.spiders.spider import HtmlSpider

    enqueue, factory, _ = queue
    enqueue("a", urls=["https://www.espn.com/nba/1"])
    consumer, requests = factory()
    crawler = get_crawler(Spider, {"AUTOTHROTTLE_ENABLED": True, "DOWNLOAD_DELAY": 0.25,
                                   "CONCURRENT_REQUESTS_PER_DOMAIN": 16})
    crawler.spider = Spider(name="slot-test")
    downloader = Downloader(crawler)
    spider = SimpleNamespace(_domain_pending={}, parse=None, errback=None)
    normal = HtmlSpider._build_request(spider, "https://www.espn.com/nba/2", 7)
    try:
        consumer.poll()
        manual_key, manual_slot = downloader._get_slot(requests[0])
        normal_key, normal_slot = downloader._get_slot(normal)
        assert manual_key == normal_key == "www.espn.com"
        assert manual_slot is normal_slot
    finally:
        consumer.close()
        downloader.close()


def test_host_cap_never_exceeds_slot_concurrency_and_zero_delay_is_uncapped(queue):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://narrow.example/{i}" for i in range(30)])
    enqueue("b", urls=[f"https://open.example/{i}" for i in range(30)])
    consumer, _ = factory(concurrency=32)
    slots(consumer, **{"narrow.example": (2, 0.1), "open.example": (12, 0)})
    try:
        consumer.poll()
        consumer.domains["narrow.example"].latency = 5.0
        consumer.domains["open.example"].latency = 5.0
        consumer.poll()
        assert consumer.domains["narrow.example"].outstanding == 2
        assert consumer.domains["open.example"].outstanding == 12
    finally:
        consumer.close()


def test_longer_delay_shrinks_the_host_cap_without_cancelling_requests(queue):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://slow.example/{i}" for i in range(30)])
    consumer, requests = factory(concurrency=32, host_headroom=0)
    slots(consumer, **{"slow.example": (16, 1.0)})
    try:
        consumer.poll()
        consumer.domains["slow.example"].latency = 2.5
        consumer.poll()
        assert len(requests) == 3
        consumer.spider.crawler.engine.downloader.slots["slow.example"].delay = 5.0
        consumer.poll()
        complete(consumer, requests[0])
        complete(consumer, requests[1])
        assert len(requests) == 3, "Two outstanding still exceed the new cap of one"
        complete(consumer, requests[2])
        assert len(requests) == 4
    finally:
        consumer.close()


def test_a_failure_without_response_leaves_the_latency_estimate(queue):
    from twisted.internet.error import TimeoutError
    from twisted.python.failure import Failure

    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://dead.example/{i}" for i in range(4)])
    consumer, requests = factory(concurrency=4)
    try:
        consumer.poll()
        error = TimeoutError()
        error.request = requests[0]
        failure = Failure(error)
        failure.request = requests[0]
        consumer.fetch_failed(failure)
        assert consumer.domains["dead.example"].latency == 1.0
    finally:
        consumer.close()


def test_full_capacity_spreads_over_many_hosts_at_their_caps(queue):
    enqueue, factory, _ = queue
    hosts = [f"h{i:03d}.example" for i in range(100)]
    for i, host in enumerate(hosts):
        enqueue(f"j{i:03d}", urls=[f"https://{host}/{n}" for n in range(10)])
    consumer, requests = factory(concurrency=256, max_active_jobs=100,
                                 slot_settings={host: {"delay": 0.5, "concurrency": 16} for host in hosts})
    try:
        consumer.poll()
        assert len(requests) == 256
        outstanding = [consumer.domains[host].outstanding for host in hosts if host in consumer.domains]
        assert max(outstanding) == 3, "latency 1.0 / delay 0.5 + 1 headroom"
        assert sum(count > 0 for count in outstanding) >= 86
    finally:
        consumer.close()


def test_waiting_small_host_overtakes_a_newer_larger_one(queue, monkeypatch):
    import containers.crawler.crawler.manual_bulk_crawls as consumer_module

    clock = [1000.0]
    monkeypatch.setattr(consumer_module, "monotonic", lambda: clock[0])
    enqueue, factory, _ = queue
    enqueue("old", urls=[f"https://small.example/{i}" for i in range(2)])
    consumer, requests = factory(concurrency=1)
    try:
        consumer.poll()
        assert requests[0].url.startswith("https://small.example/")
        clock[0] += 500
        enqueue("new", urls=[f"https://big.example/{i}" for i in range(200)])
        consumer.poll()
        complete(consumer, requests[0])
        # 1 URL left plus 500 s waited outranks 200 URLs at 1 s each that waited 0 s.
        assert requests[1].url.startswith("https://small.example/")
    finally:
        consumer.close()


def test_job_message_selects_its_output_directory_without_api_configuration(queue, monkeypatch):
    enqueue, factory, root = queue
    monkeypatch.setenv("API_CONFIG_PATH", str(root / "missing-api-config.yml"))
    first = enqueue("a", 1)
    second = enqueue("b", 1)
    output = root / "upstream-selected-results"
    payload = json.loads(second.read_text())
    payload["result_dir"] = str(output)
    second.write_text(json.dumps(payload))
    prepare_job(second, payload)
    consumer, requests = factory(concurrency=2)
    try:
        consumer.poll()
        complete(consumer, requests[1], body=b"upstream output")
        complete(consumer, requests[0], body=b"original output")
        assert not (first.parent / "crawler_00" / first.name).exists() and not (second.parent / "crawler_00" / second.name).exists()
        assert (root / "results/a/crawler_00/00000.html").read_bytes() == b"original output"
        assert (output / "b/crawler_00/00000.html").read_bytes() == b"upstream output"
        assert not (root / "results/b/crawler_00/00000.html").exists()
    finally:
        consumer.close()


@pytest.mark.parametrize("expire", [False, True])
def test_deleted_or_expired_job_stops_refill_and_discards_inflight_responses(queue, expire):
    from datetime import datetime, timedelta, timezone
    import shutil

    enqueue, factory, root = queue
    job_id = "a" * 32
    path = enqueue(job_id, 6)
    consumer, requests = factory(concurrency=2)
    staging = root / "results" / job_id
    try:
        consumer.poll()
        assert len(requests) == 2
        if expire:
            payload = consumer.jobs[job_id].batch.payload
            payload["expires_at"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            path.write_text(json.dumps(payload))
        else:
            # Simulate the shared files disappearing after API deletion.
            # The consumer must not depend on the API service or its packages.
            path.unlink()
            shutil.rmtree(staging)
        consumer.poll()
        assert len(requests) == 2
        for request in list(requests):
            complete(consumer, request)
        assert len(requests) == 2
        assert not consumer.jobs and not consumer.inflight
        consumer.spider.logger.exception.assert_not_called()
        assert not (staging / "crawler_00/00000.html").exists()
        if expire:
            # Cleanup belongs to API GC; the consumer must leave no late output.
            assert path.exists()
            assert not list(staging.glob("[0-9]*"))
        else:
            assert not staging.exists()
    finally:
        consumer.close()


def test_service_time_prioritizes_a_smaller_but_slower_domain(queue):
    enqueue, factory, _ = queue
    enqueue("fast", urls=[f"https://fast.example/{i}" for i in range(10)])
    enqueue("slow", urls=[f"https://slow.example/{i}" for i in range(3)])
    consumer, requests = factory(concurrency=1, slot_settings={
        "fast.example": {"delay": 1, "concurrency": 1},
        "slow.example": {"delay": 10, "concurrency": 1},
    })
    try:
        consumer.poll()
        assert requests[0].url == "https://slow.example/0", "30s of work outranks 10s"
        complete(consumer, requests[0])
        assert requests[1].url == "https://slow.example/1", "Keep the bottleneck supplied at its QPS"
    finally:
        consumer.close()


def test_single_domain_prefix_cannot_hide_other_domains_in_same_job(queue):
    enqueue, factory, _ = queue
    enqueue("mixed", urls=[f"https://wiki.example/{i}" for i in range(100)]
            + ["https://other.example/0", "https://third.example/0"])
    consumer, requests = factory(concurrency=6, domain_concurrency=2)
    try:
        consumer.poll()
        assert [request.url for request in requests[:2]] == [
            "https://wiki.example/0", "https://wiki.example/1"]
        assert {request.url for request in requests[2:]} == {
            "https://other.example/0", "https://third.example/0"}
        assert len(requests) == 4, "Only two wiki URLs should be admitted"
        wiki = [request for request in requests if "wiki.example" in request.url]
        complete(consumer, wiki[0])
        assert requests[-1].url == "https://wiki.example/2", "Refill wiki immediately"
        assert consumer.domains["wiki.example"].outstanding == 2
    finally:
        consumer.close()


def test_later_jobs_supply_other_domains_without_draining_wiki_job(queue):
    enqueue, factory, _ = queue
    enqueue("wiki", urls=[f"https://wiki.example/{i}" for i in range(100)])
    enqueue("other", urls=["https://other.example/0"])
    consumer, requests = factory(concurrency=8, domain_concurrency=2)
    try:
        consumer.poll()
        assert len(consumer.jobs) == 2
        assert any(request.url == "https://other.example/0" for request in requests)
        assert len(requests) == 3
        assert len(consumer.jobs["wiki"].pending) == 98
    finally:
        consumer.close()


def test_domain_cap_is_shared_across_jobs_and_failure_releases_it(queue):
    from twisted.python.failure import Failure
    from twisted.internet.error import TimeoutError

    enqueue, factory, _ = queue
    enqueue("a", 4)
    enqueue("b", 4)
    consumer, requests = factory(concurrency=8, domain_concurrency=2)
    try:
        consumer.poll()
        assert len(requests) == 2
        error = TimeoutError()
        error.request = requests[0]
        failure = Failure(error)
        failure.request = requests[0]
        consumer.fetch_failed(failure)
        assert len(requests) == 3
        assert consumer.domains["example.com"].outstanding == 2
        for request in requests:
            assert "download_slot" not in request.meta, "Do not pin redirect requests to original slots"
    finally:
        consumer.close()


def test_remaining_service_time_aggregates_domains_across_jobs(queue):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://shared.example/a/{i}" for i in range(3)])
    enqueue("b", urls=[f"https://shared.example/b/{i}" for i in range(3)])
    enqueue("c", urls=[f"https://other.example/{i}" for i in range(5)])
    consumer, requests = factory(concurrency=1)
    try:
        consumer.poll()
        assert requests[0].url.startswith("https://shared.example/"), "6 URLs outrank 5"
    finally:
        consumer.close()


def test_zero_delay_domains_use_concurrency_and_observed_latency(queue):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://serial.example/{i}" for i in range(10)])
    enqueue("b", urls=[f"https://parallel.example/{i}" for i in range(20)])
    consumer, requests = factory(concurrency=1, slot_settings={
        "serial.example": {"delay": 0, "concurrency": 1},
        "parallel.example": {"delay": 0, "concurrency": 10},
    })
    try:
        consumer.poll()
        assert requests[0].url.startswith("https://serial.example/")
        requests[0].meta["download_latency"] = 5.0
        complete(consumer, requests[0])
        assert consumer.domains["serial.example"].latency == pytest.approx(1.8)
        assert requests[1].url.startswith("https://serial.example/"), "The measured slower domain remains the bottleneck"
    finally:
        consumer.close()


def test_live_slot_settings_override_static_config_and_refresh_on_poll(queue):
    enqueue, factory, _ = queue
    enqueue("a", urls=[f"https://a.example/{i}" for i in range(20)])
    enqueue("b", urls=[f"https://b.example/{i}" for i in range(4)])
    consumer, requests = factory(concurrency=4, slot_settings={
        "a.example": {"delay": 100, "concurrency": 4},
        "b.example": {"delay": 1, "concurrency": 4},
    })
    consumer.spider.crawler.engine.downloader = SimpleNamespace(
        get_slot_key=lambda request: request.url.split("/")[2],
        slots={"a.example": SimpleNamespace(concurrency=1, delay=0),
               "b.example": SimpleNamespace(concurrency=0, delay=100)},
    )
    try:
        consumer.poll()
        assert len(requests) == 1
        consumer.spider.crawler.engine.downloader.slots["b.example"].concurrency = 2
        consumer.poll()
        assert len(requests) == 3
        assert all("b.example" in request.url for request in requests[1:])
    finally:
        consumer.close()


def test_deleting_job_removes_its_domain_backlog_without_blocking_other_jobs(queue):
    enqueue, factory, _ = queue
    path = enqueue("a", urls=[f"https://a.example/{i}" for i in range(10)])
    enqueue("b", urls=[f"https://b.example/{i}" for i in range(3)])
    consumer, requests = factory(concurrency=2, domain_concurrency=1)
    try:
        consumer.poll()
        path.unlink()
        consumer.poll()
        for request in list(requests):
            complete(consumer, request)
        assert all("b.example" in request.url for request in requests[2:])
        position = 2
        while position < len(requests):
            complete(consumer, requests[position])
            position += 1
        assert not consumer.jobs and not consumer.domains and not consumer.inflight_domains
    finally:
        consumer.close()


def test_resumed_job_keeps_original_result_indexes_after_domain_reordering(queue):
    enqueue, factory, root = queue
    path = enqueue("mixed", urls=["https://a.example/0", "https://a.example/1", "https://b.example/2"])
    batch = next_batch(path.parent)
    batch.save(0, final_url=batch.urls[0], status_code=200, body=b"already fetched")
    batch.close()
    consumer, requests = factory(concurrency=2, domain_concurrency=1)
    try:
        consumer.poll()
        assert {request.meta["manual_bulk_crawl_index"] for request in requests} == {1, 2}
        for request in requests:
            complete(consumer, request)
        output = root / "results/mixed/crawler_00"
        assert (output / "00000.html").read_bytes() == b"already fetched"
        assert json.loads((output / "00002.json").read_text())["url"] == "https://b.example/2"
        assert not consumer.domains
    finally:
        consumer.close()


@pytest.mark.parametrize("policy", ["balanced", "manual_first", "normal_first"])
def test_legacy_job_policy_is_not_propagated_to_requests(queue, policy):
    enqueue, factory, _ = queue
    path = enqueue("a", 2)
    payload = json.loads(path.read_text())
    payload["scheduling_policy"] = policy
    path.write_text(json.dumps(payload))
    consumer, requests = factory()
    try:
        consumer.poll()
        assert len(requests) == 2
        assert all("manual_bulk_crawl_scheduling_policy" not in request.meta for request in requests)
    finally:
        consumer.close()


def test_job_without_policy_does_not_add_request_policy(queue):
    enqueue, factory, _ = queue
    enqueue("a", 1)
    consumer, requests = factory()
    try:
        consumer.poll()
        assert "manual_bulk_crawl_scheduling_policy" not in requests[0].meta
    finally:
        consumer.close()


@pytest.mark.parametrize("url,expected", [
    ("https://EXAMPLE.com:8443?x=1", "example.com"),
    ("https://[2606:4700:4700::1111]:8443/a", "2606:4700:4700::1111"),
    ("https://xn--bcher-kva.example/a", "xn--bcher-kva.example"),
])
def test_loading_job_groups_by_hostname_without_allocating_requests(queue, monkeypatch, url, expected):
    enqueue, factory, _ = queue
    enqueue("a", urls=[url])
    consumer, _ = factory()
    request = Mock(side_effect=AssertionError("Hostname extraction must not create a Request"))
    monkeypatch.setattr("containers.crawler.crawler.manual_bulk_crawls.scrapy.Request", request)
    try:
        consumer._load_jobs()
        assert set(consumer.domains) == {expected}
        assert list(consumer.domains[expected].pending) == [("a", 0)]
        request.assert_not_called()
    finally:
        consumer.close()


@pytest.mark.parametrize("a_jitter,b_jitter,first", [
    (0.1, 0.9, "b"),
    (0.9, 0.1, "a"),
])
def test_fixed_domain_jitter_breaks_ties_and_survives_updates(queue, a_jitter, b_jitter, first):
    enqueue, factory, _ = queue
    enqueue("mixed", urls=[f"https://{host}.example/{i}" for host in ("a", "b") for i in range(3)])
    consumer, requests = factory(concurrency=1)
    try:
        consumer._load_jobs()
        assert all(0 <= domain.jitter < 1 for domain in consumer.domains.values())
        consumer.domains["a.example"].jitter = a_jitter
        consumer.domains["b.example"].jitter = b_jitter
        consumer.poll()
        assert requests[0].url == f"https://{first}.example/0"
        consumer.poll()
        complete(consumer, requests[0])
        other = "b" if first == "a" else "a"
        assert requests[1].url == f"https://{other}.example/0", "One more second of work outweighs jitter"
        assert consumer.domains["a.example"].jitter == a_jitter
        assert consumer.domains["b.example"].jitter == b_jitter
    finally:
        consumer.close()


def test_priority_updates_replace_entries_and_capped_domains_leave_queue(queue):
    enqueue, factory, _ = queue
    enqueue("mixed", urls=[f"https://{host}.example/{i}" for host in ("a", "b") for i in range(5)])
    consumer, requests = factory(concurrency=1)
    try:
        consumer._load_jobs()
        for latency in range(1, 101):
            consumer.domains["a.example"].latency = latency
            consumer._refresh_priorities(["a.example"])
            assert set(consumer.ready) == {"a.example", "b.example"}
        consumer.refill()
        assert requests[0].url == "https://a.example/0"
        assert "a.example" not in consumer.ready, "The domain is at its prefetch cap"

        # A changed estimate must replace the previous priority on completion.
        consumer.domains["a.example"].latency = 0.01
        complete(consumer, requests[0])
        assert requests[1].url == "https://b.example/0"
        assert set(consumer.ready) == {"a.example"}
    finally:
        consumer.close()

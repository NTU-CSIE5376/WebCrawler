"""Keep download capacity busy across jobs while preserving per-job ownership."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from scrapy.http import HtmlResponse
from scrapy.settings import Settings

from containers.crawler.crawler.manual_bulk_crawls import ManualBulkCrawlConsumer
from libs.ipc import manual_bulk_crawl as storage
from libs.ipc.manual_bulk_crawl import claim_next


@pytest.fixture
def queue(tmp_path):
    pending = tmp_path / "pending"
    pending.mkdir()

    def enqueue(job_id, count):
        path = pending / f"{job_id}.json"
        path.write_text(json.dumps({"version": 1,
                                   "urls": [f"https://example.com/{job_id}/{i}" for i in range(count)],
                                   "crawler_ids": [0] * count,
                                   "result_dir": str(tmp_path / "results")}))
        return path

    def consumer(**limits):
        requests = []
        spider = SimpleNamespace(crawler_id=0, logger=Mock(), crawler=SimpleNamespace(
            settings=Settings({"CONCURRENT_REQUESTS": 2,
                               "MANUAL_BULK_CRAWL_QUEUE_DIR": str(pending),
                               **{f"MANUAL_BULK_CRAWL_{name.upper()}": value for name, value in limits.items()}}),
            engine=SimpleNamespace(crawl=requests.append),
        ))
        return ManualBulkCrawlConsumer(spider), requests

    return enqueue, consumer, tmp_path


def complete(consumer, request, body=b"<html>saved</html>"):
    consumer.parse(HtmlResponse(request.url, request=request, body=body,
                                headers={"Content-Type": "text/html"}))


def test_next_job_finishes_while_earlier_job_still_has_a_slow_url(queue):
    enqueue, factory, root = queue
    first = enqueue("a", 2)
    second = enqueue("b", 1)
    consumer, requests = factory(prefetch_urls=2)
    try:
        consumer.poll()
        slow, fast = requests
        assert len(requests) == 2
        complete(consumer, fast)
        # URL a/0 is still outstanding. Its completion must not gate job b.
        assert requests[-1].meta["manual_bulk_crawl_job_id"] == "b"
        assert len(consumer.inflight) == 2
        complete(consumer, requests[-1], body=b"job b")
        assert not second.exists()
        assert first.exists()
        assert (root / "results/b/00000.html").read_bytes() == b"job b"
        assert not (root / "results/a").exists()
        complete(consumer, slow, body=b"job a")
        assert not first.exists()
        assert (root / "results/a/00000.html").read_bytes() == b"job a"
        assert not consumer.inflight
    finally:
        consumer.close()


def test_large_job_keeps_a_bounded_backlog_and_refills_on_completion(queue):
    enqueue, factory, root = queue
    enqueue("a", 6)
    consumer, requests = factory(prefetch_urls=2)
    try:
        consumer.poll()
        assert len(requests) == 2
        # Leave the first URL slow while the rest repeatedly refill its sibling slot.
        for position in range(1, 6):
            complete(consumer, requests[position])
            assert len(consumer.inflight) <= 2
        assert len(requests) == 6
        assert not (root / "results/a").exists()
        complete(consumer, requests[0])
        status = json.loads((root / "results/a/status.json").read_text())
        assert status["completed_urls"] == 6
        assert len({(request.meta["manual_bulk_crawl_job_id"], request.meta["manual_bulk_crawl_index"])
                    for request in requests}) == 6
    finally:
        consumer.close()


def test_active_job_limit_releases_capacity_without_waiting_for_other_jobs(queue):
    enqueue, factory, _ = queue
    for name in ("a", "b", "c"):
        enqueue(name, 1)
    consumer, requests = factory(prefetch_urls=8, max_active_jobs=2)
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
    consumer, requests = factory(prefetch_urls=2)
    try:
        consumer.poll()
        monkeypatch.setattr(consumer.jobs["a"].batch, "save", Mock(side_effect=OSError("disk full")))
        complete(consumer, requests[0])
        assert claim_next(path.parent) is None
        assert len(requests) == 2, "Unscheduled URLs must wait for the failed attempt to drain"
        complete(consumer, requests[1])
        assert not consumer.jobs
        retry = claim_next(path.parent)
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
    consumer, requests = factory(prefetch_urls=3)
    try:
        consumer.poll()
        monkeypatch.setattr(consumer.jobs["a"].batch, "save", Mock(side_effect=OSError("disk full")))
        complete(consumer, requests[0])
        complete(consumer, requests[2], body=b"unrelated job")
        assert (root / "results/b/00000.html").read_bytes() == b"unrelated job"
        assert "a" in consumer.jobs
        assert len(consumer.inflight) == 1
    finally:
        consumer.close()


def test_publication_retry_resumes_saved_results_without_refetching(queue, monkeypatch):
    enqueue, factory, root = queue
    path = enqueue("a", 1)
    consumer, requests = factory(prefetch_urls=2)
    original = storage.os.replace

    def fail_publication(source, target):
        if target == root / "results/a":
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
        assert not path.exists()
        assert len(requests) == 1
        assert (root / "results/a/00000.html").read_bytes() == b"<html>saved</html>"
    finally:
        consumer.close()


def test_shutdown_releases_every_job_and_ignores_stale_callbacks(queue):
    enqueue, factory, root = queue
    first = enqueue("a", 1)
    enqueue("b", 1)
    consumer, requests = factory(prefetch_urls=2)
    consumer.poll()
    consumer.close()
    complete(consumer, requests[0])
    consumer.poll()
    assert len(requests) == 2
    assert not consumer.jobs
    retry_a = claim_next(first.parent)
    retry_b = claim_next(first.parent)
    try:
        assert {retry_a.path.stem, retry_b.path.stem} == {"a", "b"}
    finally:
        retry_a.close()
        retry_b.close()


def test_scheduling_failure_does_not_release_an_inflight_request_claim(queue):
    enqueue, factory, root = queue
    path = enqueue("a", 2)
    consumer, requests = factory(prefetch_urls=2)

    def schedule(request):
        if requests:
            raise RuntimeError("scheduler unavailable")
        requests.append(request)

    consumer.spider.crawler.engine.crawl = schedule
    try:
        consumer.poll()
        assert len(requests) == 1
        assert claim_next(path.parent) is None
        complete(consumer, requests[0])
        retry = claim_next(path.parent)
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
        if path == consumer.pending_dir and not failed:
            failed = True
            raise OSError("temporarily unavailable")
        return original(path, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", fail_once)
    try:
        consumer.poll()
        assert len(requests) == 1
        complete(consumer, requests[0])
        assert not consumer.jobs
    finally:
        consumer.close()


@pytest.mark.parametrize("name", ["prefetch_urls", "max_active_jobs"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, None])
def test_invalid_prefetch_configuration_fails_at_startup(queue, name, value):
    _, factory, _ = queue
    with pytest.raises(ValueError, match=f"MANUAL_BULK_CRAWL_{name.upper()}"):
        factory(**{name: value})


def test_job_message_selects_its_output_directory_without_api_configuration(queue, monkeypatch):
    enqueue, factory, root = queue
    monkeypatch.setenv("API_CONFIG_PATH", str(root / "missing-api-config.yml"))
    first = enqueue("a", 1)
    second = enqueue("b", 1)
    output = root / "upstream-selected-results"
    payload = json.loads(second.read_text())
    payload["result_dir"] = str(output)
    second.write_text(json.dumps(payload))
    consumer, requests = factory(prefetch_urls=2)
    try:
        consumer.poll()
        complete(consumer, requests[1], body=b"upstream output")
        complete(consumer, requests[0], body=b"original output")
        assert not first.exists() and not second.exists()
        assert (root / "results/a/00000.html").read_bytes() == b"original output"
        assert (output / "b/00000.html").read_bytes() == b"upstream output"
        assert not (root / "results/b").exists()
    finally:
        consumer.close()


@pytest.mark.parametrize("expire", [False, True])
def test_deleted_or_expired_job_stops_refill_and_discards_inflight_responses(queue, expire):
    from datetime import datetime, timedelta, timezone
    import shutil

    enqueue, factory, root = queue
    job_id = "a" * 32
    path = enqueue(job_id, 6)
    consumer, requests = factory(prefetch_urls=2)
    staging = root / "results/.incomplete" / job_id
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
        assert not (root / "results" / job_id).exists()
        if expire:
            # Cleanup belongs to API GC; the consumer must leave no late output.
            assert path.exists()
            assert not list(staging.glob("[0-9]*"))
        else:
            assert not staging.exists()
    finally:
        consumer.close()

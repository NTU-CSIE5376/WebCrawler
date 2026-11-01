"""Crawl request frontier: dispatch order, caps, backpressure, records and restart."""
import gzip
import json
import sqlite3
from concurrent.futures import Future
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from containers.crawler.crawler.manual_bulk_crawls import ManualBulkCrawlConsumer
from libs.ipc.crawl_requests import CrawlRequestPaths, atomic_write_json, batch_key
from scrapy import Spider
from scrapy.core.downloader import Downloader
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse, Response
from scrapy.settings import Settings
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.test import get_crawler
from twisted.internet import error as twisted_error
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.web._newclient import ResponseNeverReceived

A, B, C = "a" * 32, "b" * 32, "c" * 32


class InlineExecutor:
    """Run thread work immediately so tests are deterministic."""

    def submit(self, fn, *args):
        future = Future()
        try:
            future.set_result(fn(*args))
        except Exception as exc:  # noqa: BLE001 - a Future carries any error
            future.set_exception(exc)
        return future

    def shutdown(self, wait=True):
        pass


@pytest.fixture
def env(tmp_path):
    paths = CrawlRequestPaths(tmp_path / "data")
    state = SimpleNamespace(paths=paths, clock=Clock(), free=10 ** 15, consumers=[])

    def consumer(concurrency=4, *, domain_concurrency=None, slot_settings=None, **limits):
        requests = []
        spider = SimpleNamespace(crawler_id=0, crawler=SimpleNamespace(
            settings=Settings({"CONCURRENT_REQUESTS": concurrency,
                               "CONCURRENT_REQUESTS_PER_DOMAIN": domain_concurrency or concurrency,
                               "DOWNLOAD_SLOTS": slot_settings or {},
                               "MANUAL_BULK_CRAWL_ROOT": str(paths.root),
                               **{f"MANUAL_BULK_CRAWL_{name.upper()}": value for name, value in limits.items()}}),
            engine=SimpleNamespace(crawl=requests.append),
        ))
        made = ManualBulkCrawlConsumer(
            spider, executor=InlineExecutor(), segment_executor=InlineExecutor(),
            call_from_thread=lambda fn, *args: fn(*args), clock=state.clock,
            disk_usage=lambda path: SimpleNamespace(free=state.free))
        made.start()
        state.consumers.append(made)
        return made, requests

    state.consumer = consumer
    yield state
    for made in state.consumers:
        made.close()


def create(env, request_id, *, quota_bytes=10 ** 12, expires_in=timedelta(days=1)):
    now = datetime.now(UTC)
    atomic_write_json(env.paths.meta(request_id), {
        "id": request_id, "tenant": "t", "name": "n", "created_at": now.isoformat(),
        "expires_at": (now + expires_in).isoformat(), "result_quota_bytes": quota_bytes})


def submit(env, request_id, urls, batch="b", ids=None, priorities=None):
    if not env.paths.meta(request_id).exists():
        create(env, request_id)
    ids = ids or [f"{batch}-{i}" for i in range(len(urls))]
    priorities = priorities or [0] * len(urls)
    atomic_write_json(env.paths.inbox_file(0, request_id, batch_key(batch)), {
        "request_id": request_id,
        "items": [{"id": i, "url": u, "priority": p} for i, u, p in zip(ids, urls, priorities)]})


def complete(consumer, request, body=b"<html>ok</html>", status=200, content_type="text/html", headers=None):
    consumer.parse(HtmlResponse(request.url, request=request, body=body, status=status,
                                headers={"Content-Type": content_type, **(headers or {})}))


def fail(consumer, request, exception):
    failure = Failure(exception)
    failure.request = request
    consumer.fetch_failed(failure)


def http_error(consumer, request, status, headers=None):
    response = HtmlResponse(request.url, request=request, status=status, body=b"<html>no</html>",
                            headers={"Content-Type": "text/html", **(headers or {})})
    fail(consumer, request, HttpError(response))


def flush(env):
    """Let the segment age limit close open segments."""
    env.clock.advance(60)


def records(env, request_id):
    out = []
    for path in sorted(env.paths.segments(request_id).glob("crawler_*/*.jsonl.gz")):
        out += [json.loads(line) for line in gzip.decompress(path.read_bytes()).splitlines()]
    return out


def hosts(requests):
    return [request.url.split("/")[2] for request in requests]


def rows(env):
    db = sqlite3.connect(env.paths.frontier(0))
    try:
        return db.execute("SELECT request, item_id, url, state FROM url ORDER BY rowid").fetchall()
    finally:
        db.close()


def slots(consumer, **live):
    """Live downloader slots: host -> (concurrency, delay)."""
    consumer.spider.crawler.engine.downloader = SimpleNamespace(slots={
        host: SimpleNamespace(concurrency=concurrency, delay=delay)
        for host, (concurrency, delay) in live.items()})


def held(env, **kwargs):
    """A consumer that has ingested its inbox but not dispatched (disk floor), until poll()."""
    env.free = 0
    made = env.consumer(**kwargs)
    env.free = 10 ** 15
    return made


# TA scenario 1: the most time-consuming slot goes first, even when it arrives last.
def test_longest_remaining_slot_goes_first_even_when_submitted_last(env):
    submit(env, A, [f"https://small.example/{i}" for i in range(2)])
    consumer, requests = env.consumer(concurrency=1)
    assert hosts(requests) == ["small.example"]
    submit(env, A, [f"https://big.example/{i}" for i in range(200)], batch="later")
    consumer.poll()
    complete(consumer, requests[0])
    assert hosts(requests)[1] == "big.example", "200 URLs left outrank 1"


def test_requests_take_equal_turns_and_a_new_request_does_not_burst(env):
    submit(env, A, [f"https://a.example/{i}" for i in range(20)])
    submit(env, B, [f"https://b.example/{i}" for i in range(20)])
    consumer, requests = env.consumer(concurrency=1)
    for _ in range(5):
        complete(consumer, requests[-1])
    submit(env, C, [f"https://c.example/{i}" for i in range(20)])
    consumer.poll()
    for _ in range(9):
        complete(consumer, requests[-1])
    order = hosts(requests)
    assert order[:6] == ["a.example", "b.example"] * 3
    assert order[6:] .count("c.example") == 3, "C joins at the current minimum pass"
    assert "c.example,c.example" not in ",".join(order)


# TA scenario 2: QPS-limited slots (blocked or at their cap) do not stop the others.
def test_blocked_and_capped_slots_leave_capacity_to_other_slots(env):
    submit(env, A, [f"https://limited.example/{i}" for i in range(50)]
           + [f"https://blocked.example/{i}" for i in range(50)]
           + [f"https://other.example/{i}" for i in range(10)])
    consumer, requests = env.consumer(concurrency=8, domain_concurrency=2, slot_settings={
        "other.example": {"concurrency": 8, "delay": 0}})
    assert hosts(requests).count("other.example") == 4
    blocked = [request for request in requests if "blocked.example" in request.url]
    http_error(consumer, blocked[0], 429, {"Retry-After": "300"})
    http_error(consumer, blocked[1], 503)
    assert consumer.slots["limited.example"].outstanding == 2, "At its cap"
    assert consumer.slots["blocked.example"].outstanding == 0
    assert hosts(requests).count("blocked.example") == 2, "Blocked: no new dispatch"
    assert consumer.slots["other.example"].outstanding == 6, "The freed capacity goes to the other slot"
    assert len(consumer.inflight) == 8


# TA scenario 3: the same host across batches and requests shares one cap.
def test_same_host_across_batches_and_requests_shares_the_cap(env):
    submit(env, A, [f"https://shared.example/a/{i}" for i in range(5)])
    submit(env, A, [f"https://shared.example/a2/{i}" for i in range(5)], batch="second")
    submit(env, B, [f"https://shared.example/b/{i}" for i in range(5)])
    consumer, requests = env.consumer(concurrency=8, domain_concurrency=2)
    assert len(requests) == 2
    assert consumer.slots["shared.example"].outstanding == 2
    fail(consumer, requests[0], twisted_error.TimeoutError())
    assert len(requests) == 3, "A failure releases the shared cap"
    assert {request.meta["crawl_request"][0] for request in requests} == {A, B}


# TA scenario 4: a slow host does not block fast hosts.
def test_slow_host_does_not_block_fast_hosts_in_the_same_or_other_requests(env):
    submit(env, A, [f"https://slow.example/{i}" for i in range(10)] + [f"https://fast.example/{i}" for i in range(5)])
    submit(env, B, [f"https://quick.example/{i}" for i in range(5)])
    consumer, requests = env.consumer(concurrency=6, domain_concurrency=2)
    position = 0
    while position < len(requests):
        if "slow.example" not in requests[position].url:
            complete(consumer, requests[position])
        position += 1
    done = hosts(requests)
    assert done.count("fast.example") == 5 and done.count("quick.example") == 5
    assert consumer.slots["slow.example"].outstanding == 2


# TA scenario 5: a restart resumes accepted work.
def test_clean_restart_resumes_leased_and_queued_urls(env):
    submit(env, A, [f"https://example.com/{i}" for i in range(3)])
    consumer, requests = env.consumer(concurrency=2)
    complete(consumer, requests[0])
    consumer.close()
    assert [record["id"] for record in records(env, A)] == ["b-0"], "Shutdown publishes open segments"
    resumed, again = env.consumer(concurrency=2)
    assert sorted(request.url for request in again) == ["https://example.com/1", "https://example.com/2"]
    for request in again:
        complete(resumed, request)
    flush(env)
    assert sorted(record["id"] for record in records(env, A)) == ["b-0", "b-1", "b-2"]


def test_crash_redoes_leased_urls_and_unpublished_records(env):
    submit(env, A, [f"https://example.com/{i}" for i in range(2)])
    consumer, requests = env.consumer(concurrency=2)
    complete(consumer, requests[0])
    # Simulate process death: nothing is closed, published or deleted.
    consumer.poller.stop()
    consumer.closed = True
    for part in consumer.writer.parts.values():
        part.gz.fileobj = None  # Never flushed.
        part.raw.close()
    consumer.frontier.close()
    env.consumers.remove(consumer)
    assert [state for *_, state in rows(env)] == [1, 1]
    resumed, again = env.consumer(concurrency=2)
    assert not list(env.paths.segments(A).glob("*/*.part"))
    assert sorted(request.url for request in again) == ["https://example.com/0", "https://example.com/1"]
    for request in again:
        complete(resumed, request)
    flush(env)
    assert sorted(record["id"] for record in records(env, A)) == ["b-0", "b-1"]
    assert not rows(env), "Rows are deleted once their segment is closed"


def test_segment_numbers_are_not_reused_after_ack_and_restart(env):
    submit(env, A, ["https://example.com/0"])
    consumer, requests = env.consumer()
    complete(consumer, requests[0])
    flush(env)
    [first] = env.paths.segments(A, 0).glob("*.jsonl.gz")
    first.unlink()  # Acked.
    consumer.close()
    submit(env, A, ["https://example.com/1"], batch="next")
    resumed, again = env.consumer()
    complete(resumed, again[0])
    flush(env)
    [second] = env.paths.segments(A, 0).glob("*.jsonl.gz")
    assert first.name == "00-000000000001.jsonl.gz"
    assert second.name == "00-000000000002.jsonl.gz"


def test_quota_pauses_a_request_until_its_segments_are_acked(env):
    create(env, A, quota_bytes=1)
    submit(env, A, [f"https://example.com/{i}" for i in range(4)])
    submit(env, B, [f"https://other.example/{i}" for i in range(4)])
    consumer, requests = env.consumer(concurrency=1, shards=1)
    complete(consumer, requests[0])
    consumer.poll()
    complete(consumer, requests[1])
    paused_at = len(requests)
    for _ in range(3):
        complete(consumer, requests[-1])
    assert all("other.example" in request.url for request in requests[paused_at:]), "Only A pauses"
    assert consumer.snapshot()["requests"][A]["paused"] == "quota"
    flush(env)
    assert consumer.snapshot()["requests"][A]["paused"] == "quota", "Closed but unacked"
    for path in env.paths.segments(A, 0).glob("*.jsonl.gz"):
        path.unlink()
    consumer.poll()
    assert "example.com" in requests[-1].url, "Ack resumes dispatch"


def test_low_disk_floor_pauses_all_manual_dispatch(env):
    env.free = 299 * 10 ** 9
    submit(env, A, ["https://example.com/0"])
    consumer, requests = env.consumer()
    assert not requests
    assert consumer.heartbeat()["manual_paused"] == "floor"
    env.free = 301 * 10 ** 9
    consumer.poll()
    assert len(requests) == 1
    assert consumer.heartbeat()["manual_paused"] is None


def test_same_id_is_dropped_while_pending_and_crawled_again_after_its_record(env):
    submit(env, A, ["https://example.com/first", "https://example.com/queued"], ids=["x", "a"])
    consumer, requests = env.consumer(concurrency=1)
    submit(env, A, ["https://example.com/duplicate"], batch="dup", ids=["a"])
    consumer.poll()
    assert [(item, url) for _, item, url, _ in rows(env)] == [
        ("x", "https://example.com/first"), ("a", "https://example.com/queued")]
    complete(consumer, requests[0])
    complete(consumer, requests[1])
    submit(env, A, ["https://example.com/before-close"], batch="early", ids=["a"])
    consumer.poll()
    assert len(requests) == 2, "The record is not published yet, so the id is still pending"
    flush(env)
    submit(env, A, ["https://example.com/retry"], batch="retry", ids=["a"])
    consumer.poll()
    assert requests[-1].url == "https://example.com/retry"


def test_priority_orders_urls_within_a_slot(env):
    submit(env, A, [f"https://example.com/{i}" for i in range(4)], priorities=[5, -1, 3, -1])
    consumer, requests = env.consumer(concurrency=1)
    for _ in range(3):
        complete(consumer, requests[-1])
    assert [request.url[-1] for request in requests] == ["1", "3", "2", "0"]


def test_records_carry_id_url_status_and_html_only_for_html_2xx(env):
    urls = [f"https://example.com/{name}" for name in ("html", "text", "missing", "redirect")]
    submit(env, A, urls, ids=["h", "t", "m", "r"], priorities=[0, 1, 2, 3])
    consumer, requests = env.consumer(concurrency=4)
    complete(consumer, requests[0], body="<html>é</html>".encode())
    complete(consumer, requests[1], body=b"plain", content_type="text/plain")
    http_error(consumer, requests[2], 404)
    final = requests[3].replace(url="https://elsewhere.example/landing")
    complete(consumer, final)
    flush(env)
    by_id = {record["id"]: record for record in records(env, A)}
    for record in by_id.values():
        assert set(record) == {"id", "url", "priority", "final_url", "status_code", "error",
                               "fetched_at", "finished_at", "html"}
        assert datetime.fromisoformat(record["finished_at"]).tzinfo is not None
        assert datetime.fromisoformat(record["fetched_at"]).tzinfo is not None
    assert by_id["h"]["html"] == "<html>é</html>" and by_id["h"]["error"] is None
    assert by_id["h"]["status_code"] == 200 and by_id["h"]["url"] == urls[0]
    assert by_id["t"]["error"] == "non_html" and by_id["t"]["html"] is None
    assert by_id["m"]["error"] == "http_404" and by_id["m"]["html"] is None
    assert by_id["r"]["final_url"] == "https://elsewhere.example/landing" and by_id["r"]["priority"] == 3
    assert "elsewhere.example" not in consumer.slots, "Redirects stay charged to the original slot"


@pytest.mark.parametrize("exception,expected", [
    (twisted_error.DNSLookupError(), "dns"),
    (twisted_error.TimeoutError(), "timeout"),
    (twisted_error.TCPTimedOutError(), "timeout"),
    (twisted_error.ConnectionRefusedError(), "connection"),
    (twisted_error.ConnectionLost(), "connection"),
    (ResponseNeverReceived([]), "connection"),
    (twisted_error.ConnectError(), "connection"),
    (IgnoreRequest("Forbidden by robots.txt"), "robots"),
    (ValueError("boom"), "retries_exhausted"),
])
def test_failures_without_response_map_to_the_error_enum(env, exception, expected):
    submit(env, A, ["https://dead.example/0", "https://dead.example/1"])
    consumer, requests = env.consumer()
    fail(consumer, requests[0], exception)
    assert consumer.slots["dead.example"].latency == 1.0, "No response, no latency sample"
    flush(env)
    [record] = records(env, A)
    assert record["error"] == expected
    assert record["fetched_at"] is None and record["status_code"] is None and record["final_url"] is None


@pytest.mark.parametrize("close", ["cancel", "expire"])
def test_every_accepted_url_gets_a_terminal_record_after_cancel_or_expiry(env, close):
    submit(env, A, [f"https://example.com/{i}" for i in range(6)])
    consumer, requests = env.consumer(concurrency=1, close_chunk=2)
    assert len(requests) == 1
    if close == "cancel":
        atomic_write_json(env.paths.cancelled(A), {})
    else:
        meta = json.loads(env.paths.meta(A).read_text())
        meta["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        atomic_write_json(env.paths.meta(A), meta)
    for _ in range(4):
        consumer.poll()
    complete(consumer, requests[0])
    assert len(requests) == 1, "Nothing more is dispatched"
    submit(env, A, ["https://example.com/late"], batch="late")
    for _ in range(2):
        consumer.poll()
    flush(env)
    got = records(env, A)
    reason = "cancelled" if close == "cancel" else "expired"
    expected = {f"b-{i}" for i in range(6)} | ({"late-0"} if close == "expire" else set())
    assert {record["id"] for record in got} == expected
    errors = [record["error"] for record in got]
    assert errors.count(None) == 1 and errors.count(reason) == len(expected) - 1
    assert all(record["fetched_at"] is None for record in got if record["error"] == reason)
    assert not rows(env)
    assert A not in consumer.requests


def test_cancelled_request_inbox_is_dropped_and_removed_request_is_forgotten(env):
    submit(env, A, ["https://example.com/0"])
    atomic_write_json(env.paths.cancelled(A), {})
    submit(env, B, ["https://example.org/0", "https://example.org/1"])
    consumer, requests = env.consumer(concurrency=1)
    assert not list(env.paths.inbox(0).glob("*.json"))
    assert [request_id for request_id, *_ in rows(env)] == [B, B]
    env.paths.meta(B).unlink()  # Removed by GC.
    consumer.poll()
    complete(consumer, requests[0])
    assert len(requests) == 1 and not rows(env) and not consumer.requests
    assert not env.paths.segments(B).exists()


def test_malformed_inbox_file_is_set_aside(env):
    env.paths.inbox(0).mkdir(parents=True)
    (env.paths.inbox(0) / f"{A}-x.json").write_text("{broken")
    _, requests = env.consumer()
    assert not requests
    assert (env.paths.inbox(0) / f"{A}-x.json.bad").exists()


def test_429_blocks_the_slot_and_requeues_without_a_record(env):
    submit(env, A, [f"https://q.example/{i}" for i in range(2)] + ["https://other.example/0"])
    consumer, requests = env.consumer(concurrency=4, max_requeues=1, block_base_s=10)
    first = next(request for request in requests if request.url == "https://q.example/0")
    http_error(consumer, first, 429, {"Retry-After": "120"})
    assert consumer.heartbeat()["manual_blocked_slots"] == 1
    complete(consumer, next(request for request in requests if request.url == "https://q.example/1"))
    assert [request.url for request in requests].count("https://q.example/0") == 1
    env.clock.advance(119)
    assert [request.url for request in requests].count("https://q.example/0") == 1
    env.clock.advance(1)
    retry = requests[-1]
    assert retry.url == "https://q.example/0"
    # Out of requeues: the next 429 is terminal. No Retry-After means base * 2**n, and
    # the 200 for q.example/1 reset n.
    http_error(consumer, retry, 429)
    assert consumer.slots["q.example"].blocked_until == env.clock.seconds() + 10
    submit(env, B, ["https://q.example/new"])
    consumer.poll()
    assert requests[-1] is retry, "A new URL for the blocked host waits too"
    env.clock.advance(10)
    assert requests[-1].url == "https://q.example/new"
    flush(env)
    by_id = {record["id"]: record for record in records(env, A)}
    assert by_id["b-0"]["error"] == "http_429" and by_id["b-0"]["status_code"] == 429
    assert by_id["b-1"]["error"] is None


def test_status_snapshot_reports_counts_top_hosts_and_hourly_done(env):
    submit(env, A, [f"https://big.example/{i}" for i in range(5)] + ["https://small.example/0"])
    consumer, requests = env.consumer(concurrency=2, domain_concurrency=1, status_s=1, top_hosts=1)
    complete(consumer, requests[0])
    env.clock.advance(1)
    snapshot = json.loads(env.paths.status(0).read_text())
    entry = snapshot["requests"][A]
    assert snapshot["crawler_id"] == 0 and snapshot["floor_paused"] is False
    assert entry["queued"] + entry["leased"] == 5
    assert entry["leased"] >= 2 and entry["paused"] is None
    assert entry["top_hosts"] == [{"host": "big.example", "remaining": 4, "done_1h": 1}]
    env.clock.advance(3600)
    assert json.loads(env.paths.status(0).read_text())["requests"][A]["top_hosts"][0]["done_1h"] == 0


@pytest.mark.parametrize("concurrency", [256, 10, 1])
def test_manual_alone_can_fill_all_download_capacity(env, concurrency):
    submit(env, A, ["https://example.com/a"])
    submit(env, B, [f"https://example.com/b/{i}" for i in range(300)])
    consumer, requests = env.consumer(concurrency=concurrency)
    assert len(requests) == len(consumer.inflight) == concurrency
    complete(consumer, requests[0])
    assert len(requests) == concurrency + 1 and len(consumer.inflight) == concurrency


@pytest.mark.parametrize("headroom", [0, 1, 2])
# (2.0, 0.5, 4) is AutoThrottle at TARGET_CONCURRENCY 4: the cap follows the target.
@pytest.mark.parametrize("latency,delay,in_transfer",
                         [(2.5, 1.0, 3), (3.5, 1.75, 2), (1.0, 4.0, 1), (2.0, 0.5, 4)])
def test_host_cap_follows_the_slot_delay(env, headroom, latency, delay, in_transfer):
    submit(env, A, [f"https://slow.example/{i}" for i in range(30)])
    consumer, requests = held(env, concurrency=32, host_headroom=headroom)
    slots(consumer, **{"slow.example": (16, delay)})
    consumer.slots["slow.example"].latency = latency
    consumer.poll()
    assert len(requests) == in_transfer + headroom


def test_host_cap_never_exceeds_slot_concurrency_and_zero_delay_is_uncapped(env):
    submit(env, A, [f"https://narrow.example/{i}" for i in range(30)])
    submit(env, B, [f"https://open.example/{i}" for i in range(30)])
    consumer, _ = held(env, concurrency=32)
    slots(consumer, **{"narrow.example": (2, 0.1), "open.example": (12, 0)})
    consumer.slots["narrow.example"].latency = 5.0
    consumer.slots["open.example"].latency = 5.0
    consumer.poll()
    assert consumer.slots["narrow.example"].outstanding == 2
    assert consumer.slots["open.example"].outstanding == 12


def test_longer_delay_shrinks_the_host_cap_without_cancelling_requests(env):
    submit(env, A, [f"https://slow.example/{i}" for i in range(30)])
    consumer, requests = held(env, concurrency=32, host_headroom=0)
    slots(consumer, **{"slow.example": (16, 1.0)})
    consumer.slots["slow.example"].latency = 2.5
    consumer.poll()
    assert len(requests) == 3
    consumer.spider.crawler.engine.downloader.slots["slow.example"].delay = 5.0
    consumer.poll()
    complete(consumer, requests[0])
    complete(consumer, requests[1])
    assert len(requests) == 3, "Two outstanding still exceed the new cap of one"
    complete(consumer, requests[2])
    assert len(requests) == 4


def test_live_slot_settings_override_static_config_and_refresh_on_poll(env):
    submit(env, A, [f"https://a.example/{i}" for i in range(20)])
    submit(env, B, [f"https://b.example/{i}" for i in range(4)])
    consumer, requests = held(env, concurrency=4, slot_settings={
        "a.example": {"delay": 100, "concurrency": 4}, "b.example": {"delay": 1, "concurrency": 4}})
    slots(consumer, **{"a.example": (1, 0), "b.example": (0, 100)})
    consumer.poll()
    assert hosts(requests) == ["a.example"]
    consumer.spider.crawler.engine.downloader.slots["b.example"].concurrency = 2
    consumer.poll()
    assert hosts(requests) == ["a.example", "b.example", "b.example"]


def test_service_time_prioritizes_a_smaller_but_slower_slot(env):
    submit(env, A, [f"https://fast.example/{i}" for i in range(10)] + [f"https://slow.example/{i}" for i in range(3)])
    consumer, requests = env.consumer(concurrency=1, slot_settings={
        "fast.example": {"delay": 1, "concurrency": 1}, "slow.example": {"delay": 10, "concurrency": 1}})
    assert requests[0].url == "https://slow.example/0", "30s of work outranks 10s"
    complete(consumer, requests[0])
    assert requests[1].url == "https://slow.example/1", "Keep the bottleneck supplied at its QPS"


def test_measured_latency_updates_the_estimate(env):
    submit(env, A, [f"https://serial.example/{i}" for i in range(10)] + [f"https://parallel.example/{i}" for i in range(20)])
    consumer, requests = env.consumer(concurrency=1, slot_settings={
        "serial.example": {"delay": 0, "concurrency": 1}, "parallel.example": {"delay": 0, "concurrency": 10}})
    assert requests[0].url.startswith("https://serial.example/")
    requests[0].meta["download_latency"] = 5.0
    complete(consumer, requests[0])
    assert consumer.slots["serial.example"].latency == pytest.approx(1.8)
    assert requests[1].url.startswith("https://serial.example/")


@pytest.mark.parametrize("a_jitter,b_jitter,first", [(0.1, 0.9, "b"), (0.9, 0.1, "a")])
def test_fixed_slot_jitter_breaks_ties(env, a_jitter, b_jitter, first):
    submit(env, A, [f"https://{host}.example/{i}" for host in ("a", "b") for i in range(3)])
    env.free = 0
    consumer, requests = env.consumer(concurrency=1)
    assert all(0 <= slot.jitter < 1 for slot in consumer.slots.values())
    consumer.slots["a.example"].jitter = a_jitter
    consumer.slots["b.example"].jitter = b_jitter
    for host in ("a.example", "b.example"):
        consumer._refresh_slot(host)
    env.free = 10 ** 15
    consumer.poll()
    assert requests[0].url == f"https://{first}.example/0"
    complete(consumer, requests[0])
    other = "b" if first == "a" else "a"
    assert requests[1].url == f"https://{other}.example/0", "One more second of work outweighs jitter"


def test_scheduling_failure_keeps_the_url(env):
    submit(env, A, ["https://example.com/0"])
    env.free = 0
    consumer, requests = env.consumer()
    calls = []

    def broken(request):
        calls.append(request)
        raise RuntimeError("scheduler unavailable")

    consumer.spider.crawler.engine.crawl = broken
    env.free = 10 ** 15
    consumer.poll()
    assert calls and not consumer.inflight
    consumer.spider.crawler.engine.crawl = requests.append
    consumer.poll()
    assert [request.url for request in requests] == ["https://example.com/0"]


def test_stale_callbacks_after_shutdown_are_ignored(env):
    submit(env, A, ["https://example.com/0"])
    consumer, requests = env.consumer()
    consumer.close()
    complete(consumer, requests[0])
    consumer.poll()
    assert not consumer.inflight and len(requests) == 1


def test_manual_and_normal_requests_share_the_hostname_slot(env, monkeypatch):
    # One slot per hostname means one AutoThrottle delay, so manual work cannot add a second rate.
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from crawler.spiders.spider import HtmlSpider

    submit(env, A, ["https://www.espn.com/nba/1"])
    consumer, requests = env.consumer()
    crawler = get_crawler(Spider, {"AUTOTHROTTLE_ENABLED": True, "DOWNLOAD_DELAY": 0.25,
                                   "CONCURRENT_REQUESTS_PER_DOMAIN": 16})
    crawler.spider = Spider(name="slot-test")
    downloader = Downloader(crawler)
    spider = SimpleNamespace(_domain_pending={}, parse=None, errback=None)
    normal = HtmlSpider._build_request(spider, "https://www.espn.com/nba/2", 7)
    try:
        manual_key, manual_slot = downloader._get_slot(requests[0])
        normal_key, normal_slot = downloader._get_slot(normal)
        assert manual_key == normal_key == "www.espn.com" == next(iter(consumer.slots))
        assert manual_slot is normal_slot
        assert "download_slot" not in requests[0].meta
    finally:
        downloader.close()


@pytest.mark.parametrize("name,value", [("host_headroom", -1), ("max_requeues", True), ("segment_bytes", 0),
                                        ("poll_s", 0), ("shards", 1.5)])
def test_invalid_settings_fail_at_startup(env, name, value):
    with pytest.raises(ValueError, match=f"MANUAL_BULK_CRAWL_{name.upper()}"):
        env.consumer(**{name: value})


def test_non_html_response_without_text_attribute(env):
    submit(env, A, ["https://example.com/bin"])
    consumer, requests = env.consumer()
    consumer.parse(Response(requests[0].url, request=requests[0], body=b"\x00", headers={"Content-Type": "image/png"}))
    flush(env)
    [record] = records(env, A)
    assert record["error"] == "non_html" and record["html"] is None

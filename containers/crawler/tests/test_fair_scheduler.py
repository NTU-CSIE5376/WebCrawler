"""Both sources get alternating turns, including across memory/disk queues."""
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from scrapy import Request, Spider
from scrapy.settings import Settings
from scrapy.statscollectors import MemoryStatsCollector

from containers.crawler.crawler.fair_scheduler import FairScheduler


@pytest.fixture
def scheduler_factory(tmp_path):
    schedulers = []

    def factory(disk=False, replace=None, policy="balanced", share=None):
        if replace is not None:
            replace.close("restart")
            schedulers.remove(replace)
        settings = Settings({
            "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
            "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
            "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
            "SCHEDULER_START_MEMORY_QUEUE": None,
            "SCHEDULER_START_DISK_QUEUE": None,
        })
        if policy is not None:
            settings.set("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", policy)
        if share is not None:
            settings.set("MANUAL_BULK_CRAWL_SHARE_PERCENT", share)
        if disk:
            settings.set("JOBDIR", str(tmp_path / "job"))
        downloader = SimpleNamespace(slots={}, get_slot_key=lambda request: urlsplit(request.url).hostname)
        crawler = SimpleNamespace(settings=settings, engine=SimpleNamespace(downloader=downloader))
        crawler.stats = MemoryStatsCollector(crawler)
        spider = Spider(name="fair-test")
        crawler.spider = spider
        scheduler = FairScheduler.from_crawler(crawler)
        scheduler.open(spider)
        schedulers.append(scheduler)
        return scheduler

    yield factory
    for scheduler in schedulers:
        scheduler.close("test-finished")


def request(source, index, **kwargs):
    meta = kwargs.pop("meta", {})
    if source == "manual":
        meta["manual_bulk_crawl_job_id"] = "a" * 32
    return Request(f"https://example.com/{source}/{index}", meta=meta, **kwargs)


def source(request):
    return "manual" if "manual_bulk_crawl_job_id" in request.meta else "normal"


def test_round_robin_alternates_sources_and_preserves_fifo(scheduler_factory):
    scheduler = scheduler_factory()
    for i in range(10):
        scheduler.enqueue_request(request("normal", i, meta={"is_start_request": i == 0}))
        scheduler.enqueue_request(request("manual", i))
    dispatched = [scheduler.next_request() for _ in range(20)]
    assert [source(item) for item in dispatched] == ["normal", "manual"] * 10
    for flow in ("normal", "manual"):
        assert [item.url.rsplit("/", 1)[-1] for item in dispatched if source(item) == flow] == list(map(str, range(10)))
    assert scheduler.next_request() is None
    assert not scheduler.has_pending_requests()


def test_continuously_refilled_sources_cannot_starve_each_other(scheduler_factory):
    scheduler = scheduler_factory()
    scheduler.enqueue_request(request("normal", 0))
    scheduler.enqueue_request(request("manual", 0))
    dispatched = []
    for i in range(1000):
        item = scheduler.next_request()
        flow = source(item)
        dispatched.append(flow)
        scheduler.enqueue_request(request(flow, i + 1))
    assert dispatched == ["normal", "manual"] * 500


@pytest.mark.parametrize("flow", ["normal", "manual"])
def test_single_source_uses_all_capacity_and_arriving_source_gets_the_next_turn(scheduler_factory, flow):
    scheduler = scheduler_factory()
    for i in range(20):
        scheduler.enqueue_request(request(flow, i))
    assert [source(scheduler.next_request()) for _ in range(10)] == [flow] * 10
    other = "normal" if flow == "manual" else "manual"
    for i in range(10):
        scheduler.enqueue_request(request(other, i))
    assert [source(scheduler.next_request()) for _ in range(20)] == [other, flow] * 10


def test_disk_normal_work_is_not_starved_by_memory_manual_callbacks(scheduler_factory):
    scheduler = scheduler_factory(disk=True)
    for i in range(4):
        scheduler.enqueue_request(request("normal", i))
        # Like ManualBulkCrawlConsumer.parse, this callback is not a spider
        # method, so Scrapy stores it in memory rather than its disk queue.
        scheduler.enqueue_request(request("manual", i, callback=lambda response: None))
    assert len(scheduler.normal.dqs) == 4
    assert len(scheduler.manual.mqs) == 4
    assert [source(scheduler.next_request()) for _ in range(8)] == ["normal", "manual"] * 4


def test_redirects_and_retries_keep_the_manual_source(scheduler_factory):
    scheduler = scheduler_factory()
    original = request("manual", 0)
    for i in range(4):
        scheduler.enqueue_request(request("normal", i))
        scheduler.enqueue_request(original.replace(url=f"https://example.com/redirect/{i}"))
    assert [source(scheduler.next_request()) for _ in range(8)] == ["normal", "manual"] * 4


def test_each_source_keeps_downloader_aware_domain_selection(scheduler_factory):
    scheduler = scheduler_factory()
    scheduler.normal.crawler.engine.downloader.slots["busy.example"] = SimpleNamespace(active={1, 2})
    for flow in ("normal", "manual"):
        scheduler.enqueue_request(request(flow, 0).replace(url=f"https://busy.example/{flow}"))
        scheduler.enqueue_request(request(flow, 1).replace(url=f"https://idle.example/{flow}"))
    assert [scheduler.next_request().url for _ in range(2)] == [
        "https://idle.example/normal", "https://idle.example/manual",
    ]


def test_both_disk_partitions_resume_after_restart(scheduler_factory):
    scheduler = scheduler_factory(disk=True)
    for i in range(4):
        scheduler.enqueue_request(request("normal", i))
        scheduler.enqueue_request(request("manual", i))
    assert [source(scheduler.next_request()) for _ in range(2)] == ["normal", "manual"]
    resumed = scheduler_factory(disk=True, replace=scheduler)
    assert len(resumed) == 6
    assert [resumed.next_request().url for _ in range(6)] == [
        f"https://example.com/{flow}/{i}" for i in range(1, 4) for flow in ("normal", "manual")
    ]


@pytest.mark.parametrize("policy,expected", [
    ("weighted", ["normal", "manual"] * 3),
    ("balanced", ["normal", "manual"] * 3),
    ("manual_first", ["manual"] * 3 + ["normal"] * 3),
    ("normal_first", ["normal"] * 3 + ["manual"] * 3),
])
@pytest.mark.parametrize("disk", [False, True])
def test_dispatch_policy_preserves_source_preference_across_storage(scheduler_factory, policy, expected, disk):
    scheduler = scheduler_factory(disk=disk, policy=policy)
    for i in range(3):
        scheduler.enqueue_request(request("normal", i))
        scheduler.enqueue_request(request("manual", i))
    assert [source(scheduler.next_request()) for _ in range(6)] == expected
    assert not scheduler.has_pending_requests()


@pytest.mark.parametrize("policy,expected", [
    ("weighted", ["normal", "manual"] * 3),
    ("balanced", ["normal", "manual"] * 3),
    ("manual_first", ["manual"] * 3 + ["normal"] * 3),
    ("normal_first", ["normal"] * 3 + ["manual"] * 3),
])
def test_request_metadata_cannot_override_crawler_policy(scheduler_factory, policy, expected):
    scheduler = scheduler_factory(policy=policy)
    for i, old_policy in enumerate(("normal_first", "balanced", "manual_first")):
        scheduler.enqueue_request(request("manual", i, meta={"manual_bulk_crawl_scheduling_policy": old_policy}))
        scheduler.enqueue_request(request("normal", i))
    assert len(scheduler.manual) == 3
    assert len(scheduler.normal) == 3
    assert [source(scheduler.next_request()) for _ in range(6)] == expected


@pytest.mark.parametrize("policy", ["manual_first", "normal_first"])
def test_priority_policy_survives_retry_and_restart(scheduler_factory, policy):
    scheduler = scheduler_factory(disk=True, policy=policy)
    original = request("manual", 0)
    scheduler.enqueue_request(original.replace(url="https://example.com/retry"))
    scheduler.enqueue_request(request("normal", 0))
    resumed = scheduler_factory(disk=True, replace=scheduler, policy=policy)
    expected = ["manual", "normal"] if policy == "manual_first" else ["normal", "manual"]
    assert [source(resumed.next_request()) for _ in range(2)] == expected


@pytest.mark.parametrize("policy", ["unknown", "", 1, True])
def test_invalid_crawler_policy_is_rejected_at_startup(scheduler_factory, policy):
    with pytest.raises(ValueError, match="MANUAL_BULK_CRAWL_SCHEDULING_POLICY"):
        scheduler_factory(policy=policy)


@pytest.mark.parametrize("policy", ["balanced", "manual_first", "normal_first"])
def test_restart_applies_current_policy_to_already_queued_requests(scheduler_factory, policy):
    scheduler = scheduler_factory(disk=True, policy="manual_first")
    scheduler.enqueue_request(request("manual", 0))
    scheduler.enqueue_request(request("normal", 0))
    resumed = scheduler_factory(disk=True, replace=scheduler, policy=policy)
    expected = ["manual", "normal"] if policy == "manual_first" else ["normal", "manual"]
    assert [source(resumed.next_request()) for _ in range(2)] == expected


def test_unspecified_policy_defaults_to_weighted_half(scheduler_factory):
    scheduler = scheduler_factory(policy=None)
    for i in range(3):
        scheduler.enqueue_request(request("manual", i))
        scheduler.enqueue_request(request("normal", i))
    assert scheduler.scheduling_policy == "weighted"
    assert [source(scheduler.next_request()) for _ in range(6)] == ["normal", "manual"] * 3


@pytest.mark.parametrize("policy", [None, "weighted", "balanced", "manual_first", "normal_first"])
@pytest.mark.parametrize("share", [None, "30"])
def test_crawler_environment_sets_dispatch_policy(monkeypatch, policy, share):
    import runpy

    monkeypatch.setenv("CRAWLER_USE_AUTOTHROTTLE", "true")
    for name, value in (("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", policy),
                        ("MANUAL_BULK_CRAWL_SHARE_PERCENT", share)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    settings = runpy.run_module("containers.crawler.crawler.settings")
    assert settings["MANUAL_BULK_CRAWL_SCHEDULING_POLICY"] == (policy or "weighted")
    assert settings["MANUAL_BULK_CRAWL_SHARE_PERCENT"] == int(share or 50)


def refilled(scheduler, dispatches):
    """Dispatches with both sources always having work; each served request is replaced."""
    for flow in ("normal", "manual"):
        scheduler.enqueue_request(request(flow, 0))
    served = []
    for i in range(dispatches):
        flow = source(scheduler.next_request())
        served.append(flow)
        scheduler.enqueue_request(request(flow, i + 1))
    return served


def longest_run(served, flow):
    best = run = 0
    for item in served:
        run = run + 1 if item == flow else 0
        best = max(best, run)
    return best


@pytest.mark.parametrize("share", [20, 25, 50, 75])
def test_weighted_gives_manual_its_share_while_both_have_work(scheduler_factory, share):
    scheduler = scheduler_factory(policy="weighted", share=share)
    served = refilled(scheduler, 10_000)
    assert abs(served.count("manual") - share * 100) <= 1
    majority = "normal" if share < 50 else "manual"
    stride_ratio = max(share, 100 - share) // min(share, 100 - share)
    assert longest_run(served, majority) <= stride_ratio
    assert longest_run(served, "manual" if majority == "normal" else "normal") == 1


@pytest.mark.parametrize("alone", ["normal", "manual"])
def test_weighted_alternates_once_a_lone_source_gets_company(scheduler_factory, alone):
    scheduler = scheduler_factory(policy="weighted", share=50)
    for i in range(5):
        scheduler.enqueue_request(request(alone, i))
    assert [source(scheduler.next_request()) for _ in range(5)] == [alone] * 5
    other = "normal" if alone == "manual" else "manual"
    for i in range(5, 10):
        scheduler.enqueue_request(request(alone, i))
        scheduler.enqueue_request(request(other, i))
    assert [source(scheduler.next_request()) for _ in range(10)] == [other, alone] * 5


@pytest.mark.parametrize("idle", ["normal", "manual"])
def test_weighted_idle_source_does_not_bank_turns(scheduler_factory, idle):
    scheduler = scheduler_factory(policy="weighted", share=25)
    busy = "manual" if idle == "normal" else "normal"
    for i in range(1000):
        scheduler.enqueue_request(request(busy, i))
        assert source(scheduler.next_request()) == busy
    served = refilled(scheduler, 100)
    assert 24 <= served.count("manual") <= 26
    assert longest_run(served, idle) <= 3


@pytest.mark.parametrize("share", [0, 100, -1, True, "50", 50.5])
def test_invalid_share_is_rejected_at_startup(scheduler_factory, share):
    with pytest.raises(ValueError, match="MANUAL_BULK_CRAWL_SHARE_PERCENT"):
        scheduler_factory(policy="weighted", share=share)

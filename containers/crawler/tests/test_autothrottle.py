from types import SimpleNamespace

import pytest
from scrapy import Request, Spider
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy.settings import Settings

from containers.crawler.crawler.autothrottle import SiteAutoThrottle
from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
from containers.crawler.crawler.site_policies import SitePolicies


@pytest.fixture
def policies():
    return SitePolicies({
        "default": {"crawler_mode": "autothrottle", "request_limits": {"max_qps": 4}},
        "domains": {"wikipedia.org": {"request_limits": {"max_qps": 20}},
                    "example.com": {"crawler_mode": "fixed", "request_limits": {"max_qps": 3}}},
        "hosts": {"slow.wikipedia.org": {"request_limits": {"max_qps": 1}},
                  "auto.example.com": {"crawler_mode": "autothrottle", "request_limits": {"max_qps": 10}}},
    })


@pytest.fixture
def throttle(policies, monkeypatch):
    monkeypatch.setattr(SitePolicies, "load", classmethod(lambda cls: policies))
    crawler = Crawler(Spider, Settings({"AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2, "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_START_DELAY": 0.5}))
    crawler.engine = SimpleNamespace(downloader=SimpleNamespace())
    extension = SiteAutoThrottle(crawler)
    extension._spider_opened(Spider("test"))
    return extension


@pytest.mark.parametrize("hostname,minimum", [
    ("en.wikipedia.org", 0.05), ("fr.wikipedia.org", 0.05),
    ("unmatched.org", 0.25), ("slow.wikipedia.org", 1), ("auto.example.com", 0.1),
])
def test_autothrottle_reaches_each_sites_minimum(throttle, hostname, minimum):
    slot = SimpleNamespace(delay=2)
    response = Response(f"https://{hostname}/wiki/Test")
    for _ in range(50):
        throttle._adjust_delay(slot, 0.001, response)
    assert slot.delay == pytest.approx(minimum)


def test_fixed_policy_does_not_adapt(throttle):
    slot = SimpleNamespace(delay=0.333)
    throttle._adjust_delay(slot, 10, Response("https://example.com/wiki/Test"))
    assert slot.delay == 0.333


def test_slow_and_error_responses_preserve_scrapy_backoff(throttle):
    slot = SimpleNamespace(delay=0.05)
    throttle._adjust_delay(slot, 2, Response("https://en.wikipedia.org/wiki/Test", status=429))
    assert slot.delay == 1
    throttle._adjust_delay(slot, 0.001, Response("https://en.wikipedia.org/wiki/Test", status=429))
    assert slot.delay == 1
    throttle._adjust_delay(slot, 0.001, Response("https://en.wikipedia.org/wiki/Test", status=200))
    assert 0.05 < slot.delay < 1


def test_fixed_mode_is_inherited_and_redirects_restore_auto_mode(policies):
    dispatcher = SiteMiddlewareDispatcher(policies)
    fixed = Request("https://example.com/wiki/Test")
    dispatcher.process_request(fixed)
    assert fixed.meta["autothrottle_dont_adjust_delay"] is True
    assert fixed.meta["download_slot"] == "example.com"
    redirected = fixed.replace(url="https://auto.example.com/wiki/Test")
    dispatcher.process_request(redirected)
    assert "autothrottle_dont_adjust_delay" not in redirected.meta
    assert redirected.meta["download_slot"] == "auto.example.com"
    unmatched = redirected.replace(url="https://unmatched.org/wiki/Test")
    dispatcher.process_request(unmatched)
    assert "autothrottle_dont_adjust_delay" not in unmatched.meta
    assert "download_slot" not in unmatched.meta


def test_slot_start_delays_and_fixed_override(policies):
    slots = policies.download_slots(start_delay=0.5)
    assert slots["wikipedia.org"]["delay"] == 0.5
    assert slots["example.com"]["delay"] == 1 / 3
    assert slots["auto.example.com"]["delay"] == 0.5
    assert policies.resolve("auto.example.com").request_limits.min_request_delay_sec == 0.1


@pytest.mark.parametrize("section", ["default", "domains", "hosts"])
def test_invalid_crawler_mode_fails_validation(section):
    rule = {"crawler_mode": "unknown"}
    document = {section: rule if section == "default" else {"example.com": rule}}
    with pytest.raises(ValueError):
        SitePolicies(document)


def test_global_fixed_mode_can_have_an_autothrottle_domain(monkeypatch):
    policies = SitePolicies({
        "default": {"crawler_mode": "fixed", "request_limits": {"max_qps": 1}},
        "domains": {"example.com": {"crawler_mode": "autothrottle", "request_limits": {"max_qps": 20}}},
    })
    monkeypatch.setattr(SitePolicies, "load", classmethod(lambda cls: policies))
    crawler = Crawler(Spider, Settings({"AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2, "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_START_DELAY": 0.5}))
    crawler.engine = SimpleNamespace(downloader=SimpleNamespace())
    extension = SiteAutoThrottle(crawler)
    extension._spider_opened(Spider("test"))
    assert extension._start_delay() == 1
    assert policies.resolve("unmatched.org").crawler_mode == "fixed"
    slot = SimpleNamespace(delay=1)
    for _ in range(50):
        extension._adjust_delay(slot, 0.001, Response("https://example.com/wiki/Test"))
    assert slot.delay == 0.05


def test_host_mode_only_override_inherits_domain_limits():
    policies = SitePolicies({
        "domains": {"example.com": {"request_limits": {"max_qps": 20, "max_concurrent_requests": 10}}},
        "hosts": {"fixed.example.com": {"crawler_mode": "fixed"}},
    })
    policy = policies.resolve("fixed.example.com")
    assert policy.crawler_mode == "fixed"
    assert policy.request_limits.min_request_delay_sec == 0.05
    assert policy.request_limits.max_concurrent_requests == 10
    assert policies.download_slots()["fixed.example.com"] == {"delay": 0.05, "concurrency": 10}

"""The Wikimedia concurrency bound must hold across OS processes."""
import asyncio
from pathlib import Path
import subprocess
import sys

import pytest
from scrapy import Request
from scrapy.http import Response

from containers.crawler.crawler.middlewares.shared_concurrency import RequestBudget, SharedConcurrencyMiddleware
from containers.crawler.crawler.site_policies import SitePolicies


def policies():
    return SitePolicies({"domains": {"wikipedia.org": {"request_limits": {
        "max_concurrent_requests": 9}}}})


def budget(directory):
    return RequestBudget("domain:wikipedia.org", 9, directory)


@pytest.mark.parametrize("limit", [7, 9])
def test_sixteen_processes_share_budget_and_crash_releases_one(tmp_path, limit):
    code = """
import sys
from containers.crawler.crawler.middlewares.shared_concurrency import RequestBudget
lease = RequestBudget('domain:wikipedia.org', int(sys.argv[2]), sys.argv[1]).try_acquire()
print('acquired' if lease is not None else 'full', flush=True)
sys.stdin.read(1)
"""
    workers = []
    try:
        for _ in range(16):
            workers.append(subprocess.Popen(
                [sys.executable, "-c", code, str(tmp_path), str(limit)],
                cwd=Path(__file__).resolve().parents[3],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
            ))
        results = [worker.stdout.readline().strip() for worker in workers]
        assert results.count("acquired") == limit
        assert results.count("full") == 16 - limit
        shared = RequestBudget("domain:wikipedia.org", limit, tmp_path)
        assert shared.try_acquire() is None
        victim = workers[results.index("acquired")]
        victim.kill()
        victim.wait(timeout=5)
        lease = shared.try_acquire()
        assert lease is not None
        try:
            assert shared.try_acquire() is None
        finally:
            lease.close()
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.communicate("\n", timeout=10)


def test_budget_waits_without_blocking_other_domains_and_releases_on_all_paths(tmp_path):
    async def exercise():
        middleware = SharedConcurrencyMiddleware(policies(), tmp_path)
        requests = [Request(f"https://lang{i}.wikipedia.org/wiki/Test") for i in range(9)]
        for request in requests:
            await middleware.process_request(request)
        waiting = Request("https://zh.wikipedia.org/robots.txt")
        task = asyncio.create_task(middleware.process_request(waiting))
        await asyncio.sleep(0.01)
        assert not task.done()
        await asyncio.wait_for(middleware.process_request(Request("https://example.org/")), 1)
        # 429/redirect responses must release before Retry/RedirectMiddleware.
        response = Response(requests[0].url, status=429)
        assert middleware.process_response(requests[0], response) is response
        await asyncio.wait_for(task, 1)
        assert len(middleware.leases) == 9
        middleware.process_exception(waiting, TimeoutError())
        retry = waiting.copy()
        await middleware.process_request(retry)
        assert len(middleware.leases) == 9
        middleware.close()
        assert not middleware.leases
        lease = budget(tmp_path).try_acquire()
        assert lease is not None
        lease.close()

    asyncio.run(exercise())


def test_lock_failure_fails_closed(tmp_path, monkeypatch):
    shared = budget(tmp_path)

    def denied():
        raise PermissionError("unavailable shared filesystem")

    monkeypatch.setattr(shared, "try_acquire", denied)
    middleware = SharedConcurrencyMiddleware(policies(), tmp_path)
    middleware.budgets["domain:wikipedia.org"] = shared
    with pytest.raises(PermissionError):
        asyncio.run(middleware.process_request(Request("https://en.wikipedia.org/wiki/Test")))
    assert not middleware.leases


def test_cancelling_a_wait_does_not_leak_a_budget(tmp_path):
    async def exercise():
        registry = SitePolicies({
            "domains": {"example.com": {"request_limits": {"max_concurrent_requests": 1}}},
        })
        held = RequestBudget("domain:example.com", 1, tmp_path).try_acquire()
        middleware = SharedConcurrencyMiddleware(registry, tmp_path)
        waiting = asyncio.create_task(middleware.process_request(Request("https://example.com/")))
        try:
            await asyncio.sleep(0.01)
            assert not waiting.done()
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            assert not middleware.leases
            held.close()
            available = RequestBudget("domain:example.com", 1, tmp_path).try_acquire()
            assert available is not None
            available.close()
        finally:
            held.close()
            middleware.close()

    asyncio.run(exercise())


def test_generic_domain_budget_cannot_be_relaxed_by_host_policy():
    registry = SitePolicies({"domains": {"example.com": {"request_limits": {
        "max_concurrent_requests": 3}}}, "hosts": {
            "fast.example.com": {"request_limits": None},
            "slow.example.com": {"request_limits": {"max_concurrent_requests": 1}},
        }})
    assert registry.shared_budgets("EXAMPLE.COM.") == (("domain:example.com", 3),)
    assert registry.shared_budgets("fast.example.com") == (("domain:example.com", 3),)
    assert registry.shared_budgets("slow.example.com") == (("domain:example.com", 3),)
    assert registry.resolve("slow.example.com").request_limits.max_concurrent_requests == 1
    assert registry.shared_budgets("example.com.evil.org") == ()


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_domain_limits_rejected(limit):
    with pytest.raises(ValueError):
        SitePolicies({"domains": {"example.com": {"request_limits": {
            "max_concurrent_requests": limit}}}})


def test_only_explicit_domain_concurrency_enables_shared_budget():
    registry = SitePolicies({
        "default": {"request_limits": {"max_concurrent_requests": 20}},
        "domains": {"example.com": {"request_limits": {"max_qps": 1}}},
        "hosts": {"host.example.org": {"request_limits": {"max_concurrent_requests": 2}}},
    })
    assert registry.shared_budgets("example.com") == ()
    assert registry.shared_budgets("host.example.org") == ()
    assert registry.shared_budgets("unconfigured.org") == ()



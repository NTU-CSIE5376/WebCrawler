from pathlib import Path

import pytest
from scrapy import Request
from scrapy.exceptions import IgnoreRequest

from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher
from containers.crawler.crawler.site_policies import SitePolicies


@pytest.fixture
def dispatcher(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(root))
    monkeypatch.chdir(root)
    return SiteMiddlewareDispatcher(SitePolicies.load())


@pytest.mark.parametrize("path", ["/members/insider/", "/login?next=/nba/", "/personalization/"])
def test_non_content_endpoints_stop_before_downloading(dispatcher, path):
    with pytest.raises(IgnoreRequest, match="Non-content"):
        dispatcher.process_request(Request("https://www.espn.com" + path))


@pytest.mark.parametrize("url", [
    "https://www.espn.com/nba/game/_/gameId/401?platform=web#score",
    "https://support.espn.com/members/guide/",
    "https://www.espn.com/robots.txt",
])
@pytest.mark.parametrize("manual", [False, True])
def test_content_and_robots_keep_url_and_request_context(dispatcher, url, manual):
    def callback(response):
        pass

    meta = {"manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 3} if manual else {}
    request = Request(url, callback=callback, cb_kwargs={"value": 1}, priority=7,
                      headers={"Cookie": "session=private"}, meta=meta)
    original_url = request.url
    assert dispatcher.process_request(request) is None
    assert request.url == original_url
    assert request.callback is callback
    assert request.cb_kwargs == {"value": 1}
    assert request.priority == 7
    assert request.headers["User-Agent"].startswith(b"WebCrawlerBot/")
    assert request.headers.getlist("Sec-Fetch-Site") == []
    assert request.headers["Accept-Encoding"] == b"gzip"
    assert request.headers.getlist("Cookie") == []
    assert request.meta["dont_merge_cookies"] is True
    assert "dont_obey_robotstxt" not in request.meta
    assert "dont_retry" not in request.meta
    for key, value in meta.items():
        assert request.meta[key] == value


def test_espn_limits_are_shared_without_changing_other_sites(dispatcher):
    policies = dispatcher.policies
    for hostname in ("espn.com", "www.espn.com", "scores.espn.com"):
        request = Request(f"https://{hostname}/nba/")
        dispatcher.process_request(request)
        assert request.meta["download_slot"] == "espn.com"
        limits = policies.resolve(hostname).request_limits
        assert limits.target_concurrent_requests == 32
        assert limits.max_qps == 64
        assert limits.max_concurrent_requests == 16
        assert policies.shared_budgets(hostname) == (("domain:espn.com", 16),)
    assert policies.resolve("example.com").request_limits.target_concurrent_requests is None
    assert policies.resolve("example.com").request_limits.max_qps == 4
    assert policies.shared_budgets("example.com") == ()

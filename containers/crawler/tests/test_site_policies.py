from pathlib import Path
import pytest
from scrapy import Request, Spider
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy.settings import Settings
from scrapy.downloadermiddlewares.defaultheaders import DefaultHeadersMiddleware
from scrapy.downloadermiddlewares.downloadtimeout import DownloadTimeoutMiddleware
from scrapy.downloadermiddlewares.redirect import RedirectMiddleware
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware

from containers.crawler.crawler.site_policies import SitePolicies
from containers.crawler.crawler.middlewares.wikipedia import WikipediaMiddleware
from containers.crawler.crawler.middlewares.site_dispatcher import SiteMiddlewareDispatcher


WIKIPEDIA_MIDDLEWARE = f"{WikipediaMiddleware.__module__}.WikipediaMiddleware"


class ExactHostMiddleware:
    def process_request(self, request, policy):
        request.headers["X-Handler"] = "exact-host"


def apply(middleware, request):
    middleware.process_request(request)
    # These run after the site policy in the real downloader middleware chain.
    DownloadTimeoutMiddleware(15).process_request(request)
    DefaultHeadersMiddleware({"Accept": "text/html", "Sec-Fetch-Site": "none"}.items()).process_request(request)
    UserAgentMiddleware("DefaultCrawler").process_request(request)
    return request


def redirect(request, url):
    response = Response(request.url, status=302, headers={"Location": url})
    crawler = Crawler(Spider, Settings())
    crawler.spider = Spider("test")
    return RedirectMiddleware.from_crawler(crawler).process_response(request, response)


@pytest.fixture
def middleware():
    return SiteMiddlewareDispatcher(SitePolicies({
        "domains": {"wikipedia.org": {
            "middleware": WIKIPEDIA_MIDDLEWARE,
            "headers": {"User-Agent": "WikiBot", "Sec-Fetch-Site": None, "X-Site": "wiki"},
            "download_timeout_sec": 30,
        }},
        "hosts": {"en.wikipedia.org": {"headers": {"user-agent": "EnglishWikiBot"}},
                  "other.example.com": {"headers": {"User-Agent": "OtherBot"}}},
    }))


@pytest.mark.parametrize("meta", [{}, {"manual_bulk_crawl_job_id": "job", "manual_bulk_crawl_index": 0}])
def test_host_overrides_domain_and_suppresses_default_headers(middleware, meta):
    request = apply(middleware, Request("https://en.wikipedia.org/wiki/Test", meta=meta))
    assert request.headers["User-Agent"] == b"EnglishWikiBot"
    assert request.headers["X-Site"] == b"wiki"
    assert request.headers["Accept"] == b"text/html"
    assert request.headers.getlist("Sec-Fetch-Site") == []
    assert b"Sec-Fetch-Site" not in request.headers.to_string()
    assert request.meta["download_timeout"] == 30


@pytest.mark.parametrize("hostname,expected", [
    ("fr.wikipedia.org", b"WikiBot"), ("wikipedia.org", b"WikiBot"),
    ("EN.WIKIPEDIA.ORG.", b"EnglishWikiBot"),
    ("wikipedia.org.example.com", b"DefaultCrawler"),
    ("notwikipedia.org", b"DefaultCrawler"), ("example.com", b"DefaultCrawler"),
    ("other.example.com", b"DefaultCrawler"),
    ("[::1]", b"DefaultCrawler"),
])
def test_domain_matching_and_unmatched_requests(middleware, hostname, expected):
    request = apply(middleware, Request(f"https://{hostname}/wiki/Page"))
    assert request.headers["User-Agent"] == expected


def test_redirects_reapply_destination_policy_and_restore_defaults(middleware):
    initial = apply(middleware, Request("https://en.wikipedia.org/wiki/Test"))
    french = apply(middleware, redirect(initial, "https://fr.wikipedia.org/wiki/Test"))
    assert french.headers["User-Agent"] == b"WikiBot"
    other = apply(middleware, redirect(french, "https://other.example.com/wiki/Page"))
    assert other.headers["User-Agent"] == b"DefaultCrawler"
    assert "X-Site" not in other.headers
    assert other.headers["Sec-Fetch-Site"] == b"none"
    assert other.meta["download_timeout"] == 15
    unmatched = apply(middleware, redirect(other, "https://unmatched.example.com/wiki/Page"))
    assert unmatched.headers["User-Agent"] == b"DefaultCrawler"
    assert initial.headers["User-Agent"] == b"EnglishWikiBot"


def test_retry_and_repeated_application_preserve_original_request_overrides(middleware):
    initial = apply(middleware, Request("https://en.wikipedia.org/wiki/Test",
                    headers={"User-Agent": "RequestBot", "Sec-Fetch-Site": "same-origin"},
                    meta={"download_timeout": 45}))
    retry = apply(middleware, initial.copy())
    apply(middleware, retry)
    assert retry.headers["User-Agent"] == b"EnglishWikiBot"
    unmatched = apply(middleware, redirect(retry, "https://example.com/wiki/Page"))
    assert unmatched.headers["User-Agent"] == b"RequestBot"
    assert unmatched.headers["Sec-Fetch-Site"] == b"same-origin"
    assert unmatched.meta["download_timeout"] == 45


def test_policy_header_restoration_does_not_restore_credentials_on_cross_origin_redirect():
    middleware = SiteMiddlewareDispatcher(SitePolicies({"domains": {"wikipedia.org": {
            "middleware": WIKIPEDIA_MIDDLEWARE,
        "headers": {"Authorization": "Bearer policy"},
    }}}))
    initial = apply(middleware, Request("https://en.wikipedia.org/wiki/Page",
                                      headers={"Authorization": "Bearer original"}))
    unmatched = apply(middleware, redirect(initial, "https://example.com/wiki/Page"))
    assert "Authorization" not in unmatched.headers


def test_unicode_domains_and_exact_hostname_timeout():
    policies = SitePolicies({"domains": {"BÜCHER.DE.": {"headers": {"X-Site": "books"}}},
                             "hosts": {"www.xn--bcher-kva.de": {"download_timeout_sec": 10}}})
    policy = policies.resolve("www.xn--bcher-kva.de")
    assert policy.headers[b"X-Site"] == (b"books",)
    assert policy.download_timeout_sec == 10


def test_domain_to_middleware_binding_is_controlled_entirely_by_configuration():
    middleware = SiteMiddlewareDispatcher(SitePolicies({"domains": {"example.com": {
        "middleware": WIKIPEDIA_MIDDLEWARE, "headers": {"User-Agent": "ConfiguredBot"},
    }}}))
    request = apply(middleware, Request("https://sub.example.com/wiki/Page"))
    assert request.headers["User-Agent"] == b"ConfiguredBot"
    wikipedia = apply(middleware, Request("https://en.wikipedia.org/wiki/Test"))
    assert wikipedia.headers["User-Agent"] == b"DefaultCrawler"


def test_exact_hostname_can_replace_or_disable_a_domain_middleware():
    middleware = SiteMiddlewareDispatcher(SitePolicies({
        "domains": {"example.com": {
            "middleware": WIKIPEDIA_MIDDLEWARE, "headers": {"User-Agent": "DomainBot"},
        }},
        "hosts": {
            "api.example.com": {"middleware": f"{__name__}.ExactHostMiddleware"},
            "disabled.example.com": {"middleware": None},
        },
    }))
    original = apply(middleware, Request("https://www.example.com/wiki/Page"))
    assert original.headers["User-Agent"] == b"DomainBot"
    overridden = apply(middleware, redirect(original, "https://api.example.com/wiki/Page"))
    assert overridden.headers["X-Handler"] == b"exact-host"
    assert overridden.headers["User-Agent"] == b"DefaultCrawler"
    disabled = apply(middleware, redirect(overridden, "https://disabled.example.com/wiki/Page"))
    assert disabled.headers["User-Agent"] == b"DefaultCrawler"
    assert "X-Handler" not in disabled.headers


def test_invalid_middleware_binding_fails_at_startup():
    policies = SitePolicies({"hosts": {"example.com": {"middleware": "builtins.object"}}})
    with pytest.raises(ValueError, match="must implement process_request"):
        SiteMiddlewareDispatcher(policies)


def test_load_policy_from_container_config_directory(monkeypatch):
    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    policies = SitePolicies.load()
    assert b"WebCrawlerBot/" in policies.resolve("en.wikipedia.org").headers[b"User-Agent"][0]
    assert policies.resolve("en.wikipedia.org").middleware == "crawler.middlewares.wikipedia.WikipediaMiddleware"


@pytest.mark.parametrize("document", [
    [], {"unknown": {}}, {"hosts": []}, {"hosts": {"example.com": None}},
    {"domains": {"en.wikipedia.org": {}}}, {"domains": {"co.uk": {}}},
    {"hosts": {"https://example.com": {}}}, {"hosts": {"*.example.com": {}}},
    *({"hosts": {hostname: {}}} for hostname in
      ["", "example.com..", "-bad.example.com", "bad_name.example.com", "a" * 64 + ".com"]),
    {"hosts": {"example.com": {"limits": {}}}},
    {"hosts": {"example.com": {"middleware": ""}}},
    {"hosts": {"example.com": {"middleware": False}}},
    {"hosts": {"example.com": {"headers": []}}},
    {"hosts": {"example.com": {"headers": {"Bad Header": "value"}}}},
    {"hosts": {"example.com": {"headers": {"X-Test": "bad\r\nheader"}}}},
    {"hosts": {"example.com": {"headers": {"X-Test": "trailing\n"}}}},
    {"hosts": {"example.com": {"headers": {"X-Test": 1}}}},
    {"hosts": {"EXAMPLE.COM": {}, "example.com.": {}}},
    *({"hosts": {"example.com": {"download_timeout_sec": timeout}}}
      for timeout in [True, 0, -1, "30", float("inf"), float("nan")]),
])
def test_invalid_policies_fail_at_startup(document):
    with pytest.raises(ValueError):
        SitePolicies(document)



def test_fixed_mode_download_slots_use_exact_hosts_only():
    policies = SitePolicies({
        "default": {"crawler_mode": "fixed", "request_limits": {"max_qps": 0.5, "max_concurrent_requests": 8}},
        "hosts": {"en.wikipedia.org": {"request_limits": {"max_qps": 3, "max_concurrent_requests": 4}}},
    })
    assert policies.config.default.request_limits.min_request_delay_sec == 2.0
    assert policies.config.default.request_limits.max_concurrent_requests == 8
    assert policies.download_slots() == {"en.wikipedia.org": {"delay": 1 / 3, "concurrency": 4}}
    assert policies.resolve("fr.wikipedia.org").middleware is None


@pytest.mark.parametrize("options", [
    {"max_qps": -1}, {"max_qps": float("inf")}, {"max_qps": "1"},
    {"max_concurrent_requests": 0}, {"max_concurrent_requests": True}, {"max_concurrent_requests": 1.5}, {"unknown": 1},
])
def test_invalid_download_slot_options_fail_at_startup(options):
    with pytest.raises(ValueError):
        SitePolicies({"hosts": {"example.com": {"request_limits": options}}})
    with pytest.raises(ValueError):
        SitePolicies({"default": {"request_limits": options}})


@pytest.mark.parametrize("mode", ["fixed", "autothrottle"])
def test_settings_read_mode_and_delay_from_yaml(monkeypatch, tmp_path, mode):
    import runpy
    import yaml

    root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(root))
    config = tmp_path / "config"
    config.mkdir()
    (config / "site_policies.yml").write_text(yaml.safe_dump({
        "default": {"crawler_mode": mode, "request_limits": {
            "max_qps": 1.4285714285714286, "max_concurrent_requests": 8,
        }},
    }))
    monkeypatch.chdir(tmp_path)
    # Legacy variables no longer select the mode or override YAML limits.
    monkeypatch.setenv("CRAWLER_USE_AUTOTHROTTLE", str(mode != "autothrottle"))
    monkeypatch.setenv("CRAWLER_DOWNLOAD_DELAY", "9")
    settings = runpy.run_path(str(root / "crawler/settings.py"))
    assert settings["CRAWLER_THROTTLE_MODE"] == mode
    assert settings["CONCURRENT_REQUESTS_PER_DOMAIN"] == 8
    assert settings["DOWNLOAD_DELAY"] == 0.7
    assert settings["AUTOTHROTTLE_ENABLED"] is True
    assert settings["EXTENSIONS"]["scrapy.extensions.throttle.AutoThrottle"] is None


def test_bundled_config_uses_autothrottle_with_wikipedia_minimum(monkeypatch):
    import runpy

    root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(root))
    monkeypatch.chdir(root)
    settings = runpy.run_path(str(root / "crawler/settings.py"))
    policies = SitePolicies.load()
    assert settings["CRAWLER_THROTTLE_MODE"] == "autothrottle"
    assert settings["DOWNLOAD_DELAY"] == 0.25
    assert policies.resolve("en.wikipedia.org").crawler_mode == "autothrottle"
    assert policies.resolve("en.wikipedia.org").request_limits.max_qps == 15
    assert policies.resolve("en.wikipedia.org").request_limits.min_request_delay_sec == pytest.approx(1 / 15)


def test_domains_and_hosts_accept_identical_request_limits_schema():
    options = {"max_concurrent_requests": 10,
               "max_qps": 18, "download_slot": "shared"}
    policies = SitePolicies({"domains": {"example.com": {"request_limits": options}},
                             "hosts": {"special.example.com": {"request_limits": options}}})
    domain = policies.resolve("www.example.com").request_limits
    host = policies.resolve("special.example.com").request_limits
    assert type(domain) is type(host)
    assert domain == host
    assert policies.download_slots() == {"shared": {"delay": 0.5, "concurrency": 10}}


def test_request_limits_inherit_and_exact_host_can_replace_or_disable():
    policies = SitePolicies({
        "domains": {"example.com": {"request_limits": {"max_concurrent_requests": 10, "max_qps": 18}}},
        "hosts": {"special.example.com": {"request_limits": {"max_concurrent_requests": 2, "max_qps": 1}},
                  "disabled.example.com": {"request_limits": None}},
    })
    inherited = policies.resolve("www.example.com").request_limits
    assert inherited.download_slot == "example.com"
    assert inherited.max_concurrent_requests == 10
    host = policies.resolve("special.example.com").request_limits
    assert host.download_slot == "special.example.com"
    assert host.max_concurrent_requests == 2
    assert policies.resolve("disabled.example.com").request_limits == policies.config.default.request_limits


@pytest.mark.parametrize("options", [
    {"max_qps": 0}, {"max_qps": float("inf")},
    {"max_qps": "18"}, {"download_slot": "../escape"}, {"download_slot": ""},
])
def test_invalid_request_limits_rejected_equally_for_domains_and_hosts(options):
    for section in ("domains", "hosts"):
        with pytest.raises(ValueError):
            SitePolicies({section: {"example.com": {"request_limits": options}}})


def test_shared_slot_cannot_have_conflicting_limits():
    with pytest.raises(ValueError, match="Conflicting request limits"):
        SitePolicies({"domains": {"example.com": {"request_limits": {
            "download_slot": "shared", "max_qps": 18,
        }}}, "hosts": {"other.example.com": {"request_limits": {
            "download_slot": "shared", "max_qps": 10,
        }}}})

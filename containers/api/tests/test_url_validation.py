import pytest

from containers.api.service import validate_urls
from libs.urls import MAX_URL_LEN, host_of, validate_url


@pytest.mark.parametrize("url,expected", [
    ("HTTPS://Example.COM/a?b=2&a=1#section", "https://example.com/a?a=1&b=2"),
    ("http://example.com", "http://example.com/"),
    ("https://example.com:8443/a", "https://example.com:8443/a"),
    ("https://[2606:4700:4700::1111]/a", "https://[2606:4700:4700::1111]/a"),
    ("https://1.1.1.1/a", "https://1.1.1.1/a"),
    ("https://bücher.example/a", "https://xn--bcher-kva.example/a"),
    ("http://example.com/a?flag", "http://example.com/a?flag="),
    ("https://example.com./a", "https://example.com./a"),
])
def test_validate_url_returns_canonical_fetch_url(url, expected):
    assert validate_url(url) == expected


@pytest.mark.parametrize("url", [
    None, 123, [], {}, "", "/relative", "ftp://example.com/a", "https://",
    "https://user:password@example.com/a", "https://example.com:65536/a",
    "https://example.com:not-a-port/a", "https://[broken/a",
    "https://example.com/a b", "https://example.com/a\nb",
    "https://example.com/a\x00b", "https://example.com/a\x7fb",
    "https://example.com\\other/a",
    "https://bad..example/a", "https://invalid_hostname.example/a",
    "https://256.256.256.256/a", "https://[::gg]/a",
    "https://example.com/a?q=\x00", "\x00https://example.com/a",
    "http://127.0.0.1:8080/", "http://localhost/",
    "http://169.254.169.254/latest/meta-data/", "http://10.0.0.5/admin",
    "http://172.16.191.1:5432/", "http://scheduler-ingest:8080/healthz",
    "http://[::1]/", "https://[::1]/a", "http://localhost/a?flag",
    "http://LOCALHOST./", "http://sub.localhost/", "http://scheduler-ingest./",
    "http://127.0.0.1./", "http://192.168.1.1/", "http://100.64.0.1/",
    "http://0.0.0.0/", "http://192.0.2.1/",
    "http://[::]/", "http://[fc00::1]/", "http://[fe80::1]/",
])
def test_validate_url_rejects_invalid_input(url):
    with pytest.raises(ValueError, match="^Invalid HTTP\\(S\\) URL$"):
        validate_url(url)


def test_url_length_limit_includes_canonical_encoding():
    prefix = "https://example.com/"
    boundary = prefix + "a" * (MAX_URL_LEN - len(prefix))
    assert validate_url(boundary) == boundary
    with pytest.raises(ValueError):
        validate_url(boundary + "a")
    with pytest.raises(ValueError):
        validate_url(prefix + "中" * 400)


def test_shared_url_length_limit_can_be_overridden_in_both_directions():
    short = "https://example.com/a"
    with pytest.raises(ValueError):
        validate_url(short, max_length=len(short) - 1)
    assert validate_url(short, max_length=len(short)) == short
    long = "https://example.com/" + "a" * MAX_URL_LEN
    assert validate_url(long, max_length=len(long)) == long
    with pytest.raises(ValueError):
        validate_url(long)


def test_batch_validation_keeps_normalized_order_and_deduplicates():
    assert validate_urls([
        "https://example.com/b#one", "https://example.com/a", "https://example.com/b#two",
    ], max_urls=3) == ["https://example.com/b", "https://example.com/a"]


def test_batch_validation_adds_zero_based_index_to_shared_error():
    with pytest.raises(ValueError, match="^Invalid HTTP\\(S\\) URL at index 1$"):
        validate_urls(["https://example.com/a", "invalid"], max_urls=3)


@pytest.mark.parametrize("url,expected", [
    (None, ""),
    ("", ""),
    ("/relative", ""),
    ("https://User:password@EXAMPLE.com:8443?x=1", "example.com"),
    ("https://[2606:4700:4700::1111]:8443/a", "2606:4700:4700::1111"),
    ("https://1.1.1.1:8080/a", "1.1.1.1"),
    ("https://EXAMPLE.com./a", "example.com."),
    ("https://bücher.example/a", "bücher.example"),
])
def test_host_of_extracts_hostname_without_validation(url, expected):
    assert host_of(url) == expected


def test_host_of_uses_canonical_hostname_after_validation():
    assert host_of(validate_url("https://bücher.example/a")) == "xn--bcher-kva.example"


def test_host_of_reports_malformed_ipv6():
    with pytest.raises(ValueError):
        host_of("https://[broken/a")

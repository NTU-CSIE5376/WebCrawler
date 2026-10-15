import pytest

from containers.scheduler_ingest.api.service import validate_urls
from libs.urls import MAX_URL_LEN, validate_url


@pytest.mark.parametrize("url,expected", [
    ("HTTPS://Example.COM/a?b=2&a=1#section", "https://example.com/a?a=1&b=2"),
    ("http://example.com", "http://example.com/"),
    ("https://example.com:8443/a", "https://example.com:8443/a"),
    ("https://[::1]/a", "https://[::1]/a"),
    ("https://bücher.example/a", "https://xn--bcher-kva.example/a"),
    ("http://localhost/a?flag", "http://localhost/a?flag="),
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


def test_batch_validation_keeps_normalized_order_and_deduplicates():
    assert validate_urls({"urls": [
        "https://example.com/b#one", "https://example.com/a", "https://example.com/b#two",
    ]}, max_urls=3) == ["https://example.com/b", "https://example.com/a"]


def test_batch_validation_adds_zero_based_index_to_shared_error():
    with pytest.raises(ValueError, match="^Invalid HTTP\\(S\\) URL at index 1$"):
        validate_urls({"urls": ["https://example.com/a", "invalid"]}, max_urls=3)

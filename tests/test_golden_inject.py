"""golden_inject must write golden URLs under the same key and in the same
shard as the crawler writes their fetch results: canonicalize_url() for the
url string, and the router's ShardRouter rules for domain_state key / shard."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import golden_inject as gi  # noqa: E402
from constants import NUM_SHARDS  # noqa: E402
from libs.db.sharding.key import compute_shard  # noqa: E402
from libs.db.sharding.router import ShardRouter  # noqa: E402

OVERRIDES = {"wikipedia.org": 0, "youtube.com": 64}
SPLIT = {"en.wikipedia.org", "ja.wikipedia.org"}


@pytest.mark.parametrize(
    "raw, expected",
    [
        (
            "https://www.youtube.com/watch?v=X&list=Y&index=2",
            "https://www.youtube.com/watch?index=2&list=Y&v=X",
        ),
        (
            "https://ja.wikipedia.org/wiki/東京",
            "https://ja.wikipedia.org/wiki/%E6%9D%B1%E4%BA%AC",
        ),
        ("https://en.wikipedia.org/wiki/Caf%c3%a9", "https://en.wikipedia.org/wiki/Caf%C3%A9"),
        ("https://example.com", "https://example.com/"),
        ("https://example.com/a#tab", "https://example.com/a"),
    ],
)
def test_canonical_url_matches_spider_key(raw, expected):
    assert gi.canonical_url(raw) == expected


def test_canonical_url_unparseable_returns_none():
    assert gi.canonical_url(None) is None


def test_split_subdomain_keeps_host_and_its_own_shard():
    url = "https://en.wikipedia.org/wiki/Tokyo"
    domain, shard = gi.resolve_domain_and_shard(url, OVERRIDES, SPLIT)
    assert domain == "en.wikipedia.org"
    assert shard == compute_shard("en.wikipedia.org", NUM_SHARDS, OVERRIDES, SPLIT)
    # The old eTLD+1 path sent it to wikipedia.org's override shard instead.
    assert shard != 0


def test_non_split_subdomain_collapses_to_etld1():
    domain, shard = gi.resolve_domain_and_shard(
        "https://de.wikipedia.org/wiki/Berlin", OVERRIDES, SPLIT
    )
    assert (domain, shard) == ("wikipedia.org", 0)


@pytest.mark.parametrize(
    "url",
    [
        "https://en.wikipedia.org/wiki/Tokyo",
        "https://de.wikipedia.org/wiki/Berlin",
        "https://m.youtube.com/watch?v=abc",
        "https://news.bbc.co.uk/x",
        "https://example.com/",
    ],
)
def test_matches_router(url):
    router = ShardRouter(
        num_shards=NUM_SHARDS,
        shards_per_ingestor=16,
        domain_overrides=OVERRIDES,
        split_subdomains=SPLIT,
    )
    domain_key, shard_id, _ = router.route(url)
    assert gi.resolve_domain_and_shard(url, OVERRIDES, SPLIT) == (domain_key, shard_id)


def test_no_registrable_domain_returns_none():
    assert gi.resolve_domain_and_shard("http://127.0.0.1/x", OVERRIDES, SPLIT) is None
    assert gi.resolve_domain_and_shard("not-a-url", OVERRIDES, SPLIT) is None

"""YouTube outlink extraction: videos, channel uploads playlists with
subscriber-based priority, and watch-URL normalization."""
from __future__ import annotations

import json

import pytest
from scrapy.http import HtmlResponse

from containers.crawler.crawler.youtube_extractor import (
    channel_priority,
    extract_youtube_outlinks,
    normalize_youtube_url,
    parse_subscriber_count,
)

OWNER = "UCuAXFkgsw1L7xaCfnd5JJOw"
OTHER = "UC-9-kyTW8ZkZNDHQJ6FgpwQ"
BIG = "UCX6OQ3DkcsbYNE6H8uQQuVA"


def _uu(channel_id):
    return f"https://www.youtube.com/playlist?list=UU{channel_id[2:]}"


def _page(url, data):
    body = f"<html><script>var ytInitialData = {json.dumps(data)};</script></html>"
    return HtmlResponse(url=url, body=body.encode(), encoding="utf-8")


def _priorities(outlinks):
    return {o["url"]: o["priority"] for o in outlinks if "priority" in o}


WATCH_DATA = {
    "contents": [
        {"videoSecondaryInfoRenderer": {"owner": {"videoOwnerRenderer": {
            "navigationEndpoint": {"browseEndpoint": {"browseId": OWNER}},
            "subscriberCountText": {"simpleText": "4.55M subscribers"},
        }}}},
        {"lockupViewModel": {
            "contentId": "aaaaaaaaaaa",
            "contentType": "LOCKUP_CONTENT_TYPE_VIDEO",
            "byline": {"browseEndpoint": {"browseId": OTHER}},
        }},
        {"compactVideoRenderer": {"videoId": "bbbbbbbbbbb",
                                  "browseEndpoint": {"browseId": OWNER}}},
    ]
}


def test_watch_page_emits_videos_and_channel_playlists():
    out = extract_youtube_outlinks(_page("https://www.youtube.com/watch?v=xxxxxxxxxxx", WATCH_DATA), [])
    urls = {o["url"] for o in out}
    assert "https://www.youtube.com/watch?v=aaaaaaaaaaa" in urls
    assert "https://www.youtube.com/watch?v=bbbbbbbbbbb" in urls
    prios = _priorities(out)
    # Owner seen both with and without a subscriber count: the count wins.
    assert prios[_uu(OWNER)] == pytest.approx(1.455)
    # Recommended-video channel without a count: just above the ranker range.
    assert prios[_uu(OTHER)] == pytest.approx(1.000001)
    assert len(prios) == 2


def test_channel_page_header_subscribers_and_cap():
    data = {
        "metadata": {"channelMetadataRenderer": {"externalId": BIG}},
        "header": {"pageHeaderRenderer": {"content": {"metadataRows": [
            {"metadataParts": [{"text": {"content": "@MrBeast"}}]},
            {"metadataParts": [{"text": {"content": "524M subscribers"}},
                               {"text": {"content": "1K videos"}}]},
        ]}}},
    }
    out = extract_youtube_outlinks(_page(f"https://www.youtube.com/channel/{BIG}/videos", data), [])
    assert _priorities(out) == {_uu(BIG): 2.0}


def test_existing_outlink_keeps_highest_priority():
    existing = [{"url": _uu(OWNER), "domain": "youtube.com", "anchor": "", "priority": 1.9}]
    out = extract_youtube_outlinks(_page("https://www.youtube.com/watch?v=xxxxxxxxxxx", WATCH_DATA), existing)
    assert [o["priority"] for o in out if o["url"] == _uu(OWNER)] == [1.9]


@pytest.mark.parametrize("text,expected", [
    ("4.55M subscribers", 4_550_000),
    ("524M subscribers", 524_000_000),
    ("1.2K subscribers", 1_200),
    ("1,234 subscribers", 1_234),
    ("1 subscriber", 1),
    ("1.1B subscribers", 1_100_000_000),
    ("No subscribers", None),
    ("1K videos", None),
    (None, None),
])
def test_parse_subscriber_count(text, expected):
    assert parse_subscriber_count(text) == expected


def test_channel_priority_range():
    assert channel_priority(None) == pytest.approx(1.000001)
    assert channel_priority(0) == pytest.approx(1.000001)
    assert channel_priority(5_000_000) == pytest.approx(1.5)
    assert channel_priority(10_000_000) == 2.0
    assert channel_priority(400_000_000) == 2.0
    assert channel_priority(1) > 1.0


@pytest.mark.parametrize("url,expected", [
    ("https://www.youtube.com/watch?t=30s&v=dQw4w9WgXcQ", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
    ("https://www.youtube.com/watch?list=PLx&v=dQw4w9WgXcQ", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
    ("https://m.youtube.com/watch?v=dQw4w9WgXcQ", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
    ("https://www.youtube.com/shorts/dQw4w9WgXcQ", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
    ("https://youtu.be/dQw4w9WgXcQ?si=abc", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
    ("https://www.youtube.com/@MrBeast", "https://www.youtube.com/@MrBeast"),
    ("https://www.youtube.com/watch?v=short", "https://www.youtube.com/watch?v=short"),
    ("https://example.com/watch?v=dQw4w9WgXcQ", "https://example.com/watch?v=dQw4w9WgXcQ"),
])
def test_normalize_youtube_url(url, expected):
    assert normalize_youtube_url(url) == expected

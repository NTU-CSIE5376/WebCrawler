from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import parse_qs, urlsplit

from scrapy.http.response import Response

logger = logging.getLogger("crawler")

VIDEO_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
CHANNEL_ID_RE = re.compile(r"^UC[a-zA-Z0-9_-]{22}$")

# Channel outlinks point at the channel's uploads playlist (UU + channel id
# without its UC prefix): one fetch lists ~100 videos, versus ~30 on
# /channel/UC.../videos and ~20 on a watch page.
UPLOADS_PLAYLIST_URL = "https://www.youtube.com/playlist?list=UU{suffix}"

# Channel priority sits on the manual tier (url_score > 1), which the ranker
# never rescores: 1 + subscribers / 10M, capped at 2.0 so it stays below
# golden_inject's 3.0. Channels whose subscriber count the page does not show
# get the floor, which must stay strictly above 1.0 for the same reason.
CHANNEL_PRIORITY_BASE = 1.0
CHANNEL_PRIORITY_SUBSCRIBER_SCALE = 10_000_000
CHANNEL_PRIORITY_MAX_BONUS = 1.0
CHANNEL_PRIORITY_UNKNOWN_BONUS = 1e-6

# The crawler sends Accept-Language: en-US, so counts render as "4.55M subscribers".
_SUBSCRIBER_RE = re.compile(r"^([0-9][0-9,]*(?:\.[0-9]+)?)\s*([KMB]?)\s+subscribers?$", re.IGNORECASE)
_SUBSCRIBER_MULTIPLIER = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}

_YOUTUBE_HOSTS = ("youtube.com", "www.youtube.com", "m.youtube.com")

_YT_INITIAL_DATA_RE = re.compile(
    r"""
    (?:
        var\s+ytInitialData
        |window\["ytInitialData"\]
        |ytInitialData
    )
    \s*=\s*
    """,
    re.VERBOSE,
)

_YT_INITIAL_PLAYER_RE = re.compile(
    r"""
    (?:
        var\s+ytInitialPlayerResponse
        |window\["ytInitialPlayerResponse"\]
        |ytInitialPlayerResponse
    )
    \s*=\s*
    """,
    re.VERBOSE,
)

_YT_JSON_PREFIXES = (_YT_INITIAL_DATA_RE, _YT_INITIAL_PLAYER_RE)

_WATCH_PATH_RE = re.compile(r"/watch\?v=([a-zA-Z0-9_-]{11})")
_SHORTS_PATH_RE = re.compile(r"/shorts/([a-zA-Z0-9_-]{11})")


def extract_youtube_outlinks(
    response: Response,
    outlinks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Extract YouTube video and channel URLs from any YouTube page.

    Iterative tree walk over *ytInitialData* (and *ytInitialPlayerResponse*)
    discovers video IDs and channel IDs from every renderer type.  Falls back
    to regex scanning of raw HTML for video IDs if JSON parsing failed.

    Channels are emitted as uploads-playlist URLs carrying a ``priority``
    field (see ``channel_priority``).

    Mutates and returns *outlinks*.
    """
    existing_urls: set[str] = {
        o["url"]
        for o in outlinks
        if isinstance(o.get("url"), str)
    }

    ids: dict[str, None] = {}
    channels: dict[str, int | None] = {}
    parsed_ok = _extract_from_yt_json(response.text, ids, channels)

    if not parsed_ok:
        _extract_from_html(response.text, ids)

    for vid in ids:
        url = f"https://www.youtube.com/watch?v={vid}"
        if url not in existing_urls:
            existing_urls.add(url)
            outlinks.append(_yt_outlink(vid))

    by_url = {o["url"]: o for o in outlinks if isinstance(o.get("url"), str)}
    for channel_id, subscribers in channels.items():
        link = _channel_outlink(channel_id, subscribers)
        existing = by_url.get(link["url"])
        if existing is None:
            by_url[link["url"]] = link
            outlinks.append(link)
        elif link["priority"] > (existing.get("priority") or 0.0):
            existing["priority"] = link["priority"]

    if ids or channels:
        logger.info(
            "youtube.video_urls_extracted",
            extra={
                "event": "youtube.video_urls_extracted",
                "count": len(ids),
                "channel_count": len(channels),
                "url": response.url,
            },
        )

    return outlinks


def channel_priority(subscribers: int | None) -> float:
    if subscribers is None:
        bonus = CHANNEL_PRIORITY_UNKNOWN_BONUS
    else:
        bonus = min(subscribers / CHANNEL_PRIORITY_SUBSCRIBER_SCALE, CHANNEL_PRIORITY_MAX_BONUS)
        bonus = max(bonus, CHANNEL_PRIORITY_UNKNOWN_BONUS)
    return CHANNEL_PRIORITY_BASE + bonus


def parse_subscriber_count(text: Any) -> int | None:
    """Parse "4.55M subscribers" / "1,234 subscribers" into an int."""
    if not isinstance(text, str):
        return None
    m = _SUBSCRIBER_RE.match(text.strip())
    if not m:
        return None
    number = float(m.group(1).replace(",", ""))
    return int(round(number * _SUBSCRIBER_MULTIPLIER[m.group(2).upper()]))


def normalize_youtube_url(url: str) -> str:
    """Collapse watch / shorts / youtu.be variants of a video to
    https://www.youtube.com/watch?v=ID; any other URL is returned unchanged."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    host = (parts.hostname or "").lower()
    video_id = None
    if host in _YOUTUBE_HOSTS:
        if parts.path == "/watch":
            values = parse_qs(parts.query).get("v") or []
            video_id = values[0] if values else None
        else:
            m = _SHORTS_PATH_RE.match(parts.path)
            video_id = m.group(1) if m else None
    elif host == "youtu.be":
        video_id = parts.path.strip("/")
    if video_id and VIDEO_ID_RE.match(video_id):
        return f"https://www.youtube.com/watch?v={video_id}"
    return url


def _find_all_yt_json(text: str, prefix_re: re.Pattern) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    decoder = json.JSONDecoder()
    for m in prefix_re.finditer(text):
        try:
            obj, _ = decoder.raw_decode(text, m.end())
            results.append(obj)
        except json.JSONDecodeError:
            continue
    return results


_DICT = dict
_LIST = list


def _iterative_walk(root: Any, ids: dict[str, None], channels: dict[str, int | None]) -> None:
    stack = [root]

    while stack:
        node = stack.pop()

        if isinstance(node, _DICT):
            _try_extract_video_id(node, ids)
            _try_extract_channel(node, channels)

            for v in node.values():
                if isinstance(v, (_DICT, _LIST)):
                    stack.append(v)

        elif isinstance(node, _LIST):
            for i in range(len(node) - 1, -1, -1):
                item = node[i]
                if isinstance(item, (_DICT, _LIST)):
                    stack.append(item)


def _try_extract_video_id(d: dict, ids: dict[str, None]) -> None:
    raw = d.get("videoId")
    if isinstance(raw, str) and VIDEO_ID_RE.match(raw):
        ids[raw] = None
        return

    if d.get("contentType") == "LOCKUP_CONTENT_TYPE_VIDEO":
        raw = d.get("contentId")
        if isinstance(raw, str) and VIDEO_ID_RE.match(raw):
            ids[raw] = None


def _add_channel(channels: dict[str, int | None], channel_id: Any, subscribers: int | None = None) -> None:
    if not isinstance(channel_id, str) or not CHANNEL_ID_RE.match(channel_id):
        return
    if subscribers is not None:
        channels[channel_id] = subscribers
    else:
        channels.setdefault(channel_id, None)


def _try_extract_channel(d: dict, channels: dict[str, int | None]) -> None:
    # Any browse link to a channel (owner, recommended-video bylines, ...).
    _add_channel(channels, d.get("browseId"))
    # ytInitialPlayerResponse.videoDetails.channelId
    _add_channel(channels, d.get("channelId"))

    # Watch page owner block carries the owner's subscriber count.
    owner = d.get("videoOwnerRenderer")
    if isinstance(owner, _DICT):
        browse = (owner.get("navigationEndpoint") or {}).get("browseEndpoint") or {}
        sub_text = (owner.get("subscriberCountText") or {}).get("simpleText")
        _add_channel(channels, browse.get("browseId"), parse_subscriber_count(sub_text))


def _channel_page_subscribers(data: dict, channels: dict[str, int | None]) -> None:
    """On a channel page the header shows "524M subscribers"; the channel id
    is in metadata.channelMetadataRenderer.externalId."""
    meta = (data.get("metadata") or {}).get("channelMetadataRenderer") or {}
    channel_id = meta.get("externalId")
    if not isinstance(channel_id, str) or not CHANNEL_ID_RE.match(channel_id):
        return
    header = data.get("header")
    stack = [header]
    while stack:
        node = stack.pop()
        if isinstance(node, _DICT):
            stack.extend(node.values())
        elif isinstance(node, _LIST):
            stack.extend(node)
        else:
            count = parse_subscriber_count(node)
            if count is not None:
                _add_channel(channels, channel_id, count)
                return


def _extract_from_yt_json(text: str, ids: dict[str, None], channels: dict[str, int | None]) -> bool:
    parsed_any = False
    for prefix_re in _YT_JSON_PREFIXES:
        for data in _find_all_yt_json(text, prefix_re):
            parsed_any = True
            _iterative_walk(data, ids, channels)
            if isinstance(data, _DICT):
                _channel_page_subscribers(data, channels)
    return parsed_any


def _extract_from_html(text: str, ids: dict[str, None]) -> None:
    for m in _WATCH_PATH_RE.finditer(text):
        ids[m.group(1)] = None

    for m in _SHORTS_PATH_RE.finditer(text):
        ids[m.group(1)] = None


def _channel_outlink(channel_id: str, subscribers: int | None) -> dict[str, Any]:
    return {
        "url": UPLOADS_PLAYLIST_URL.format(suffix=channel_id[2:]),
        "domain": "youtube.com",
        "anchor": "",
        "priority": channel_priority(subscribers),
    }


def _yt_outlink(video_id: str) -> dict[str, str | None]:
    return {
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "domain": "youtube.com",
        "anchor": "",
    }

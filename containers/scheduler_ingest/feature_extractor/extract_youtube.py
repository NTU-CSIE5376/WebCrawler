from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlparse

# YouTube watch / live / shorts pages embed a server-rendered
# ytInitialPlayerResponse JSON blob; videoDetails carries the metadata
# without any JS execution.
_YT_HOSTS = ("youtube.com", "youtu.be")
_MARKER = "ytInitialPlayerResponse"


def _is_youtube(host: str) -> bool:
    return any(host == h or host.endswith("." + h) for h in _YT_HOSTS)


def _as_int(value) -> int | None:
    return int(value) if isinstance(value, str) and value.isdigit() else None


def extract_youtube(rec: dict):
    """Return a youtube_video_meta row dict, or None if rec is not a YouTube
    video page or the metadata blob is missing/unparseable."""
    if rec.get("status") != "ok":
        return None

    url = rec.get("url") or ""
    host = (urlparse(url).hostname or "").lower()
    if not _is_youtube(host):
        return None

    content = rec.get("content") or ""
    i = content.find(_MARKER)
    if i == -1:
        return None
    start = content.find("{", i)
    if start == -1:
        return None
    try:
        # raw_decode stops at the matching closing brace, so no regex needed.
        player, _ = json.JSONDecoder().raw_decode(content, start)
    except ValueError:
        return None

    vd = player.get("videoDetails") or {}
    video_id = vd.get("videoId")
    if not video_id:
        return None

    return {
        "video_id": video_id,
        "url": url,
        "channel_id": vd.get("channelId"),
        "channel_title": vd.get("author"),
        "video_title": vd.get("title"),
        "view_count": _as_int(vd.get("viewCount")),
        "length_seconds": _as_int(vd.get("lengthSeconds")),
        "keywords": vd.get("keywords") or [],
        "fetched_at": (
            datetime.fromisoformat(rec["fetched_at"])
            if rec.get("fetched_at")
            else datetime.now(timezone.utc)
        ),
    }

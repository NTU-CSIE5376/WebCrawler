from __future__ import annotations

import json
import logging
import re
from typing import Any
from scrapy.http.response import Response

logger = logging.getLogger("crawler")

VIDEO_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")

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
    outlinks: list[dict[str, str | None]],
) -> list[dict[str, str | None]]:
    """Extract YouTube video URLs from any YouTube page.

    Iterative tree walk over *ytInitialData* (and *ytInitialPlayerResponse*)
    discovers video IDs from every renderer type.  Falls back to regex scanning
    of raw HTML if JSON parsing failed.

    Mutates and returns *outlinks*.
    """
    existing_urls: set[str] = {
        o["url"]
        for o in outlinks
        if isinstance(o.get("url"), str)
    }

    ids: dict[str, None] = {}
    parsed_ok = _extract_from_yt_json(response.text, ids)

    if not parsed_ok:
        _extract_from_html(response.text, ids)

    for vid in ids:
        url = f"https://www.youtube.com/watch?v={vid}"
        if url not in existing_urls:
            existing_urls.add(url)
            outlinks.append(_yt_outlink(vid))

    if ids:
        logger.info(
            "youtube.video_urls_extracted",
            extra={
                "event": "youtube.video_urls_extracted",
                "count": len(ids),
                "url": response.url,
            },
        )

    return outlinks


def _scan_json_object(text: str, start: int) -> tuple[int, int] | None:
    if start >= len(text) or text[start] != "{":
        return None

    stack: list[str] = []
    i = start
    in_string = False
    while i < len(text):
        c = text[i]
        if in_string:
            if c == "\\":
                i += 2
                continue
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == "{":
                stack.append("}")
            elif c == "[":
                stack.append("]")
            elif c == "}" or c == "]":
                if not stack or stack[-1] != c:
                    return None
                stack.pop()
                if not stack:
                    return (start, i + 1)
        i += 1
    return None


def _find_all_yt_json(text: str, prefix_re: re.Pattern) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for m in prefix_re.finditer(text):
        span = _scan_json_object(text, m.end())
        if not span:
            continue
        try:
            results.append(json.loads(text[span[0] : span[1]]))
        except json.JSONDecodeError:
            continue
    return results


_DICT = dict
_LIST = list


def _iterative_walk(root: Any, ids: dict[str, None]) -> None:
    stack = [root]

    while stack:
        node = stack.pop()

        if isinstance(node, _DICT):
            _try_extract_video_id(node, ids)

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


def _extract_from_yt_json(text: str, ids: dict[str, None]) -> bool:
    parsed_any = False
    for prefix_re in _YT_JSON_PREFIXES:
        for data in _find_all_yt_json(text, prefix_re):
            parsed_any = True
            _iterative_walk(data, ids)
    return parsed_any


def _extract_from_html(text: str, ids: dict[str, None]) -> None:
    for m in _WATCH_PATH_RE.finditer(text):
        ids[m.group(1)] = None

    for m in _SHORTS_PATH_RE.finditer(text):
        ids[m.group(1)] = None


def _yt_outlink(video_id: str) -> dict[str, str | None]:
    return {
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "domain": "youtube.com",
        "anchor": "",
    }

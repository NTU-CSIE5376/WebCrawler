"""Strip session-id and tracking parameters from URLs.

Phase 1 found ~5.3e8 pending URLs that differ from a canonical URL only by a
session id (`;jsessionid=`, phpBB `sid`) or a tracking parameter (`utm_*`,
`gclid`, `srsltid`). They are rediscovered on every visit, so stripping them
before a URL enters the frontier would stop that inflow.

Used by the ingest trap filter (containers/scheduler_ingest/ingestor/
trap_filter.py) on newly discovered links.

Stripping changes a URL's identity, so the rules are stricter than the trap
report (`trap_rules.A_session_id`):
  * tracking params (`utm_*`, `gclid`, `fbclid`, `srsltid`, ...)    default on
  * explicitly named session params (`jsessionid`, `phpsessid`,
    `aspsessionid*`, `cfid`, `cftoken`, `zenid`, `oscsid`)          default on
  * generic names (`sid`, `s`, `sessionid`, ...) holding a 32-hex
    token                                                           default OFF
    (`saskatchewan.ca/...?s=<32 hex>` is a search id, not a session)
The remaining query keeps its original encoding and order byte for byte.
"""
from __future__ import annotations

import re
from urllib.parse import unquote_plus, urlsplit, urlunsplit

from libs.frontier.trap_rules import SESSION_PARAMS_STRICT, TRACKING_PARAMS

GENERIC_SESSION_PARAMS = frozenset({"sid", "s", "sessionid", "session_id", "sessid", "sess"})
_SESSION_TOKEN = re.compile(r"^[0-9a-f]{32}$", re.I)
_PATH_SESSION = re.compile(r";(?:jsessionid|phpsessid)=[^/?#;]*", re.I)


def _is_tracking(name: str) -> bool:
    return name in TRACKING_PARAMS or name.startswith("utm_")


def _is_named_session(name: str) -> bool:
    return name in SESSION_PARAMS_STRICT or name.startswith("aspsessionid")


def strip_params(
    url: str,
    *,
    tracking: bool = True,
    named_sessions: bool = True,
    generic_session_tokens: bool = False,
) -> str:
    """Return `url` without the selected parameter classes (unchanged if none)."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    path = parts.path
    if named_sessions and ";" in path:
        path = _PATH_SESSION.sub("", path)
    query = parts.query
    if query:
        kept = []
        for piece in query.split("&"):
            name, _, value = piece.partition("=")
            name = unquote_plus(name).lower()
            drop = (
                (tracking and _is_tracking(name))
                or (named_sessions and _is_named_session(name))
                or (generic_session_tokens and name in GENERIC_SESSION_PARAMS
                    and bool(_SESSION_TOKEN.match(unquote_plus(value))))
            )
            if not drop:
                kept.append(piece)
        query = "&".join(kept)
    if path == parts.path and query == parts.query:
        return url
    return urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))

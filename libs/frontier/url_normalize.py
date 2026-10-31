"""Strip session-id and tracking parameters from URLs.

Phase 1 found ~5.3e8 pending URLs that differ from a canonical URL only by a
session id (`;jsessionid=`, phpBB `sid`) or a tracking parameter (`utm_*`,
`gclid`, `srsltid`). They are rediscovered on every visit, so stripping them
before a URL enters the frontier would stop that inflow.

The ingest trap filter (containers/scheduler_ingest/ingestor/trap_filter.py)
strips only a configured, validated subset with `strip_named_params`
(`srsltid`, `jsessionid`, `phpsessid`) and logs what `strip_params` would
remove beyond that, without applying it.

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
from typing import Callable
from urllib.parse import unquote_plus, urlsplit, urlunsplit

from libs.frontier.trap_rules import SESSION_PARAMS_STRICT, TRACKING_PARAMS

GENERIC_SESSION_PARAMS = frozenset({"sid", "s", "sessionid", "session_id", "sessid", "sess"})
_SESSION_TOKEN = re.compile(r"^[0-9a-f]{32}$", re.I)
_PATH_SESSION = re.compile(r";(jsessionid|phpsessid)=[^/?#;]*", re.I)
PATH_SESSION_PARAMS = frozenset({"jsessionid", "phpsessid"})


def _is_tracking(name: str) -> bool:
    return name in TRACKING_PARAMS or name.startswith("utm_")


def _is_named_session(name: str) -> bool:
    return name in SESSION_PARAMS_STRICT or name.startswith("aspsessionid")


def _strip(
    url: str,
    drop_param: Callable[[str, str], bool],
    path_sessions: frozenset[str],
) -> str:
    """Remove query pieces where drop_param(lowercased name, raw value) is true,
    and `;name=value` path parameters for names in path_sessions."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    path = parts.path
    if path_sessions and ";" in path:
        path = _PATH_SESSION.sub(
            lambda m: "" if m.group(1).lower() in path_sessions else m.group(0), path,
        )
    query = parts.query
    if query:
        kept = []
        for piece in query.split("&"):
            name, _, value = piece.partition("=")
            if not drop_param(unquote_plus(name).lower(), value):
                kept.append(piece)
        query = "&".join(kept)
    if path == parts.path and query == parts.query:
        return url
    return urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


def strip_params(
    url: str,
    *,
    tracking: bool = True,
    named_sessions: bool = True,
    generic_session_tokens: bool = False,
) -> str:
    """Return `url` without the selected parameter classes (unchanged if none)."""
    def drop(name: str, value: str) -> bool:
        return (
            (tracking and _is_tracking(name))
            or (named_sessions and _is_named_session(name))
            or (generic_session_tokens and name in GENERIC_SESSION_PARAMS
                and bool(_SESSION_TOKEN.match(unquote_plus(value))))
        )
    return _strip(url, drop, PATH_SESSION_PARAMS if named_sessions else frozenset())


def strip_named_params(url: str, names: frozenset[str]) -> str:
    """Return `url` without the query params whose (case-insensitive) name is in
    `names`; `jsessionid` / `phpsessid` in `names` also match the `;name=` path
    form. Unchanged if none match."""
    names = frozenset(n.lower() for n in names)
    return _strip(url, lambda name, _value: name in names, names & PATH_SESSION_PARAMS)

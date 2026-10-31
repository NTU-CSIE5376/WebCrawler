"""URL-only crawler-trap heuristics (pure functions, no I/O).

Tier A follows rules shipped by mainstream crawlers (Nutch regex-urlfilter,
StormCrawler BasicURLFilter, Heritrix PathologicalPath / TooManyPathSegments,
Archive-It session-id guidance). Tier B are parameter heuristics; v2
thresholds were tightened after checking false positives against the golden
set (see crawler-db-url-eviction/reports/phase1/trap_ab/SUMMARY.md).

`match_rules(url)` returns the names of every rule the URL hits.
"""
from __future__ import annotations

import re
from urllib.parse import parse_qsl, unquote, urlsplit

# --- Tier A ---------------------------------------------------------------
PATH_REPEAT_MAX = 3          # same segment 3+ times (Nutch / StormCrawler)
MAX_PATH_SEGMENTS = 20       # Heritrix TooManyPathSegmentsDecideRule
# Always a session id, whatever the value looks like.
SESSION_PARAMS_STRICT = frozenset({
    "phpsessid", "jsessionid", "cfid", "cftoken", "zenid", "oscsid",
})
# Session id only when the value looks like a random token (sid=154 is a category id).
SESSION_PARAMS_TOKEN = frozenset({"sid", "sessionid", "session_id", "sessid", "sess", "s"})
_PATH_SESSION = re.compile(r";(jsessionid|phpsessid)=", re.I)
_SESSION_VALUE = re.compile(r"^(?:[0-9a-f]{16,}|(?=.*\d)(?=.*[a-z])[0-9a-z]{20,})$", re.I)

# --- Tier B ---------------------------------------------------------------
MAX_PAGE = 1000
MAX_OFFSET = 10000
YEAR_MAX = 2028              # dates beyond now+2y; past dates are legit archives
MIN_FACET_PARAMS = 3
MAX_PARAMS = 5               # >= this many non-tracking query params
MAX_URL_LEN = 300            # measured on the percent-decoded URL

# `p` is deliberately excluded: ?p=N is a WordPress / phpBB post id.
PAGE_PARAMS = frozenset({"page", "pg", "paged", "pagenum", "page_no", "pageno",
                         "pageindex", "page_number", "pagina", "seite"})
OFFSET_PARAMS = frozenset({"offset", "start", "skip"})
FACET_PARAMS = frozenset({
    "sort", "sortby", "sort_by", "order", "orderby", "order_by", "dir", "direction",
    "filter", "filters", "view", "display", "layout", "mode", "limit", "per_page",
    "perpage", "pagesize", "page_size", "rpp", "show", "color", "colour", "size", "brand",
    "price", "min_price", "max_price", "price_min", "price_max", "f", "fq", "facet",
    "rating", "material", "type", "format", "starts_with", "etal",
})
TRACKING_PARAMS = frozenset({
    "gclid", "fbclid", "msclkid", "yclid", "dclid", "gbraid", "wbraid", "srsltid",
    "mc_cid", "mc_eid", "_ga", "_gl", "igshid", "elqtrackid", "elqaid", "elqat", "elq",
    "ref_src", "spm", "scid",
})
_PAGE_PATH = re.compile(r"/(?:page|seite|pagina)[/-](\d+)(?:/|$)", re.I)
_DATE_PATH = re.compile(r"(?<!\d)((?:19|20|21|22)\d{2})[/-](0?[1-9]|1[0-2])(?:[/-](\d{1,2}))?(?!\d)")
_DATE_PARAM = re.compile(r"^((?:19|20|21|22)\d{2})(?:-?(0[1-9]|1[0-2]))?")
_CAL_PLUGIN_PATH = re.compile(
    r"/events?/(?:.+/)?(?:day|month|week|list|photo|map|today)/(?:\d{4}-\d{2}(?:-\d{2})?)?"
    r"|/(?:calendar|kalender|agenda|calendario)(?:/[^/?]*)*/\d{4}-\d{2}", re.I)
_CAL_PLUGIN_PARAMS = frozenset({"tribe-bar-date", "eventdisplay", "ical", "tribe_eventcategory"})
_NESTED_URL = re.compile(r"(?:https?:|https?%3a)(?:/|%2f){2}", re.I)
_HEX_TOKEN = re.compile(r"^[0-9a-f]{32,}$", re.I)
_B64_TOKEN = re.compile(r"^[A-Za-z0-9_\-+/=]{40,}$")


def _segments(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def path_repeat(path: str) -> bool:
    seen: dict[str, int] = {}
    for s in _segments(path):
        if s.isdigit() and len(s) <= 4:      # /2022/02/02/... archive dates
            continue
        seen[s] = seen.get(s, 0) + 1
        if seen[s] >= PATH_REPEAT_MAX:
            return True
    return False


def _int(v: str) -> int | None:
    v = v.strip()
    if not v.isdigit() or len(v) >= 12 or (len(v) > 1 and v[0] == "0"):
        return None
    return int(v)


def _is_tracking(name: str) -> bool:
    return name in TRACKING_PARAMS or name.startswith("utm_")


def match_rules(url: str) -> list[str]:
    try:
        parts = urlsplit(url)
    except ValueError:
        return ["unparseable"]
    path = parts.path or "/"
    params = [(k.lower(), v) for k, v in parse_qsl(parts.query, keep_blank_values=True)]
    names = [k for k, _ in params]
    hits: list[str] = []

    # Tier A
    if path_repeat(path):
        hits.append("A_path_repeat")
    if len(_segments(path)) > MAX_PATH_SEGMENTS:
        hits.append("A_too_many_segments")
    if (SESSION_PARAMS_STRICT.intersection(names) or _PATH_SESSION.search(path)
            or any(n.startswith("aspsessionid") for n in names)
            or any(k in SESSION_PARAMS_TOKEN and _SESSION_VALUE.match(v) for k, v in params)):
        hits.append("A_session_id")

    # Tier B
    if any(k in PAGE_PARAMS and (_int(v) or 0) > MAX_PAGE for k, v in params) \
            or any(int(m) > MAX_PAGE for m in _PAGE_PATH.findall(path) if len(m) < 12):
        hits.append("B_big_page")
    if any(k in OFFSET_PARAMS and (_int(v) or 0) > MAX_OFFSET for k, v in params):
        hits.append("B_big_offset")
    if _CAL_PLUGIN_PATH.search(path) or _CAL_PLUGIN_PARAMS.intersection(names):
        hits.append("B_calendar")
    future = any(int(m[0]) > YEAR_MAX for m in _DATE_PATH.findall(path))
    future = future or any(
        k in ("date", "year", "month", "day", "start_date", "end_date") and (m := _DATE_PARAM.match(v))
        and int(m.group(1)) > YEAR_MAX for k, v in params)
    if future:
        hits.append("B_future_date")
    if any(_NESTED_URL.search(v) and ("?" in unquote(v) or "%3f" in v.lower()) for _, v in params) \
            or len(_NESTED_URL.findall(parts.query)) >= 2:
        hits.append("B_nested_redirect")
    facets = {n for n in names if n in FACET_PARAMS or n.startswith(("filter[", "f[", "facet"))}
    if len(facets) >= MIN_FACET_PARAMS:
        hits.append("B_faceted")
    if any(_is_tracking(n) for n in names):
        hits.append("B_tracking_params")
    if sum(not _is_tracking(n) for n in names) >= MAX_PARAMS:
        hits.append("B_many_params")
    if len(unquote(url)) > MAX_URL_LEN:
        hits.append("B_long_url")
    if any(not _is_tracking(k) and (_HEX_TOKEN.match(v) or _B64_TOKEN.match(v)) for k, v in params):
        hits.append("B_random_token")
    return hits


ALL_RULES = (
    "A_path_repeat", "A_too_many_segments", "A_session_id",
    "B_big_page", "B_big_offset", "B_calendar", "B_future_date",
    "B_nested_redirect", "B_faceted", "B_tracking_params", "B_many_params",
    "B_long_url", "B_random_token",
)
# Rules judged precise enough to act on after review (see SUMMARY.md).
HIGH_CONFIDENCE = frozenset({
    "A_path_repeat", "A_too_many_segments", "B_nested_redirect", "B_calendar", "B_faceted",
})
# Not traps but duplicates of a canonical URL: strip the parameter instead.
DUPLICATE_CLASS = frozenset({"A_session_id", "B_tracking_params"})

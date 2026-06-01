"""URL normalization for golden parent patrol identity.

The patrol watchlist must dedupe URL variants that resolve to the same parent
page (e.g. http vs https, www. vs apex, trailing slash, tracking params).
Without deduping, the same physical page would be patrolled N times under
different identities, multiplying server load and splitting the
golden-child-count signal across rows.

This normalizer is intentionally string-only: it does not resolve redirects.
The output is used as `golden_parent_patrol_state.parent_key` for dedup; the
actual URL handed to the crawler is the raw `fetch_url` we have observed
working in `url_state_current_*`. So normalization bugs degrade dedup
precision but never invent unverified URLs to fetch.

Tracking-parameter whitelist: drop only widely-recognised analytics params.
Keep all other query keys (some look tracking-like but are real content keys,
e.g. `?id=123`).
"""
from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


# Drop these query parameters during normalization. Adding to this list is a
# config decision that affects the patrol_state primary-key partition; only add
# parameters that are universally tracking, never content keys.
#
# NOTE: `ref` is deliberately NOT here even though many sites use it for
# affiliate tracking — GitHub (and a few others) use `?ref=<branch>` as a
# content key, so dropping it would collapse different content pages into
# the same parent_key. Per the safety rule in the module docstring
# ("normalization bugs degrade dedup precision but never invent unverified
# URLs"), we prefer to keep a few extra rows over a misleading merge.
_TRACKING_PARAMS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
    }
)

_DEFAULT_PORTS = {"http": "80", "https": "443"}


def _strip_port(host: str, scheme: str) -> str:
    if ":" not in host:
        return host
    h, port = host.rsplit(":", 1)
    if _DEFAULT_PORTS.get(scheme) == port:
        return h
    return host


def _strip_tracking_query(query: str) -> str:
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    # Case-insensitive key match so `?UTM_SOURCE=...` is dropped too;
    # parse_qsl is case-sensitive but real-world URLs are mixed.
    kept = [(k, v) for k, v in pairs if k.lower() not in _TRACKING_PARAMS]
    if not kept:
        return ""
    # Sort by (key, value) so URLs that differ only in query-param order
    # (`?a=1&b=2` vs `?b=2&a=1`) collapse to the same parent_key.
    kept.sort()
    return urlencode(kept, doseq=True)


def normalize_parent_url(url: str) -> str:
    """Return a canonical key for `url` suitable for patrol dedup.

    Rules (string-only, no redirect resolution):
      - force scheme to https
      - lowercase host, strip leading "www."
      - strip default port (80/443)
      - strip trailing "/" except for the root path
      - drop tracking query params (utm_*, fbclid, gclid, mc_cid, mc_eid, ref)
      - drop fragment

    Empty / unparseable input returns the original string unchanged so callers
    can decide whether to filter or surface as-is.
    """
    if not url:
        return url

    parts = urlsplit(url.strip())

    # Without a host we cannot meaningfully normalize; pass through. Callers
    # filter these before enrolling into patrol_state.
    if not parts.netloc:
        return url

    scheme = "https"
    host = parts.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    host = _strip_port(host, scheme)

    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    query = _strip_tracking_query(parts.query)

    return urlunsplit((scheme, host, path, query, ""))

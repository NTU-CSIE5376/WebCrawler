from __future__ import annotations

import re
from urllib.parse import urlparse, urlsplit, urlunsplit

from libs.urls import host_of


_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def normalize_domain(value: str | None) -> str:
    return (value or "").strip().lower()


def normalize_url(url: object) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return ""
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        return ""
    host = (parts.hostname or "").lower()
    if not host:
        return ""
    path = parts.path or "/"
    return urlunsplit((scheme, host, path, parts.query, ""))


def domain_of(url: str) -> str:
    try:
        return normalize_domain(host_of(url))
    except Exception:
        return ""


def tld_of(domain: str) -> str:
    parts = domain.split(".")
    return parts[-1] if parts else ""


def tokenize_url(url: str) -> list[str]:
    parsed = urlparse(url)
    raw = " ".join([parsed.netloc, parsed.path, parsed.query])
    return [tok.lower() for tok in _TOKEN_RE.findall(raw) if tok]


def path_depth(url: str) -> int:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    return len(parts)


def bucket_key(url: str) -> str:
    tokens = tokenize_url(url)
    token_set = set(tokens)
    depth = min(path_depth(url), 6)
    has_query = 1 if "?" in url else 0
    has_numeric = 1 if any(tok.isdigit() for tok in tokens) else 0
    article_like = 1 if any(
        tok in token_set for tok in {"article", "articles", "news", "story", "stories"}
    ) else 0
    sports_like = 1 if any(
        tok in token_set for tok in {"nba", "mlb", "nhl", "score", "scores", "match", "game", "team", "player"}
    ) else 0
    entity_like = 1 if any(tok in token_set for tok in {"person", "actor", "artist", "player", "team"}) else 0
    return (
        f"depth={depth}|query={has_query}|num={has_numeric}|"
        f"article={article_like}|sports={sports_like}|entity={entity_like}"
    )

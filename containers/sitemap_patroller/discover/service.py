"""Discovery worker logic.

For each golden domain (domain_state.domain_score >= score_min), fetch
robots.txt for `Sitemap:` directives, fall back to /sitemap.xml, upsert
each candidate into the `domain_sitemap` table. The patrol worker is the
consumer.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg2

from containers.sitemap_patroller import SITEMAP_USER_AGENT


logger = logging.getLogger("sitemap_discover")

ROBOTS_TIMEOUT_SEC = 5.0
ROBOTS_MAX_BYTES = 1_000_000


@dataclass(frozen=True)
class DiscoverConfig:
    dsn: str
    score_min: float
    domain_limit: int | None
    global_delay_sec: float


def fetch_robots_txt(domain: str) -> str | None:
    url = f"https://{domain}/robots.txt"
    req = Request(url, headers={"User-Agent": SITEMAP_USER_AGENT})
    try:
        with urlopen(req, timeout=ROBOTS_TIMEOUT_SEC) as resp:
            raw = resp.read(ROBOTS_MAX_BYTES)
        return raw.decode("utf-8", errors="replace")
    except (HTTPError, URLError, TimeoutError, ConnectionError) as e:
        logger.info(
            "discover.robots_fetch_fail",
            extra={"event": "discover.robots_fetch_fail",
                   "domain": domain, "err": type(e).__name__},
        )
        return None
    except Exception as e:
        logger.warning(
            "discover.robots_fetch_unexpected",
            extra={"event": "discover.robots_fetch_unexpected",
                   "domain": domain, "err": repr(e)},
        )
        return None


def parse_sitemap_directives(robots_text: str) -> list[str]:
    """Extract `Sitemap: <url>` URLs from robots.txt. Case-insensitive prefix.
    Absolute http(s) URLs only — the robots.txt spec mandates absolute, and
    relative paths in the wild are usually buggy editors we shouldn't trust.
    """
    out: list[str] = []
    for line in robots_text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s[:8].lower().startswith("sitemap:"):
            url = s.split(":", 1)[1].strip()
            if url.startswith("http://") or url.startswith("https://"):
                out.append(url)
    return out


def discover_for_domain(domain: str) -> list[str]:
    robots = fetch_robots_txt(domain)
    if robots:
        urls = parse_sitemap_directives(robots)
        if urls:
            return urls
    return [f"https://{domain}/sitemap.xml"]


def fetch_golden_domains(cur, score_min: float, limit: int | None) -> list[tuple[int, str]]:
    sql = (
        "SELECT domain_id, domain FROM domain_state "
        "WHERE domain_score >= %s ORDER BY domain"
    )
    params: tuple = (score_min,)
    if limit is not None:
        sql += " LIMIT %s"
        params = (score_min, limit)
    cur.execute(sql, params)
    return [(int(r[0]), str(r[1])) for r in cur.fetchall()]


def upsert_sitemap(cur, domain_id: int, sitemap_url: str) -> bool:
    cur.execute(
        """
        INSERT INTO domain_sitemap (domain_id, sitemap_url)
        VALUES (%s, %s)
        ON CONFLICT (sitemap_url) DO NOTHING
        RETURNING id
        """,
        (domain_id, sitemap_url),
    )
    return cur.fetchone() is not None


def run_once(cfg: DiscoverConfig) -> dict:
    """Single end-to-end discovery sweep. Returns counters for logging."""
    started = time.monotonic()
    counters = {"domains": 0, "new_sitemaps": 0, "existing_sitemaps": 0, "errors": 0}

    conn = psycopg2.connect(cfg.dsn)
    try:
        cur = conn.cursor()
        domains = fetch_golden_domains(cur, cfg.score_min, cfg.domain_limit)
        counters["domains"] = len(domains)
        logger.info(
            "discover.start",
            extra={"event": "discover.start",
                   "domain_count": len(domains),
                   "score_min": cfg.score_min,
                   "limit": cfg.domain_limit},
        )

        for i, (domain_id, domain) in enumerate(domains):
            if i > 0:
                time.sleep(cfg.global_delay_sec)
            try:
                urls = discover_for_domain(domain)
            except Exception as e:
                counters["errors"] += 1
                logger.warning(
                    "discover.domain_error",
                    extra={"event": "discover.domain_error",
                           "domain": domain, "err": repr(e)},
                )
                continue

            for url in urls:
                if upsert_sitemap(cur, domain_id, url):
                    counters["new_sitemaps"] += 1
                else:
                    counters["existing_sitemaps"] += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    counters["elapsed_sec"] = round(time.monotonic() - started, 2)
    logger.info(
        "discover.done",
        extra={"event": "discover.done", **counters},
    )
    return counters

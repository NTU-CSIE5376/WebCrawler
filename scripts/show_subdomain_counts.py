"""
Show fetch_ok counts per hostname under an eTLD+1, aggregated across the
parent's domain_state row and any already-split subdomain rows. Also checks
robots.txt for each host so we don't whitelist a subdomain we cannot crawl.

    scripts/show_subdomain_counts.py facebook.com
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.robotparser import RobotFileParser

import psycopg2

from constants import CRAWLERDB
from libs.obslog import configure as configure_logging


log = logging.getLogger(__name__)

HOST_EXTRACT = "LOWER(SUBSTRING(url FROM '^https?://([^/:]+)'))"

# The crawler runs Scrapy with no custom USER_AGENT, so robots.txt sees the
# default "Scrapy/<ver> (+https://scrapy.org)" UA. Match on the bot token.
ROBOTS_UA = "Scrapy"
ROBOTS_TIMEOUT = 5.0


def check_robots(host: str) -> str:
    """Return 'ok' / 'blocked' / 'err:<reason>' for fetching '/' as Scrapy."""
    rp = RobotFileParser()
    rp.set_url(f"https://{host}/robots.txt")
    try:
        import socket
        socket.setdefaulttimeout(ROBOTS_TIMEOUT)
        rp.read()
    except Exception as e:
        return f"err:{type(e).__name__}"
    return "ok" if rp.can_fetch(ROBOTS_UA, f"https://{host}/") else "blocked"


def fetch_host_counts(cur, shard_id: int, domain_id: int) -> list[tuple[str, int]]:
    cur.execute(
        f"""
        SELECT {HOST_EXTRACT} AS host, COUNT(*)
        FROM url_state_current_{shard_id:03d}
        WHERE domain_id = %s AND last_fetch_ok IS NOT NULL
        GROUP BY 1
        """,
        (domain_id,),
    )
    return [(h or "(null)", c) for h, c in cur.fetchall()]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("domain", help="eTLD+1, e.g. facebook.com")
    p.add_argument("--top", type=int, default=50, help="rows to display (default 50)")
    args = p.parse_args()
    configure_logging(service="show_subdomain_counts")

    parent = args.domain.lower()
    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT domain, shard_id, domain_id FROM domain_state
                WHERE domain = %s OR domain LIKE %s
                ORDER BY domain
                """,
                (parent, f"%.{parent}"),
            )
            rows = cur.fetchall()
            if not rows:
                log.warning(f'no domain_state rows for {parent}')
                return

            counts: dict[str, int] = defaultdict(int)
            sources: dict[str, set[int]] = defaultdict(set)
            for _domain, shard_id, domain_id in rows:
                for host, cnt in fetch_host_counts(cur, shard_id, domain_id):
                    counts[host] += cnt
                    sources[host].add(shard_id)

    items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: args.top]
    total = sum(counts.values())

    robots = {}
    if items:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for host, status in zip(
                (h for h, _ in items),
                ex.map(check_robots, (h for h, _ in items)),
            ):
                robots[host] = status

    log.info(f'{parent}: {len(counts)} hostnames, {total:,} URLs with fetch_ok')
    log.info(f'  scanned {len(rows)} domain_state row(s)')
    log.info(f"  {'host':55s} {'fetch_ok':>12s}  {'robots':>8s}  shards")
    for host, c in items:
        sh = ",".join(f"{s:03d}" for s in sorted(sources[host]))
        log.info(f"  {host[:55]:55s} {c:>12,}  {robots.get(host, '?'):>8s}  {sh}")


if __name__ == "__main__":
    main()

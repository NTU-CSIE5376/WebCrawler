#!/usr/bin/env python3
"""
Refresh the shard_split_subdomain table from live fetch_ok counts.

For each eTLD+1 in scope, aggregate per-hostname fetch_ok counts and check
robots.txt for the Scrapy UA. Scope selection (in order of precedence):

  --domain D            scan only D
  --top-error-rate N    top-N eTLD+1s by fail_pct over the last --days
                        days, restricted to >= --min-attempts attempts
  (default)             keys of ingest.yaml::domain_overrides

The script only grows the whitelist:

  ADD     host has fetch_ok >= --min-fetch-ok and robots=ok and is not
          the apex; not currently whitelisted.
  REMOVE  host is whitelisted but robots.txt now blocks the Scrapy UA.
          Low fetch_ok alone never triggers removal, that stays a human
          decision via migrate_unsplit_subdomains.py.

Dry-run prints the proposed diff. --execute INSERTs / DELETEs against
shard_split_subdomain. domain_overrides in ingest.yaml is left alone,
that one stays human-curated.

    scripts/refresh_split_whitelist.py
    scripts/refresh_split_whitelist.py --domain facebook.com
    scripts/refresh_split_whitelist.py --top-error-rate 10
    scripts/refresh_split_whitelist.py --execute

After --execute, run scripts/migrate_shard_split.py --execute (with
scheduler_ingest paused) to move existing rows for the new entries.
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

import psycopg2

from constants import CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import SPLIT_TABLE, load_split_subdomains
from libs.obslog import configure as configure_logging


log = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parents[1]
INGEST_CFG = REPO / "containers/scheduler_ingest/config/ingest.yaml"

HOST_EXTRACT = "LOWER(SUBSTRING(url FROM '^https?://([^/:]+)'))"
ROBOTS_UA = "Scrapy"
ROBOTS_TIMEOUT = 5.0


def check_robots(host: str) -> str:
    """Return 'ok' / 'blocked' / 'err:<reason>'."""
    rp = RobotFileParser()
    rp.set_url(f"https://{host}/robots.txt")
    try:
        import socket
        socket.setdefaulttimeout(ROBOTS_TIMEOUT)
        rp.read()
    except Exception as e:
        return f"err:{type(e).__name__}"
    return "ok" if rp.can_fetch(ROBOTS_UA, f"https://{host}/") else "blocked"


def check_apex_host(host: str) -> str:
    """Return 'self' / 'parent:<final>' / 'other:<final>' / 'err:<reason>'.

    Splitting a host that 301s to its parent is useless, the eventual fetch
    still lands under the parent's shard.
    """
    req = Request(f"https://{host}/", method="HEAD", headers={"User-Agent": ROBOTS_UA})
    try:
        with urlopen(req, timeout=ROBOTS_TIMEOUT) as resp:
            final = (urlparse(resp.url).hostname or "").lower()
    except Exception as e:
        return f"err:{type(e).__name__}"
    if final == host:
        return "self"
    if host.endswith("." + final):
        return f"parent:{final}"
    return f"other:{final}"


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
    return [(h, c) for h, c in cur.fetchall() if h]


def fetch_top_error_parents(cur, n: int, days: int, min_attempts: int) -> list[str]:
    cur.execute(
        """
        SELECT ds.domain
        FROM domain_stats_daily dsd
        JOIN domain_state ds USING (domain_id)
        WHERE dsd.event_date >= CURRENT_DATE - %s::int
        GROUP BY ds.domain
        HAVING SUM(dsd.num_fetch_ok + dsd.num_fetch_fail) >= %s
        ORDER BY SUM(dsd.num_fetch_fail)::float
                 / NULLIF(SUM(dsd.num_fetch_ok + dsd.num_fetch_fail), 0) DESC NULLS LAST,
                 SUM(dsd.num_fetch_fail) DESC
        LIMIT %s
        """,
        (days, min_attempts, n),
    )
    return [r[0] for r in cur.fetchall()]


def collect_counts(cur, parent: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT shard_id, domain_id FROM domain_state
        WHERE domain = %s OR domain LIKE %s
        """,
        (parent, f"%.{parent}"),
    )
    counts: dict[str, int] = defaultdict(int)
    for shard_id, domain_id in cur.fetchall():
        for host, cnt in fetch_host_counts(cur, shard_id, domain_id):
            counts[host] += cnt
    return counts


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", help="scan only this eTLD+1")
    p.add_argument("--top-error-rate", type=int, metavar="N",
                   help="scan top-N eTLD+1s by fail_pct over --days")
    p.add_argument("--days", type=int, default=7,
                   help="window for --top-error-rate (default 7)")
    p.add_argument("--min-attempts", type=int, default=10000,
                   help="--top-error-rate filter: min total attempts (default 10000)")
    p.add_argument("--min-fetch-ok", type=int, default=100)
    p.add_argument("--max-add", type=int, default=10,
                   help="cap ADDs per run, top by fetch_ok (default 10)")
    p.add_argument("--execute", action="store_true",
                   help="apply INSERT/DELETE to shard_split_subdomain")
    args = p.parse_args()
    configure_logging(service="refresh_split_whitelist")

    if args.domain and args.top_error_rate:
        p.error("--domain and --top-error-rate are mutually exclusive")

    ingest = load_yaml(str(INGEST_CFG))
    overrides = (ingest.get("router") or {}).get("domain_overrides") or {}

    counts: dict[str, int] = {}
    with psycopg2.connect(**CRAWLERDB) as conn:
        current = load_split_subdomains(conn)
        with conn.cursor() as cur:
            if args.domain:
                parents = [args.domain.lower()]
            elif args.top_error_rate:
                parents = fetch_top_error_parents(
                    cur, args.top_error_rate, args.days, args.min_attempts)
            else:
                parents = sorted(overrides.keys())

            log.info(f"scope: {len(parents)} parent(s): {', '.join(parents)}")
            log.info(f'min_fetch_ok: {args.min_fetch_ok:,}')

            parent_set = set(parents)
            for parent in parents:
                for host, cnt in collect_counts(cur, parent).items():
                    if host in parent_set:
                        continue  # apex
                    counts[host] = counts.get(host, 0) + cnt

        candidates = sorted(
            ((h, c) for h, c in counts.items() if c >= args.min_fetch_ok or h in current),
            key=lambda kv: kv[1], reverse=True,
        )

        robots: dict[str, str] = {}
        apex: dict[str, str] = {}
        if candidates:
            hosts = [h for h, _ in candidates]
            with ThreadPoolExecutor(max_workers=8) as ex:
                for host, status in zip(hosts, ex.map(check_robots, hosts)):
                    robots[host] = status
                for host, status in zip(hosts, ex.map(check_apex_host, hosts)):
                    apex[host] = status

        add, remove, keep = [], [], []
        for host, cnt in candidates:
            in_list = host in current
            rb = robots.get(host)
            ap = apex.get(host)
            if in_list and rb == "blocked":
                remove.append((host, cnt))
            elif in_list:
                keep.append((host, cnt))
            elif cnt >= args.min_fetch_ok and rb == "ok" and ap == "self":
                add.append((host, cnt))

        def _log_diff(label: str, sign: str, items: list[tuple[str, int]]) -> None:
            log.info(f'{label} ({len(items)}):')
            for host, cnt in items:
                log.info(
                    f"  {sign} {host:55s} fetch_ok={cnt:>10,}  robots={robots.get(host, '?')}  apex="
                    f"{apex.get(host, '?')}"
                )

        deferred: list[tuple[str, int]] = []
        if len(add) > args.max_add:
            deferred = add[args.max_add:]
            add = add[:args.max_add]

        _log_diff("ADD", "+", add)
        _log_diff("REMOVE", "-", remove)
        _log_diff("KEEP", "=", keep)

        new_counts = {h: c for h, c in counts.items() if h not in current}
        discovered = len(new_counts)
        above_threshold = sum(1 for c in new_counts.values() if c >= args.min_fetch_ok)
        eligible = len(add) + len(deferred)
        add_ok = sum(c for _, c in add)
        keep_ok = sum(c for _, c in keep)
        deferred_ok = sum(c for _, c in deferred)
        pct = lambda n, d: f"{100 * n / d:.1f}%" if d else "-"
        log.info('analytics (excluding hosts already in whitelist):')
        log.info(f'  subdomains discovered under {len(parents)} parent(s): {discovered:,}')
        log.info(
            f'  passed fetch_ok >= {args.min_fetch_ok:,}: {above_threshold:,} ('
            f'{pct(above_threshold, discovered)})'
        )
        log.info(
            f'  eligible after robots + apex: {eligible:,} ({pct(eligible, above_threshold)}'
            f' of above-threshold)'
        )
        log.info(f'  ADD this run (max {args.max_add}): {len(add):,}')
        log.info(f'  deferred to next run: {len(deferred):,} ({deferred_ok:,} fetch_ok URLs)')
        log.info(f'  fetch_ok URLs newly rescued by ADD: {add_ok:,}')
        log.info(f'  fetch_ok URLs already covered by KEEP: {keep_ok:,}')

        final = sorted((current | {h for h, _ in add}) - {h for h, _ in remove})

        if args.execute:
            if args.domain:
                log.error(
                    'refusing to --execute with --domain (partial scan); rerun without --domain',
                )
                return
            with conn.cursor() as cur:
                if add:
                    cur.executemany(
                        f"INSERT INTO {SPLIT_TABLE}(host) VALUES (%s) ON CONFLICT DO NOTHING",
                        [(h,) for h, _ in add],
                    )
                if remove:
                    cur.executemany(
                        f"DELETE FROM {SPLIT_TABLE} WHERE host = %s",
                        [(h,) for h, _ in remove],
                    )
            conn.commit()
            log.info(
                f'updated {SPLIT_TABLE}: +{len(add)} / -{len(remove)} ({len(final)} entries total)'
            )
            log.info(
                'next: scripts/migrate_shard_split.py --execute (with scheduler_ingest paused)',
            )
        else:
            log.info(f'dry-run. final list would be {len(final)} entries.')


if __name__ == "__main__":
    main()

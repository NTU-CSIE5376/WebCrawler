"""
Probe currently-paused domains with the new Scrapy headers from
containers/crawler/crawler/settings.py to estimate how many will start
succeeding after redeploy.

Two header sets are tested per sample URL:
  * "bare"    -- old Scrapy defaults (Scrapy UA, minimal headers)
  * "browser" -- new USER_AGENT + DEFAULT_REQUEST_HEADERS

For URLs that were paused because of robots.txt, the live robots.txt is
re-evaluated under both UAs with protego (Scrapy's parser) rather than
making an HTTP request, since Scrapy still enforces ROBOTSTXT_OBEY.

Verdicts:
  UNBLOCKED          bare 4xx/5xx, browser 2xx/3xx       -> change helps
  BOTH_OK            both succeed                        -> wasn't header issue
  BOTH_FAIL          both fail                           -> real block (anti-bot/paywall)
  REGRESSION         browser fails but bare succeeds     -> change makes it worse
  ROBOTS_UA_HELPS    robots: bare disallowed, browser allowed
  ROBOTS_STILL_BLOCKS robots blocks under both UAs       -> headers won't help
  ROBOTS_NOW_ALLOWED robots allows both (rule may have changed)

Usage:
    uv run scripts/check_pause_unblock.py [--limit N] [--concurrency 16]
                                          [--domains a.com,b.com]
                                          [--print-unblock-sql]

With --print-unblock-sql, emits an UPDATE that clears pause + fail_count
on every domain classified UNBLOCKED or ROBOTS_UA_HELPS, so they get
retried immediately once the new headers are deployed.
"""

import argparse
import concurrent.futures as cf
import logging
from urllib.parse import urlsplit

import psycopg2
import requests
from protego import Protego

from constants import CRAWLERDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROBOTS_FAIL_PREFIX = "IgnoreRequest Forbidden by robots.txt"

BARE_UA = "Scrapy/2.11.0 (+https://scrapy.org)"
BARE_HEADERS = {"User-Agent": BARE_UA, "Accept": "text/html,*/*"}

# Mirror containers/crawler/crawler/settings.py
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
BROWSER_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

TIMEOUT = 10


def fetch_paused(limit: int | None, domains: list[str] | None
                 ) -> list[tuple[str, int, int]]:
    sql = """
        SELECT domain, domain_id, shard_id
        FROM domain_state
        WHERE crawl_paused_until IS NOT NULL
          AND crawl_paused_until > NOW()
    """
    params: list = []
    if domains:
        sql += " AND domain = ANY(%s)"
        params.append(domains)
    sql += " ORDER BY crawl_paused_until DESC"
    if limit:
        sql += f" LIMIT {limit}"
    with psycopg2.connect(**CRAWLERDB) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def sample_url(cur, shard_id: int, domain_id: int
               ) -> tuple[str | None, str | None]:
    cur.execute(
        f"""SELECT url, last_fail_reason
              FROM url_state_current_{shard_id:03d}
             WHERE domain_id = %s AND last_fail_reason IS NOT NULL
             ORDER BY random()
             LIMIT 1""",
        (domain_id,),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def probe(url: str, headers: dict[str, str]) -> tuple[int, str]:
    try:
        r = requests.get(url, headers=headers, timeout=TIMEOUT,
                         allow_redirects=True)
        return r.status_code, ""
    except requests.exceptions.Timeout:
        return 0, "timeout"
    except requests.exceptions.ConnectionError as e:
        cause = type(e.__cause__).__name__ if e.__cause__ else "err"
        return 0, f"conn:{cause}"
    except Exception as e:
        return 0, f"err:{type(e).__name__}"


def ok(status: int) -> bool:
    return 200 <= status < 400


def http_verdict(bare: int, brow: int) -> str:
    bo, wo = ok(bare), ok(brow)
    if not bo and wo:
        return "UNBLOCKED"
    if bo and wo:
        return "BOTH_OK"
    if not bo and not wo:
        return "BOTH_FAIL"
    return "REGRESSION"


def check_robots(url: str) -> str:
    """Verdict for robots.txt-paused URLs."""
    parts = urlsplit(url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    try:
        # robots.txt itself usually allows bots; fetch with bare so we see
        # what Scrapy actually sees in production.
        r = requests.get(robots_url, headers=BARE_HEADERS, timeout=TIMEOUT,
                         allow_redirects=True)
        if r.status_code >= 400 or not r.text:
            return "ROBOTS_FETCH_FAIL"
        rp = Protego.parse(r.text)
        bare_ok = rp.can_fetch(url, BARE_UA)
        brow_ok = rp.can_fetch(url, BROWSER_UA)
        if not bare_ok and brow_ok:
            return "ROBOTS_UA_HELPS"
        if bare_ok and brow_ok:
            return "ROBOTS_NOW_ALLOWED"
        return "ROBOTS_STILL_BLOCKS"
    except Exception as e:
        return f"ROBOTS_ERR:{type(e).__name__}"


def evaluate(args) -> dict:
    domain, did, shard, url, reason = args
    if reason and reason.startswith(ROBOTS_FAIL_PREFIX):
        verdict = check_robots(url)
        return {
            "domain": domain, "url": url, "fail_reason": reason,
            "bare": (None, "skip"), "browser": (None, "skip"),
            "verdict": verdict,
        }
    bare_code, bare_note = probe(url, BARE_HEADERS)
    brow_code, brow_note = probe(url, BROWSER_HEADERS)
    return {
        "domain": domain, "url": url, "fail_reason": reason,
        "bare": (bare_code, bare_note),
        "browser": (brow_code, brow_note),
        "verdict": http_verdict(bare_code, brow_code),
    }


def fmt_probe(p: tuple) -> str:
    code, note = p
    if code is None:
        return "-"
    return f"{code}" + (f" {note}" if note else "")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None,
                   help="Probe at most N paused domains")
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--domains", type=str, default=None,
                   help="Comma-separated whitelist of domains to test")
    p.add_argument("--print-unblock-sql", action="store_true",
                   help="Emit UPDATE statement to clear pause on domains "
                        "the new headers would unblock")
    args = p.parse_args()

    dom_filter = ([d.strip() for d in args.domains.split(",") if d.strip()]
                  if args.domains else None)
    paused = fetch_paused(args.limit, dom_filter)
    log.info("Found %d paused domains", len(paused))

    work: list[tuple] = []
    with psycopg2.connect(**CRAWLERDB) as conn, conn.cursor() as cur:
        for domain, did, shard in paused:
            url, reason = sample_url(cur, shard, did)
            if url:
                work.append((domain, did, shard, url, reason))

    results: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        for r in ex.map(evaluate, work):
            results.append(r)

    order = [
        "UNBLOCKED", "ROBOTS_UA_HELPS",
        "REGRESSION",
        "BOTH_FAIL", "ROBOTS_STILL_BLOCKS",
        "BOTH_OK", "ROBOTS_NOW_ALLOWED",
        "ROBOTS_FETCH_FAIL",
    ]
    buckets: dict[str, list[dict]] = {k: [] for k in order}
    for r in results:
        buckets.setdefault(r["verdict"], []).append(r)

    print()
    print(f"{'Verdict':<22}{'Domain':<28}{'Bare':<14}{'Browser':<14}"
          f"{'Top fail reason'}")
    print("-" * 110)
    for v in order:
        for r in sorted(buckets[v], key=lambda x: x["domain"]):
            print(f"{v:<22}{r['domain']:<28}{fmt_probe(r['bare']):<14}"
                  f"{fmt_probe(r['browser']):<14}{r['fail_reason'] or ''}")
    for v, items in buckets.items():
        if v not in order and items:
            for r in items:
                print(f"{v:<22}{r['domain']:<28}{fmt_probe(r['bare']):<14}"
                      f"{fmt_probe(r['browser']):<14}{r['fail_reason'] or ''}")

    print()
    print("Summary:")
    for v in order:
        if buckets[v]:
            print(f"  {v:<22}{len(buckets[v])}")
    for v, items in buckets.items():
        if v not in order and items:
            print(f"  {v:<22}{len(items)}")
    print(f"  {'TOTAL':<22}{len(results)}")

    if args.print_unblock_sql:
        unblock = sorted({r["domain"] for v in ("UNBLOCKED", "ROBOTS_UA_HELPS")
                          for r in buckets[v]})
        if not unblock:
            print("\n-- No domains to unblock.")
        else:
            domains_sql = ", ".join(f"'{d}'" for d in unblock)
            print("\n-- Run after the new headers are deployed:")
            print("UPDATE domain_state")
            print("   SET crawl_paused_until = NULL, domain_fail_count = 0")
            print(f" WHERE domain IN ({domains_sql});")


if __name__ == "__main__":
    main()

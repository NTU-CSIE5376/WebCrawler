#!/usr/bin/env python3
"""Weekly script: download Wikimedia pageviews, extract Top-K URLs, and inject
them into crawlerdb with pageview-based scheduler priority."""
from __future__ import annotations

import argparse
import bz2
import heapq
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

N = 1000000
BASE_URL = "https://dumps.wikimedia.org/other/pageview_complete"
MAX_URL_LEN = 2500
DEFAULT_INJECT_BATCH_SIZE = 1000

WEBCRAWLER_SCRIPTS_DIR = Path(__file__).resolve().parent
WEBCRAWLER_DIR = WEBCRAWLER_SCRIPTS_DIR.parent
INGEST_CONFIG = (
    WEBCRAWLER_DIR / "containers/scheduler_ingest/config/ingest.yaml"
)


@dataclass(frozen=True)
class PageviewRecord:
    url: str
    pageviews: int


@dataclass(frozen=True)
class PageviewInjectRow:
    url: str
    domain_id: int
    domain_score: float
    url_score: float
    source: int


@dataclass
class InjectionStats:
    max_pageviews: int = 0
    valid_file_rows: int = 0
    scanned: int = 0
    invalid: int = 0
    duplicates: int = 0
    oversized: int = 0
    bad_domain: int = 0
    queued: int = 0
    dry_run_urls: int = 0
    inserted: int = 0
    updated: int = 0


def fetch_listing(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read().decode()


def latest_available_date() -> datetime:
    today = datetime.now()
    for day_offset in range(2, 10):
        dt = today - timedelta(days=day_offset)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        dir_url = f"{BASE_URL}/{year}/{year}-{month}/"
        try:
            html = fetch_listing(dir_url)
        except urllib.error.HTTPError:
            continue
        pattern = re.escape(dt.strftime("%Y%m%d"))
        if re.search(rf"pageviews-{pattern}-automated\.bz2", html):
            return dt
    raise RuntimeError("no recent automated pageview dump found")


def download(url: str, dest: str) -> None:
    print(f"Downloading {url} ...", file=sys.stderr)
    urllib.request.urlretrieve(url, dest)
    size = os.path.getsize(dest)
    print(f"  saved {dest} ({size:,} bytes)", file=sys.stderr)


def extract_project_title(line: bytes) -> tuple[str, str] | None:
    first_space = line.find(b" ")
    if first_space == -1:
        return None
    project = line[:first_space].decode("utf-8", errors="replace")
    rest = line[first_space + 1 :]
    if rest.startswith(b'"'):
        i = 1
        while i < len(rest):
            if rest[i] == 92 and i + 1 < len(rest) and rest[i + 1] == 34:
                i += 2
                continue
            if rest[i] == 34:
                title = rest[1:i].decode("utf-8", errors="replace")
                return project, title
            i += 1
        return project, rest[1:].decode("utf-8", errors="replace")
    next_space = rest.find(b" ")
    if next_space == -1:
        return project, rest.decode("utf-8", errors="replace")
    return project, rest[:next_space].decode("utf-8", errors="replace")


def line_to_urls(views: int, line: bytes) -> list[str]:
    parsed = extract_project_title(line)
    if parsed is None:
        return []
    project, title = parsed
    host = f"{project}.org"
    encoded_title = urllib.parse.quote(title, safe=":/(),'")
    if project == "zh.wikipedia":
        variants = ["zh-cn", "zh-hans", "zh-hant", "zh-tw"]
        urls = [f"https://{host}/{v}/{encoded_title}\t{views}" for v in variants]
        urls.append(f"https://{host}/wiki/{encoded_title}\t{views}")
        return urls
    return [f"https://{host}/wiki/{encoded_title}\t{views}"]


def extract_top(input_path: str, output_path: str, n: int) -> None:
    print(f"Pass 1: finding threshold (top {n}) ...", file=sys.stderr)
    heap: list[int] = []
    with bz2.open(input_path, "rb") as f:
        for i, line in enumerate(f):
            parts = line.rstrip(b"\n").rsplit(b" ", 3)
            if len(parts) < 4:
                continue
            try:
                views = int(parts[2])
            except ValueError:
                continue
            if len(heap) < n:
                heapq.heappush(heap, views)
            elif views > heap[0]:
                heapq.heapreplace(heap, views)
            if i > 0 and i % 10_000_000 == 0:
                print(f"  pass1: {i:,} lines, heap[0]={heap[0]}", file=sys.stderr)

    threshold = heap[0]
    print(f"Threshold: {threshold}", file=sys.stderr)

    print("Pass 2: collecting lines ...", file=sys.stderr)
    above: list[tuple[int, bytes]] = []
    at: list[tuple[int, bytes]] = []
    with bz2.open(input_path, "rb") as f:
        for i, line in enumerate(f):
            parts = line.rstrip(b"\n").rsplit(b" ", 3)
            if len(parts) < 4:
                continue
            try:
                views = int(parts[2])
            except ValueError:
                continue
            if views > threshold:
                above.append((views, line))
            elif views == threshold:
                at.append((views, line))
            if i > 0 and i % 10_000_000 == 0:
                print(f"  pass2: {i:,} lines, above={len(above)}, at={len(at)}", file=sys.stderr)

    above.sort(key=lambda x: (-x[0], x[1]))
    at.sort(key=lambda x: (-x[0], x[1]))
    result = above + at[: max(0, n - len(above))]
    result = result[:n]

    print(f"Writing {output_path} ...", file=sys.stderr)
    count = 0
    with open(output_path, "w") as out:
        for views, line in result:
            for url_line in line_to_urls(views, line):
                out.write(url_line + "\n")
                count += 1
    print(f"  {count} URLs written", file=sys.stderr)

    os.unlink(input_path)
    print("Done.", file=sys.stderr)


def parse_pageview_line(line: str) -> PageviewRecord | None:
    line = line.rstrip("\n")
    if not line:
        return None
    try:
        url, pageviews_raw = line.rsplit("\t", 1)
        pageviews = int(pageviews_raw)
    except ValueError:
        return None
    if not url or pageviews <= 0:
        return None
    return PageviewRecord(url=url, pageviews=pageviews)


def compute_url_score(pageviews: int, max_pageviews: int) -> float:
    if max_pageviews <= 0:
        raise ValueError("max_pageviews must be positive")
    return 1.0 + (float(pageviews) / float(max_pageviews))


def find_max_pageviews(path: str | Path) -> tuple[int, int]:
    max_pageviews = 0
    valid_rows = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            rec = parse_pageview_line(line)
            if rec is None:
                continue
            valid_rows += 1
            max_pageviews = max(max_pageviews, rec.pageviews)
    return max_pageviews, valid_rows


def iter_injectable_pageviews(
    path: str | Path,
    stats: InjectionStats,
    max_url_len: int = MAX_URL_LEN,
):
    seen: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            stats.scanned += 1
            rec = parse_pageview_line(line)
            if rec is None:
                stats.invalid += 1
                continue
            if rec.url in seen:
                stats.duplicates += 1
                continue
            seen.add(rec.url)
            if len(rec.url) > max_url_len:
                stats.oversized += 1
                continue
            stats.queued += 1
            yield rec


def load_crawler_deps() -> SimpleNamespace:
    # tldextract defaults to $HOME/.cache, which is not writable in some
    # sandboxed/cron environments. Set this before importing crawler helpers.
    os.environ.setdefault("TLDEXTRACT_CACHE", "/tmp/tldextract")
    for path in (WEBCRAWLER_DIR, WEBCRAWLER_SCRIPTS_DIR):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)

    import psycopg2
    import tldextract
    from constants import (
        CRAWLERDB,
        NUM_SHARDS,
        SOURCE_GOLDEN,
        SOURCE_PAGEVIEW,
    )
    from libs.db.sharding.key import compute_shard, load_sharding_config
    from psycopg2.extras import execute_values

    return SimpleNamespace(
        psycopg2=psycopg2,
        tldextract=tldextract,
        execute_values=execute_values,
        CRAWLERDB=CRAWLERDB,
        NUM_SHARDS=NUM_SHARDS,
        SOURCE_GOLDEN=SOURCE_GOLDEN,
        SOURCE_PAGEVIEW=SOURCE_PAGEVIEW,
        compute_shard=compute_shard,
        load_sharding_config=load_sharding_config,
    )


def extract_domain(url: str, tldextract_module) -> str | None:
    # Match crawler spider / golden_inject eTLD+1 behavior so rows land on the
    # same domain_state entry as naturally discovered URLs.
    e = tldextract_module.extract(url)
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


def ensure_domain(crawler_cur, domain: str, shard_id: int) -> tuple[int, float]:
    crawler_cur.execute(
        """
        INSERT INTO domain_state (domain, shard_id)
        VALUES (%s, %s)
        ON CONFLICT (domain) DO NOTHING
        """,
        (domain, shard_id),
    )
    crawler_cur.execute(
        "SELECT domain_id, COALESCE(domain_score, 0.0) FROM domain_state WHERE domain = %s",
        (domain,),
    )
    row = crawler_cur.fetchone()
    return int(row[0]), float(row[1])


def current_upsert_sql(shard_id: int, source_golden: int) -> str:
    tcur = f"url_state_current_{shard_id:03d}"
    return f"""
    INSERT INTO {tcur} (url, domain_id, domain_score, url_score, source)
    VALUES %s
    ON CONFLICT (url) DO UPDATE SET
      url_score = EXCLUDED.url_score,
      source = CASE
        WHEN {tcur}.source = {int(source_golden)} THEN {tcur}.source
        ELSE EXCLUDED.source
      END
    RETURNING url, (xmax = 0) AS inserted
    """


def history_insert_sql(shard_id: int) -> str:
    thist = f"url_state_history_{shard_id:03d}"
    return f"""
    INSERT INTO {thist} (url, domain_id, domain_score, url_score, source)
    VALUES %s
    """


def inject_pageview_batch(
    crawler_cur,
    execute_values,
    shard_id: int,
    rows: list[PageviewInjectRow],
    source_golden: int,
) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (row.url, row.domain_id, row.domain_score, row.url_score, row.source)
        for row in rows
    ]
    returned = execute_values(
        crawler_cur,
        current_upsert_sql(shard_id, source_golden),
        values,
        page_size=len(values),
        fetch=True,
    )
    inserted_urls = {url for url, inserted in returned if inserted}
    if inserted_urls:
        history_rows = [row for row in values if row[0] in inserted_urls]
        execute_values(
            crawler_cur,
            history_insert_sql(shard_id),
            history_rows,
            page_size=len(history_rows),
        )
    return len(inserted_urls), len(rows) - len(inserted_urls)


def inject_top_pageviews(
    pageview_path: str | Path,
    *,
    dry_run: bool = False,
    batch_size: int = DEFAULT_INJECT_BATCH_SIZE,
) -> InjectionStats:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    stats = InjectionStats()
    stats.max_pageviews, stats.valid_file_rows = find_max_pageviews(pageview_path)
    if stats.max_pageviews <= 0:
        print(f"No valid pageview rows found in {pageview_path}", file=sys.stderr)
        return stats

    deps = load_crawler_deps()
    crawler_conn = deps.psycopg2.connect(**deps.CRAWLERDB)
    pending: dict[int, list[PageviewInjectRow]] = defaultdict(list)

    def flush_shard(crawler_cur, shard_id: int) -> None:
        rows = pending[shard_id]
        if not rows:
            return
        inserted, updated = inject_pageview_batch(
            crawler_cur,
            deps.execute_values,
            shard_id,
            rows,
            deps.SOURCE_GOLDEN,
        )
        stats.inserted += inserted
        stats.updated += updated
        rows.clear()

    try:
        crawler_cur = crawler_conn.cursor()
        overrides, split_subdomains = deps.load_sharding_config(
            INGEST_CONFIG, crawler_conn
        )
        domain_cache: dict[str, tuple[int, int, float]] = {}
        last_progress = 0

        for rec in iter_injectable_pageviews(pageview_path, stats):
            domain = extract_domain(rec.url, deps.tldextract)
            if not domain:
                stats.bad_domain += 1
                continue

            if domain not in domain_cache:
                shard_id = deps.compute_shard(
                    domain, deps.NUM_SHARDS, overrides, split_subdomains
                )
                if dry_run:
                    domain_cache[domain] = (0, shard_id, 0.0)
                else:
                    domain_id, domain_score = ensure_domain(
                        crawler_cur, domain, shard_id
                    )
                    domain_cache[domain] = (domain_id, shard_id, domain_score)

            domain_id, shard_id, domain_score = domain_cache[domain]
            if dry_run:
                stats.dry_run_urls += 1
            else:
                pending[shard_id].append(
                    PageviewInjectRow(
                        url=rec.url,
                        domain_id=domain_id,
                        domain_score=domain_score,
                        url_score=compute_url_score(
                            rec.pageviews, stats.max_pageviews
                        ),
                        source=deps.SOURCE_PAGEVIEW,
                    )
                )
                if len(pending[shard_id]) >= batch_size:
                    flush_shard(crawler_cur, shard_id)

            if stats.queued - last_progress >= 100_000:
                last_progress = stats.queued
                print(
                    f"  inject scan: {stats.queued:,} unique URLs queued",
                    file=sys.stderr,
                )

        if not dry_run:
            for shard_id in list(pending):
                flush_shard(crawler_cur, shard_id)
            crawler_conn.commit()

        action = "Would inject" if dry_run else "Injected"
        print(
            (
                f"{action} pageview URLs from {pageview_path}: "
                f"max_pageviews={stats.max_pageviews:,}, "
                f"unique_valid={stats.queued:,}, "
                f"inserted={stats.inserted:,}, updated={stats.updated:,}, "
                f"dry_run_urls={stats.dry_run_urls:,}, "
                f"invalid={stats.invalid:,}, duplicates={stats.duplicates:,}, "
                f"oversized={stats.oversized:,}, bad_domain={stats.bad_domain:,}"
            ),
            file=sys.stderr,
        )
        return stats
    except Exception:
        if not dry_run:
            crawler_conn.rollback()
        raise
    finally:
        crawler_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download weekly Wikimedia pageviews, extract Top-K URLs, and inject crawlerdb scores."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve routing and print a summary without writing crawlerdb.",
    )
    parser.add_argument(
        "--skip-inject",
        action="store_true",
        help="Only download/extract the top pageview file; do not write crawlerdb.",
    )
    parser.add_argument(
        "--inject-batch-size",
        type=int,
        default=DEFAULT_INJECT_BATCH_SIZE,
        help="Rows per shard upsert batch during crawlerdb injection.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dt = latest_available_date()
    date_str = dt.strftime("%Y%m%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    fname = f"pageviews-{date_str}-automated.bz2"
    url = f"{BASE_URL}/{year}/{year}-{month}/{fname}"
    bz2_path = f"pageviews-{date_str}-automated.bz2"
    out_path = f"top{N}_pageviews_{date_str}.txt"

    print(f"Target date: {date_str}", file=sys.stderr)

    if not os.path.exists(bz2_path):
        download(url, bz2_path)
    else:
        print(f"{bz2_path} already exists, skipping download", file=sys.stderr)

    if not os.path.exists(out_path):
        extract_top(bz2_path, out_path, N)
    else:
        print(f"{out_path} already exists, skipping extraction", file=sys.stderr)

    if args.skip_inject:
        print("Skipping crawlerdb injection", file=sys.stderr)
    else:
        inject_top_pageviews(
            out_path,
            dry_run=args.dry_run,
            batch_size=args.inject_batch_size,
        )


if __name__ == "__main__":
    main()

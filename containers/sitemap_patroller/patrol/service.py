"""Patrol worker logic.

Selects rows from `domain_sitemap` whose `last_patrolled_at` has aged past
`due_interval_hours`, fetches each with conditional GET, parses
urlset/sitemapindex, and emits "new outlink candidate" records into the
ingestor's IPC dir (the same path the router writes to). The existing
ingestor consumes them with no changes.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

import psycopg2

from containers.sitemap_patroller import SITEMAP_USER_AGENT
from libs.db.sharding.router import ShardRouter, host_of
from libs.ipc.folder_reader import current_interval
from libs.ipc.jsonio import append_jsonl
from libs.ipc.new_link_record import (
    DISCOVERY_SOURCE_SITEMAP,
    build_new_link_record,
)


logger = logging.getLogger("sitemap_patrol")

FETCH_TIMEOUT_SEC = 10.0
MAX_RESPONSE_BYTES = 50 * 1024 * 1024
MAX_URL_LEN = 2500  # match ingestor's MAX_URL_LEN
SNIFF_PREFIX_BYTES = 4096


@dataclass(frozen=True)
class PatrolConfig:
    dsn: str
    ingestor_dir_template: str
    interval_minutes: int
    # Shared with the router via libs.db.sharding.router.ShardRouter — guarantees
    # we route URLs to the same shard / ingestor the router would.
    sharder: ShardRouter
    due_interval_hours: int
    batch_limit: int
    global_delay_sec: float
    per_domain_cooldown_sec: float


# ----- HTTP -----

def fetch_sitemap(url: str, etag: str | None, last_modified: str | None) -> tuple[int, bytes, dict]:
    """Returns (status, body, response_headers). 304 returns (304, b'', {})."""
    headers = {"User-Agent": SITEMAP_USER_AGENT,
               "Accept": "application/xml, text/xml, */*"}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=FETCH_TIMEOUT_SEC) as resp:
            body = resp.read(MAX_RESPONSE_BYTES)
            return resp.status, body, dict(resp.headers.items())
    except HTTPError as e:
        if e.code == 304:
            return 304, b"", dict(e.headers.items()) if e.headers else {}
        raise


# ----- XML parsing -----

def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def parse_sitemap(body: bytes) -> tuple[str, list[str]]:
    """Return (kind, urls). kind is 'urlset', 'sitemapindex', or 'unknown'.
    URLs are <loc> text, trimmed, absolute http(s) only, capped at MAX_URL_LEN."""
    head = body[:SNIFF_PREFIX_BYTES].lower()
    if b"<urlset" not in head and b"<sitemapindex" not in head:
        return "unknown", []

    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        logger.info(
            "patrol.xml_parse_error",
            extra={"event": "patrol.xml_parse_error", "err": str(e)},
        )
        return "unknown", []

    kind = _localname(root.tag)
    if kind not in ("urlset", "sitemapindex"):
        return "unknown", []

    urls: list[str] = []
    for child in root:
        for sub in child:
            if _localname(sub.tag) != "loc":
                continue
            txt = (sub.text or "").strip()
            if (txt.startswith("http://") or txt.startswith("https://")) and len(txt) <= MAX_URL_LEN:
                urls.append(txt)
            break
    return kind, urls


# ----- DB -----

def select_due_rows(cur, due_interval_hours: int, limit: int) -> list[dict]:
    # DISTINCT ON (domain_id) returns at most one row per domain so the
    # per-run cooldown in run_once() can't reject 99% of the batch when
    # nested sitemapindex children cluster the table by domain. The outer
    # ORDER BY rotates fairly across domains by oldest-due first.
    cur.execute(
        """
        WITH per_domain_oldest AS (
            SELECT DISTINCT ON (ds.domain_id)
                   ds.id, ds.sitemap_url, ds.etag, ds.last_modified,
                   ds.domain_id, dst.domain, ds.last_patrolled_at
            FROM domain_sitemap ds
            JOIN domain_state dst USING (domain_id)
            WHERE ds.last_patrolled_at IS NULL
               OR ds.last_patrolled_at < NOW() - (%s * INTERVAL '1 hour')
            ORDER BY ds.domain_id, ds.last_patrolled_at NULLS FIRST
        )
        SELECT id, sitemap_url, etag, last_modified, domain_id, domain
        FROM per_domain_oldest
        ORDER BY last_patrolled_at NULLS FIRST
        LIMIT %s
        """,
        (due_interval_hours, limit),
    )
    cols = ("id", "sitemap_url", "etag", "last_modified", "domain_id", "domain")
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_row(
    cur,
    row_id: int,
    *,
    status: str,
    url_count: int | None,
    new_count: int | None,
    etag: str | None,
    last_modified: str | None,
) -> None:
    cur.execute(
        """
        UPDATE domain_sitemap
           SET last_patrolled_at = NOW(),
               last_url_count    = COALESCE(%s, last_url_count),
               last_new_count    = COALESCE(%s, last_new_count),
               etag              = COALESCE(%s, etag),
               last_modified     = COALESCE(%s, last_modified),
               status            = %s
         WHERE id = %s
        """,
        (url_count, new_count, etag, last_modified, status, row_id),
    )


def insert_nested_sitemap(cur, domain_id: int, sitemap_url: str) -> bool:
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


def ensure_domain(cur, domain: str, shard_id: int) -> tuple[int, float]:
    """Insert domain_state if missing; return (domain_id, domain_score).
    Mirrors scripts/golden_inject.py:ensure_domain."""
    cur.execute(
        """
        INSERT INTO domain_state (domain, shard_id)
        VALUES (%s, %s)
        ON CONFLICT (domain) DO NOTHING
        """,
        (domain, shard_id),
    )
    cur.execute(
        "SELECT domain_id, COALESCE(domain_score, 0.0) FROM domain_state WHERE domain = %s",
        (domain,),
    )
    row = cur.fetchone()
    return int(row[0]), float(row[1])


# ----- IPC emit -----

class IngestorEmitter:
    """Writes router "new outlink" records to the per-ingestor time-bucketed
    JSONL files the ingestor's FolderReader scans."""

    def __init__(self, ingestor_dir_template: str, interval_minutes: int, run_tag: str):
        self.template = ingestor_dir_template
        self.interval_minutes = interval_minutes
        self.run_tag = run_tag

    def _out_path(self, ingestor_id: int) -> Path:
        base = Path(self.template.format(id=ingestor_id))
        date, time_ = current_interval(self.interval_minutes)
        out_dir = base / date / time_
        fname = f"{datetime.now(timezone.utc).strftime('%H%M')}_sitemap_{self.run_tag}.jsonl"
        return out_dir / fname

    def emit(self, *, ingestor_id: int, record: dict) -> None:
        append_jsonl(str(self._out_path(ingestor_id)), record)


# ----- per-row processing -----

def process_row(
    row: dict,
    cfg: PatrolConfig,
    emitter: IngestorEmitter,
    cur,
    domain_cache: dict,
) -> dict:
    counters = {"urls_emitted": 0, "nested_registered": 0, "status": "ok"}

    try:
        status_code, body, headers = fetch_sitemap(
            row["sitemap_url"], row["etag"], row["last_modified"]
        )
    except HTTPError as e:
        counters["status"] = f"http_{e.code}"
        update_row(cur, row["id"], status=counters["status"],
                   url_count=None, new_count=None, etag=None, last_modified=None)
        return counters
    except (URLError, TimeoutError, ConnectionError) as e:
        counters["status"] = "timeout" if isinstance(e, TimeoutError) else f"err_{type(e).__name__}"
        update_row(cur, row["id"], status=counters["status"],
                   url_count=None, new_count=None, etag=None, last_modified=None)
        return counters

    if status_code == 304:
        counters["status"] = "not_modified"
        update_row(cur, row["id"], status="not_modified",
                   url_count=None, new_count=None, etag=None, last_modified=None)
        return counters

    if status_code != 200:
        counters["status"] = f"http_{status_code}"
        update_row(cur, row["id"], status=counters["status"],
                   url_count=None, new_count=None, etag=None, last_modified=None)
        return counters

    kind, locs = parse_sitemap(body)
    if kind == "unknown":
        counters["status"] = "parse_error"
        update_row(cur, row["id"], status="parse_error",
                   url_count=0, new_count=0,
                   etag=headers.get("ETag"), last_modified=headers.get("Last-Modified"))
        return counters

    src_domain_key = cfg.sharder.domain_key(row["domain"])

    if kind == "sitemapindex":
        for nested_url in locs:
            insert_nested_sitemap(cur, row["domain_id"], nested_url)
            counters["nested_registered"] += 1
        update_row(cur, row["id"], status="ok",
                   url_count=len(locs), new_count=None,
                   etag=headers.get("ETag"), last_modified=headers.get("Last-Modified"))
        return counters

    # urlset — route each <loc> URL the same way the router does for outlinks.
    for loc_url in locs:
        host = host_of(loc_url)
        if not host:
            continue
        dkey, sid, iid = cfg.sharder.route(host)

        cached = domain_cache.get(dkey)
        if cached is None:
            cached = ensure_domain(cur, dkey, sid)
            domain_cache[dkey] = cached
        domain_id, domain_score = cached

        record = build_new_link_record(
            url=loc_url,
            shard_id=sid,
            domain_id=domain_id,
            domain_score=domain_score,
            discovered_from=row["sitemap_url"],
            discovery_source_type=DISCOVERY_SOURCE_SITEMAP,
            inlink_count_external=1 if dkey != src_domain_key else 0,
        )
        emitter.emit(ingestor_id=iid, record=record)
        counters["urls_emitted"] += 1

    update_row(cur, row["id"], status="ok",
               url_count=len(locs), new_count=counters["urls_emitted"],
               etag=headers.get("ETag"), last_modified=headers.get("Last-Modified"))
    return counters


# ----- run loop -----

def run_once(cfg: PatrolConfig) -> dict:
    started = time.monotonic()
    run_tag = f"{os.getpid()}_{int(time.time())}"
    emitter = IngestorEmitter(cfg.ingestor_dir_template, cfg.interval_minutes, run_tag)

    totals = {
        "rows_due": 0,
        "ok": 0, "not_modified": 0, "parse_error": 0,
        "http_err": 0, "other_err": 0, "skipped_cooldown": 0,
        "urls_emitted": 0, "nested_registered": 0,
    }

    conn = psycopg2.connect(cfg.dsn)
    try:
        cur = conn.cursor()
        rows = select_due_rows(cur, cfg.due_interval_hours, cfg.batch_limit)
        totals["rows_due"] = len(rows)
        logger.info(
            "patrol.start",
            extra={"event": "patrol.start",
                   "rows_due": len(rows),
                   "due_interval_hours": cfg.due_interval_hours,
                   "batch_limit": cfg.batch_limit},
        )

        domain_cache: dict[str, tuple[int, float]] = {}
        per_domain_last: dict[str, float] = {}
        last_fetch = 0.0

        for row in rows:
            now = time.monotonic()
            last = per_domain_last.get(row["domain"], 0.0)
            if now - last < cfg.per_domain_cooldown_sec:
                totals["skipped_cooldown"] += 1
                continue

            gap = now - last_fetch
            if last_fetch != 0.0 and gap < cfg.global_delay_sec:
                time.sleep(cfg.global_delay_sec - gap)
            last_fetch = time.monotonic()
            per_domain_last[row["domain"]] = last_fetch

            result = process_row(row, cfg, emitter, cur, domain_cache)
            totals["urls_emitted"] += result["urls_emitted"]
            totals["nested_registered"] += result["nested_registered"]

            s = result["status"]
            if s == "ok":
                totals["ok"] += 1
            elif s == "not_modified":
                totals["not_modified"] += 1
            elif s == "parse_error":
                totals["parse_error"] += 1
            elif s.startswith("http_"):
                totals["http_err"] += 1
            else:
                totals["other_err"] += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    totals["elapsed_sec"] = round(time.monotonic() - started, 2)
    logger.info("patrol.done", extra={"event": "patrol.done", **totals})
    return totals

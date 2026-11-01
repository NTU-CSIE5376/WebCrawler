#!/usr/bin/env python3
"""Weekly script: download Wikimedia pageviews, extract the Top-K Wikipedia
articles, and inject them into crawlerdb with pageview-based scheduler priority.

Ranking uses the `user` agent dump (human traffic) by default and sums each
article's views across access methods (desktop / mobile-web / mobile-app)."""
from __future__ import annotations

import argparse
import bz2
import heapq
import http.client
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

DEFAULT_TOP_K = 1_000_000
AGENTS = ("user", "automated")
DEFAULT_AGENT = "user"
TOP_PROJECT_SUFFIX = b".wikipedia"
# Dump rows that are not crawlable articles: unknown title, and the English
# Special: namespace (e.g. Special:Search sits near the top every day).
NON_ARTICLE_TITLES = {"-"}
NON_ARTICLE_PREFIXES = ("Special:",)
BASE_URL = "https://dumps.wikimedia.org/other/pageview_complete"
MAX_URL_LEN = 2500
DEFAULT_INJECT_BATCH_SIZE = 1000
DEFAULT_DOWNLOAD_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 5.0
DEFAULT_RETENTION_DAYS = 45
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 120
PARTIAL_RETENTION_SECONDS = 24 * 60 * 60

WEBCRAWLER_SCRIPTS_DIR = Path(__file__).resolve().parent
WEBCRAWLER_DIR = WEBCRAWLER_SCRIPTS_DIR.parent
DEFAULT_DATA_DIR = Path("/data/wiki_pageviews")
INGEST_CONFIG = (
    WEBCRAWLER_DIR / "containers/scheduler_ingest/config/ingest.yaml"
)

DUMP_ARTIFACT_RE = re.compile(r"^pageviews-(\d{8})-(?:user|automated)\.bz2(?:\.part)?$")
TOP_ARTIFACT_RE = re.compile(
    r"^top\d+_pageviews_(\d{8})(?:_(?:user|automated))?\.txt(?:\.part)?$"
)
PRIORITY_ARTIFACT_RE = re.compile(r"^priority_(\d{8})_(user|automated)\.txt(?:\.part)?$")
CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")


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
    skipped: int = 0
    demote_candidates: int = 0
    demoted: int = 0
    priority_urls: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DumpValidationStats:
    compressed_size: int
    total_rows: int
    valid_rows: int
    max_views: int


@dataclass(frozen=True)
class ContentRange:
    start: int
    end: int
    total: int | None


class DownloadValidationError(RuntimeError):
    pass


class IncompleteDownloadError(DownloadValidationError):
    pass


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {408, 429} or exc.code >= 500
    return isinstance(
        exc,
        (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            http.client.HTTPException,
        ),
    )


def retry_sleep_seconds(backoff_seconds: float, attempt: int) -> float:
    return backoff_seconds * (2 ** max(0, attempt - 1))


def fetch_listing(
    url: str,
    *,
    retries: int = DEFAULT_DOWNLOAD_RETRIES,
    backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
) -> str:
    if retries <= 0:
        raise ValueError("retries must be positive")

    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return resp.read().decode()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            if attempt >= retries or not is_retryable_error(exc):
                raise
            sleep_for = retry_sleep_seconds(backoff_seconds, attempt)
            print(
                f"Listing fetch failed ({exc}); retrying in {sleep_for:.1f}s "
                f"({attempt}/{retries})",
                file=sys.stderr,
            )
            time.sleep(sleep_for)

    raise RuntimeError(f"failed to fetch listing {url}")


def latest_available_date(
    *,
    agent: str = DEFAULT_AGENT,
    retries: int = DEFAULT_DOWNLOAD_RETRIES,
    backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
) -> datetime:
    today = datetime.now()
    for day_offset in range(1, 10):
        dt = today - timedelta(days=day_offset)
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        dir_url = f"{BASE_URL}/{year}/{year}-{month}/"
        try:
            html = fetch_listing(
                dir_url,
                retries=retries,
                backoff_seconds=backoff_seconds,
            )
        except urllib.error.HTTPError as exc:
            if is_retryable_error(exc):
                raise
            continue
        pattern = re.escape(dt.strftime("%Y%m%d"))
        if re.search(rf"pageviews-{pattern}-{re.escape(agent)}\.bz2", html):
            return dt
    raise RuntimeError(f"no recent {agent} pageview dump found")


def parse_pageview_dump_views(line: bytes) -> int | None:
    parts = line.rstrip(b"\n").rsplit(b" ", 3)
    if len(parts) < 4:
        return None
    try:
        views = int(parts[2])
    except ValueError:
        return None
    if views <= 0:
        return None
    return views


def validate_pageview_dump(path: str | Path) -> DumpValidationStats:
    dump_path = Path(path)
    try:
        compressed_size = dump_path.stat().st_size
    except FileNotFoundError as exc:
        raise DownloadValidationError(f"{dump_path} does not exist") from exc

    if compressed_size <= 0:
        raise DownloadValidationError(f"{dump_path} is empty")

    total_rows = 0
    valid_rows = 0
    max_views = 0
    try:
        with bz2.open(dump_path, "rb") as f:
            for line in f:
                total_rows += 1
                views = parse_pageview_dump_views(line)
                if views is None:
                    continue
                valid_rows += 1
                max_views = max(max_views, views)
    except (EOFError, OSError) as exc:
        raise DownloadValidationError(
            f"{dump_path} is not a valid bzip2 pageview dump: {exc}"
        ) from exc

    if total_rows <= 0:
        raise DownloadValidationError(f"{dump_path} contains no rows")
    if valid_rows <= 0:
        raise DownloadValidationError(f"{dump_path} contains no valid pageview rows")

    return DumpValidationStats(
        compressed_size=compressed_size,
        total_rows=total_rows,
        valid_rows=valid_rows,
        max_views=max_views,
    )


def validate_existing_dump(path: str | Path) -> bool:
    try:
        stats = validate_pageview_dump(path)
    except DownloadValidationError as exc:
        print(f"Existing dump {path} is invalid: {exc}", file=sys.stderr)
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        return False

    print(
        f"{path} already exists and validated "
        f"({stats.compressed_size:,} bytes, {stats.valid_rows:,} valid rows)",
        file=sys.stderr,
    )
    return True


def expected_content_length(resp) -> int | None:
    value = resp.info().get("Content-Length")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def response_content_range(resp) -> ContentRange | None:
    value = resp.info().get("Content-Range")
    if value is None:
        return None
    match = CONTENT_RANGE_RE.match(value.strip())
    if match is None:
        return None
    total_raw = match.group(3)
    return ContentRange(
        start=int(match.group(1)),
        end=int(match.group(2)),
        total=None if total_raw == "*" else int(total_raw),
    )


def download_request(url: str, resume_from: int) -> urllib.request.Request:
    headers = {}
    if resume_from > 0:
        headers["Range"] = f"bytes={resume_from}-"
    return urllib.request.Request(url, headers=headers)


def response_download_plan(resp, resume_from: int) -> tuple[bool, int | None]:
    status = resp.getcode()
    content_length = expected_content_length(resp)

    if resume_from > 0 and status == 206:
        content_range = response_content_range(resp)
        if content_range is not None:
            if content_range.start != resume_from:
                raise DownloadValidationError(
                    f"server resumed at byte {content_range.start:,}, "
                    f"expected {resume_from:,}"
                )
            if content_range.end < content_range.start:
                raise DownloadValidationError(
                    f"invalid Content-Range: end {content_range.end:,} "
                    f"is before start {content_range.start:,}"
                )
            if (
                content_range.total is not None
                and content_range.end >= content_range.total
            ):
                raise DownloadValidationError(
                    f"invalid Content-Range: end {content_range.end:,} "
                    f"is outside total {content_range.total:,}"
                )
            range_length = content_range.end - content_range.start + 1
            if content_length is not None and content_length != range_length:
                raise DownloadValidationError(
                    f"Content-Length {content_length:,} does not match "
                    f"Content-Range length {range_length:,}"
                )
            expected_total = content_range.total
            if expected_total is None and content_length is not None:
                expected_total = resume_from + content_length
            return True, expected_total

        if content_length is not None:
            return True, resume_from + content_length
        return True, None

    if resume_from > 0 and status == 200:
        print("  server ignored Range request; restarting download", file=sys.stderr)

    return False, content_length


def promote_valid_partial(part_path: Path, dest_path: Path) -> bool:
    try:
        stats = validate_pageview_dump(part_path)
    except DownloadValidationError:
        return False

    part_path.replace(dest_path)
    print(
        f"  saved {dest_path} "
        f"({stats.compressed_size:,} bytes, {stats.valid_rows:,} valid rows)",
        file=sys.stderr,
    )
    return True


def should_keep_partial(exc: BaseException) -> bool:
    if isinstance(exc, IncompleteDownloadError):
        return True
    if isinstance(exc, urllib.error.HTTPError):
        return is_retryable_error(exc)
    return isinstance(
        exc,
        (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            http.client.HTTPException,
        ),
    )


def download(
    url: str,
    dest: str | Path,
    *,
    retries: int = DEFAULT_DOWNLOAD_RETRIES,
    backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
) -> None:
    if retries <= 0:
        raise ValueError("retries must be positive")

    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = Path(f"{dest_path}.part")

    for attempt in range(1, retries + 1):
        try:
            resume_from = part_path.stat().st_size if part_path.exists() else 0
            action = "Resuming" if resume_from > 0 else "Downloading"
            print(
                f"{action} {url} to {part_path} "
                f"(attempt {attempt}/{retries}) ...",
                file=sys.stderr,
            )
            if resume_from > 0:
                print(f"  starting at byte {resume_from:,}", file=sys.stderr)

            request = download_request(url, resume_from)
            with urllib.request.urlopen(
                request, timeout=DOWNLOAD_TIMEOUT_SECONDS
            ) as resp:
                append, expected_total = response_download_plan(resp, resume_from)
                if not append:
                    resume_from = 0

                bytes_written = resume_from
                mode = "ab" if append else "wb"
                with open(part_path, mode) as out:
                    while True:
                        try:
                            chunk = resp.read(DOWNLOAD_CHUNK_SIZE)
                        except http.client.IncompleteRead as exc:
                            if exc.partial:
                                out.write(exc.partial)
                                bytes_written += len(exc.partial)
                            raise
                        if not chunk:
                            break
                        out.write(chunk)
                        bytes_written += len(chunk)

            if expected_total is not None and bytes_written != expected_total:
                message = f"expected {expected_total:,} bytes, got {bytes_written:,}"
                if bytes_written < expected_total:
                    raise IncompleteDownloadError(message)
                raise DownloadValidationError(message)

            stats = validate_pageview_dump(part_path)
            part_path.replace(dest_path)
            print(
                f"  saved {dest_path} "
                f"({stats.compressed_size:,} bytes, {stats.valid_rows:,} valid rows)",
                file=sys.stderr,
            )
            return
        except (
            DownloadValidationError,
            urllib.error.HTTPError,
            urllib.error.URLError,
            http.client.HTTPException,
            TimeoutError,
            ConnectionError,
            OSError,
        ) as exc:
            reset_partial = False
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 416:
                if part_path.exists() and promote_valid_partial(part_path, dest_path):
                    return
                reset_partial = True

            keep_partial = should_keep_partial(exc) and not reset_partial
            if not keep_partial:
                try:
                    part_path.unlink()
                except FileNotFoundError:
                    pass

            retryable = (
                reset_partial
                or isinstance(exc, DownloadValidationError)
                or is_retryable_error(exc)
            )
            if attempt >= retries or not retryable:
                raise

            sleep_for = retry_sleep_seconds(backoff_seconds, attempt)
            partial_note = ""
            if keep_partial and part_path.exists():
                try:
                    partial_note = (
                        f"; keeping partial ({part_path.stat().st_size:,} bytes)"
                    )
                except OSError:
                    partial_note = "; keeping partial"
            print(
                f"Download failed ({exc}){partial_note}; "
                f"retrying in {sleep_for:.1f}s "
                f"({attempt}/{retries})",
                file=sys.stderr,
            )
            time.sleep(sleep_for)


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


def article_urls(project: str, title: str) -> list[str]:
    host = f"{project}.org"
    encoded_title = urllib.parse.quote(title, safe=":/(),'")
    if project == "zh.wikipedia":
        variants = ["zh-cn", "zh-hans", "zh-hant", "zh-tw", "zh-hk", "zh-sg"]
        urls = [f"https://{host}/{v}/{encoded_title}" for v in variants]
        urls.append(f"https://{host}/wiki/{encoded_title}")
        return urls
    return [f"https://{host}/wiki/{encoded_title}"]


def line_to_urls(views: int, line: bytes) -> list[str]:
    parsed = extract_project_title(line)
    if parsed is None:
        return []
    project, title = parsed
    return [f"{url}\t{views}" for url in article_urls(project, title)]


def top_articles(
    lines,
    n: int,
    project_suffix: bytes = TOP_PROJECT_SUFFIX,
) -> list[tuple[int, str, str]]:
    """Top-n (views, project, title) articles, views summed over access methods.

    Dumps are sorted by (project, title), so an article's rows are adjacent and
    can be summed in one streaming pass with an n-sized heap. If the sort order
    breaks, an article can enter the heap as two partial sums; those are merged
    before ranking (a partial sum that fell out of the heap is lost, which only
    under-counts such rare articles).
    """
    if n <= 0:
        raise ValueError("n must be positive")
    heap: list[tuple[int, bytes, bytes]] = []
    cur: tuple[bytes, bytes] | None = None
    cur_views = 0

    def push(key: tuple[bytes, bytes], views: int) -> None:
        item = (views, key[0], key[1])
        if len(heap) < n:
            heapq.heappush(heap, item)
        elif item > heap[0]:
            heapq.heapreplace(heap, item)

    for i, line in enumerate(lines):
        if i > 0 and i % 10_000_000 == 0:
            print(f"  scan: {i:,} lines, heap={len(heap):,}", file=sys.stderr)
        first_space = line.find(b" ")
        if first_space <= 0 or not line[:first_space].endswith(project_suffix):
            continue
        views = parse_pageview_dump_views(line)
        if views is None:
            continue
        parsed = extract_project_title(line)
        if (
            parsed is None
            or parsed[1] in NON_ARTICLE_TITLES
            or parsed[1].startswith(NON_ARTICLE_PREFIXES)
        ):
            continue
        key = (parsed[0].encode(), parsed[1].encode())
        if key != cur:
            if cur is not None:
                push(cur, cur_views)
            cur, cur_views = key, 0
        cur_views += views
    if cur is not None:
        push(cur, cur_views)

    merged: dict[tuple[bytes, bytes], int] = defaultdict(int)
    for views, project, title in heap:
        merged[(project, title)] += views
    ranked = sorted(
        ((views, p.decode(), t.decode()) for (p, t), views in merged.items()),
        key=lambda x: (-x[0], x[1], x[2]),
    )
    return ranked[:n]


def extract_top(input_path: str | Path, output_path: str | Path, n: int) -> None:
    if n <= 0:
        raise ValueError("n must be positive")

    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    part_output_path = Path(f"{output_path}.part")
    if part_output_path.exists():
        part_output_path.unlink()

    print(f"Summing article views (top {n:,} Wikipedia articles) ...", file=sys.stderr)
    try:
        with bz2.open(input_path, "rb") as f:
            ranked = top_articles(f, n)
        if not ranked:
            raise RuntimeError(f"no valid Wikipedia pageview rows found in {input_path}")

        print(f"Writing {part_output_path} ...", file=sys.stderr)
        count = 0
        with open(part_output_path, "w", encoding="utf-8") as out:
            for views, project, title in ranked:
                for url in article_urls(project, title):
                    out.write(f"{url}\t{views}\n")
                    count += 1

        max_pageviews, valid_rows = find_max_pageviews(part_output_path)
        if max_pageviews <= 0 or valid_rows <= 0:
            raise RuntimeError(f"no valid extracted pageview rows in {part_output_path}")

        part_output_path.replace(output_path)
        print(
            f"  {len(ranked):,} articles / {count:,} URLs written to {output_path} "
            f"(views {ranked[-1][0]:,}..{ranked[0][0]:,})",
            file=sys.stderr,
        )
    except Exception:
        try:
            part_output_path.unlink()
        except FileNotFoundError:
            pass
        raise


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


def validate_existing_top_file(path: str | Path) -> bool:
    try:
        max_pageviews, valid_rows = find_max_pageviews(path)
    except (OSError, UnicodeDecodeError) as exc:
        print(f"Existing extracted file {path} is invalid: {exc}", file=sys.stderr)
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        return False

    if max_pageviews <= 0 or valid_rows <= 0:
        print(f"Existing extracted file {path} has no valid rows", file=sys.stderr)
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        return False

    print(
        f"{path} already exists and validated "
        f"({valid_rows:,} valid rows, max_pageviews={max_pageviews:,})",
        file=sys.stderr,
    )
    return True


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


def delete_previous_dumps(data_dir: str | Path, date_str: str) -> list[Path]:
    """Delete pageview dumps (and partials) dated before date_str.

    Each run only needs its own dump once the top list is extracted, and
    a user dump is ~730 MB, so older dumps are removed right away instead of
    waiting for the retention window.
    """
    deleted = []
    for path in Path(data_dir).iterdir():
        match = DUMP_ARTIFACT_RE.match(path.name)
        if not match or match.group(1) >= date_str:
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        deleted.append(path)
    if deleted:
        print(
            "Deleted previous dump(s): " + ", ".join(p.name for p in sorted(deleted)),
            file=sys.stderr,
        )
    return deleted


def artifact_date_from_name(name: str) -> datetime | None:
    for pattern in (DUMP_ARTIFACT_RE, TOP_ARTIFACT_RE, PRIORITY_ARTIFACT_RE):
        match = pattern.match(name)
        if match:
            return datetime.strptime(match.group(1), "%Y%m%d")
    return None


def cleanup_old_artifacts(
    data_dir: str | Path,
    retention_days: int,
    *,
    now: datetime | None = None,
) -> list[Path]:
    if retention_days < 0:
        raise ValueError("retention_days must be non-negative")

    root = Path(data_dir)
    if not root.exists():
        return []

    now = now or datetime.now()
    artifact_cutoff_date = (now - timedelta(days=retention_days)).date()
    partial_cutoff_ts = now.timestamp() - PARTIAL_RETENTION_SECONDS
    deleted: list[Path] = []

    for path in root.iterdir():
        if not path.is_file():
            continue
        artifact_date = artifact_date_from_name(path.name)
        if artifact_date is None:
            continue

        try:
            stat = path.stat()
        except FileNotFoundError:
            continue

        expired = artifact_date.date() < artifact_cutoff_date
        if path.name.endswith(".part"):
            expired = expired or stat.st_mtime < partial_cutoff_ts
        if not expired:
            continue

        try:
            path.unlink()
        except FileNotFoundError:
            continue
        deleted.append(path)

    if deleted:
        print(
            f"Cleaned {len(deleted)} old pageview artifact(s) from {root}",
            file=sys.stderr,
        )
    return deleted


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


# Every run the top-K list is compared against crawlerdb in this one upsert:
#   - new URLs are inserted (should_crawl defaults to TRUE in the table);
#   - URLs never fetched successfully (last_fetch_ok IS NULL) are put back on
#     the schedule (should_crawl = TRUE) unless they were scheduled within the
#     last day (likely still queued) or keep failing;
#   - URLs already crawled and golden rows are left untouched.
# url_score_updated_at is stamped like golden_inject.inject_url: the offerer
# sorts rows without it last, and the ranker scorer only rescores rows with
# url_score <= 1, so a pageview score (1, 2] stays above every ranker score and
# below golden (3.0).
UPSERT_TEMPLATE = "(%s, %s, %s, %s, NOW(), %s)"
RESCHEDULE_AFTER = "1 day"
RESCHEDULE_MAX_CONSECUTIVE_FAILS = 3


def current_upsert_sql(shard_id: int, source_golden: int) -> str:
    tcur = f"url_state_current_{shard_id:03d}"
    return f"""
    INSERT INTO {tcur} (url, domain_id, domain_score, url_score, url_score_updated_at, source)
    VALUES %s
    ON CONFLICT (url) DO UPDATE SET
      url_score = EXCLUDED.url_score,
      url_score_updated_at = EXCLUDED.url_score_updated_at,
      source = EXCLUDED.source,
      should_crawl = TRUE
    WHERE {tcur}.source IS DISTINCT FROM {int(source_golden)}
      AND {tcur}.last_fetch_ok IS NULL
      AND (
        {tcur}.should_crawl
        OR (
          ({tcur}.last_scheduled IS NULL
           OR {tcur}.last_scheduled < NOW() - INTERVAL '{RESCHEDULE_AFTER}')
          AND COALESCE({tcur}.num_consecutive_fail, 0) < {RESCHEDULE_MAX_CONSECUTIVE_FAILS}
        )
      )
    RETURNING url
    """


def existing_urls_sql(shard_id: int) -> str:
    # xmax is not available on all PG-wire-compatible targets (e.g. YSQL), so
    # "newly inserted vs. pre-existing" is determined from a pre-upsert
    # existence check rather than RETURNING ... (xmax = 0).
    tcur = f"url_state_current_{shard_id:03d}"
    return f"SELECT url FROM {tcur} WHERE url = ANY(%s)"


def demote_sql(shard_id: int, source_golden: int) -> str:
    """Hand rows that left the priority list back to the ranker scorer."""
    tcur = f"url_state_current_{shard_id:03d}"
    return f"""
    UPDATE {tcur}
    SET url_score = NULL, url_score_updated_at = NULL
    WHERE url = ANY(%s)
      AND should_crawl
      AND url_score > 1
      AND source IS DISTINCT FROM {int(source_golden)}
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
) -> tuple[int, int, int]:
    """Upsert one shard batch; returns (inserted, updated, skipped)."""
    if not rows:
        return 0, 0, 0

    values = [
        (row.url, row.domain_id, row.domain_score, row.url_score, row.source)
        for row in rows
    ]
    crawler_cur.execute(existing_urls_sql(shard_id), ([row.url for row in rows],))
    existing_before = {r[0] for r in crawler_cur.fetchall()}
    returned = execute_values(
        crawler_cur,
        current_upsert_sql(shard_id, source_golden),
        values,
        template=UPSERT_TEMPLATE,
        page_size=len(values),
        fetch=True,
    )
    inserted_urls = {url for (url,) in returned} - existing_before
    if inserted_urls:
        history_rows = [row for row in values if row[0] in inserted_urls]
        execute_values(
            crawler_cur,
            history_insert_sql(shard_id),
            history_rows,
            page_size=len(history_rows),
        )
    return (
        len(inserted_urls),
        len(returned) - len(inserted_urls),
        len(rows) - len(returned),
    )


def demotion_urls(previous: set[str] | None, current: list[str]) -> list[str]:
    """Previous run's priority URLs that are not in this run's list."""
    if not previous:
        return []
    return sorted(previous.difference(current))


def priority_list_path(data_dir: Path, date_str: str, agent: str) -> Path:
    return data_dir / f"priority_{date_str}_{agent}.txt"


def load_previous_priority(data_dir: Path, date_str: str, agent: str) -> set[str] | None:
    """URLs from the newest priority list strictly before date_str, if any."""
    candidates = []
    for path in Path(data_dir).glob(f"priority_*_{agent}.txt"):
        match = PRIORITY_ARTIFACT_RE.match(path.name)
        if match and match.group(1) < date_str:
            candidates.append((match.group(1), path))
    if not candidates:
        return None
    _, path = max(candidates)
    with open(path, encoding="utf-8") as f:
        urls = {line.rstrip("\n") for line in f if line.strip()}
    print(f"Previous priority list: {path} ({len(urls):,} URLs)", file=sys.stderr)
    return urls


def write_priority_list(path: Path, urls: list[str]) -> None:
    part = Path(f"{path}.part")
    with open(part, "w", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")
    part.replace(path)


def inject_top_pageviews(
    pageview_path: str | Path,
    *,
    dry_run: bool = False,
    batch_size: int = DEFAULT_INJECT_BATCH_SIZE,
    previous_priority: set[str] | None = None,
) -> InjectionStats:
    """Upsert this run's top URLs as priority rows, and demote the previous run's
    priority URLs that dropped out of the list, in one transaction."""
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
        inserted, updated, skipped = inject_pageview_batch(
            crawler_cur,
            deps.execute_values,
            shard_id,
            rows,
            deps.SOURCE_GOLDEN,
        )
        stats.inserted += inserted
        stats.updated += updated
        stats.skipped += skipped
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
            stats.priority_urls.append(rec.url)
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

        demote = demotion_urls(previous_priority, stats.priority_urls)
        stats.demote_candidates = len(demote)
        demote_by_shard: dict[int, list[str]] = defaultdict(list)
        for url in demote:
            domain = extract_domain(url, deps.tldextract)
            if domain:
                demote_by_shard[
                    deps.compute_shard(domain, deps.NUM_SHARDS, overrides, split_subdomains)
                ].append(url)
        if not dry_run:
            for shard_id, urls in sorted(demote_by_shard.items()):
                sql = demote_sql(shard_id, deps.SOURCE_GOLDEN)
                for i in range(0, len(urls), batch_size):
                    crawler_cur.execute(sql, (urls[i : i + batch_size],))
                    stats.demoted += crawler_cur.rowcount
            crawler_conn.commit()

        action = "Would inject" if dry_run else "Injected"
        print(
            (
                f"{action} pageview URLs from {pageview_path}: "
                f"max_pageviews={stats.max_pageviews:,}, "
                f"unique_valid={stats.queued:,}, "
                f"inserted={stats.inserted:,}, updated={stats.updated:,}, "
                f"skipped_golden_or_scheduled={stats.skipped:,}, "
                f"demote_candidates={stats.demote_candidates:,}, demoted={stats.demoted:,}, "
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
        description="Download weekly Wikimedia pageviews, extract the Top-K Wikipedia articles, and inject crawlerdb scores."
    )
    parser.add_argument(
        "--agent",
        choices=AGENTS,
        default=DEFAULT_AGENT,
        help=f"pageview_complete agent dump to rank by (default: {DEFAULT_AGENT}; 'automated' is bot traffic).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K,
        help=f"Number of Wikipedia articles (views summed over access methods) to inject (default: {DEFAULT_TOP_K:,}).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory for downloaded dumps and extracted top files (default: {DEFAULT_DATA_DIR}).",
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
    parser.add_argument(
        "--download-retries",
        type=int,
        default=DEFAULT_DOWNLOAD_RETRIES,
        help="Attempts for Wikimedia listing and dump downloads.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
        help="Initial retry backoff for Wikimedia requests.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help="Delete pageview artifacts in --data-dir older than this many days.",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Do not apply pageview artifact retention cleanup.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.download_retries <= 0:
        raise ValueError("--download-retries must be positive")
    if args.retry_backoff_seconds < 0:
        raise ValueError("--retry-backoff-seconds must be non-negative")
    if args.retention_days < 0:
        raise ValueError("--retention-days must be non-negative")
    if args.top_k <= 0:
        raise ValueError("--top-k must be positive")

    data_dir = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    if not args.no_cleanup:
        cleanup_old_artifacts(data_dir, args.retention_days)

    dt = latest_available_date(
        agent=args.agent,
        retries=args.download_retries,
        backoff_seconds=args.retry_backoff_seconds,
    )
    date_str = dt.strftime("%Y%m%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    fname = f"pageviews-{date_str}-{args.agent}.bz2"
    url = f"{BASE_URL}/{year}/{year}-{month}/{fname}"
    bz2_path = data_dir / fname
    out_path = data_dir / f"top{args.top_k}_pageviews_{date_str}_{args.agent}.txt"

    print(f"Target date: {date_str}", file=sys.stderr)
    print(f"Data directory: {data_dir}", file=sys.stderr)

    top_file_ready = out_path.exists() and validate_existing_top_file(out_path)
    if not top_file_ready:
        if not bz2_path.exists() or not validate_existing_dump(bz2_path):
            download(
                url,
                bz2_path,
                retries=args.download_retries,
                backoff_seconds=args.retry_backoff_seconds,
            )
        extract_top(bz2_path, out_path, args.top_k)
    delete_previous_dumps(data_dir, date_str)

    if args.skip_inject:
        print("Skipping crawlerdb injection", file=sys.stderr)
    else:
        previous = load_previous_priority(data_dir, date_str, args.agent)
        if previous is None:
            print(
                "WARNING: no earlier priority list found; rows that left the "
                "top list will keep their pageview score until they are crawled",
                file=sys.stderr,
            )
        stats = inject_top_pageviews(
            out_path,
            dry_run=args.dry_run,
            batch_size=args.inject_batch_size,
            previous_priority=previous,
        )
        if not args.dry_run and stats.priority_urls:
            write_priority_list(
                priority_list_path(data_dir, date_str, args.agent),
                stats.priority_urls,
            )

    if not args.no_cleanup:
        cleanup_old_artifacts(data_dir, args.retention_days)


if __name__ == "__main__":
    main()

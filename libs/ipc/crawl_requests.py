"""Shared file layout and records for crawl requests (bulk crawl v2).

Ownership: the API writes requests/ and inbox/ and deletes acked segments.
Crawler NN alone writes frontier/crawler_NN.sqlite, segments/*/crawler_NN/
and status/crawler_NN.json. The API never opens SQLite.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

GB = 1_000_000_000
REQUEST_ID = re.compile(r"[0-9a-f]{32}")
SEGMENT_NAME = re.compile(r"([0-9]{2})-([0-9]{12})\.jsonl\.gz")
CRAWLER_DIR = re.compile(r"crawler_([0-9]{2})")
PART_SUFFIX = ".part"


def utc_now() -> datetime:
    return datetime.now(UTC)


def is_expired(meta: dict, now: datetime | None = None) -> bool:
    return datetime.fromisoformat(meta["expires_at"]) <= (now or utc_now())


def batch_key(batch_id: str) -> str:
    """File-safe, deterministic name for a client-supplied batch_id."""
    return hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:32]


def segment_name(crawler_id: int, seq: int) -> str:
    return f"{crawler_id:02d}-{seq:012d}.jsonl.gz"


def parse_segment_name(name: object) -> tuple[int, int] | None:
    match = SEGMENT_NAME.fullmatch(name) if isinstance(name, str) else None
    return (int(match[1]), int(match[2])) if match else None


class CrawlRequestPaths:
    def __init__(self, root):
        self.root = Path(root)

    def meta(self, request_id: str) -> Path:
        return self.root / "requests" / f"{request_id}.json"

    def sealed(self, request_id: str) -> Path:
        return self.root / "requests" / f"{request_id}.sealed"

    def cancelled(self, request_id: str) -> Path:
        return self.root / "requests" / f"{request_id}.cancel"

    def request_dir(self, request_id: str) -> Path:
        return self.root / "requests" / request_id

    def batch(self, request_id: str, key: str) -> Path:
        return self.request_dir(request_id) / "batches" / f"{key}.json"

    def inbox(self, crawler_id: int) -> Path:
        return self.root / "inbox" / f"crawler_{crawler_id:02d}"

    def inbox_file(self, crawler_id: int, request_id: str, key: str) -> Path:
        return self.inbox(crawler_id) / f"{request_id}-{key}.json"

    def frontier(self, crawler_id: int) -> Path:
        return self.root / "frontier" / f"crawler_{crawler_id:02d}.sqlite"

    def segments(self, request_id: str, crawler_id: int | None = None) -> Path:
        base = self.root / "segments" / request_id
        return base if crawler_id is None else base / f"crawler_{crawler_id:02d}"

    def status(self, crawler_id: int | None = None) -> Path:
        base = self.root / "status"
        return base if crawler_id is None else base / f"crawler_{crawler_id:02d}.json"


def atomic_write_json(path: Path, value) -> None:
    """Write tmp + fsync + os.replace; unique tmp names let writers race safely."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def read_json(path: Path):
    with path.open("rb") as handle:
        return json.loads(handle.read())


def terminal_record(item_id: str, url: str, priority: int, *, final_url=None, status_code=None,
                    error=None, fetched_at: datetime | None = None, html=None) -> dict:
    return {
        "id": item_id, "url": url, "priority": priority, "final_url": final_url,
        "status_code": status_code, "error": error,
        "fetched_at": fetched_at.isoformat() if fetched_at else None,
        "finished_at": utc_now().isoformat(), "html": html,
    }

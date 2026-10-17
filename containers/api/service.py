from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, timezone
import re
from uuid import uuid4

from libs.urls import MAX_URL_LEN, validate_url
from libs.ipc.manual_bulk_crawl import job_status, queue_lock


RESULT_FILENAME = re.compile(r"(?:(?:request|status|[0-9]{5,})\.json|[0-9]{5,}\.html)")


class JobNotCompleted(Exception):
    pass


def validate_urls(payload: object, max_urls: int, max_url_length: int = MAX_URL_LEN) -> list[str]:
    """Validate the whole request before any DB/file writes; deduplicate canonically."""
    if not isinstance(payload, dict) or set(payload) != {"urls"}:
        raise ValueError('Expected an object containing only "urls"')
    urls = payload["urls"]
    if not isinstance(urls, list) or not 1 <= len(urls) <= max_urls:
        raise ValueError(f'"urls" must contain 1 to {max_urls} URLs')
    normalized = {}
    for index, url in enumerate(urls):
        try:
            canonical = validate_url(url, max_length=max_url_length)
        except ValueError:
            raise ValueError(f"Invalid HTTP(S) URL at index {index}") from None
        normalized[canonical] = None
    return list(normalized)


class ManualBulkCrawlService:
    def __init__(self, crawl_config: dict, *, route_urls):
        self.pending_dir = Path(crawl_config["pending_dir"])
        self.result_dir = Path(crawl_config["result_dir"])
        self.route_urls = route_urls

    def submit(self, urls: list[str]) -> str:
        file_tag = uuid4().hex
        records = {"version": 1, "urls": urls,
                   "crawler_ids": self.route_urls(urls),
                   "result_dir": str(self.result_dir.resolve()),
                   "created_at": datetime.now(timezone.utc).isoformat()}

        # One rename is the acceptance boundary, regardless of shard count.
        # No fallible work follows it: a returned storage error means no batch
        # was published. Existing crawlers handle the accepted job separately.
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        target = self.pending_dir / f"{file_tag}.json"
        temporary = target.with_suffix(".json.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as handle:
                json.dump(records, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            # Preparation is independent across requests. Only publication
            # needs the queue lock; .tmp files are never dispatched.
            with queue_lock(self.pending_dir):
                os.replace(temporary, target)
        except OSError:
            # A leftover .tmp is never accepted or read by crawlers.
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return file_tag

    def status(self, job_id: str) -> dict:
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise FileNotFoundError(job_id)
        completed = self.result_dir / job_id / "status.json"
        pending = self.pending_dir / f"{job_id}.json"
        try:
            return json.loads(completed.read_text(encoding="utf-8"))
        except FileNotFoundError:
            pass
        try:
            payload = json.loads(pending.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return json.loads(completed.read_text(encoding="utf-8"))
        result = job_status(job_id, payload, self.result_dir / ".incomplete" / job_id)
        # The worker may publish while the progress files are being read.
        try:
            return json.loads(completed.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return result

    def _completed_directory(self, job_id: str) -> Path:
        status = self.status(job_id)
        if status["status"] != "completed":
            raise JobNotCompleted(job_id)
        directory = self.result_dir / job_id
        if directory.is_symlink():
            raise FileNotFoundError(job_id)
        return directory

    def result_files(self, job_id: str) -> list[dict]:
        directory = self._completed_directory(job_id)
        urls = json.loads((directory / "request.json").read_text(encoding="utf-8"))["urls"]
        return [
            {"name": path.name, "size_bytes": path.stat().st_size,
             "url": urls[int(path.stem)] if path.stem.isdecimal() else None}
            for path in sorted(directory.iterdir())
            if RESULT_FILENAME.fullmatch(path.name) and not path.is_symlink() and path.is_file()
        ]

    def result_file(self, job_id: str, filename: str) -> Path:
        directory = self._completed_directory(job_id)
        if not RESULT_FILENAME.fullmatch(filename):
            raise FileNotFoundError(filename)
        path = directory / filename
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(filename)
        return path

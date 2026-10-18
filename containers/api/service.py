from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import re
import shutil
from uuid import uuid4

from libs.urls import MAX_URL_LEN, validate_url
from libs.ipc.manual_bulk_crawl import (
    DEFAULT_TTL_DAYS, expiration, is_expired, job_status, queue_lock, validate_ttl_days,
)


RESULT_FILENAME = re.compile(r"(?:(?:request|status|[0-9]{5,})\.json|[0-9]{5,}\.html)")


class JobNotCompleted(Exception):
    pass


def validate_manual_bulk_crawl_request(
    payload: object, max_urls: int, max_url_length: int = MAX_URL_LEN,
) -> tuple[list[str], int]:
    """Validate the request envelope and options before routing or writing."""
    if not isinstance(payload, dict) or "urls" not in payload or set(payload) - {"urls", "ttl_days"}:
        raise ValueError('Expected an object containing "urls" and optional "ttl_days"')
    ttl_days = validate_ttl_days(payload.get("ttl_days", DEFAULT_TTL_DAYS))
    return validate_urls(payload["urls"], max_urls, max_url_length), ttl_days


def validate_urls(urls: object, max_urls: int, max_url_length: int = MAX_URL_LEN) -> list[str]:
    """Validate a URL list and deduplicate in canonical order."""
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

    def submit(self, urls: list[str], ttl_days: int = DEFAULT_TTL_DAYS) -> str:
        validate_ttl_days(ttl_days)
        created = datetime.now(timezone.utc)
        file_tag = uuid4().hex
        records = {"version": 1, "urls": urls,
                   "crawler_ids": self.route_urls(urls),
                   "result_dir": str(self.result_dir.resolve()),
                   "created_at": created.isoformat(), "ttl_days": ttl_days,
                   "expires_at": (created + timedelta(days=ttl_days)).isoformat()}

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
            return self._visible_status(json.loads(completed.read_text(encoding="utf-8")))
        except FileNotFoundError:
            pass
        try:
            payload = json.loads(pending.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return self._visible_status(json.loads(completed.read_text(encoding="utf-8")))
        if is_expired(payload):
            raise FileNotFoundError(job_id)
        result = job_status(job_id, payload, self.result_dir / ".incomplete" / job_id)
        # The worker may publish while the progress files are being read.
        try:
            published = json.loads(completed.read_text(encoding="utf-8"))
        except FileNotFoundError:
            if not pending.exists():
                # A deletion can remove both pending and completed files.
                return self._visible_status(json.loads(completed.read_text(encoding="utf-8")))
            return self._visible_status(result)
        return self._visible_status(published)

    @staticmethod
    def _visible_status(status):
        if is_expired(status):
            raise FileNotFoundError(status.get("job_id"))
        return status

    def delete(self, job_id: str) -> None:
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise FileNotFoundError(job_id)
        # The coordination inode remains stable for workers already holding a
        # claim. No URL data or results are retained in this directory.
        with queue_lock(self.pending_dir / ".claims" / job_id):
            pending = self.pending_dir / f"{job_id}.json"
            directories = [self.result_dir / job_id, self.result_dir / ".incomplete" / job_id]
            if not pending.exists() and not any(path.exists() for path in directories):
                raise FileNotFoundError(job_id)
            pending.unlink(missing_ok=True)
            for directory in directories:
                if directory.is_symlink():
                    directory.unlink()
                elif directory.exists():
                    shutil.rmtree(directory)

    def collect_expired(self, *, now=None) -> int:
        now = now or datetime.now(timezone.utc)
        candidates = {path.stem: path for path in self.pending_dir.glob("*.json")}
        for root in (self.result_dir / ".incomplete", self.result_dir):
            for directory in root.glob("*"):
                if re.fullmatch(r"[a-f0-9]{32}", directory.name) and not directory.is_symlink():
                    candidates.setdefault(directory.name, directory / "request.json")
        removed = 0
        for job_id, path in candidates.items():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                deadline = expiration(payload, fallback=datetime.fromtimestamp(path.stat().st_mtime, timezone.utc))
                if deadline <= now:
                    self.delete(job_id)
                    removed += 1
            except FileNotFoundError:
                # Publication or another collector may have won the race.
                continue
            except (OSError, ValueError, TypeError):
                logging.getLogger(__name__).exception("manual_bulk_crawl.gc_failed", extra={"job_id": job_id})
        return removed

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

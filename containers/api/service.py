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
    DEFAULT_TTL_DAYS, expiration, is_expired, job_status,
    worker_directory, validate_ttl_days, validate_processing_mode, publish_job,
)


RESULT_FILENAME = re.compile(r"(?:(?:request|status|[0-9]{5,})\.json|[0-9]{5,}\.html)")


class JobNotCompleted(Exception):
    pass


def validate_manual_bulk_crawl_request(
    payload: object, max_urls: int, max_url_length: int = MAX_URL_LEN,
) -> tuple[list[str], int]:
    """Validate the request envelope and options before routing or writing."""
    if not isinstance(payload, dict) or "urls" not in payload or set(payload) - {"urls", "ttl_days", "processing_mode", "client_reference", "validate_urls"}:
        raise ValueError('Expected an object containing "urls" and optional "ttl_days"')
    reference = payload.get("client_reference")
    if reference is not None and (not isinstance(reference, str) or len(reference) > 128):
        raise ValueError("Invalid client_reference")
    validate_processing_mode(payload.get("processing_mode", "fetch"))
    ttl_days = validate_ttl_days(payload.get("ttl_days", DEFAULT_TTL_DAYS))
    validation_enabled = payload.get("validate_urls", True)
    if not isinstance(validation_enabled, bool):
        raise ValueError("validate_urls must be a boolean")
    return validate_urls(payload["urls"], max_urls, max_url_length, enabled=validation_enabled), ttl_days


def validate_urls(urls: object, max_urls: int, max_url_length: int = MAX_URL_LEN, *, enabled: bool = True) -> list[str]:
    """Validate a URL list and deduplicate in canonical order."""
    if not isinstance(urls, list) or not 1 <= len(urls) <= max_urls:
        raise ValueError(f'"urls" must contain 1 to {max_urls} URLs')
    normalized = {}
    for index, url in enumerate(urls):
        if not isinstance(url, str) or not url or len(url) > max_url_length:
            raise ValueError(f"Invalid URL string at index {index}")
        if not enabled:
            normalized[url] = None
            continue
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

    def submit(self, urls: list[str], ttl_days: int = DEFAULT_TTL_DAYS, *, processing_mode: str = "fetch",
               client_reference: str | None = None) -> str:
        validate_ttl_days(ttl_days)
        validate_processing_mode(processing_mode)
        return publish_job(self.pending_dir, self.result_dir, urls, self.route_urls(urls),
                           ttl_days=ttl_days, processing_mode=processing_mode,
                           client_reference=client_reference)

    def crawler_performance(self, crawler_id):
        path = self.pending_dir / "feedback" / f"crawler_{crawler_id:02d}.json"
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError:
            return {"version": 1, "available": False, "domains": {}}
        age = datetime.now(timezone.utc).timestamp() - data.get("generated_at", 0)
        if data.get("version") != 1 or not 0 <= age <= 30:
            return {"version": 1, "available": False, "domains": {}}
        return {**data, "available": True}

    def active_jobs(self, *, crawler_id=None, client_prefix="", limit=200, after=""):
        # Completed jobs retain manifests for result retrieval. Scan unfinished
        # partition markers instead, so hourly/daily history does not grow this query.
        pattern = f"crawler_{crawler_id:02d}/*.json" if crawler_id is not None else "crawler_*/*.json"
        ids = sorted({p.stem for p in self.pending_dir.glob(pattern) if p.stem > after})
        jobs = []
        cursor = None
        for job_id in ids:
            try:
                _, payload = self._manifest(job_id)
                if not (payload.get("client_reference") or "").startswith(client_prefix):
                    continue
                status = self.status(job_id)
            except FileNotFoundError:
                continue
            if status["status"] == "completed":
                continue
            status.pop("result_dir", None)
            if len(jobs) == limit:
                cursor = jobs[-1]["job_id"]
                break
            jobs.append(status)
        return {"jobs": jobs, "next_cursor": cursor}

    def _manifest(self, job_id: str) -> tuple[Path, dict]:
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise FileNotFoundError(job_id)
        path = self.pending_dir / f"{job_id}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        if is_expired(payload):
            raise FileNotFoundError(job_id)
        return path, payload

    def status(self, job_id: str) -> dict:
        path, payload = self._manifest(job_id)
        result = job_status(job_id, payload, self.result_dir / job_id)
        if not path.exists():
            raise FileNotFoundError(job_id)
        return result

    def delete(self, job_id: str) -> None:
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise FileNotFoundError(job_id)
        # Only API/GC retires a job. Rename removes visibility atomically, even
        # when DELETE and GC race. Workers never recreate output directories.
        retired = self.pending_dir / ".deleted"
        retired.mkdir(parents=True, exist_ok=True)
        marker = retired / f"{job_id}.json"
        os.replace(self.pending_dir / marker.name, marker)
        self._purge_deleted(marker)

    def _purge_deleted(self, marker: Path):
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return  # Another API/GC invocation already completed cleanup.
        for crawler_id in set(payload["crawler_ids"]):
            (self.pending_dir / f"crawler_{crawler_id:02d}" / marker.name).unlink(missing_ok=True)
        root = self.result_dir / marker.stem
        trash = self.result_dir / ".deleted" / marker.stem
        trash.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(root, trash)
        except FileNotFoundError:
            pass
        if trash.is_symlink():
            trash.unlink()
        elif trash.exists():
            try:
                shutil.rmtree(trash)
            except FileNotFoundError:
                pass
        marker.unlink(missing_ok=True)

    def collect_expired(self, *, now=None) -> int:
        now = now or datetime.now(timezone.utc)
        # Retry incomplete cleanup after API/GC death or a transient disk error.
        for marker in (self.pending_dir / ".deleted").glob("*.json"):
            try:
                self._purge_deleted(marker)
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).exception("manual_bulk_crawl.gc_failed", extra={"job_id": marker.stem})
        candidates = {path.stem: path for path in self.pending_dir.glob("*.json")}
        # Uncommitted preparation can survive API process death. Its private
        # request copy lets GC remove it after its retention window too.
        for directory in self.result_dir.glob("*"):
            if re.fullmatch(r"[a-f0-9]{32}", directory.name) and not directory.is_symlink():
                candidates.setdefault(directory.name, directory / "request.json")
        removed = 0
        for job_id, path in candidates.items():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                deadline = expiration(payload, fallback=datetime.fromtimestamp(path.stat().st_mtime, timezone.utc))
                if deadline <= now:
                    if path.parent == self.pending_dir:
                        self.delete(job_id)
                    else:
                        retired = self.pending_dir / ".deleted"
                        retired.mkdir(parents=True, exist_ok=True)
                        marker = retired / f"{job_id}.json"
                        os.replace(path, marker)
                        self._purge_deleted(marker)
                    removed += 1
            except FileNotFoundError:
                continue
            except (OSError, ValueError, TypeError):
                logging.getLogger(__name__).exception("manual_bulk_crawl.gc_failed", extra={"job_id": job_id})
        return removed

    def _completed_directory(self, job_id: str) -> Path:
        status = self.status(job_id)
        if status["status"] != "completed":
            raise JobNotCompleted(job_id)
        directory = self.result_dir / job_id
        if directory.is_symlink() or not directory.is_dir():
            raise FileNotFoundError(job_id)
        return directory

    def _status_bytes(self, job_id):
        status = self.status(job_id)
        status.pop("result_dir", None)
        return json.dumps(status).encode("utf-8")

    def result_files(self, job_id: str) -> list[dict]:
        directory = self._completed_directory(job_id)
        _, payload = self._manifest(job_id)
        files = []
        for crawler_id in sorted(set(payload["crawler_ids"])):
            partition = worker_directory(self.result_dir, job_id, crawler_id)
            if partition.is_symlink():
                continue
            for path in partition.iterdir():
                if (path.stem.isdecimal() and RESULT_FILENAME.fullmatch(path.name)
                        and not path.is_symlink() and path.is_file()):
                    index = int(path.stem)
                    if index < len(payload["urls"]) and payload["crawler_ids"][index] == crawler_id:
                        files.append({"name": path.name, "size_bytes": path.stat().st_size,
                                      "url": payload["urls"][index]})
        request = directory / "request.json"
        if not request.is_symlink() and request.is_file():
            files.append({"name": "request.json", "size_bytes": request.stat().st_size, "url": None})
        files.append({"name": "status.json", "size_bytes": len(self._status_bytes(job_id)), "url": None})
        return sorted(files, key=lambda item: item["name"])

    def result_file(self, job_id: str, filename: str) -> Path:
        directory = self._completed_directory(job_id)
        if not RESULT_FILENAME.fullmatch(filename):
            raise FileNotFoundError(filename)
        if filename == "status.json":
            # An API-generated download snapshot, never used as job state.
            # Unique temporary names allow concurrent downloads without locks.
            path = directory / filename
            temporary = directory / f".status-{uuid4().hex}.tmp"
            try:
                temporary.write_bytes(self._status_bytes(job_id))
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            return path
        if filename == "request.json":
            path = directory / filename
        else:
            _, payload = self._manifest(job_id)
            index = int(Path(filename).stem)
            if index >= len(payload["urls"]):
                raise FileNotFoundError(filename)
            partition = worker_directory(self.result_dir, job_id, payload["crawler_ids"][index])
            if partition.is_symlink():
                raise FileNotFoundError(filename)
            path = partition / filename
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(filename)
        return path

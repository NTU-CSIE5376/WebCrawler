"""File-backed jobs shared by the existing crawler workers."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


DEFAULT_TTL_DAYS = 14
MAX_TTL_DAYS = 60
SCHEDULING_POLICIES = ("balanced", "manual_first", "normal_first")


def validate_scheduling_policy(value):
    if value not in SCHEDULING_POLICIES:
        raise ValueError("manual_bulk_crawl.scheduling_policy must be balanced, manual_first or normal_first")
    return value


class JobUnavailable(Exception):
    """The job was deleted or expired while a worker held it."""


def validate_ttl_days(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_TTL_DAYS:
        raise ValueError('"ttl_days" must be an integer between 1 and 60')
    return value


def expiration(payload, *, fallback=None):
    if payload.get("expires_at"):
        return datetime.fromisoformat(payload["expires_at"])
    created = payload.get("created_at")
    if created:
        return datetime.fromisoformat(created) + timedelta(days=DEFAULT_TTL_DAYS)
    return fallback + timedelta(days=DEFAULT_TTL_DAYS) if fallback else None


def is_expired(payload, *, now=None):
    deadline = expiration(payload)
    return deadline is not None and deadline <= (now or datetime.now(timezone.utc))


def worker_directory(result_dir, job_id, crawler_id):
    return Path(result_dir) / job_id / f"crawler_{crawler_id:02d}"


def prepare_job(path, payload):
    """API-only preparation. The manifest rename makes all partitions visible."""
    root = Path(payload["result_dir"]) / path.stem
    root.mkdir(parents=True)
    (root / "request.json").write_text(json.dumps(payload), encoding="utf-8")
    for crawler_id in set(payload["crawler_ids"]):
        worker_directory(payload["result_dir"], path.stem, crawler_id).mkdir()
        queue = path.parent / f"crawler_{crawler_id:02d}"
        queue.mkdir(exist_ok=True)
        (queue / path.name).write_text(json.dumps({"job_id": path.stem}), encoding="utf-8")


def write_file(path: Path, content: bytes) -> None:
    with path.open("wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def write_json(path, value):
    temporary = path.with_suffix(".json.tmp")
    write_file(temporary, json.dumps(value, ensure_ascii=False).encode("utf-8"))
    os.replace(temporary, path)


def job_status(job_id, payload, directory):
    workers = sorted(set(payload["crawler_ids"]))
    progress = []
    for worker in workers:
        try:
            progress.append(json.loads((directory / f"crawler_{worker:02d}" / "progress.json").read_text()))
        except FileNotFoundError:
            pass
    completed = (len(progress) == len(workers)
                 and all(item.get("completed_at") for item in progress))
    started = [item["started_at"] for item in progress]
    return {
        "job_id": job_id, "status": "completed" if completed else ("running" if progress else "pending"),
        "total_urls": len(payload["urls"]),
        **{key: sum(item[key] for item in progress) for key in
           ("completed_urls", "succeeded_urls", "failed_urls", "html_files")},
        "created_at": payload.get("created_at"),
        "ttl_days": payload.get("ttl_days", DEFAULT_TTL_DAYS),
        "expires_at": expiration(payload).isoformat() if expiration(payload) else None,
        "started_at": min(started) if started else None,
        "completed_at": max(item["completed_at"] for item in progress) if completed else None,
        "result_dir": str(directory) if completed else None,
    }


class ManualBulkCrawlBatch:
    """Single-consumer partition: only this crawler writes its output/progress.

    The API creates directories before publishing the immutable manifest.
    Crawlers never create directories, move job roots, or delete shared state.
    Run exactly one live crawler process per crawler ID, as for normal IPC.
    """
    def __init__(self, path, payload, crawler_id):
        self.closed = False
        self.path = path
        self.payload = payload
        self.urls = payload["urls"]
        self.manifest = path.parent.parent / path.name
        self.directory = worker_directory(payload["result_dir"], path.stem, crawler_id)
        self.progress_path = self.directory / "progress.json"
        self.indices = [i for i, worker in enumerate(payload["crawler_ids"]) if worker == crawler_id]
        self.remaining = set()
        self._check_available()
        self.progress = {
            "started_at": datetime.now(timezone.utc).isoformat(), "completed_at": None,
            "completed_urls": 0, "succeeded_urls": 0, "failed_urls": 0, "html_files": 0,
        }
        try:
            previous = json.loads(self.progress_path.read_text())
            self.progress["started_at"] = previous["started_at"]
            self.progress["completed_at"] = previous.get("completed_at")
        except FileNotFoundError:
            pass
        for index in self.indices:
            try:
                item = json.loads((self.directory / f"{index:05d}.json").read_text())
            except FileNotFoundError:
                self.remaining.add(index)
            else:
                self._count(item)
        self._write_progress()

    def _check_available(self):
        if self.closed or not self.manifest.exists() or is_expired(self.payload) or not self.directory.is_dir():
            raise JobUnavailable(self.path.stem)

    def _count(self, item):
        self.progress["completed_urls"] += 1
        self.progress["failed_urls" if item["error"] else "succeeded_urls"] += 1
        self.progress["html_files"] += int(item["html_file"] is not None)

    def _write_progress(self):
        self._check_available()
        if not self.remaining:
            self.progress["completed_at"] = self.progress["completed_at"] or datetime.now(timezone.utc).isoformat()
        write_json(self.progress_path, self.progress)
        if not self.remaining:
            self.path.unlink(missing_ok=True)

    def save(self, index, *, final_url, status_code, body=None, error=None):
        self._check_available()
        if index not in self.remaining:
            raise ValueError("URL already completed or outside this worker's part")
        filename = f"{index:05d}.html" if body is not None else None
        try:
            if filename:
                write_file(self.directory / filename, body)
            else:
                # A previous attempt may have died after saving HTML but before
                # committing metadata. Do not expose it for a failed retry.
                (self.directory / f"{index:05d}.html").unlink(missing_ok=True)
            item = {
                "url": self.urls[index], "final_url": final_url, "status_code": status_code,
                "error": error, "html_file": filename, "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            write_json(self.directory / f"{index:05d}.json", item)
            self.remaining.remove(index)
            self._count(item)
            self._write_progress()
        except FileNotFoundError:
            # Delete/GC retires the root. Never mkdir to recover a late write.
            raise JobUnavailable(self.path.stem) from None

    def close(self):
        self.closed = True


def next_batch(pending_dir: Path, crawler_id: int = 0, *,
               exclude_job_ids: set[str] | None = None) -> ManualBulkCrawlBatch | None:
    """Select this crawler's oldest partition; its sole consumer retains it until done."""
    return next(iter_batches(pending_dir, crawler_id, exclude_job_ids=exclude_job_ids), None)


def iter_batches(pending_dir: Path, crawler_id: int = 0, *,
                 exclude_job_ids: set[str] | None = None):
    """Scan once and yield this crawler's unfinished partitions in submission order."""
    queued = []
    for path in (pending_dir / f"crawler_{crawler_id:02d}").glob("*.json"):
        if exclude_job_ids and path.stem in exclude_job_ids:
            continue
        try:
            manifest = pending_dir / path.name
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            created_at = payload.get("created_at")
            submitted_at = (datetime.fromisoformat(created_at).timestamp()
                            if created_at else manifest.stat().st_mtime)
        except FileNotFoundError:
            continue
        queued.append((submitted_at, path, payload))
    for _, path, payload in sorted(queued, key=lambda item: (item[0], item[1])):
        validate_scheduling_policy(payload.get("scheduling_policy", "balanced"))
        if (payload.get("version") != 1 or not isinstance(payload.get("urls"), list)
                or len(payload.get("crawler_ids", [])) != len(payload["urls"])
                or not isinstance(payload.get("result_dir"), str)
                or not Path(payload["result_dir"]).is_absolute()):
            raise ValueError("Invalid crawl request batch")
        if is_expired(payload):
            continue
        if crawler_id not in payload["crawler_ids"]:
            continue
        try:
            batch = ManualBulkCrawlBatch(path, payload, crawler_id)
        except (FileNotFoundError, JobUnavailable):
            continue
        if batch.remaining:
            yield batch
        else:
            batch.close()

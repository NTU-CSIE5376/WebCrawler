"""File-backed jobs shared by the existing crawler workers."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from contextlib import contextmanager, suppress
import fcntl


DEFAULT_TTL_DAYS = 14
MAX_TTL_DAYS = 60


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


@contextmanager
def queue_lock(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    # Keep this inode permanently: unlinking lock files allows two callers
    # to lock different inodes and both believe they own the queue.
    descriptor = os.open(directory / ".lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        # Closing also releases the lock after worker death. Cleanup errors
        # must not turn an already accepted batch into a reported rejection.
        with suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        with suppress(OSError):
            os.close(descriptor)


@contextmanager
def batch_claim(batch: Path):
    """Try an exclusive claim; process death automatically releases ownership."""
    try:
        descriptor = os.open(batch, os.O_RDONLY)
    except FileNotFoundError:
        yield False
        return
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        # A previous owner may have removed the file after we opened it.
        try:
            current = batch.stat()
            opened = os.fstat(descriptor)
            owned = (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino)
        except FileNotFoundError:
            owned = False
        yield owned
    finally:
        with suppress(OSError):
            os.close(descriptor)


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
            progress.append(json.loads((directory / f"worker_{worker:02d}.json").read_text()))
        except FileNotFoundError:
            pass
    started = [item["started_at"] for item in progress]
    return {
        "job_id": job_id, "status": "running" if progress else "pending",
        "total_urls": len(payload["urls"]),
        **{key: sum(item[key] for item in progress) for key in
           ("completed_urls", "succeeded_urls", "failed_urls", "html_files")},
        "created_at": payload.get("created_at"),
        "ttl_days": payload.get("ttl_days", DEFAULT_TTL_DAYS),
        "expires_at": expiration(payload).isoformat() if expiration(payload) else None,
        "started_at": min(started) if started else None,
        "completed_at": None, "result_dir": None,
    }


class ManualBulkCrawlBatch:
    """One worker's part of a job; distinct workers retain their domain slots.

    Worker claim files stay outside the result directory so publication cannot
    replace a lock inode. Per-URL records and progress are replaced atomically.
    """
    def __init__(self, path, payload, claim, crawler_id):
        self.path, self.claim = path, claim
        result_dir = Path(payload["result_dir"])
        self.target = result_dir / path.stem
        self.staging = result_dir / ".incomplete" / path.stem
        self.locks = path.parent / ".claims" / path.stem
        self.payload = payload
        self.urls = self.payload["urls"]
        self.indices = [i for i, worker in enumerate(self.payload["crawler_ids"]) if worker == crawler_id]
        self.progress_path = self.staging / f"worker_{crawler_id:02d}.json"
        self.remaining = set()
        if self.target.exists():
            return
        # Serialize setup with final publication; never recreate staging after
        # another worker has completed and published the whole job.
        with queue_lock(self.locks):
            if self.target.exists():
                return
            if not self.path.exists() or is_expired(self.payload):
                raise JobUnavailable(self.path.stem)
            self.staging.mkdir(parents=True, exist_ok=True)
            write_json(self.staging / "request.json", self.payload)
            self.progress = {
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_urls": 0, "succeeded_urls": 0, "failed_urls": 0, "html_files": 0,
            }
            try:
                previous = json.loads(self.progress_path.read_text())
                self.progress["started_at"] = previous["started_at"]
            except FileNotFoundError:
                pass
            # Resume completed URL records after process death, without crawling
            # them again. An HTML write without its metadata is safe to overwrite.
            for index in self.indices:
                try:
                    item = json.loads((self.staging / f"{index:05d}.json").read_text())
                except FileNotFoundError:
                    self.remaining.add(index)
                else:
                    self._count(item)
            write_json(self.progress_path, self.progress)
        self.finish_if_complete()

    def _count(self, item):
        self.progress["completed_urls"] += 1
        self.progress["failed_urls" if item["error"] else "succeeded_urls"] += 1
        self.progress["html_files"] += int(item["html_file"] is not None)

    def save(self, index, *, final_url, status_code, body=None, error=None):
        with queue_lock(self.locks):
            if not self.path.exists() or is_expired(self.payload):
                raise JobUnavailable(self.path.stem)
            self._save(index, final_url=final_url, status_code=status_code, body=body, error=error)

    def _save(self, index, *, final_url, status_code, body=None, error=None):
        if index not in self.remaining:
            raise ValueError("URL already completed or outside this worker's part")
        filename = f"{index:05d}.html" if body is not None else None
        if filename:
            write_file(self.staging / filename, body)
        item = {
            "url": self.urls[index], "final_url": final_url, "status_code": status_code,
            "error": error, "html_file": filename, "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json(self.staging / f"{index:05d}.json", item)
        self.remaining.remove(index)
        self._count(item)
        write_json(self.progress_path, self.progress)
        self._finish_if_complete()

    def finish_if_complete(self):
        with queue_lock(self.locks):
            self._finish_if_complete()

    def _finish_if_complete(self):
        if self.remaining:
            return
        if self.target.exists():
            self.path.unlink(missing_ok=True)
            return
        if not self.path.exists() or is_expired(self.payload):
            raise JobUnavailable(self.path.stem)
        status = job_status(self.path.stem, self.payload, self.staging)
        if status["completed_urls"] != len(self.urls):
            return
        status.update(status="completed", completed_at=datetime.now(timezone.utc).isoformat(),
                      result_dir=str(self.target))
        write_json(self.staging / "status.json", status)
        os.replace(self.staging, self.target)
        self.path.unlink(missing_ok=True)

    def close(self):
        self.claim.__exit__(None, None, None)


def claim_next(pending_dir: Path, crawler_id: int = 0, *,
               exclude_job_ids: set[str] | None = None) -> ManualBulkCrawlBatch | None:
    """Claim the oldest eligible submission for this worker."""
    queued = []
    for path in pending_dir.glob("*.json"):
        if exclude_job_ids and path.stem in exclude_job_ids:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            created_at = payload.get("created_at")
            submitted_at = (datetime.fromisoformat(created_at).timestamp()
                            if created_at else path.stat().st_mtime)
        except FileNotFoundError:
            continue
        queued.append((submitted_at, path, payload))
    # UUID filenames are unrelated to submission order. Break timestamp ties
    # by filename so all workers see the same order; legacy jobs use mtime.
    for _, path, payload in sorted(queued, key=lambda item: (item[0], item[1])):
        if (payload.get("version") != 1 or not isinstance(payload.get("urls"), list)
                or len(payload.get("crawler_ids", [])) != len(payload["urls"])
                or not isinstance(payload.get("result_dir"), str)
                or not Path(payload["result_dir"]).is_absolute()):
            raise ValueError("Invalid crawl request batch")
        if is_expired(payload):
            continue
        if (Path(payload["result_dir"]) / path.stem).exists():
            path.unlink(missing_ok=True)
            continue
        if crawler_id not in payload["crawler_ids"]:
            continue
        directory = pending_dir / ".claims" / path.stem
        directory.mkdir(parents=True, exist_ok=True)
        lock = directory / f"worker_{crawler_id:02d}.lock"
        lock.touch(exist_ok=True)
        claim = batch_claim(lock)
        if not claim.__enter__():
            claim.__exit__(None, None, None)
            continue
        try:
            try:
                batch = ManualBulkCrawlBatch(path, payload, claim, crawler_id)
            except (FileNotFoundError, JobUnavailable):
                claim.__exit__(None, None, None)
                continue
        except Exception:
            claim.__exit__(None, None, None)
            raise
        if batch.remaining:
            return batch
        batch.close()
    return None

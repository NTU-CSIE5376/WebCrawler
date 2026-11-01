"""File-backed crawl requests: publish URLs to crawler inboxes, serve segments."""
from __future__ import annotations

import logging
import os
import shutil
from collections import defaultdict
from datetime import datetime, timedelta
from uuid import uuid4

import psycopg2
from libs.ipc.crawl_requests import (
    CRAWLER_DIR,
    GB,
    REQUEST_ID,
    SEGMENT_NAME,
    CrawlRequestPaths,
    atomic_write_json,
    batch_key,
    is_expired,
    parse_segment_name,
    read_json,
    utc_now,
)
from libs.urls import MAX_URL_LEN, validate_url
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class RequestNotFound(Exception):
    pass


class RequestClosed(Exception):
    """The request is sealed, cancelled or expired and takes no more URLs."""


class Unavailable(Exception):
    """Admission refused for now; the client retries after Retry-After."""


def _number(config, name, *, minimum, kind=int, maximum=None):
    value = config.get(name)
    if (isinstance(value, bool) or not isinstance(value, (int, float) if kind is float else int)
            or value < minimum or (maximum is not None and value > maximum)):
        raise ValueError(f"crawl_requests.{name} must be a number from {minimum}"
                         + (f" to {maximum}" if maximum is not None else ""))
    return value


class CrawlRequestService:
    def __init__(self, config: dict, *, route_urls=None, disk_usage=None):
        if not isinstance(config.get("root"), str) or not os.path.isabs(config["root"]):
            raise ValueError("crawl_requests.root must be an absolute path")
        self.paths = CrawlRequestPaths(config["root"])
        self.max_urls = _number(config, "max_urls", minimum=1)
        self.max_url_length = _number(config, "max_url_length", minimum=1, maximum=MAX_URL_LEN)
        self.max_result_quota_gb = _number(config, "max_result_quota_gb", minimum=0, kind=float)
        self.max_expires_days = _number(config, "max_expires_days", minimum=1)
        self.default_expires_days = _number(config, "default_expires_days", minimum=1,
                                            maximum=self.max_expires_days)
        self.max_inbox_files = _number(config, "max_inbox_files", minimum=1)
        self.min_free_gb = _number(config, "min_free_gb", minimum=0, kind=float)
        self.max_tenant_queued_urls = _number(config, "max_tenant_queued_urls", minimum=1)
        self.retry_after_s = _number(config, "retry_after_s", minimum=1)
        self.gc_grace_days = _number(config, "gc_grace_days", minimum=0)
        self.long_poll_max_s = _number(config, "long_poll_max_s", minimum=0, kind=float)
        self.long_poll_interval_s = _number(config, "long_poll_interval_s", minimum=0.01, kind=float)
        self.top_hosts = _number(config, "top_hosts", minimum=1)
        self.route_urls = route_urls
        self.disk_usage = disk_usage or shutil.disk_usage

    # Request lifecycle

    def create(self, tenant: str, name: str, result_quota_gb=None, expires_days=None) -> dict:
        quota = min(result_quota_gb or self.max_result_quota_gb, self.max_result_quota_gb)
        days = min(expires_days or self.default_expires_days, self.max_expires_days)
        created = utc_now()
        request_id = uuid4().hex
        meta = {
            "id": request_id, "tenant": tenant, "name": name, "created_at": created.isoformat(),
            "expires_at": (created + timedelta(days=days)).isoformat(),
            "result_quota_bytes": int(quota * GB),
        }
        atomic_write_json(self.paths.meta(request_id), meta)
        return {"request_id": request_id, "name": name, "result_quota_gb": quota,
                "expires_at": meta["expires_at"]}

    def _meta(self, tenant: str, request_id: str) -> dict:
        if not REQUEST_ID.fullmatch(request_id):
            raise RequestNotFound(request_id)
        try:
            meta = read_json(self.paths.meta(request_id))
        except FileNotFoundError:
            raise RequestNotFound(request_id) from None
        if meta.get("tenant") != tenant:
            raise RequestNotFound(request_id)
        return meta

    def seal(self, tenant: str, request_id: str) -> None:
        self._meta(tenant, request_id)
        atomic_write_json(self.paths.sealed(request_id), {"at": utc_now().isoformat()})

    def cancel(self, tenant: str, request_id: str) -> None:
        self._meta(tenant, request_id)
        if not self.paths.cancelled(request_id).exists():
            atomic_write_json(self.paths.cancelled(request_id), {"at": utc_now().isoformat()})

    # Submission

    def submit(self, tenant: str, request_id: str, batch_id: str, items: list[dict]) -> dict:
        meta = self._meta(tenant, request_id)
        key = batch_key(batch_id)
        record_path = self.paths.batch(request_id, key)
        try:
            return read_json(record_path)["response"]
        except FileNotFoundError:
            pass
        if (self.paths.sealed(request_id).exists() or self.paths.cancelled(request_id).exists()
                or is_expired(meta)):
            raise RequestClosed(request_id)

        accepted, skipped, seen = [], [], set()
        for index, item in enumerate(items):
            if item["id"] in seen:
                skipped.append({"index": index, "id": item["id"], "reason": "duplicate"})
                continue
            seen.add(item["id"])
            try:
                url = validate_url(item["url"], max_length=self.max_url_length)
            except ValueError:
                skipped.append({"index": index, "id": item["id"], "reason": "invalid"})
                continue
            accepted.append({"id": item["id"], "url": url, "priority": item["priority"]})

        by_crawler = defaultdict(list)
        if accepted:
            self._admit(tenant)
            try:
                crawler_ids = self.route_urls([item["url"] for item in accepted])
            except (SQLAlchemyError, psycopg2.Error, OSError) as exc:
                logger.warning("crawl_requests.routing_failed", exc_info=exc)
                raise Unavailable("routing") from exc
            for item, crawler_id in zip(accepted, crawler_ids):
                by_crawler[crawler_id].append(item)
            for crawler_id in by_crawler:
                if self._inbox_backlog(crawler_id) > self.max_inbox_files:
                    raise Unavailable("inbox")
            # A retried POST rewrites the same inbox files, then the batch
            # record; the crawler ignores ids it already holds.
            for crawler_id, chunk in by_crawler.items():
                atomic_write_json(self.paths.inbox_file(crawler_id, request_id, key),
                                  {"request_id": request_id, "items": chunk})
        response = {"accepted": len(accepted), "skipped": skipped}
        atomic_write_json(record_path, {
            "response": response,
            "crawlers": {f"{crawler_id:02d}": len(chunk) for crawler_id, chunk in by_crawler.items()},
        })
        return response

    def _inbox_backlog(self, crawler_id: int) -> int:
        try:
            with os.scandir(self.paths.inbox(crawler_id)) as entries:
                return sum(entry.name.endswith(".json") and not entry.name.startswith(".")
                           for entry in entries)
        except FileNotFoundError:
            return 0

    def _admit(self, tenant: str) -> None:
        if self.disk_usage(self.paths.root).free < self.min_free_gb * GB:
            raise Unavailable("disk")
        if self.tenant_queued(tenant) > self.max_tenant_queued_urls:
            raise Unavailable("tenant_queued")

    def tenant_queued(self, tenant: str) -> int:
        """Crawler snapshots plus inbox files the crawlers have not ingested yet."""
        request_ids = {meta["id"] for meta in self._metas() if meta.get("tenant") == tenant}
        total = 0
        for snapshot in self._snapshots():
            for request_id, entry in snapshot.get("requests", {}).items():
                if request_id in request_ids:
                    total += entry.get("queued", 0) + entry.get("leased", 0)
        inbox = self.paths.root / "inbox"
        for directory in inbox.glob("crawler_*"):
            for path in directory.glob("*.json"):
                request_id, _, key = path.stem.partition("-")
                if request_id not in request_ids:
                    continue
                try:
                    total += read_json(self.paths.batch(request_id, key))["crawlers"].get(directory.name[-2:], 0)
                except FileNotFoundError:
                    pass  # Its batch record is not written yet, or the POST failed.
        return total

    # Status and results

    def _metas(self) -> list[dict]:
        metas = []
        for path in (self.paths.root / "requests").glob("*.json"):
            if REQUEST_ID.fullmatch(path.stem):
                try:
                    metas.append(read_json(path))
                except FileNotFoundError:
                    continue
        return metas

    def _snapshots(self) -> list[dict]:
        snapshots = []
        for path in sorted(self.paths.status().glob("crawler_*.json")):
            try:
                snapshots.append(read_json(path))
            except FileNotFoundError:
                continue
            except ValueError:
                logger.warning("crawl_requests.status_unreadable", extra={"path": str(path)})
        return snapshots

    def _status(self, meta: dict, snapshots: list[dict]) -> dict:
        request_id = meta["id"]
        queued = leased = 0
        hosts = defaultdict(lambda: {"remaining": 0, "done_1h": 0})
        for snapshot in snapshots:
            entry = snapshot.get("requests", {}).get(request_id)
            if not entry:
                continue
            queued += entry["queued"]
            leased += entry["leased"]
            for host in entry["top_hosts"]:
                hosts[host["host"]]["remaining"] += host["remaining"]
                hosts[host["host"]]["done_1h"] += host["done_1h"]
        segments = self.segments(request_id)
        top = sorted(hosts.items(), key=lambda item: (-item[1]["remaining"], item[0]))[:self.top_hosts]
        return {
            "request_id": request_id, "name": meta["name"],
            "created_at": meta["created_at"], "expires_at": meta["expires_at"],
            "sealed": self.paths.sealed(request_id).exists(),
            "cancelled": self.paths.cancelled(request_id).exists(),
            "queued": queued, "leased": leased,
            "unacked_segments": len(segments),
            "unacked_bytes": sum(segment["bytes"] for segment in segments),
            "top_hosts": [{"host": host, **counts} for host, counts in top],
            # The oldest snapshot bounds how stale the summed counts can be.
            "updated_at": min((snapshot["updated_at"] for snapshot in snapshots), default=None),
        }

    def status(self, tenant: str, request_id: str) -> dict:
        return self._status(self._meta(tenant, request_id), self._snapshots())

    def list(self, tenant: str) -> list[dict]:
        snapshots = self._snapshots()
        metas = sorted((meta for meta in self._metas() if meta.get("tenant") == tenant),
                       key=lambda meta: meta["created_at"])
        return [self._status(meta, snapshots) for meta in metas]

    def segments(self, request_id: str) -> list[dict]:
        """Closed, unacked segments of every crawler, oldest first."""
        found = []
        try:
            directories = [entry for entry in os.scandir(self.paths.segments(request_id))
                           if CRAWLER_DIR.fullmatch(entry.name) and entry.is_dir(follow_symlinks=False)]
        except FileNotFoundError:
            return []
        for directory in directories:
            try:
                with os.scandir(directory.path) as entries:
                    for entry in entries:
                        if SEGMENT_NAME.fullmatch(entry.name) and entry.is_file(follow_symlinks=False):
                            try:
                                stat = entry.stat(follow_symlinks=False)
                            except FileNotFoundError:
                                continue  # Acked concurrently.
                            found.append((stat.st_mtime, entry.name, stat.st_size))
            except FileNotFoundError:
                continue
        return [{"name": name, "bytes": size} for _, name, size in sorted(found)]

    def list_segments(self, tenant: str, request_id: str) -> list[dict]:
        self._meta(tenant, request_id)
        return self.segments(request_id)

    def segment_path(self, tenant: str, request_id: str, name: str):
        self._meta(tenant, request_id)
        parsed = parse_segment_name(name)
        if parsed is None:
            raise RequestNotFound(name)
        path = self.paths.segments(request_id, parsed[0]) / name
        if path.is_symlink() or not path.is_file():
            raise RequestNotFound(name)
        return path

    def ack(self, tenant: str, request_id: str, names: list[str]) -> int:
        self._meta(tenant, request_id)
        deleted = 0
        for name in set(names):
            parsed = parse_segment_name(name)
            if parsed is None:
                continue
            try:
                (self.paths.segments(request_id, parsed[0]) / name).unlink()
                deleted += 1
            except FileNotFoundError:
                pass
        return deleted

    # Garbage collection

    def collect_garbage(self, now: datetime | None = None) -> int:
        now = now or utc_now()
        snapshots = self._snapshots()
        removed = 0
        for meta in self._metas():
            request_id = meta["id"]
            try:
                expired = datetime.fromisoformat(meta["expires_at"]) + timedelta(days=self.gc_grace_days) <= now
                if expired or self._cancelled_and_drained(request_id, snapshots):
                    self._remove(request_id)
                    removed += 1
            except FileNotFoundError:
                continue
            except OSError:
                logger.exception("crawl_requests.gc_failed", extra={"request_id": request_id})
        self._remove_orphans()
        return removed

    def _cancelled_and_drained(self, request_id: str, snapshots: list[dict]) -> bool:
        try:
            cancelled_at = self.paths.cancelled(request_id).stat().st_mtime
        except FileNotFoundError:
            return False
        if any(path.is_file() for path in self.paths.segments(request_id).glob("crawler_*/*")):
            return False  # Unacked or still open (.part) segments.
        if any(self.paths.root.glob(f"inbox/crawler_*/{request_id}-*.json")):
            return False
        for snapshot in snapshots:
            # A snapshot from before the cancel cannot prove the crawler drained it.
            if datetime.fromisoformat(snapshot["updated_at"]).timestamp() <= cancelled_at:
                return False
            entry = snapshot.get("requests", {}).get(request_id)
            if entry and (entry["queued"] or entry["leased"]):
                return False
        return True

    def _remove(self, request_id: str) -> None:
        for path in self.paths.root.glob(f"inbox/crawler_*/{request_id}-*.json"):
            path.unlink(missing_ok=True)
        shutil.rmtree(self.paths.segments(request_id), ignore_errors=True)
        shutil.rmtree(self.paths.request_dir(request_id), ignore_errors=True)
        self.paths.sealed(request_id).unlink(missing_ok=True)
        self.paths.cancelled(request_id).unlink(missing_ok=True)
        # The meta goes last so an interrupted removal is retried.
        self.paths.meta(request_id).unlink(missing_ok=True)

    def _remove_orphans(self) -> None:
        for parent in (self.paths.root / "segments", self.paths.root / "requests"):
            for directory in parent.glob("*"):
                if (REQUEST_ID.fullmatch(directory.name) and directory.is_dir()
                        and not self.paths.meta(directory.name).exists()):
                    shutil.rmtree(directory, ignore_errors=True)


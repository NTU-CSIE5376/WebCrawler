"""Read-only bounded IPC metrics. HTTP requests serve a background-refreshed cache."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

PREFIX = "crawler_pipeline_"
MAX_JSON_BYTES = 65536


@dataclass(frozen=True)
class Worker:
    stage: str
    worker: int
    input_path: Path
    progress_path: Path
    interval_seconds: float


@dataclass(frozen=True)
class Golden:
    state_dir: Path
    deadline_grace_seconds: float
    stall_seconds: float


@dataclass(frozen=True)
class Config:
    ipc_root: Path
    workers: tuple[Worker, ...]
    refresh_seconds: float = 60
    max_scan_entries: int = 10000
    golden: Golden | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Config:
        root = Path(raw["ipc_root"])
        if not root.is_absolute() or ".." in root.parts:
            raise ValueError("ipc_root must be absolute without '..'")
        refresh = float(raw.get("refresh_seconds", 60))
        budget = raw.get("max_scan_entries", 10000)
        if not math.isfinite(refresh) or not 1 <= refresh <= 3600:
            raise ValueError("refresh_seconds must be between 1 and 3600")
        if (
            isinstance(budget, bool)
            or not isinstance(budget, int)
            or not 1 <= budget <= 1000000
        ):
            raise ValueError(
                "max_scan_entries must be an integer between 1 and 1000000"
            )
        workers: list[Worker] = []
        keys: set[tuple[str, int]] = set()
        for stage in raw["stages"]:
            name = stage["name"]
            if not isinstance(name, str) or not re.fullmatch(
                r"[a-z][a-z0-9_]{0,31}", name
            ):
                raise ValueError("stage name must be a short lowercase identifier")
            interval = float(stage["interval_minutes"]) * 60
            if not math.isfinite(interval) or interval <= 0:
                raise ValueError("interval_minutes must be positive and finite")
            for worker in stage["workers"]:
                if (
                    isinstance(worker, bool)
                    or not isinstance(worker, int)
                    or not 0 <= worker <= 999
                ):
                    raise ValueError("worker must be an integer between 0 and 999")
                key = (name, worker)
                if key in keys:
                    raise ValueError("duplicate stage/worker")
                keys.add(key)
                paths = []
                for field in ("input_dir_template", "progress_template"):
                    relative = Path(stage[field].format(worker=worker))
                    if (
                        relative.is_absolute()
                        or ".." in relative.parts
                        or relative == Path(".")
                    ):
                        raise ValueError(f"{field} must stay relative to ipc_root")
                    paths.append(root / relative)
                workers.append(Worker(name, worker, *paths, interval))
        if not workers or len(workers) > 1024:
            raise ValueError("configure between 1 and 1024 stage/worker pairs")
        golden = None
        spec = raw.get("golden_inject", {})
        if not isinstance(spec, dict) or type(spec.get("enabled", False)) is not bool:
            raise ValueError("golden_inject must be an object with boolean enabled")
        if spec.get("enabled", False):
            relative = Path(spec.get("state_dir", "maintenance/golden_inject"))
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or relative == Path(".")
            ):
                raise ValueError("golden state_dir must stay relative to ipc_root")
            grace = float(spec.get("deadline_grace_seconds", 1800))
            stall = float(spec.get("stall_seconds", 900))
            if (
                not math.isfinite(grace)
                or grace < 0
                or not math.isfinite(stall)
                or stall <= 0
            ):
                raise ValueError(
                    "golden grace/stall must be finite nonnegative/positive seconds"
                )
            golden = Golden(root / relative, grace, stall)
        return cls(root, tuple(workers), refresh, budget, golden)


def _safe_path(path: Path, root: Path) -> None:
    path.relative_to(root)
    for component in (path, *path.parents):
        if component.is_symlink():
            raise ValueError("symlink in monitored path")
        if component == root:
            break


def _bucket(date: str, clock: str) -> datetime:
    if not re.fullmatch(r"[0-9]{8}", date) or not re.fullmatch(r"[0-9]{4}", clock):
        raise ValueError("invalid IPC bucket name")
    return datetime.strptime(date + clock, "%Y%m%d%H%M").replace(tzinfo=UTC)


def _read_json(path: Path, root: Path) -> dict:
    _safe_path(path, root)
    # A maintenance FIFO must neither block collection nor lose business data.
    # Check the opened descriptor, so replacing the final path after validation
    # cannot turn this bounded metadata read into a pipe or symlink read.
    descriptor = os.open(
        path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC
    )
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("metadata must be a regular file")
        data = stream.read(MAX_JSON_BYTES + 1)
    if len(data) > MAX_JSON_BYTES:
        raise ValueError("JSON exceeds small metadata limit")
    state = json.loads(data)
    if not isinstance(state, dict):
        raise TypeError("JSON must be an object")
    return state


def _read_progress(path: Path, root: Path) -> tuple[str, str] | None:
    state = _read_json(path, root)
    date, clock = state["date"], state["time"]
    if date is None and clock is None:
        return None
    _bucket(date, clock)
    return date, clock


def _timestamp(value: Any, now: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise TypeError("checkpoint timestamps must be epoch seconds")
    stamp = float(value)
    # A writer may commit during this refresh; allow one second of overlap.
    if not math.isfinite(stamp) or stamp < 0 or stamp > now + 1:
        raise ValueError("invalid or future checkpoint timestamp")
    return stamp


def _golden_metrics(spec: Golden, root: Path, now: float) -> dict[str, float]:
    state = _read_json(spec.state_dir / "checkpoint.json", root)
    if type(state.get("complete")) is not bool:
        raise ValueError("checkpoint complete must be boolean")
    started = _timestamp(state["started_at"], now)
    updated = _timestamp(state.get("updated_at", started), now)
    if updated < started:
        raise ValueError("checkpoint update predates start")
    complete = state["complete"]
    finished = _timestamp(state["completed_at"], now) if complete else 0
    if complete and finished < updated:
        raise ValueError("checkpoint completion predates last update")
    # Monday 09:00 Asia/Taipei is Monday 01:00 UTC (no DST).
    current = datetime.fromtimestamp(now, UTC)
    scheduled = current.replace(hour=1, minute=0, second=0, microsecond=0)
    scheduled -= timedelta(days=current.weekday())
    if scheduled.timestamp() > now:
        scheduled -= timedelta(days=7)
    overdue = now >= scheduled.timestamp() + spec.deadline_grace_seconds and (
        not complete or finished < scheduled.timestamp()
    )
    row = {
        "scan_success": 1.0,
        "schedule_overdue": float(overdue),
        "stalled": float(not complete and now - updated > spec.stall_seconds),
    }
    report = spec.state_dir / "latest-run.json"
    _safe_path(report, root)
    if report.exists():
        count = _read_json(report, root)["skipped_count"]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("skipped_count must be a nonnegative integer")
        row["skipped_count"] = float(count)
    return row


class ScanBudget:
    def __init__(self, limit: int):
        self.remaining = limit

    def entries(self, path: Path):
        with os.scandir(path) as entries:
            for entry in entries:
                self.remaining -= 1
                if self.remaining < 0:
                    raise ValueError("directory scan budget exhausted")
                if entry.is_symlink():
                    raise ValueError("symlink in IPC directory")
                yield entry


def _scan(
    worker: Worker, root: Path, seen: tuple[str, str] | None, now: float, limit: int
) -> tuple[int, float]:
    _safe_path(worker.input_path, root)
    budget = ScanBudget(limit)
    count, oldest = 0, None
    # Only date and time directories; never list/open bucket contents.
    for date_entry in budget.entries(worker.input_path):
        if not date_entry.is_dir(follow_symlinks=False):
            continue
        date = date_entry.name
        _bucket(date, "0000")
        if seen is not None and date < seen[0]:
            continue
        for clock_entry in budget.entries(Path(date_entry.path)):
            if not clock_entry.is_dir(follow_symlinks=False):
                continue
            clock = clock_entry.name
            start = _bucket(date, clock).timestamp()
            if seen is not None and (date, clock) <= seen:
                continue
            ready = start + 2 * worker.interval_seconds
            if ready > now:
                continue
            count += 1
            oldest = ready if oldest is None else min(oldest, ready)
    return count, 0.0 if oldest is None else max(0.0, now - oldest)


class Collector:
    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.Lock()
        self._snapshot: dict[tuple[str, int], dict[str, float]] = {}
        self._last_success: dict[tuple[str, int], float] = {}
        self._last_run = 0.0
        self._last_duration = 0.0
        self._refresh_success = False
        self._golden_snapshot: dict[str, float] = {}
        self._last_run_monotonic = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def refresh(self, now: float | None = None) -> None:
        started = time.monotonic()
        now = time.time() if now is None else now
        snapshot = {}
        for worker in self.config.workers:
            key = (worker.stage, worker.worker)
            row = {"scan_success": 0.0, "progress_present": 0.0}
            try:
                _safe_path(worker.progress_path, self.config.ipc_root)
                row["progress_present"] = float(worker.progress_path.is_file())
                seen = _read_progress(worker.progress_path, self.config.ipc_root)
                count, lag = _scan(
                    worker,
                    self.config.ipc_root,
                    seen,
                    now,
                    self.config.max_scan_entries,
                )
                row.update(
                    scan_success=1.0,
                    ready_buckets=float(count),
                    oldest_ready_lag_seconds=lag,
                )
                self._last_success[key] = now
            except (OSError, ValueError, TypeError, KeyError):
                # Non-atomic progress writes appear as unknown this refresh;
                # alert pending periods absorb these transient failures.
                pass
            row["last_success_timestamp_seconds"] = self._last_success.get(key, 0.0)
            snapshot[key] = row
        golden = {}
        if self.config.golden is not None:
            try:
                golden = _golden_metrics(self.config.golden, self.config.ipc_root, now)
            except (OSError, ValueError, TypeError, KeyError):
                golden = {"scan_success": 0.0}
        with self._lock:
            self._snapshot = snapshot
            self._last_run = now
            self._last_run_monotonic = time.monotonic()
            self._last_duration = self._last_run_monotonic - started
            self._golden_snapshot = golden
            self._refresh_success = bool(
                all(row["scan_success"] for row in snapshot.values())
                and golden.get("scan_success", 1)
            )

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh()
            self._stop.wait(self.config.refresh_seconds)

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="pipeline-collector", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def healthy(self) -> bool:
        with self._lock:
            return bool(
                self._last_run_monotonic
                and self._refresh_success
                and time.monotonic() - self._last_run_monotonic
                <= 3 * self.config.refresh_seconds
            )

    def render(self) -> str:
        with self._lock:
            snapshot = dict(self._snapshot)
            metrics = {
                "collector_last_run_timestamp_seconds": self._last_run,
                "collector_refresh_duration_seconds": self._last_duration,
                "collector_refresh_success": float(self._refresh_success),
            }
            golden = dict(self._golden_snapshot)
        rows = [
            f"# TYPE {PREFIX}{name} gauge\n{PREFIX}{name} {value}"
            for name, value in metrics.items()
        ]
        for worker in self.config.workers:
            key = (worker.stage, worker.worker)
            values = snapshot.get(
                key,
                {
                    "scan_success": 0.0,
                    "progress_present": 0.0,
                    "last_success_timestamp_seconds": 0.0,
                },
            )
            label = f'{{stage="{worker.stage}",worker="{worker.worker:02d}"}}'
            for name, value in values.items():
                rows.append(f"{PREFIX}{name}{label} {value}")
        if self.config.golden is not None:
            for name, value in (golden or {"scan_success": 0.0}).items():
                rows.append(f"crawler_golden_{name} {value}")
        return "\n".join(rows) + "\n"


def handler_for(collector: Collector):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                status, body = 200, collector.render().encode()
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            elif self.path == "/healthz":
                good = collector.healthy()
                status, body = (
                    (200, b"ok\n") if good else (503, b"collector unhealthy or stale\n")
                )
                content_type = "text/plain; charset=utf-8"
            else:
                status, body, content_type = 404, b"not found\n", "text/plain"
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9812)
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as stream:
        config = Config.from_dict(json.load(stream))
    collector = Collector(config)
    server = ThreadingHTTPServer((args.listen, args.port), handler_for(collector))
    collector.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        collector.close()


if __name__ == "__main__":
    main()

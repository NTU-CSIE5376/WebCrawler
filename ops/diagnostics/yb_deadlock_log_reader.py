"""Follow existing PostgreSQL error logs, emitting deadlock IDs only.

This helper neither changes database logging nor emits SQL/URL/key payloads.
stdin EOF or the time limit stops it, including when run through docker exec -i.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import stat
import sys
import threading
import time

HEADER = re.compile(r"^(\d{4}-\d{2}-\d{2}[^\[]*)\[(\d+)\]\s*(ERROR|DETAIL|STATEMENT|LOG|WARNING):\s*(.*)$")
UUID = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
MAX_LINE = 8192


class Filter:
    def __init__(self):
        self.pending: dict[str, float] = {}

    def line(self, raw: bytes, now: float) -> str | None:
        # Only emit reconstructed fields, never an original log message.
        match = HEADER.match(raw[:MAX_LINE].decode("utf-8", errors="replace"))
        if not match:
            return None
        stamp, pid, level, message = match.groups()
        if level == "ERROR":
            self.pending.pop(pid, None)
            if "deadlock detected" not in message.lower():
                return None
            self.pending[pid] = now
        elif level != "DETAIL" or now - self.pending.get(pid, float("-inf")) > 10:
            return None
        ids = list(dict.fromkeys(UUID.findall(message)))[:4]
        suffix = " transactions=" + ",".join(ids) if ids else ""
        kind = "ERROR: deadlock detected" if level == "ERROR" else "DETAIL: deadlock transaction IDs"
        self.pending = {key: value for key, value in self.pending.items() if now - value <= 10}
        return f"{stamp}[{pid}] {kind}{suffix}"


def newest(directory: Path) -> Path | None:
    found = []
    for count, path in enumerate(directory.iterdir()):
        if count >= 4096:
            raise RuntimeError("Log-directory metadata budget exceeded")
        if not path.name.startswith("postgresql-") or not path.name.endswith(".log"):
            continue
        info = path.lstat()
        if stat.S_ISREG(info.st_mode):
            found.append((info.st_mtime_ns, path.name, path))
    return max(found)[2] if found else None


def follow(directory: Path, duration: float, stop: threading.Event) -> None:
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("Expected a regular PostgreSQL log directory")
    until = time.monotonic() + duration
    parser = Filter()
    stream = None
    current = None
    identity = None
    first = True
    buffer = bytearray()
    rescan_at = 0.0
    try:
        while time.monotonic() < until and not stop.is_set():
            now = time.monotonic()
            if now >= rescan_at:
                target = newest(directory)
                rescan_at = now + 2
                if target is not None:
                    info = target.lstat()
                    if current != target or identity != (info.st_dev, info.st_ino):
                        if stream:
                            stream.close()
                        fd = os.open(target, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
                        if not stat.S_ISREG(os.fstat(fd).st_mode):
                            os.close(fd)
                            raise ValueError("Expected a regular log file")
                        stream = os.fdopen(fd, "rb", buffering=0)
                        if first:
                            stream.seek(0, os.SEEK_END)
                        first = False
                        current, identity = target, (info.st_dev, info.st_ino)
                        buffer.clear()
                    elif stream and info.st_size < stream.tell():
                        stream.seek(0)
                        buffer.clear()
            data = stream.read(65536) if stream else b""
            if not data:
                stop.wait(.1)
                continue
            for part in data.splitlines(keepends=True):
                ends = part.endswith(b"\n")
                if len(buffer) < MAX_LINE:
                    buffer.extend(part[:MAX_LINE - len(buffer)])
                if ends:
                    result = parser.line(bytes(buffer), now)
                    if result:
                        print(result, flush=True)
                    buffer.clear()
    finally:
        if stream:
            stream.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--duration-seconds", type=float, default=21660)
    args = parser.parse_args(argv)
    if not 0 < args.duration_seconds <= 86400:
        parser.error("Duration must be between zero and one day")
    if newest(args.directory) is None:
        parser.error("No PostgreSQL logfile is available to follow")
    stop = threading.Event()

    def stdin_guard():
        try:
            while sys.stdin.buffer.read(1):
                pass
        finally:
            stop.set()

    threading.Thread(target=stdin_guard, daemon=True).start()
    follow(args.directory, args.duration_seconds, stop)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

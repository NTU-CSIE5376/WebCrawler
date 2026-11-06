from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def atomic_write_json(path: str, payload: Any) -> None:
    """
    Atomic write: write <name>.tmp then os.replace to final path.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    tmp = p.with_suffix(p.suffix + ".tmp")
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        #os.fsync(f.fileno())

    os.replace(tmp, p)

def append_jsonl(path: str, payload: Any) -> None:
    append_jsonl_many(path, [payload])

def append_jsonl_many(path: str, payloads: list[Any]) -> None:
    """Append multiple records with a single open/write/close instead of one
    syscall round-trip per record. No-op for an empty list (does not even
    create the file), matching append_jsonl's "write what you're given"
    behavior for a single record."""
    if not payloads:
        return

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    lines = b"".join(
        (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        for payload in payloads
    )

    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, lines)
    finally:
        os.close(fd)

def read_json(path: Path) -> Any:
    with open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))

def read_jsonl(path: Path) -> Any:
    out = []
    with open(path, "rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            out.append(json.loads(raw.decode("utf-8")))
    return out


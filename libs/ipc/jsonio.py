from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator


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
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    line = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.write(fd, line)
    os.close(fd)

def read_json(path: Path) -> Any:
    with open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))

def read_jsonl(path: Path) -> Iterator[Any]:
    """Yield records lazily, retaining only the current line/decoded record.

    Memory scales with the largest record, rather than the file size. Callers
    must also consume incrementally; collecting this iterator into a list
    still retains the entire file. Parse and I/O errors occur on iteration.
    """
    with open(path, "rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            yield json.loads(raw.decode("utf-8"))


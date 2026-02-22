from __future__ import annotations

from pathlib import Path


def count_ready_batches(queue_dir: str) -> int:
    """
    Count only final *.json files (ignore *.tmp, *.done).
    """
    p = Path(queue_dir)
    if not p.exists():
        return 0
    return sum(1 for _ in p.glob("*.json"))


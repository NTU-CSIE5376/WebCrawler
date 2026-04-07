from __future__ import annotations

import re
from pathlib import Path

_DOMAIN_FILE_RE = re.compile(r"^domain_(\d+)\.json$")


def count_ready_batches(queue_dir: str) -> int:
    """
    Count only final *.json files (ignore *.tmp, *.done).
    """
    p = Path(queue_dir)
    if not p.exists():
        return 0
    return sum(1 for _ in p.glob("*.json"))


def list_queued_domain_ids(queue_dir: str) -> set[int]:
    """
    Parse domain IDs from domain_{id}.json filenames in the queue directory.
    """
    p = Path(queue_dir)
    if not p.exists():
        return set()
    ids: set[int] = set()
    for f in p.iterdir():
        m = _DOMAIN_FILE_RE.match(f.name)
        if m:
            ids.add(int(m.group(1)))
    return ids


def count_domain_files(queue_dir: str) -> int:
    """
    Count domain_*.json files (ignore *.tmp).
    """
    p = Path(queue_dir)
    if not p.exists():
        return 0
    return sum(1 for f in p.iterdir() if _DOMAIN_FILE_RE.match(f.name))

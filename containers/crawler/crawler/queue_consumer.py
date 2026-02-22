from __future__ import annotations

from pathlib import Path
from typing import Optional

from libs.ipc.jsonio import read_json


class QueueConsumer:
    """
    Simple FIFO:
      - list *.json
      - sort by filename (unix_ts prefix)
      - read then DELETE immediately
    """

    def __init__(self, queue_dir: str):
        self.queue_dir = Path(queue_dir)

    def pop_batch(self) -> list[str]:
        if not self.queue_dir.exists():
            return []

        files = sorted(self.queue_dir.glob("*.json"))
        if not files:
            return []

        p = files[0]
        data = read_json(p)
        try:
            p.unlink()   # delete immediately
        except FileNotFoundError:
            return []

        urls = data.get("urls")
        if not isinstance(urls, list):
            return []
        return [str(u) for u in urls]


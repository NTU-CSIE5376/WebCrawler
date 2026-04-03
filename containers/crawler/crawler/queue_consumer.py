from __future__ import annotations

import re
from pathlib import Path

from libs.ipc.jsonio import read_json

_DOMAIN_FILE_RE = re.compile(r"^domain_(\d+)\.json$")


class QueueConsumer:
    """
    Per-domain queue consumer:
      - list domain_*.json files
      - read then DELETE immediately
      - return {domain_id: [urls]}
    """

    def __init__(self, queue_dir: str):
        self.queue_dir = Path(queue_dir)

    def pop_domain_batches(
        self,
        limit: int = 0,
        exclude_domain_ids: set[int] | None = None,
    ) -> dict[int, list[str]]:
        """
        Read up to `limit` domain files (0 = all available).
        Files whose domain_id is in `exclude_domain_ids` are skipped
        (left on disk for a future pop).
        Returns {domain_id: [url, ...]}.
        Each consumed file is deleted immediately after reading.
        """
        if not self.queue_dir.exists():
            return {}

        candidates = sorted(
            f for f in self.queue_dir.iterdir()
            if _DOMAIN_FILE_RE.match(f.name)
        )
        if not candidates:
            return {}

        # Filter out domains that the caller already has in-flight.
        if exclude_domain_ids:
            candidates = [
                f for f in candidates
                if int(_DOMAIN_FILE_RE.match(f.name).group(1)) not in exclude_domain_ids
            ]

        if not candidates:
            return {}

        if limit > 0:
            candidates = candidates[:limit]

        result: dict[int, list[str]] = {}
        for p in candidates:
            m = _DOMAIN_FILE_RE.match(p.name)
            domain_id = int(m.group(1))

            try:
                data = read_json(p)
            except Exception:
                continue
            try:
                p.unlink()
            except FileNotFoundError:
                continue

            urls = data.get("urls")
            if not isinstance(urls, list) or not urls:
                continue

            result[domain_id] = [str(u) for u in urls]

        return result

    def pop_batch(self) -> list[str]:
        """
        Legacy compatibility: pop a single domain file and return flat URL list.
        """
        batches = self.pop_domain_batches(limit=1)
        if not batches:
            return []
        return next(iter(batches.values()))

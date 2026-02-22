from __future__ import annotations

from collections import deque
from typing import Iterable


def round_robin_mix(per_shard_urls: dict[int, list[str]]) -> list[str]:
    """
    Interleave URLs across shards to improve diversity in each batch.
    Deterministic order: shard_id ascending.
    """
    shard_ids = sorted([sid for sid, urls in per_shard_urls.items() if urls])
    deques = {sid: deque(per_shard_urls[sid]) for sid in shard_ids}
    mixed: list[str] = []

    while deques:
        empty: list[int] = []
        for sid in shard_ids:
            dq = deques.get(sid)
            if not dq:
                continue
            mixed.append(dq.popleft())
            if not dq:
                empty.append(sid)

        for sid in empty:
            deques.pop(sid, None)

        shard_ids = [sid for sid in shard_ids if sid in deques]

    return mixed


def chunk(urls: list[str], batch_size: int) -> list[list[str]]:
    return [urls[i : i + batch_size] for i in range(0, len(urls), batch_size) if urls[i : i + batch_size]]


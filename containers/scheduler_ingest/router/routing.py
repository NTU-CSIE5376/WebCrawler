from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict

@dataclass(frozen=True)
class ShardRouter:
    num_shards: int
    shards_per_ingestor: int
    domain_overrides: Dict[str, int]

    def domain_to_shard(self, domain: str) -> int:
        if domain in self.domain_overrides:
            return int(self.domain_overrides[domain])
        h = hashlib.md5((domain or "unknown").encode("utf-8")).hexdigest()
        return int(h, 16) % self.num_shards

    def shard_to_ingestor(self, shard_id: int) -> int:
        return shard_id // self.shards_per_ingestor


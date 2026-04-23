from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set

from libs.db.sharding.key import compute_shard, shard_key


@dataclass(frozen=True)
class ShardRouter:
    num_shards: int
    shards_per_ingestor: int
    domain_overrides: Dict[str, int]
    split_etld1: Set[str] = field(default_factory=set)

    def domain_key(self, name: str) -> str:
        return shard_key(name, self.split_etld1)

    def domain_to_shard(self, name: str) -> int:
        return compute_shard(
            name,
            num_shards=self.num_shards,
            overrides=self.domain_overrides,
            split_etld1=self.split_etld1,
        )

    def shard_to_ingestor(self, shard_id: int) -> int:
        return shard_id // self.shards_per_ingestor


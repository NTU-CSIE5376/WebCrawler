"""Single source of truth for `URL -> (domain_key, shard_id, ingestor_id)`
routing.

Both `containers/scheduler_ingest/router` and `containers/sitemap_patroller`
emit "new outlink candidate" records into `/data/ipc/crawl_result/ingestor_NN/`
for the ingestor to consume. They MUST route a given URL to the same shard
and the same ingestor directory, otherwise URLs land in the wrong shard's
table or never get consumed.

Keep all sharding decisions in one place so a future change (new override,
new split-subdomain semantics, different hash) can't drift between callers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set, Tuple
from urllib.parse import urlparse

from .key import compute_shard, shard_key


def host_of(url: str | None) -> str:
    """Lowercased hostname for a URL string. Empty input returns ''."""
    if not url:
        return ""
    return (urlparse(url).hostname or "").lower()


@dataclass(frozen=True)
class ShardRouter:
    """Stateless `host -> (domain_key, shard_id, ingestor_id)` resolver.

    `domain_overrides` and `split_subdomains` are typically loaded once at
    service startup via `libs.db.sharding.key.load_sharding_config`; the
    instance is then reused across the loop.
    """
    num_shards: int
    shards_per_ingestor: int
    domain_overrides: Dict[str, int]
    split_subdomains: Set[str] = field(default_factory=set)

    def domain_key(self, name: str) -> str:
        return shard_key(name, self.split_subdomains)

    def domain_to_shard(self, name: str) -> int:
        return compute_shard(
            name,
            num_shards=self.num_shards,
            overrides=self.domain_overrides,
            split_subdomains=self.split_subdomains,
        )

    def shard_to_ingestor(self, shard_id: int) -> int:
        return shard_id // self.shards_per_ingestor

    def route(self, url_or_host: str) -> Tuple[str, int, int]:
        """One-shot resolver. Accepts either a full URL or a bare hostname.
        Returns (domain_key, shard_id, ingestor_id)."""
        host = host_of(url_or_host) if "://" in url_or_host else (url_or_host or "").lower()
        shard_id = self.domain_to_shard(host)
        return (
            self.domain_key(host),
            shard_id,
            self.shard_to_ingestor(shard_id),
        )

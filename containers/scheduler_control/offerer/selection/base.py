from __future__ import annotations

from abc import ABC, abstractmethod
from typing import NamedTuple
import math


class CandidateDomain(NamedTuple):
    domain_id: int
    shard_id: int
    domain_score: float
    available_urls: int | None = None
    url_score: float | None = None

    @property
    def allocation_value(self) -> float:
        # URL-level manual priorities and refreshed model scores already share
        # the operational score scale. Domain score bootstraps unscored frontiers.
        if self.url_score is not None and math.isfinite(self.url_score):
            return max(0.0, self.url_score)
        return self.domain_score


class SelectionStrategy(ABC):
    """
    Strategy contract for per-domain URL selection:
      - pick up to `per_domain_cap` eligible URLs for up to `max_domains` distinct domains
      - skip domains in `exclude_domain_ids`
      - perform all necessary updates atomically (e.g. should_crawl=false, last_scheduled)
      - return {domain_id: [url, ...]}
    """

    @abstractmethod
    def select_by_domain(
        self,
        shard_id: int,
        exclude_domain_ids: set[int],
        per_domain_cap: int,
        max_domains: int,
    ) -> dict[int, list[str]]:
        raise NotImplementedError

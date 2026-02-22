from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol


class SelectionStrategy(ABC):
    """
    Strategy contract:
      - pick up to `limit` eligible URLs from shard table
      - perform all necessary updates atomically (e.g. should_crawl=false, last_scheduled)
      - return selected URLs
    """

    @abstractmethod
    def select_and_update(self, shard_id: int, limit: int) -> list[str]:
        raise NotImplementedError


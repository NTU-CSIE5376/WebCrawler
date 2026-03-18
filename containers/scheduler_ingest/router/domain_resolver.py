from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


class DomainResolver:
    """
    Router-side domain_state resolver:
      - INSERT missing domains (ON CONFLICT DO NOTHING)
      - SELECT domain_id, domain_score
    """
    def __init__(self, session: Session, cache: Optional[Dict[str, tuple[int, float]]] = None):
        self.session = session
        self.cache = cache if cache is not None else {}

    def ensure_and_get(self, domain: str, shard_id: int) -> Tuple[int, float]:
        cached = self.cache.get(domain)
        if cached is not None:
            return cached

        # 1) insert if missing
        self.session.execute(
            text("""
                INSERT INTO domain_state(domain, shard_id)
                VALUES (:domain, :shard_id)
                ON CONFLICT (domain) DO NOTHING
            """),
            {"domain": domain, "shard_id": shard_id},
        )

        # 2) select id + score
        row = self.session.execute(
            text("""
                SELECT domain_id, COALESCE(domain_score, 0.0) AS domain_score
                FROM domain_state
                WHERE domain = :domain
            """),
            {"domain": domain},
        ).first()

        if row is None:
            # Should not happen; fallback
            raise RuntimeError(f"domain_state insert/select failed for domain={domain}")

        result = int(row.domain_id), float(row.domain_score)
        self.cache[domain] = result
        return result


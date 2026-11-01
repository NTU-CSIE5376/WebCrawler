from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Tuple, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session


class DomainResolver:
    """Insert missing domain_state rows, return domain_id + score."""
    def __init__(self, session: Session, cache: Optional[Dict[str, tuple[int, float]]] = None):
        self.session = session
        self.cache = cache if cache is not None else {}

    def batch_ensure_and_get(
        self, domains: Mapping[str, int], *, batch_size: int = 1000,
    ) -> Dict[str, tuple[int, float]]:
        """Resolve domains in bounded batches within the caller's transaction.

        Existing domains need only a SELECT. Missing domains are inserted in
        domain order, then read in a separate statement so concurrent inserts
        are visible under PostgreSQL's default READ COMMITTED isolation.
        batch_size limits the number of domains per SQL batch and must be a
        positive integer. Shard transitions and scores remain owned by their
        existing writers.
        """
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        pending = sorted(domain for domain in domains if domain not in self.cache)
        lookup = text("""
            SELECT domain, domain_id, COALESCE(domain_score, 0.0) AS domain_score
            FROM domain_state WHERE domain IN :domains
        """).bindparams(bindparam("domains", expanding=True))
        # Bound SQL parameters and statement size even for large submissions.
        for offset in range(0, len(pending), batch_size):
            batch = pending[offset:offset + batch_size]
            resolved = {
                row.domain: (int(row.domain_id), float(row.domain_score))
                for row in self.session.execute(lookup, {"domains": batch})
            }
            missing = [domain for domain in batch if domain not in resolved]
            if missing:
                parameters = {}
                values = []
                for index, domain in enumerate(missing):
                    parameters[f"domain_{index}"] = domain
                    parameters[f"shard_{index}"] = domains[domain]
                    values.append(f"(:domain_{index}, :shard_{index})")
                self.session.execute(text(
                    "INSERT INTO domain_state (domain, shard_id) VALUES "
                    + ", ".join(values) + " ON CONFLICT (domain) DO NOTHING"
                ), parameters)
                resolved.update({
                    row.domain: (int(row.domain_id), float(row.domain_score))
                    for row in self.session.execute(lookup, {"domains": missing})
                })
            if any(domain not in resolved for domain in batch):
                raise RuntimeError("domain_state batch insert/select failed")
            self.cache.update(resolved)
        return {domain: self.cache[domain] for domain in domains}

    def ensure_and_get(self, domain: str, shard_id: int) -> Tuple[int, float]:
        cached = self.cache.get(domain)
        if cached is not None:
            return cached

        # 1) insert if missing; shard_id transitions are owned by
        # migrate_shard_split.py, so the hot path doesn't need to lock to re-check.
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


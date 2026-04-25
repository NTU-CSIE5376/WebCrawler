from __future__ import annotations

from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .base import SelectionStrategy


class ReadOnlyStrategy(SelectionStrategy):
    """Test-only strategy: pulls eligible URLs from the DB without touching it.

    Unlike ExampleStrategy this performs no UPDATE/INSERT, so running it against
    a live production database does not flip should_crawl, advance
    last_scheduled, or append to url_event_counter_*. The same rows may be
    re-selected on every scan, which is fine for generating log traffic in the
    Loki test stack.
    """

    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _table(self, shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def select_by_domain(
        self,
        shard_id: int,
        exclude_domain_ids: set[int],
        per_domain_cap: int,
        max_domains: int,
    ) -> dict[int, list[str]]:
        if max_domains <= 0 or per_domain_cap <= 0:
            return {}

        table = self._table(shard_id)

        exclude_clause = ""
        params: dict = {
            "max_domains": max_domains,
            "per_domain_cap": per_domain_cap,
        }
        if exclude_domain_ids:
            exclude_clause = "AND domain_id NOT IN :exclude"
            params["exclude"] = tuple(exclude_domain_ids)

        sql = text(f"""
        WITH eligible_domains AS (
            SELECT DISTINCT domain_id
            FROM {table}
            WHERE should_crawl = TRUE
              {exclude_clause}
            ORDER BY domain_id
            LIMIT :max_domains
        )
        SELECT u.url, u.domain_id
        FROM eligible_domains d,
        LATERAL (
            SELECT url, domain_id
            FROM {table}
            WHERE should_crawl = TRUE AND domain_id = d.domain_id
            ORDER BY url_score DESC NULLS LAST,
                     domain_score DESC NULLS LAST,
                     last_scheduled ASC NULLS FIRST,
                     first_seen ASC
            LIMIT :per_domain_cap
        ) u;
        """)

        with self.Session() as sess:
            rows = sess.execute(sql, params).fetchall()

        result: dict[int, list[str]] = defaultdict(list)
        for r in rows:
            result[r.domain_id].append(r.url)
        return dict(result)

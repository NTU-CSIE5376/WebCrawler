from __future__ import annotations

from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .base import CandidateDomain, SelectionStrategy


class GoldenDiscoveryRankerV1Strategy(SelectionStrategy):
    """Offerer strategy for Golden Discovery Ranker v1 production scheduling.

    The ranker writes operational priority into url_score.
    url_score_updated_at is used only to prefer rows already refreshed by the
    background scorer; there is no separate score-version or experiment
    metadata in the scheduling path.
    """

    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _table(self, shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def _event_table(self, shard_id: int) -> str:
        return f"url_event_counter_{shard_id:03d}"

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
        event_table = self._event_table(shard_id)

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
            SELECT
                domain_id,
                MAX(CASE WHEN url_score_updated_at IS NOT NULL THEN url_score END) AS best_golden_discovery_score,
                MAX(url_score) AS best_any_score,
                MAX(domain_score) AS best_domain_score,
                MIN(first_seen) AS oldest_first_seen
            FROM {table}
            WHERE should_crawl = TRUE
              {exclude_clause}
              AND NOT EXISTS (
                SELECT 1 FROM domain_state d
                WHERE d.domain_id = {table}.domain_id
                  AND d.crawl_paused_until > NOW()
              )
            GROUP BY domain_id
            ORDER BY
                best_golden_discovery_score DESC NULLS LAST,
                best_any_score DESC NULLS LAST,
                best_domain_score DESC NULLS LAST,
                oldest_first_seen ASC NULLS LAST,
                domain_id
            LIMIT :max_domains
        ),
        picked AS (
            SELECT u.url, u.domain_id
            FROM eligible_domains d,
            LATERAL (
                SELECT url, domain_id
                FROM {table}
                WHERE should_crawl = TRUE AND domain_id = d.domain_id
                ORDER BY
                    CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END,
                    url_score DESC NULLS LAST,
                    domain_score DESC NULLS LAST,
                    last_scheduled ASC NULLS FIRST,
                    first_seen ASC
                LIMIT :per_domain_cap
                FOR UPDATE SKIP LOCKED
            ) u
        ),
        updated AS (
            UPDATE {table} x
            SET
                should_crawl = FALSE,
                last_scheduled = CURRENT_TIMESTAMP,
                num_scheduled_90d = x.num_scheduled_90d + 1
            FROM picked
            WHERE x.url = picked.url
            RETURNING x.url, x.domain_id
        ),
        event_upsert AS (
            INSERT INTO {event_table} (url, event_date, num_scheduled, accounted)
            SELECT url, CURRENT_DATE, 1, TRUE
            FROM updated
            ON CONFLICT (url, event_date)
            DO UPDATE SET
                num_scheduled = {event_table}.num_scheduled + 1,
                accounted = TRUE
        )
        SELECT u.url, u.domain_id
        FROM updated u;
        """)

        with self.Session() as sess:
            rows = sess.execute(sql, params).fetchall()
            sess.commit()

        result: dict[int, list[str]] = defaultdict(list)
        for r in rows:
            result[r.domain_id].append(r.url)
        return dict(result)

    def peek_global_candidates(
        self,
        limit: int,
        exclude_domain_ids: set[int],
        shard_start: int,
        shard_end: int,
    ) -> list[CandidateDomain]:
        if limit <= 0 or shard_start > shard_end:
            return []

        # One query per shard instead of a single domain_state scan guarded by
        # an OR of per-shard EXISTS branches. On PostgreSQL the planner only
        # evaluates the OR branch whose shard_id matches a given row, but on
        # YSQL it does not short-circuit that way: it can fall back to a full
        # domain_state scan that probes all (shard_end - shard_start + 1)
        # EXISTS subqueries per row, which is catastrophically slow at
        # production scale (observed: single peek taking minutes instead of
        # sub-second). Splitting by shard lets each query hit
        # idx_domain_state_shard_score (shard_id, domain_score DESC, domain_id)
        # directly. Results are merged and re-sorted in Python so the output
        # is identical to the original single-query ordering (domain_score
        # DESC NULLS LAST, domain_id) and LIMIT.
        exclude_clause = ""
        base_params: dict = {"limit": limit}
        if exclude_domain_ids:
            exclude_clause = "AND d.domain_id <> ALL(:exclude_arr)"
            base_params["exclude_arr"] = list(exclude_domain_ids)

        rows: list = []
        with self.Session() as sess:
            for sid in range(shard_start, shard_end + 1):
                sql = text(f"""
                SELECT d.domain_id, d.shard_id, d.domain_score
                FROM domain_state d
                WHERE d.shard_id = :shard_id
                  AND (d.crawl_paused_until IS NULL OR d.crawl_paused_until <= NOW())
                  {exclude_clause}
                  AND EXISTS (
                    SELECT 1 FROM url_state_current_{sid:03d} u
                    WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE
                  )
                ORDER BY d.domain_score DESC NULLS LAST, d.domain_id
                LIMIT :limit
                """)
                params = dict(base_params)
                params["shard_id"] = sid
                rows.extend(sess.execute(sql, params).fetchall())

        def sort_key(row):
            score = row.domain_score
            has_score = score is not None
            return (0 if has_score else 1, -float(score) if has_score else 0.0, row.domain_id)

        rows.sort(key=sort_key)

        return [
            CandidateDomain(
                domain_id=r.domain_id,
                shard_id=r.shard_id,
                domain_score=float(r.domain_score) if r.domain_score is not None else 0.0,
            )
            for r in rows[:limit]
        ]

    def claim_domain_urls(
        self,
        shard_id: int,
        domain_id: int,
        per_domain_cap: int,
    ) -> list[str]:
        if per_domain_cap <= 0:
            return []

        table = self._table(shard_id)
        event_table = self._event_table(shard_id)

        sql = text(f"""
        WITH picked AS (
            SELECT url
            FROM {table}
            WHERE should_crawl = TRUE AND domain_id = :domain_id
            ORDER BY
                CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END,
                url_score DESC NULLS LAST,
                domain_score DESC NULLS LAST,
                last_scheduled ASC NULLS FIRST,
                first_seen ASC
            LIMIT :per_domain_cap
            FOR UPDATE SKIP LOCKED
        ),
        updated AS (
            UPDATE {table} x
            SET
                should_crawl = FALSE,
                last_scheduled = CURRENT_TIMESTAMP,
                num_scheduled_90d = x.num_scheduled_90d + 1
            FROM picked
            WHERE x.url = picked.url
            RETURNING x.url
        ),
        event_upsert AS (
            INSERT INTO {event_table} (url, event_date, num_scheduled, accounted)
            SELECT url, CURRENT_DATE, 1, TRUE
            FROM updated
            ON CONFLICT (url, event_date)
            DO UPDATE SET
                num_scheduled = {event_table}.num_scheduled + 1,
                accounted = TRUE
        )
        SELECT url FROM updated;
        """)

        params = {
            "domain_id": domain_id,
            "per_domain_cap": per_domain_cap,
        }

        with self.Session() as sess:
            rows = sess.execute(sql, params).fetchall()
            sess.commit()

        return [r.url for r in rows]

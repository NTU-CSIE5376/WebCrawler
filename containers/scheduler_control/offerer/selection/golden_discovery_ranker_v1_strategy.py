from __future__ import annotations

from collections import defaultdict
import logging
import time

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .base import CandidateDomain, SelectionStrategy

logger = logging.getLogger("offerer")


class GoldenDiscoveryRankerV1Strategy(SelectionStrategy):
    """Offerer strategy for Golden Discovery Ranker v1 production scheduling.

    The ranker writes operational priority into url_score.
    url_score_updated_at is used only to prefer rows already refreshed by the
    background scorer; there is no separate score-version or experiment
    metadata in the scheduling path.
    """

    def __init__(self, Session: sessionmaker, use_url_priority: bool = False):
        self.Session = Session
        self.use_url_priority = use_url_priority

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
                head_column = ", head.url_score" if self.use_url_priority else ""
                head_join = f"""
                CROSS JOIN LATERAL (
                    SELECT CASE WHEN u.url_score_updated_at IS NOT NULL THEN u.url_score END AS url_score
                    FROM url_state_current_{sid:03d} u
                    WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE
                    ORDER BY CASE WHEN u.url_score_updated_at IS NULL THEN 1 ELSE 0 END,
                             u.url_score DESC NULLS LAST, u.domain_score DESC NULLS LAST,
                             u.last_scheduled ASC NULLS FIRST, u.first_seen ASC
                    LIMIT 1
                ) head
                """ if self.use_url_priority else ""
                eligibility = "" if self.use_url_priority else f"""
                  AND EXISTS (
                    SELECT 1 FROM url_state_current_{sid:03d} u
                    WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE
                    OFFSET 0
                  )
                """
                sql = text(f"""
                SELECT d.domain_id, d.shard_id, d.domain_score {head_column}
                FROM domain_state d
                {head_join}
                WHERE d.shard_id = :shard_id
                  AND (d.crawl_paused_until IS NULL OR d.crawl_paused_until <= NOW())
                  {exclude_clause}
                  {eligibility}
                ORDER BY d.domain_score DESC NULLS LAST, d.domain_id
                LIMIT :limit
                """)
                params = dict(base_params)
                params["shard_id"] = sid
                # Keep EXISTS correlated. Without this optimization fence, YSQL
                # can aggregate the entire crawlable URL index (21M rows on
                # shard 146) before sorting domains and applying the LIMIT.
                # OFFSET 0 preserves results and allows ordered domain-index
                # traversal with a short-circuiting URL probe per domain.
                started = time.monotonic()
                shard_rows = sess.execute(sql, params).fetchall()
                rows.extend(shard_rows)
                logger.info("offer.peek_shard", extra={
                    "event": "offer.peek_shard", "shard_id": sid,
                    "elapsed_sec": round(time.monotonic() - started, 3),
                    "candidates_returned": len(shard_rows),
                })

        def sort_key(row):
            score = (CandidateDomain(row.domain_id, row.shard_id, float(row.domain_score or 0),
                                     url_score=row.url_score).allocation_value
                     if self.use_url_priority else row.domain_score)
            has_score = score is not None
            return (0 if has_score else 1, -float(score) if has_score else 0.0, row.domain_id)

        rows.sort(key=sort_key)

        return [
            CandidateDomain(
                domain_id=r.domain_id,
                shard_id=r.shard_id,
                domain_score=float(r.domain_score) if r.domain_score is not None else 0.0,
                url_score=r.url_score if self.use_url_priority else None,
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
              AND NOT EXISTS (
                  SELECT 1 FROM domain_state d
                  WHERE d.domain_id = :domain_id AND d.crawl_paused_until > NOW()
              )
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

    def demand_candidates(self, domain_limits: dict[int, int], shard_start: int, shard_end: int):
        """Measure only the eligible frontier usable in this bounded offer."""
        if not domain_limits:
            return []
        with self.Session() as sess:
            rows = sess.execute(text("""
                SELECT domain_id, shard_id, domain_score FROM domain_state
                WHERE domain_id = ANY(:domain_ids)
                  AND shard_id BETWEEN :start AND :end
                  AND (crawl_paused_until IS NULL OR crawl_paused_until <= NOW())
                ORDER BY domain_score DESC, domain_id
            """), {"domain_ids": sorted(domain_limits), "start": shard_start, "end": shard_end}).fetchall()
            inventory = {}
            by_shard = defaultdict(list)
            for row in rows:
                by_shard[row.shard_id].append(row.domain_id)
            for shard_id, ids in by_shard.items():
                matches = sess.execute(text(f"""
                    SELECT requested.domain_id, frontier.available_urls, frontier.url_score
                    FROM unnest(CAST(:domain_ids AS bigint[]), CAST(:url_caps AS integer[]))
                        AS requested(domain_id, url_cap)
                    CROSS JOIN LATERAL (
                        SELECT count(*) AS available_urls, max(usable.url_score) AS url_score
                        FROM (
                            SELECT CASE WHEN u.url_score_updated_at IS NOT NULL THEN u.url_score END AS url_score
                            FROM {self._table(shard_id)} u
                            WHERE u.domain_id = requested.domain_id AND u.should_crawl = TRUE
                            ORDER BY CASE WHEN u.url_score_updated_at IS NULL THEN 1 ELSE 0 END,
                                     u.url_score DESC NULLS LAST, u.domain_score DESC NULLS LAST,
                                     u.last_scheduled ASC NULLS FIRST, u.first_seen ASC
                            LIMIT requested.url_cap
                        ) usable
                    ) frontier
                    WHERE frontier.available_urls > 0
                """), {"domain_ids": ids, "url_caps": [domain_limits[key] for key in ids]}).fetchall()
                inventory.update((row.domain_id, row) for row in matches)
        return [CandidateDomain(r.domain_id, r.shard_id, float(r.domain_score),
                                int(inventory[r.domain_id].available_urls), inventory[r.domain_id].url_score)
                for r in rows if r.domain_id in inventory]

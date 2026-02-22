from __future__ import annotations

from typing import List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from .base import SelectionStrategy


class ExampleStrategy(SelectionStrategy):
    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _table(self, shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def _event_table(self, shard_id: int) -> str:
        return f"url_event_counter_{shard_id:03d}"

    def _select_and_update(
        self,
        sess,
        table: str,
        event_table: str,
        limit: int,
        order_by_sql: str,
    ) -> List[Tuple[str, int]]:
        if limit <= 0:
            return []

        sql = text(f"""
        WITH picked AS (
            SELECT url, domain_id
            FROM {table}
            WHERE should_crawl = TRUE
            ORDER BY {order_by_sql}
            LIMIT :limit
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

        rows = sess.execute(
            sql,
            {"limit": limit},
        ).fetchall()

        return [(r.url, r.domain_id) for r in rows]

    def select_and_update(self, shard_id: int, limit: int) -> List[Tuple[str, int]]:
        if limit <= 0:
            return []

        table = self._table(shard_id)
        event_table = self._event_table(shard_id)
        half = limit // 2
        rest = limit - half

        results: List[Tuple[str, int]] = []

        with self.Session() as sess:
            # Phase A: score-aware
            if half > 0:
                results += self._select_and_update(
                    sess,
                    table,
                    event_table,
                    half,
                    order_by_sql="""
                        url_score DESC NULLS LAST,
                        domain_score DESC NULLS LAST,
                        last_scheduled ASC NULLS FIRST,
                        first_seen ASC
                    """,
                )

            # Phase B: fairness / recency-based
            if rest > 0:
                results += self._select_and_update(
                    sess,
                    table,
                    event_table,
                    rest,
                    order_by_sql="""
                        last_scheduled ASC NULLS FIRST,
                        first_seen ASC
                    """,
                )

            sess.commit()

        return results



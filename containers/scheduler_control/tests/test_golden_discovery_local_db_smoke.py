from __future__ import annotations

import os
import time
import unittest

import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig,
    GoldenDiscoveryRankerService,
)
from scripts import migrate_add_url_score_updated_at as migration


def _sqlalchemy_url(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg2://" + dsn[len("postgresql://") :]
    return dsn


class _FakeScorer:
    def score_many(self, urls):
        self.urls = urls
        return [0.31 + i / 100 for i, _ in enumerate(urls)]


@unittest.skipUnless(
    os.environ.get("GOLDEN_DISCOVERY_LOCAL_DB_SMOKE_DSN"),
    "set GOLDEN_DISCOVERY_LOCAL_DB_SMOKE_DSN to run the PostgreSQL smoke test",
)
class GoldenDiscoveryLocalDBSmokeTest(unittest.TestCase):
    def test_migration_index_and_scorer_against_local_postgres(self):
        dsn = os.environ["GOLDEN_DISCOVERY_LOCAL_DB_SMOKE_DSN"]
        schema = f"golden_discovery_smoke_{os.getpid()}_{int(time.time())}"

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {schema}")
                cur.execute(f"SET search_path TO {schema}")
                cur.execute(
                    """
                    CREATE TABLE url_state_current_000 (
                        url TEXT PRIMARY KEY,
                        should_crawl BOOLEAN NOT NULL,
                        first_seen TIMESTAMPTZ,
                        url_score DOUBLE PRECISION,
                        domain_id BIGINT,
                        domain_score DOUBLE PRECISION,
                        last_scheduled TIMESTAMPTZ,
                        num_scheduled_90d INTEGER NOT NULL DEFAULT 0,
                        inlink_count_approx INTEGER NOT NULL DEFAULT 0,
                        inlink_count_external INTEGER NOT NULL DEFAULT 0,
                        anchor_text VARCHAR
                    )
                    """
                )
                cur.execute(
                    "CREATE TABLE url_state_history_000 "
                    "(LIKE url_state_current_000 INCLUDING DEFAULTS)"
                )

            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(migration.add_url_score_updated_at_sql("url_state_current_000"))
                cur.execute(migration.add_url_score_updated_at_sql("url_state_history_000"))
            conn.commit()

            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {schema}")
                cur.execute(migration.create_golden_discovery_unscored_index_sql(0))
                cur.execute(migration.create_golden_discovery_selection_index_sql(0))
                cur.execute(
                    """
                    INSERT INTO url_state_current_000
                        (url, should_crawl, first_seen, url_score, domain_id, domain_score)
                    VALUES
                        ('https://example.com/b', TRUE, '2026-01-02 00:00:00+00', 0.1, 1, 0.5),
                        ('https://example.com/a', TRUE, '2026-01-01 00:00:00+00', 0.2, 1, 0.5),
                        ('https://example.com/skip', FALSE, '2026-01-01 00:00:00+00', 0.9, 1, 0.5)
                    """
                )
                cur.execute(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = %s
                      AND indexname = %s
                    """,
                    (schema, migration.golden_discovery_unscored_index_name(0)),
                )
                indexdef = cur.fetchone()[0]
                self.assertIn("WHERE ((should_crawl = true)", indexdef)
                self.assertIn("(url_score_updated_at IS NULL)", indexdef)

                cur.execute(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = %s
                      AND indexname = %s
                    """,
                    (schema, migration.golden_discovery_selection_index_name(0)),
                )
                selection_indexdef = cur.fetchone()[0]
                self.assertIn("domain_id", selection_indexdef)
                self.assertIn("url_score DESC NULLS LAST", selection_indexdef)
                self.assertIn("WHERE (should_crawl = true)", selection_indexdef)

                cur.execute("SET enable_seqscan = off")
                cur.execute(
                    """
                    EXPLAIN
                    SELECT url
                    FROM url_state_current_000
                    WHERE should_crawl = TRUE
                      AND url_score_updated_at IS NULL
                    ORDER BY first_seen ASC NULLS LAST
                    FOR UPDATE SKIP LOCKED
                    LIMIT 2
                    """
                )
                plan = "\n".join(row[0] for row in cur.fetchall())
                self.assertIn(migration.golden_discovery_unscored_index_name(0), plan)

                cur.execute(
                    """
                    EXPLAIN
                    SELECT url, domain_id
                    FROM url_state_current_000
                    WHERE should_crawl = TRUE
                      AND domain_id = 1
                    ORDER BY
                        CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END,
                        url_score DESC NULLS LAST,
                        domain_score DESC NULLS LAST,
                        last_scheduled ASC NULLS FIRST,
                        first_seen ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 2
                    """
                )
                selection_plan = "\n".join(row[0] for row in cur.fetchall())
                self.assertIn(migration.golden_discovery_selection_index_name(0), selection_plan)

            engine = create_engine(
                _sqlalchemy_url(dsn),
                connect_args={"options": f"-csearch_path={schema}"},
            )
            try:
                fake_scorer = _FakeScorer()
                service = GoldenDiscoveryRankerService(
                    cfg=GoldenDiscoveryRankerConfig(
                        total_shards=1,
                        num_workers=1,
                        worker_id=0,
                        batch_size=2,
                        scan_interval_sec=60,
                        max_batches_per_shard=1,
                    ),
                    Session=sessionmaker(bind=engine),
                    scorer=fake_scorer,
                )
                self.assertEqual(service._score_batch(0), 2)
                self.assertEqual(
                    fake_scorer.urls,
                    ["https://example.com/a", "https://example.com/b"],
                )

                with engine.connect() as sa_conn:
                    rows = sa_conn.execute(
                        text(
                            """
                            SELECT url, url_score, url_score_updated_at IS NOT NULL AS scored
                            FROM url_state_current_000
                            ORDER BY url
                            """
                        )
                    ).fetchall()

                self.assertEqual(
                    [(row.url, round(row.url_score, 2), row.scored) for row in rows],
                    [
                        ("https://example.com/a", 0.31, True),
                        ("https://example.com/b", 0.32, True),
                        ("https://example.com/skip", 0.9, False),
                    ],
                )
            finally:
                engine.dispose()
        finally:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.close()


if __name__ == "__main__":
    unittest.main()

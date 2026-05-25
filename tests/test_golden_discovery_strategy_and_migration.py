from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from containers.scheduler_control.offerer.selection.golden_discovery_ranker_v1_strategy import (
    GoldenDiscoveryRankerV1Strategy,
)
from containers.scheduler_control.scorer import service as scorer_service
from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig,
    GoldenDiscoveryRankerService,
)
from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB
from scripts import migrate_add_url_score_updated_at as migration
from scripts import migrate_add_golden_rescore_index as rescore_migration


class GoldenDiscoveryMigrationSqlTest(unittest.TestCase):
    def test_unscored_index_is_partial_current_shard_index(self):
        sql = migration.create_golden_discovery_unscored_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v1_unscored",
            sql,
        )
        self.assertIn(
            "ON url_state_current_007 (first_seen ASC NULLS LAST)",
            sql,
        )
        self.assertIn(
            "WHERE should_crawl = TRUE AND url_score_updated_at IS NULL",
            sql,
        )
        self.assertNotIn("url_state_history", sql)

    def test_selection_index_is_partial_current_shard_index(self):
        sql = migration.create_golden_discovery_selection_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v1_selection",
            sql,
        )
        self.assertIn(
            "ON url_state_current_007 (domain_id, "
            "((CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END)), "
            "url_score DESC NULLS LAST, "
            "domain_score DESC NULLS LAST, "
            "last_scheduled ASC NULLS FIRST, "
            "first_seen ASC)",
            sql,
        )
        self.assertIn("WHERE should_crawl = TRUE", sql)
        self.assertNotIn("url_state_history", sql)

    def test_column_migration_targets_current_and_history_shards(self):
        tables = list(migration.iter_state_tables(num_shards=2))

        self.assertEqual(
            tables,
            [
                "url_state_current_000",
                "url_state_current_001",
                "url_state_history_000",
                "url_state_history_001",
            ],
        )
        self.assertEqual(
            migration.add_url_score_updated_at_sql("url_state_current_000"),
            "ALTER TABLE url_state_current_000 "
            "ADD COLUMN IF NOT EXISTS url_score_updated_at TIMESTAMPTZ",
        )

    def test_rescore_index_is_partial_composite_current_shard_index(self):
        sql = rescore_migration.create_golden_rescore_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v2_rescore",
            sql,
        )
        # Composite (domain_id, url_score_updated_at) with NULLS FIRST so the
        # planner can nested-loop over golden domains and walk each domain's
        # rows in the scorer's ORDER BY order without a full sort.
        self.assertIn(
            "ON url_state_current_007 (domain_id, url_score_updated_at ASC NULLS FIRST)",
            sql,
        )
        self.assertIn("WHERE should_crawl = TRUE", sql)
        self.assertNotIn("url_state_history", sql)


class _FakeResult:
    def __init__(self, rows=None):
        if rows is None:
            rows = [SimpleNamespace(domain_id=7, url="https://example.com/a")]
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeOffererSession:
    def __init__(self, rows=None):
        self.calls = []
        self.committed = False
        # If `rows` is a list-of-lists, return one batch per execute() call.
        # Otherwise the same rows are returned for every call.
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.calls.append((str(sql), params))
        if self._rows is None:
            return _FakeResult()
        if self._rows and isinstance(self._rows[0], list):
            idx = min(len(self.calls) - 1, len(self._rows) - 1)
            return _FakeResult(self._rows[idx])
        return _FakeResult(self._rows)

    def commit(self):
        self.committed = True


class _FakeOffererSessionFactory:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return self.session


class GoldenDiscoveryOffererStrategyTest(unittest.TestCase):
    def test_strategy_prefers_refreshed_ranker_scores_in_sql(self):
        session = _FakeOffererSession()
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        selected = strategy.select_by_domain(
            shard_id=3,
            exclude_domain_ids={11, 22},
            per_domain_cap=2,
            max_domains=5,
        )

        self.assertEqual(selected, {7: ["https://example.com/a"]})
        self.assertTrue(session.committed)

        sql, params = session.calls[0]
        self.assertIn("FROM url_state_current_003", sql)
        self.assertIn(
            "MAX(CASE WHEN url_score_updated_at IS NOT NULL THEN url_score END)",
            sql,
        )
        self.assertNotIn("WHEN MAX(CASE", sql)
        self.assertIn("NOT EXISTS", sql)
        self.assertIn("FROM domain_state d", sql)
        self.assertIn("d.crawl_paused_until > NOW()", sql)
        self.assertIn(
            "CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END",
            sql,
        )
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params["exclude"], (11, 22))
        self.assertEqual(params["per_domain_cap"], 2)
        self.assertEqual(params["max_domains"], 5)


class GoldenDiscoveryPeekGlobalCandidatesTest(unittest.TestCase):
    def test_peek_queries_domain_state_with_global_order(self):
        rows = [
            SimpleNamespace(domain_id=42, shard_id=3, domain_score=0.9),
            SimpleNamespace(domain_id=17, shard_id=11, domain_score=0.5),
        ]
        session = _FakeOffererSession(rows=rows)
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        candidates = strategy.peek_global_candidates(
            limit=10, exclude_domain_ids={99, 100},
            shard_start=0, shard_end=15,
        )

        self.assertEqual(
            [(c.domain_id, c.shard_id, c.domain_score) for c in candidates],
            [(42, 3, 0.9), (17, 11, 0.5)],
        )

        sql, params = session.calls[0]
        self.assertIn("FROM domain_state d", sql)
        self.assertIn(
            "d.shard_id BETWEEN :shard_start AND :shard_end", sql,
        )
        self.assertIn(
            "d.crawl_paused_until IS NULL OR d.crawl_paused_until <= NOW()", sql,
        )
        self.assertIn(
            "ORDER BY d.domain_score DESC NULLS LAST, d.domain_id", sql,
        )
        self.assertIn("LIMIT :limit", sql)
        self.assertNotIn("FOR UPDATE", sql)
        self.assertEqual(params["limit"], 10)
        self.assertEqual(params["shard_start"], 0)
        self.assertEqual(params["shard_end"], 15)
        self.assertEqual(sorted(params["exclude_arr"]), [99, 100])

    def test_peek_emits_per_shard_exists_subqueries(self):
        session = _FakeOffererSession(rows=[])
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        strategy.peek_global_candidates(
            limit=5, exclude_domain_ids=set(),
            shard_start=2, shard_end=4,
        )

        sql, _ = session.calls[0]
        # Each shard in the range has a guarded EXISTS subquery pointing at its
        # url_state_current_NNN partition. Only the matching shard's branch is
        # evaluated per domain_state row.
        self.assertIn(
            "(d.shard_id = 2 AND EXISTS (SELECT 1 FROM url_state_current_002 u "
            "WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE))",
            sql,
        )
        self.assertIn(
            "(d.shard_id = 3 AND EXISTS (SELECT 1 FROM url_state_current_003 u "
            "WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE))",
            sql,
        )
        self.assertIn(
            "(d.shard_id = 4 AND EXISTS (SELECT 1 FROM url_state_current_004 u "
            "WHERE u.domain_id = d.domain_id AND u.should_crawl = TRUE))",
            sql,
        )
        # Shards outside the range must not appear.
        self.assertNotIn("url_state_current_001", sql)
        self.assertNotIn("url_state_current_005", sql)

    def test_peek_returns_empty_when_limit_is_zero(self):
        session = _FakeOffererSession()
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        candidates = strategy.peek_global_candidates(
            limit=0, exclude_domain_ids=set(),
            shard_start=0, shard_end=15,
        )

        self.assertEqual(candidates, [])
        self.assertEqual(session.calls, [])

    def test_peek_returns_empty_when_shard_range_is_inverted(self):
        session = _FakeOffererSession()
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        candidates = strategy.peek_global_candidates(
            limit=5, exclude_domain_ids=set(),
            shard_start=10, shard_end=5,
        )

        self.assertEqual(candidates, [])
        self.assertEqual(session.calls, [])

    def test_peek_handles_empty_exclude_set(self):
        session = _FakeOffererSession(rows=[])
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        candidates = strategy.peek_global_candidates(
            limit=5, exclude_domain_ids=set(),
            shard_start=0, shard_end=15,
        )

        self.assertEqual(candidates, [])
        _, params = session.calls[0]
        self.assertEqual(params["exclude_arr"], [])


class GoldenDiscoveryClaimDomainUrlsTest(unittest.TestCase):
    def test_claim_scopes_to_one_domain_and_updates_state(self):
        rows = [
            SimpleNamespace(url="https://example.com/a"),
            SimpleNamespace(url="https://example.com/b"),
        ]
        session = _FakeOffererSession(rows=rows)
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        urls = strategy.claim_domain_urls(
            shard_id=3, domain_id=42, per_domain_cap=10
        )

        self.assertEqual(urls, ["https://example.com/a", "https://example.com/b"])
        self.assertTrue(session.committed)

        sql, params = session.calls[0]
        self.assertIn("FROM url_state_current_003", sql)
        self.assertIn("INSERT INTO url_event_counter_003", sql)
        self.assertIn("WHERE should_crawl = TRUE AND domain_id = :domain_id", sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("UPDATE url_state_current_003 x", sql)
        self.assertIn("should_crawl = FALSE", sql)
        self.assertIn("last_scheduled = CURRENT_TIMESTAMP", sql)
        self.assertNotIn("eligible_domains", sql)  # not the multi-domain CTE
        self.assertEqual(params["domain_id"], 42)
        self.assertEqual(params["per_domain_cap"], 10)

    def test_claim_returns_empty_for_non_positive_cap(self):
        session = _FakeOffererSession()
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        self.assertEqual(
            strategy.claim_domain_urls(shard_id=3, domain_id=42, per_domain_cap=0),
            [],
        )
        self.assertEqual(session.calls, [])


class _FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return [
            ("https://example.com/a",),
            ("https://example.com/b",),
        ]


class _FakeRawConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class _FakeSqlAlchemyConnection:
    def __init__(self, cursor):
        self.connection = _FakeRawConnection(cursor)


class _FakeScorerSession:
    def __init__(self, cursor):
        self._cursor = cursor

    def connection(self):
        return _FakeSqlAlchemyConnection(self._cursor)


class _FakeBeginContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeScorerSessionFactory:
    def __init__(self, cursor):
        self.session = _FakeScorerSession(cursor)

    def begin(self):
        return _FakeBeginContext(self.session)


class _FakeScorer:
    def score_many(self, urls):
        self.urls = urls
        return [0.25, 0.75]


class GoldenDiscoveryRankerServiceTest(unittest.TestCase):
    def test_scorer_claims_unscored_urls_and_updates_timestamp(self):
        cursor = _FakeCursor()
        fake_scorer = _FakeScorer()
        cfg = GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=4,
            worker_id=0,
            batch_size=1000,
            scan_interval_sec=60,
            max_batches_per_shard=1,
        )
        service = GoldenDiscoveryRankerService(
            cfg=cfg,
            Session=_FakeScorerSessionFactory(cursor),
            scorer=fake_scorer,
        )
        execute_values_calls = []

        def fake_execute_values(cur, sql, rows, page_size):
            execute_values_calls.append((cur, sql, rows, page_size))

        with patch.object(scorer_service, "execute_values", fake_execute_values):
            count = service._score_batch(5)

        self.assertEqual(count, 2)
        self.assertEqual(
            fake_scorer.urls,
            ["https://example.com/a", "https://example.com/b"],
        )

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_005", select_sql)
        self.assertIn("url_score_updated_at IS NULL", select_sql)
        self.assertIn("ORDER BY first_seen ASC NULLS LAST", select_sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", select_sql)
        self.assertEqual(select_params, (1000,))

        _, update_sql, rows, page_size = execute_values_calls[0]
        self.assertIn("url_score_updated_at = CURRENT_TIMESTAMP", update_sql)
        self.assertEqual(
            rows,
            [
                ("https://example.com/a", 0.25),
                ("https://example.com/b", 0.75),
            ],
        )
        self.assertEqual(page_size, 2)


def _make_steering_service(cursor, *, steering: bool, batch_size: int = 100):
    return GoldenDiscoveryRankerService(
        cfg=GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=1,
            worker_id=0,
            batch_size=batch_size,
            scan_interval_sec=60,
            max_batches_per_shard=1,
            domain_priority_steering_enabled=steering,
        ),
        Session=_FakeScorerSessionFactory(cursor),
        scorer=_FakeScorer(),
    )


class GoldenDiscoveryRankerSteeringTest(unittest.TestCase):
    def test_steering_disabled_uses_legacy_first_seen_query(self):
        cursor = _FakeCursor()
        service = _make_steering_service(cursor, steering=False, batch_size=1000)

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(7)

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_007", select_sql)
        self.assertIn("ORDER BY first_seen ASC NULLS LAST", select_sql)
        self.assertNotIn("domain_state", select_sql)
        self.assertNotIn("domain_score", select_sql)
        self.assertEqual(select_params, (1000,))

    def test_steering_enabled_joins_domain_state_and_filters_by_score(self):
        cursor = _FakeCursor()
        # Defaults: min_age_days=3, rescore_ttl_days=5 (see config dataclass).
        service = _make_steering_service(cursor, steering=True, batch_size=1000)

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(7)

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_007 u", select_sql)
        self.assertIn("JOIN domain_state d ON d.domain_id = u.domain_id", select_sql)
        self.assertIn("d.domain_score > 0", select_sql)
        self.assertIn("FOR UPDATE OF u SKIP LOCKED", select_sql)

        # min_age gate: don't score URLs younger than min_age_days.
        self.assertIn("u.first_seen < NOW() - make_interval(days => %s)", select_sql)
        # TTL refresh: re-pick never-scored OR stale-scored rows (v2 features
        # drift, so url_score is no longer write-once).
        self.assertIn("u.url_score_updated_at IS NULL", select_sql)
        self.assertIn(
            "u.url_score_updated_at < NOW() - make_interval(days => %s)",
            select_sql,
        )
        # LRU ordering: golden tier first, then never-scored, then stalest.
        self.assertIn("ORDER BY d.domain_score DESC", select_sql)
        self.assertIn("u.url_score_updated_at ASC NULLS FIRST", select_sql)
        # Params are (min_age_days, rescore_ttl_days, batch_size) in that order.
        self.assertEqual(select_params, (3, 5, 1000))

    def test_steering_query_uses_no_write_once_only_filter(self):
        # Guard against regressing to the v1 write-once behavior: the steering
        # query must NOT gate solely on `url_score_updated_at IS NULL` — that
        # would score each URL once and never refresh as inlinks accumulate.
        cursor = _FakeCursor()
        service = _make_steering_service(cursor, steering=True, batch_size=500)

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(3)

        select_sql, _ = cursor.executed[0]
        self.assertIn("OR u.url_score_updated_at <", select_sql)

    def test_steering_gate_days_are_configurable(self):
        cursor = _FakeCursor()
        service = GoldenDiscoveryRankerService(
            cfg=GoldenDiscoveryRankerConfig(
                total_shards=256,
                num_workers=1,
                worker_id=0,
                batch_size=250,
                scan_interval_sec=60,
                max_batches_per_shard=1,
                domain_priority_steering_enabled=True,
                rescore_ttl_days=30,
                min_age_days=7,
            ),
            Session=_FakeScorerSessionFactory(cursor),
            scorer=_FakeScorer(),
        )

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(1)

        _, select_params = cursor.executed[0]
        self.assertEqual(select_params, (7, 30, 250))

    def test_steering_ttl_zero_is_write_once(self):
        # rescore_ttl_days<=0 must be true write-once: only never-scored rows,
        # with the TTL OR-branch (and its interval param) dropped entirely.
        # Emitting `< NOW() - interval '0'` instead would match every
        # already-scored row and re-score the whole pool on every pass.
        cursor = _FakeCursor()
        service = GoldenDiscoveryRankerService(
            cfg=GoldenDiscoveryRankerConfig(
                total_shards=256,
                num_workers=1,
                worker_id=0,
                batch_size=100,
                scan_interval_sec=60,
                max_batches_per_shard=1,
                domain_priority_steering_enabled=True,
                rescore_ttl_days=0,
                min_age_days=3,
            ),
            Session=_FakeScorerSessionFactory(cursor),
            scorer=_FakeScorer(),
        )

        with patch.object(scorer_service, "execute_values", lambda *a, **kw: None):
            service._score_batch(2)

        select_sql, select_params = cursor.executed[0]
        self.assertIn("u.url_score_updated_at IS NULL", select_sql)
        self.assertNotIn("OR u.url_score_updated_at <", select_sql)
        # Only (min_age_days, batch_size) — the TTL interval param is gone.
        self.assertEqual(select_params, (3, 100))


class _FakeInlineRanker:
    def __init__(self):
        self.calls = []
        self.urls = []

    def score_many(self, urls):
        self.urls = list(urls)
        self.calls.append(list(urls))
        return [0.42 + i / 100 for i, _ in enumerate(urls)]


class _FakeLinkCursor:
    def __init__(self, existing_urls=()):
        self.existing_urls = set(existing_urls)
        self.executed = []
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        urls = params[0] if params else []
        self._rows = [(url,) for url in urls if url in self.existing_urls]

    def fetchall(self):
        return self._rows


class GoldenDiscoveryIngestInlineScoringTest(unittest.TestCase):
    def test_new_links_are_inserted_with_inline_ranker_scores_when_enabled(self):
        ranker = _FakeInlineRanker()
        db = IngestDB(Session=None, inline_ranker=ranker)
        cursor = _FakeLinkCursor(existing_urls={"https://example.com/b"})
        execute_values_calls = []

        records = [
            (
                0,
                {
                    "url": "https://example.com/a",
                    "domain_id": 7,
                    "domain_score": 0.1,
                    "discovered_from": "https://example.com/",
                },
            ),
            (
                1,
                {
                    "url": "https://example.com/b",
                    "domain_id": 7,
                    "domain_score": 0.1,
                    "discovered_from": "https://example.com/",
                },
            ),
        ]

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            materialized_rows = list(rows)
            execute_values_calls.append((sql, materialized_rows, page_size, fetch))
            if fetch:
                return [
                    (row[0], row[0] not in cursor.existing_urls)
                    for row in materialized_rows
                ]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            result = db._bulk_links(cur=cursor, shard_id=3, items=records)

        self.assertEqual(result, [(0, True), (1, False)])
        self.assertEqual(ranker.calls, [["https://example.com/a"]])
        # _bulk_links first checks for frozen domains, then (because inline
        # scoring is active) looks up which URLs already exist before scoring
        # only the new ones.
        self.assertIn("discovery_frozen = TRUE", cursor.executed[0][0])
        self.assertIn("FROM url_state_current_003", cursor.executed[1][0])

        current_sql, current_rows, _, current_fetch = execute_values_calls[0]
        self.assertTrue(current_fetch)
        self.assertIn("url_score, url_score_updated_at", current_sql)
        self.assertEqual(current_rows[0][3], 0.42)
        self.assertEqual(current_rows[1][3], 0.0)
        self.assertIsNotNone(current_rows[0][4])
        self.assertIsNone(current_rows[1][4])

        history_sql, history_rows, _, history_fetch = execute_values_calls[1]
        self.assertFalse(history_fetch)
        self.assertIn("url_score, url_score_updated_at", history_sql)
        self.assertEqual(history_rows, [current_rows[0]])

    def test_new_links_fall_back_to_unscored_when_inline_ranker_times_out(self):
        ranker = _FakeInlineRanker()
        db = IngestDB(
            Session=None,
            inline_ranker=ranker,
            inline_score_timeout_sec=0.0,
        )
        cursor = _FakeLinkCursor()
        execute_values_calls = []
        records = [
            (
                0,
                {
                    "url": "https://example.com/a",
                    "domain_id": 7,
                    "domain_score": 0.1,
                },
            )
        ]

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            materialized_rows = list(rows)
            execute_values_calls.append((sql, materialized_rows, page_size, fetch))
            if fetch:
                return [(row[0], True) for row in materialized_rows]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            result = db._bulk_links(cur=cursor, shard_id=3, items=records)

        self.assertEqual(result, [(0, True)])
        self.assertEqual(ranker.calls, [])
        # The frozen-domain guard still runs; but with the inline timeout at 0
        # the scoring path is skipped, so there is no existing-URL lookup.
        self.assertEqual(len(cursor.executed), 1)
        self.assertIn("discovery_frozen = TRUE", cursor.executed[0][0])
        current_rows = execute_values_calls[0][1]
        self.assertEqual(current_rows[0][3], 0.0)
        self.assertIsNone(current_rows[0][4])

    def test_new_links_remain_unscored_when_inline_ranker_is_disabled(self):
        db = IngestDB(Session=None, inline_ranker=None)
        cursor = _FakeLinkCursor()
        execute_values_calls = []
        records = [
            (
                0,
                {
                    "url": "https://example.com/a",
                    "domain_id": 7,
                    "domain_score": 0.1,
                },
            )
        ]

        def fake_execute_values(cur, sql, rows, page_size, fetch=False):
            materialized_rows = list(rows)
            execute_values_calls.append((sql, materialized_rows, page_size, fetch))
            if fetch:
                return [(row[0], True) for row in materialized_rows]
            return None

        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            result = db._bulk_links(cur=cursor, shard_id=3, items=records)

        self.assertEqual(result, [(0, True)])
        # No inline ranker, so scoring is skipped and no existing-URL lookup
        # runs — only the frozen-domain guard touches the cursor.
        self.assertEqual(len(cursor.executed), 1)
        self.assertIn("discovery_frozen = TRUE", cursor.executed[0][0])
        current_rows = execute_values_calls[0][1]
        self.assertEqual(current_rows[0][3], 0.0)
        self.assertIsNone(current_rows[0][4])


if __name__ == "__main__":
    unittest.main()

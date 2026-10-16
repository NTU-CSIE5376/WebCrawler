from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from containers.scheduler_ingest.router.domain_resolver import DomainResolver


@pytest.fixture
def database(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'domains.db'}")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE domain_state (
                domain_id INTEGER PRIMARY KEY,
                domain TEXT UNIQUE NOT NULL,
                shard_id INTEGER NOT NULL,
                domain_score FLOAT DEFAULT 0
            )
        """))
    statements = []

    @event.listens_for(engine, "before_cursor_execute")
    def record_sql(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.strip())

    yield engine, statements
    engine.dispose()


def test_batch_reuses_existing_domains_and_inserts_only_missing(database):
    engine, statements = database
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO domain_state VALUES
            (42, 'existing.example', 7, 0.9),
            (43, 'null-score.example', 8, NULL)
        """))
    statements.clear()
    with Session(engine) as session, session.begin():
        result = DomainResolver(session).batch_ensure_and_get({
            "existing.example": 99, "new.example": 12, "null-score.example": 8,
        })
        assert result["existing.example"] == (42, 0.9)
        assert result["null-score.example"] == (43, 0.0)
        assert result["new.example"][1] == 0.0
        assert len(statements) == 3
        rows = {row.domain: row for row in session.execute(text("SELECT * FROM domain_state"))}
        assert len(rows) == 3
        assert rows["existing.example"].shard_id == 7
        assert rows["new.example"].shard_id == 12


def test_known_domains_need_only_one_select_and_cache_avoids_repeat_queries(database):
    engine, statements = database
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO domain_state VALUES (1, 'known.example', 4, 0.5)"))
    statements.clear()
    with Session(engine) as session, session.begin():
        resolver = DomainResolver(session)
        assert resolver.batch_ensure_and_get({}) == {}
        assert statements == []
        expected = {"known.example": (1, 0.5)}
        assert resolver.batch_ensure_and_get({"known.example": 4}) == expected
        assert len(statements) == 1
        assert statements[0].startswith("SELECT")
        assert resolver.batch_ensure_and_get({"known.example": 4}) == expected
        assert resolver.ensure_and_get("known.example", 4) == (1, 0.5)
        assert len(statements) == 1


def test_large_batch_bounds_query_count_and_parameters(database):
    engine, statements = database
    domains = {f"domain-{index}.example": index % 256 for index in range(1001)}
    with Session(engine) as session, session.begin():
        result = DomainResolver(session).batch_ensure_and_get(domains)
        assert set(result) == set(domains)
        assert len({domain_id for domain_id, _ in result.values()}) == 1001
        assert len(statements) == 6  # SELECT, INSERT, SELECT for each chunk.
    statements.clear()
    with Session(engine) as session, session.begin():
        assert DomainResolver(session).batch_ensure_and_get(domains) == result
        assert len(statements) == 2
        assert all(statement.startswith("SELECT") for statement in statements)


def test_batch_size_can_be_overridden(database):
    engine, statements = database
    domains = {f"domain-{index}.example": index for index in range(5)}
    with Session(engine) as session, session.begin():
        result = DomainResolver(session).batch_ensure_and_get(domains, batch_size=2)
        assert set(result) == set(domains)
        assert len({domain_id for domain_id, _ in result.values()}) == 5
        assert len(statements) == 9  # Three SQL statements for each of three chunks.
    statements.clear()
    with Session(engine) as session, session.begin():
        assert DomainResolver(session).batch_ensure_and_get(domains, batch_size=2) == result
        assert len(statements) == 3
        assert all(statement.startswith("SELECT") for statement in statements)


@pytest.mark.parametrize("batch_size", [0, -1, 1.5, True])
def test_invalid_batch_size_is_rejected_before_sql(database, batch_size):
    engine, statements = database
    with Session(engine) as session:
        with pytest.raises(ValueError, match="batch_size must be a positive integer"):
            DomainResolver(session).batch_ensure_and_get({"new.example": 1}, batch_size=batch_size)
    assert statements == []


def test_domain_created_after_lookup_is_reused(database):
    engine, _ = database

    inserted = False

    @event.listens_for(engine, "before_cursor_execute")
    def insert_between_lookup_and_insert(conn, cursor, statement, parameters, context, executemany):
        nonlocal inserted
        if not inserted and statement.startswith("INSERT INTO domain_state (domain, shard_id)"):
            inserted = True
            # Simulate a writer creating the domain after the first SELECT.
            # Exercise the real unique constraint and ON CONFLICT handling.
            conn.execute(text("INSERT INTO domain_state VALUES (71, 'racing.example', 9, 0.8)"))

    with Session(engine) as session, session.begin():
        assert DomainResolver(session).batch_ensure_and_get({"racing.example": 9}) == {
            "racing.example": (71, 0.8),
        }
        assert inserted

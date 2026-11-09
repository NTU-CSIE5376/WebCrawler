"""Real transactions on isolated local PostgreSQL; never use a production DSN."""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
import pytest
import tldextract

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import golden_inject as g


@pytest.fixture(scope="module")
def postgres(tmp_path_factory):
    supplied = os.environ.get("GOLDEN_INJECT_TEST_DSN")
    if supplied:
        config = parse_dsn(supplied)
        if (config.get("host") not in {"localhost", "127.0.0.1", "::1"}
                or not config.get("dbname", "").startswith("golden_test")):
            raise ValueError("GOLDEN_INJECT_TEST_DSN must be a localhost golden_test database")
        yield config
        return
    pg_config = shutil.which("pg_config")
    if pg_config is None or os.geteuid() == 0:
        pytest.skip("Local unprivileged PostgreSQL binaries or GOLDEN_INJECT_TEST_DSN required")
    binaries = Path(subprocess.check_output([pg_config, "--bindir"], text=True).strip())
    if not (binaries / "initdb").exists():
        pytest.skip("PostgreSQL server binaries are unavailable")
    root = tmp_path_factory.mktemp("golden-postgres")
    data, sockets = root / "data", root / "socket"
    sockets.mkdir()
    with socket.socket() as handle:
        handle.bind(("127.0.0.1", 0))
        port = handle.getsockname()[1]
    subprocess.run([str(binaries / "initdb"), "-D", str(data), "-U", "postgres",
                    "--auth=trust", "--no-locale", "-E", "UTF8"], check=True, capture_output=True)
    subprocess.run([str(binaries / "pg_ctl"), "-D", str(data), "-l", str(root / "log"),
                    "-o", f"-h 127.0.0.1 -p {port} -k {sockets}", "-w", "start"],
                   check=True, capture_output=True)
    try:
        yield dict(host="127.0.0.1", port=port, dbname="postgres", user="postgres")
    finally:
        subprocess.run([str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"],
                       check=True, capture_output=True)


@pytest.fixture
def database(postgres, monkeypatch):
    schema = "golden_test_" + uuid.uuid4().hex
    with closing(psycopg2.connect(**postgres)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            cur.execute("""
                CREATE TABLE domain_state (domain_id bigserial PRIMARY KEY, domain text UNIQUE,
                  shard_id integer, domain_score double precision DEFAULT 0);
                CREATE TABLE shard_split_subdomain (host text, migrated_at timestamptz);
                CREATE TABLE metric_batches (id bigint PRIMARY KEY, created_at timestamptz);
                CREATE TABLE metric_queries (id bigint PRIMARY KEY, batch_id bigint);
                CREATE TABLE metric_url (id bigint PRIMARY KEY, query_id bigint, url text);
            """)
            for shard in range(4):
                cur.execute(f"""CREATE TABLE url_state_current_{shard:03d} (
                    url text PRIMARY KEY, domain_id bigint REFERENCES domain_state,
                    domain_score double precision, source smallint, url_score double precision,
                    url_score_updated_at timestamptz, should_crawl boolean)""")
                cur.execute(f"""CREATE TABLE url_state_history_{shard:03d} (
                    id bigserial PRIMARY KEY, url text, domain_id bigint REFERENCES domain_state,
                    domain_score double precision, source smallint)""")
    config = {**postgres, "options": f"-c search_path={schema}"}
    monkeypatch.setattr(g, "CRAWLERDB", config)
    monkeypatch.setattr(g, "METRICDB", config)
    monkeypatch.setattr(g, "NUM_SHARDS", 4)
    monkeypatch.setattr(g, "_retry_delay", lambda attempt: None)
    monkeypatch.setattr(tldextract, "extract", tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None))

    def query(statement, params=None):
        with closing(psycopg2.connect(**config)) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(statement, params)
                return cursor.fetchall() if cursor.description else None

    try:
        yield query
    finally:
        with closing(psycopg2.connect(**postgres)) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def plan(n=5):
    return g.prepare_plan([{"id": i, "url": f"https://example.com/{i}"}
                           for i in range(1, n + 1)], [1], {}, set())


def saved_rows(query, p):
    sid = p.records[0].shard_id
    current = query(f"SELECT url,source,url_score,should_crawl FROM url_state_current_{sid:03d} ORDER BY url")
    history = query(f"SELECT url FROM url_state_history_{sid:03d} ORDER BY id")
    return current, history


def test_real_commit_rollback_resume_and_completed_weekly_rerun(database, tmp_path, monkeypatch):
    p = plan()
    original = g.inject_batch
    calls = []

    def fail_second(cur, records, cache):
        calls.append([r.metric_id for r in records])
        result = original(cur, records, cache)
        if records[0].metric_id == 3:
            raise RuntimeError("failure after uncommitted writes")
        return result

    monkeypatch.setattr(g, "inject_batch", fail_second)
    with pytest.raises(RuntimeError, match="uncommitted"):
        g.run_plan(p, tmp_path, commit_interval=2, max_retries=0, batch_pause=0)
    current, history = saved_rows(database, p)
    assert len(current) == len(history) == 2
    assert calls == [[1, 2], [3, 4]]
    calls.clear()

    def record(cur, records, cache):
        calls.append([r.metric_id for r in records])
        return original(cur, records, cache)

    monkeypatch.setattr(g, "inject_batch", record)
    summary = g.run_plan(p, tmp_path, commit_interval=2, max_retries=0, batch_pause=0)
    assert calls == [[3, 4], [5]]
    assert summary["processed"] == 5
    current, history = saved_rows(database, p)
    assert len(current) == len(history) == 5
    assert all(row[1:] == (1, 3.0, True) for row in current)
    calls.clear()
    g.run_plan(p, tmp_path, commit_interval=2, max_retries=0, batch_pause=0)
    assert calls == [[1, 2], [3, 4], [5]], "Completed weekly runs must reassert the whole list"
    assert len(saved_rows(database, p)[1]) == 5


def test_transient_rollback_discards_uncommitted_domain_cache(database, tmp_path, monkeypatch):
    original = g.inject_batch
    attempts = []

    def fail_first(cur, records, cache):
        result = original(cur, records, cache)
        attempts.append(result.domains["example.com"][0])
        if len(attempts) == 1:
            raise psycopg2.OperationalError("simulated connection interruption")
        return result

    monkeypatch.setattr(g, "inject_batch", fail_first)
    p = plan(2)
    g.run_plan(p, tmp_path, max_retries=1, batch_pause=0)
    assert len(attempts) == 2 and attempts[0] != attempts[1]
    assert len(saved_rows(database, p)[0]) == len(saved_rows(database, p)[1]) == 2


def test_commit_acknowledgement_lost_replays_without_duplicate_history(database, tmp_path, monkeypatch):
    original = g._connect
    interrupted = False

    class CommitLost:
        def __init__(self, raw):
            self.raw = raw

        def __getattr__(self, name):
            return getattr(self.raw, name)

        def commit(self):
            self.raw.commit()
            raise psycopg2.OperationalError("commit succeeded but acknowledgement was lost")

    def connect(config, *, read_only=False):
        nonlocal interrupted
        raw = original(config, read_only=read_only)
        if not read_only and not interrupted:
            interrupted = True
            return CommitLost(raw)
        return raw

    monkeypatch.setattr(g, "_connect", connect)
    p = plan(2)
    summary = g.run_plan(p, tmp_path, max_retries=1, batch_pause=0)
    assert summary["processed"] == 2
    current, history = saved_rows(database, p)
    assert len(current) == len(history) == 2


def test_checkpoint_failure_after_commit_is_visible_and_replay_safe(database, tmp_path, monkeypatch):
    original = g._atomic_json

    def fail_progress(path, value):
        if path.name == "checkpoint.json" and value["cursor"] > 0:
            raise OSError("checkpoint disk failure")
        return original(path, value)

    monkeypatch.setattr(g, "_atomic_json", fail_progress)
    p = plan(3)
    with pytest.raises(OSError, match="disk failure"):
        g.run_plan(p, tmp_path, commit_interval=2, max_retries=8, batch_pause=0)
    assert len(saved_rows(database, p)[0]) == 2
    monkeypatch.setattr(g, "_atomic_json", original)
    g.run_plan(p, tmp_path, commit_interval=2, max_retries=0, batch_pause=0)
    assert len(saved_rows(database, p)[0]) == len(saved_rows(database, p)[1]) == 3


def test_permanent_schema_error_is_not_retried(database, tmp_path, monkeypatch):
    p = plan(2)
    database(f"ALTER TABLE url_state_current_{p.records[0].shard_id:03d} DROP COLUMN url_score_updated_at")
    monkeypatch.setattr(g, "_retry_delay", lambda _: pytest.fail("Permanent errors must not retry"))
    with pytest.raises(psycopg2.errors.UndefinedColumn):
        g.run_plan(p, tmp_path, max_retries=8, batch_pause=0)
    assert saved_rows(database, p) == ([], [])


def test_dry_run_uses_real_readonly_connections_and_creates_no_state(database, tmp_path):
    database("INSERT INTO metric_batches VALUES (1,NOW()-INTERVAL '8 weeks')")
    database("INSERT INTO metric_queries VALUES (1,1)")
    database("INSERT INTO metric_url VALUES (1,1,%s)", ("https://example.com/1",))
    state_dir = tmp_path / "absent"
    summary = g.main(["--dry-run", "--state-dir", str(state_dir), "--max-retries", "0"])
    assert summary["valid_rows"] == 1 and not state_dir.exists()
    assert saved_rows(database, plan(1)) == ([], [])

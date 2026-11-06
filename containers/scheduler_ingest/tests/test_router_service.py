from __future__ import annotations

import json

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.router import service as router_service
from containers.scheduler_ingest.router.service import RouterConfig, RouterService
from libs.db.sharding.router import ShardRouter

NUM_SHARDS = 4


class _Stats:
    def write(self, **kwargs):
        self.last = kwargs


@pytest.fixture
def router(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'crawler.db'}")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE domain_state (
                domain_id INTEGER PRIMARY KEY,
                domain TEXT UNIQUE NOT NULL,
                shard_id INTEGER NOT NULL,
                domain_score FLOAT DEFAULT 0
            )
        """))
        for shard in range(NUM_SHARDS):
            conn.execute(text(
                f"CREATE TABLE url_state_current_{shard:03d} (url TEXT PRIMARY KEY, url_score FLOAT)"
            ))
        conn.execute(text("INSERT INTO domain_state VALUES (1, 'known.example', 0, 0.7)"))
    statements = []

    @event.listens_for(engine, "before_cursor_execute")
    def record_sql(conn, cursor, statement, parameters, context, executemany):
        statements.append(" ".join(statement.split()))

    svc = RouterService.__new__(RouterService)
    svc.cfg = RouterConfig(
        router_id=3,
        crawler_dir_template=str(tmp_path / "crawler_{id:02d}"),
        ingestor_dir_template=str(tmp_path / "ingestor_{id:02d}"),
        progress_template=str(tmp_path / "progress_{id:02d}.json"),
        stats_dir=str(tmp_path / "stats"),
        interval_minutes=10,
        scan_sleep_minutes=5,
        num_shards=NUM_SHARDS,
        shards_per_ingestor=2,
        domain_overrides={},
        split_subdomains=set(),
        postgres_dsn="unused",
    )
    svc.sharder = ShardRouter(num_shards=NUM_SHARDS, shards_per_ingestor=2, domain_overrides={})
    svc.engine = engine
    svc.Session = sessionmaker(bind=engine, future=True)
    svc.stats = _Stats()
    svc._domain_cache = {}
    svc._domain_cache_born = router_service.time.monotonic()
    svc._made_dirs = set()
    yield svc, statements, tmp_path
    engine.dispose()


def _crawl_folder(tmp_path, name, records):
    folder = tmp_path / "in" / name
    folder.mkdir(parents=True)
    (folder / "part.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
    return folder


def _record(url, links):
    return {
        "url": url, "status": "ok", "fetched_at": "2026-11-05T00:00:00+00:00",
        "content": "<html></html>",
        "outlinks": [{"url": u, "anchor": "a"} for u in links],
    }


def _output_lines(tmp_path):
    lines = []
    for f in sorted(tmp_path.glob("ingestor_*/*/*/*.jsonl")):
        lines.extend(json.loads(line) for line in f.read_text().splitlines())
    return lines


def _domain_sql(statements):
    return [s for s in statements if "domain_state" in s]


def test_outlinks_resolve_domains_in_one_batch_and_write_once_per_file(router, monkeypatch):
    svc, statements, tmp_path = router
    links = [
        "https://known.example/a", "https://new-one.example/b",
        "https://new-two.example/c", "https://new-one.example/d",
    ]
    folder = _crawl_folder(tmp_path, "f1", [_record("https://known.example/", links)])

    opened = []
    real_open = router_service.os.open
    monkeypatch.setattr(
        router_service.os, "open", lambda path, *a: opened.append(path) or real_open(path, *a),
    )
    svc.process_folder(folder)

    out = _output_lines(tmp_path)
    new_links = [r for r in out if r["status"] == "new"]
    results = [r for r in out if r["status"] == "ok"]
    assert sorted(r["url"] for r in new_links) == sorted(links)
    assert len(results) == 1
    assert results[0]["outlinks"] and len(results[0]["outlinks"]) == 4
    ids = {r["url"].split("/")[2]: r["domain_id"] for r in new_links}
    assert ids["known.example"] == 1
    assert len(set(ids.values())) == 3
    # One append per distinct ingestor file for the whole record.
    opened = [p for p in opened if "_router03.jsonl" in p]
    assert opened and len(opened) == len(set(opened))
    # SELECT for the crawled domain, then one SELECT / INSERT / SELECT for
    # the two unknown outlink domains together.
    domain_sql = _domain_sql(statements)
    assert len(domain_sql) == 4
    assert sum(s.startswith("INSERT") for s in domain_sql) == 1


def test_domain_cache_survives_across_folders(router):
    svc, statements, tmp_path = router
    links = ["https://known.example/a", "https://other.example/b"]
    svc.process_folder(_crawl_folder(tmp_path, "f1", [_record("https://known.example/", links)]))
    statements.clear()
    svc.process_folder(_crawl_folder(tmp_path, "f2", [_record("https://known.example/x", links)]))
    assert _domain_sql(statements) == []


def test_domain_cache_expires(router, monkeypatch):
    svc, statements, tmp_path = router
    links = ["https://known.example/a"]
    svc.process_folder(_crawl_folder(tmp_path, "f1", [_record("https://known.example/", links)]))
    statements.clear()
    born = svc._domain_cache_born
    monkeypatch.setattr(
        router_service.time, "monotonic",
        lambda: born + router_service.DOMAIN_CACHE_TTL_SEC,
    )
    svc.process_folder(_crawl_folder(tmp_path, "f2", [_record("https://known.example/x", links)]))
    assert len(_domain_sql(statements)) == 1


def test_failed_record_writes_nothing(router, monkeypatch):
    svc, _, tmp_path = router

    def boom(*args, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(svc, "_parent_url_score", boom)
    svc.process_folder(_crawl_folder(
        tmp_path, "f1", [_record("https://known.example/", ["https://known.example/a"])],
    ))
    assert _output_lines(tmp_path) == []


def test_folder_done_reports_records_links_and_duration(router, caplog):
    svc, _, tmp_path = router
    folder = _crawl_folder(tmp_path, "f1", [
        _record("https://known.example/", ["https://known.example/a", "https://b.example/"]),
        _record("https://known.example/2", ["https://known.example/c"]),
    ])
    with caplog.at_level("INFO", logger="router"):
        svc.process_folder(folder)
    (done,) = [r for r in caplog.records if getattr(r, "event", None) == "route.folder_done"]
    assert (done.records, done.links, done.errors) == (2, 3, 0)
    assert done.duration_s >= 0

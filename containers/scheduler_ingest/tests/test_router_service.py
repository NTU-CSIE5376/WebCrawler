from __future__ import annotations

import json
from dataclasses import replace

import pytest
from sqlalchemy import create_engine, event, text

from containers.scheduler_ingest.router import service as router_service
from containers.scheduler_ingest.router.service import RouterConfig, RouterService

NUM_SHARDS = 4


class _Stats:
    def write(self, **kwargs):
        self.last = kwargs


@pytest.fixture
def router(tmp_path, monkeypatch):
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

    cfg = RouterConfig(
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
    monkeypatch.setattr(router_service, "create_engine", lambda *args, **kwargs: engine)
    svc = RouterService(cfg)
    svc.stats = _Stats()
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
        lambda: born + router_service.DOMAIN_CACHE_TTL_SEC + 1,
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


def test_batched_scores_match_point_reads_and_reduce_queries(router, monkeypatch):
    svc, statements, root = router
    records = [_record(f"https://known.example/page{i}", [
        "https://known.example/a", "https://new.example/b",
    ]) for i in range(300)]
    shard = svc.sharder.domain_to_shard("known.example")
    with svc.engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO url_state_current_{shard:03d} (url,url_score) VALUES (:url,:score)"
        ), [{"url": r["url"], "score": i / 1000} for i, r in enumerate(records)])
    folder = _crawl_folder(root, "sample", records)
    prefetch = svc._prefetch_parent_scores
    monkeypatch.setattr(svc, "_prefetch_parent_scores", lambda records: {})
    statements.clear()
    svc.process_folder(folder)
    expected = _output_lines(root)
    assert sum(s.startswith("SELECT url_score FROM") for s in statements) == 300

    monkeypatch.setattr(svc, "_prefetch_parent_scores", prefetch)
    svc.cfg = replace(svc.cfg, ingestor_dir_template=str(root / "fast/ingestor_{id:02d}"))
    statements.clear()
    svc.process_folder(folder)
    assert _output_lines(root / "fast") == expected
    assert sum(s.startswith("SELECT url, url_score FROM") for s in statements) == 2
    assert not any(s.startswith("SELECT url_score FROM") for s in statements)


def test_missing_scores_are_batched_and_records_without_links_skip_lookup(router):
    svc, statements, root = router
    records = [_record("https://known.example/no-links", []),
               _record("https://known.example/missing", ["https://known.example/a"])]
    svc.process_folder(_crawl_folder(root, "missing", records))
    assert sum(s.startswith("SELECT url, url_score FROM") for s in statements) == 1
    assert not any(s.startswith("SELECT url_score FROM") for s in statements)
    assert len([r for r in _output_lines(root) if r["status"] == "ok"]) == 2


def test_failed_prefetch_falls_back_to_normal_point_lookup(router, monkeypatch):
    svc, statements, root = router
    factory = svc.Session
    calls = 0

    def sessions():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("prefetch unavailable")
        return factory()

    monkeypatch.setattr(svc, "Session", sessions)
    records = [_record("https://known.example/", ["https://known.example/a"])]
    svc.process_folder(_crawl_folder(root, "fallback", records))
    assert any(s.startswith("SELECT url_score FROM") for s in statements)
    assert len([r for r in _output_lines(root) if r["status"] == "ok"]) == 1


def test_scores_refresh_at_next_batch(router):
    svc, _, _ = router
    record = _record("https://known.example/", ["https://known.example/a"])
    shard = svc.sharder.domain_to_shard("known.example")
    with svc.engine.begin() as conn:
        conn.execute(text(f"INSERT INTO url_state_current_{shard:03d} VALUES (:url,0.1)"),
                     {"url": record["url"]})
    iterator = svc._records_with_parent_scores([record] * 257)
    _, first = next(iterator)
    with svc.engine.begin() as conn:
        conn.execute(text(f"UPDATE url_state_current_{shard:03d} SET url_score=0.9"))
    for _ in range(255):
        assert next(iterator)[1][(shard, record["url"])] == 0.1
    assert first[(shard, record["url"])] == 0.1
    assert next(iterator)[1][(shard, record["url"])] == 0.9


def test_route_cache_preserves_overrides_split_hosts_and_is_instance_local(router):
    svc, _, _ = router
    cfg = replace(svc.cfg, domain_overrides={"example.com": 2},
                  split_subdomains={"en.wikipedia.org"})
    other = RouterService(cfg)
    for host in ("example.com", "www.example.com", "en.wikipedia.org", "127.0.0.1", ""):
        assert svc._route_host(host) == svc.sharder.route(host)
        assert other._route_host(host) == other.sharder.route(host)
    assert other._route_host("example.com")[1] == 2
    assert svc._route_host.cache_info().maxsize == router_service.HOST_ROUTE_CACHE_MAX
    svc._route_host.cache_clear()
    assert other._route_host.cache_info().currsize > 0


def test_partial_prefetch_failure_discards_all_scores(router, monkeypatch):
    svc, statements, root = router
    hosts = ["known.example"]
    for i in range(20):
        host = f"other{i}.example"
        if svc.sharder.domain_to_shard(host) != svc.sharder.domain_to_shard(hosts[0]):
            hosts.append(host)
            break
    assert len(hosts) == 2
    prefetch_queries = 0

    @event.listens_for(svc.engine, "before_cursor_execute")
    def fail_second_shard(conn, cursor, statement, parameters, context, executemany):
        nonlocal prefetch_queries
        if statement.startswith("SELECT url, url_score FROM"):
            prefetch_queries += 1
            if prefetch_queries == 2:
                raise RuntimeError("second shard unavailable")

    lookup = svc._parent_url_score
    fallback_calls = []

    def point_read(sess, shard, url, prefetched=None):
        assert prefetched == {}
        fallback_calls.append(url)
        return lookup(sess, shard, url, prefetched)

    monkeypatch.setattr(svc, "_parent_url_score", point_read)
    records = [_record(f"https://{host}/", ["https://known.example/a"]) for host in hosts]
    svc.process_folder(_crawl_folder(root, "partial", records))
    assert prefetch_queries == 2
    assert fallback_calls == [r["url"] for r in records]
    assert len([r for r in _output_lines(root) if r["status"] == "ok"]) == 2


def test_folder_log_counts_input_records_and_duration(router, caplog):
    svc, _, root = router
    with caplog.at_level("INFO", logger="router"):
        svc.process_folder(_crawl_folder(root, "metrics", [
            _record("https://known.example/", ["https://known.example/a"]),
            _record("https://known.example/empty", []),
        ]))
    done, = [record for record in caplog.records if record.msg == "route.folder_done"]
    assert done.record_cnt == 2
    assert done.file_cnt == 1
    assert done.errors == 0
    assert done.duration_sec >= 0


def test_folder_metrics_separate_input_attempts_from_flushed_results(router, monkeypatch, caplog):
    svc, _, root = router
    lookup = svc._parent_url_score

    def reject_one_record(session, shard, url, prefetched=None):
        if url.endswith('/rejected'):
            raise RuntimeError('record rejected before output')
        return lookup(session, shard, url, prefetched)

    monkeypatch.setattr(svc, '_parent_url_score', reject_one_record)
    folder = _crawl_folder(root, 'mixed-outcomes', [
        _record('https://known.example/first', [
            'https://known.example/a', 'https://known.example/b',
        ]),
        _record('https://known.example/rejected', ['https://known.example/skipped']),
        _record('https://known.example/last', ['https://known.example/c']),
    ])
    with caplog.at_level('INFO', logger='router'):
        svc.process_folder(folder)
    done, = [rec for rec in caplog.records if getattr(rec, 'event', None) == 'route.folder_done']
    assert (done.record_cnt, done.records, done.links, done.errors) == (3, 2, 3, 1)
    assert len([rec for rec in _output_lines(root) if rec['status'] == 'ok']) == 2


def test_folder_duration_log_names_share_one_measurement(router, monkeypatch, caplog):
    svc, _, root = router
    tick = svc._domain_cache_born

    def monotonic():
        nonlocal tick
        tick += 0.125
        return tick

    monkeypatch.setattr(router_service.time, 'monotonic', monotonic)
    with caplog.at_level('INFO', logger='router'):
        svc.process_folder(_crawl_folder(root, 'duration-alias', []))
    done, = [rec for rec in caplog.records if getattr(rec, 'event', None) == 'route.folder_done']
    assert done.duration_sec > 0
    assert done.duration_s == done.duration_sec

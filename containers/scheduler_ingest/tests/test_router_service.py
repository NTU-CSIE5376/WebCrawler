from __future__ import annotations

import json
from dataclasses import replace

import pytest
from sqlalchemy import create_engine, event, text

from containers.scheduler_ingest.router import service as router_service
from containers.scheduler_ingest.router.service import RouterConfig, RouterService
from libs.ipc.parquet import read_routed_records

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
    for folder in sorted(tmp_path.glob('ingestor_*/*/*')):
        for kind in ('discovery','content'):
            lines.extend(read_routed_records(folder, kind))
    return lines


def _domain_sql(statements):
    return [s for s in statements if "domain_state" in s]


def test_outlinks_resolve_domains_in_one_batch_and_publish_batched_files(router, monkeypatch):
    svc, statements, tmp_path = router
    links = [
        "https://known.example/a", "https://new-one.example/b",
        "https://new-two.example/c", "https://new-one.example/d",
    ]
    folder = _crawl_folder(tmp_path, "f1", [_record("https://known.example/", links)])

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
    # One closed output file for each destination/stream, not each outlink.
    files = list(tmp_path.glob('ingestor_*/*/*/*.parquet'))
    assert len(files) <= 3
    assert not list(tmp_path.glob('ingestor_*/*/*/*.tmp'))
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


def test_parent_scores_are_point_read_for_each_record_and_propagated_to_outlinks(router):
    svc, statements, root = router
    records = [_record(f"https://known.example/page{i}", [
        "https://known.example/a", "https://new.example/b",
    ]) for i in range(300)]
    shard = svc.sharder.domain_to_shard("known.example")
    scores = {record["url"]: i / 1000 for i, record in enumerate(records)}
    with svc.engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO url_state_current_{shard:03d} (url,url_score) VALUES (:url,:score)"
        ), [{"url": url, "score": score} for url, score in scores.items()])
    statements.clear()
    svc.process_folder(_crawl_folder(root, "sample", records))
    new_links = [row for row in _output_lines(root) if row["status"] == "new"]
    assert len(new_links) == 600
    assert all(row["parent_page_score"] == scores[row["discovered_from"]] for row in new_links)
    assert sum(s.startswith("SELECT url_score FROM") for s in statements) == len(records)
    assert not any("SELECT url, url_score FROM" in s for s in statements)

def test_missing_parent_scores_return_none_and_records_without_links_skip_lookup(router):
    svc, statements, root = router
    records = [_record("https://known.example/no-links", []),
               _record("https://known.example/missing", ["https://known.example/a"])]
    svc.process_folder(_crawl_folder(root, "missing", records))
    assert sum(s.startswith("SELECT url_score FROM") for s in statements) == 1
    output = _output_lines(root)
    assert len([row for row in output if row["status"] == "ok"]) == 2
    (link,) = [row for row in output if row["status"] == "new"]
    assert link["parent_page_score"] is None

def test_session_failure_does_not_skip_later_records_or_write_failed_record(router, monkeypatch):
    svc, statements, root = router
    factory = svc.Session
    calls = 0

    def sessions():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("session unavailable")
        return factory()

    monkeypatch.setattr(svc, "Session", sessions)
    records = [_record("https://known.example/failed", ["https://known.example/skipped"]),
               _record("https://known.example/next", ["https://known.example/kept"])]
    svc.process_folder(_crawl_folder(root, "failure", records))
    output = _output_lines(root)
    assert [row["url"] for row in output if row["status"] == "ok"] == [records[1]["url"]]
    assert [row["url"] for row in output if row["status"] == "new"] == ["https://known.example/kept"]
    assert sum(s.startswith("SELECT url_score FROM") for s in statements) == 1

def test_parent_score_refreshes_at_next_record_in_same_folder(router, monkeypatch):
    svc, _, root = router
    record = _record("https://known.example/", ["https://known.example/a"])
    shard = svc.sharder.domain_to_shard("known.example")
    with svc.engine.begin() as conn:
        conn.execute(text(f"INSERT INTO url_state_current_{shard:03d} VALUES (:url,0.1)"),
                     {"url": record["url"]})
    lookup = svc._parent_url_score
    seen = []

    def read_then_update(sess, sid, url):
        score = lookup(sess, sid, url)
        seen.append(score)
        if len(seen) == 1:
            sess.execute(text(f"UPDATE url_state_current_{sid:03d} SET url_score=0.9"))
        return score

    monkeypatch.setattr(svc, "_parent_url_score", read_then_update)
    svc.process_folder(_crawl_folder(root, "changing-score", [record, record]))
    assert seen == [0.1, 0.9]
    assert [row["parent_page_score"] for row in _output_lines(root)
            if row["status"] == "new"] == [0.1, 0.9]

def test_direct_routing_preserves_overrides_split_hosts_and_instance_configuration(router):
    svc, _, _ = router
    cfg = replace(svc.cfg, domain_overrides={"example.com": 2},
                  split_subdomains={"en.wikipedia.org"})
    other = RouterService(cfg)
    hosts = ("example.com", "www.example.com", "en.wikipedia.org", "127.0.0.1")
    try:
        for instance in (svc, other):
            links = [{"url": f"https://{host}/", "anchor": host} for host in hosts]
            routed = instance._route_links(links + [{"anchor": "missing URL"}])
            assert [row[0] for row in routed] == links
            assert [row[1:] for row in routed] == [instance.sharder.route(host) for host in hosts]
        assert other._route_links([{"url": "https://example.com/"}])[0][2] == 2
        assert other._route_links([{"url": "https://en.wikipedia.org/"}])[0][1] == "en.wikipedia.org"
    finally:
        other.engine.dispose()

def test_point_reads_use_each_records_shard_and_keep_output_order(router):
    svc, statements, root = router
    hosts = ["known.example"]
    for i in range(20):
        host = f"other{i}.example"
        if svc.sharder.domain_to_shard(host) != svc.sharder.domain_to_shard(hosts[0]):
            hosts.append(host)
            break
    assert len(hosts) == 2
    records = [_record(f"https://{host}/", ["https://known.example/a"]) for host in hosts]
    expected = {}
    with svc.engine.begin() as conn:
        for i, record in enumerate(records):
            sid = svc.sharder.domain_to_shard(hosts[i])
            expected[record["url"]] = (sid, 0.2 + i / 10)
            conn.execute(text(f"INSERT INTO url_state_current_{sid:03d} VALUES (:url,:score)"),
                         {"url": record["url"], "score": expected[record["url"]][1]})
    statements.clear()
    svc.process_folder(_crawl_folder(root, "different-shards", records))
    output = _output_lines(root)
    links = [row for row in output if row["status"] == "new"]
    assert [row["discovered_from"] for row in links] == [record["url"] for record in records]
    assert [row["parent_page_score"] for row in links] == [expected[record["url"]][1] for record in records]
    queries = [s for s in statements if s.startswith("SELECT url_score FROM")]
    assert len(queries) == 2
    assert all(f"url_state_current_{expected[record['url']][0]:03d}" in query
               for record, query in zip(records, queries))

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

    def reject_one_record(session, shard, url):
        if url.endswith('/rejected'):
            raise RuntimeError('record rejected before output')
        return lookup(session, shard, url)

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

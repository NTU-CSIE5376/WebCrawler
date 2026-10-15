from __future__ import annotations

import json
import errno
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import Mock

import pytest
import psycopg2
import tldextract
import yaml
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from containers.scheduler_ingest.api.app import create_app
from containers.scheduler_ingest.api import service as service_module
from containers.scheduler_ingest.api.service import SubmissionService
from containers.scheduler_ingest.api.dispatcher import SubmissionDispatcher
from containers.scheduler_ingest.api import dispatcher as dispatcher_module
from libs.db.sharding import key
from libs.ipc.jsonio import read_jsonl


@pytest.fixture
def setup_api(tmp_path, monkeypatch):
    config = {
        "router": {
            "num_shards": 256,
            "shards_per_ingestor": 16,
            "interval_minutes": 10,
            "ingestor_dir_template": str(tmp_path / "ipc" / "ingestor_{id:02d}"),
            "domain_overrides": {"example.com": 37, "example.org": 98,
                                 "special.example.com": 65},
        },
        "postgres": {"dsn": f"sqlite:///{tmp_path / 'test.db'}"},
        "api": {
            "max_content_length": 8192,
            "crawl_request": {"max_urls": 3, "pending_dir": str(tmp_path / "pending")},
        },
    }
    path = tmp_path / "ingest.yaml"
    path.write_text(yaml.safe_dump(config))
    # Exercise shared sharding and YAML overrides without PostgreSQL or network.
    monkeypatch.setattr(key, "load_split_subdomains", lambda conn: {"special.example.com"})
    monkeypatch.setattr(key.tldextract, "extract", tldextract.TLDExtract(
        suffix_list_urls=(), cache_dir=None,
    ))
    engine = create_engine(config["postgres"]["dsn"], connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE domain_state (
                domain_id INTEGER PRIMARY KEY,
                domain TEXT UNIQUE NOT NULL,
                shard_id INTEGER NOT NULL,
                domain_score FLOAT DEFAULT 0
            )
        """))
        for shard in (37, 65, 98):
            conn.execute(text(f"CREATE TABLE url_state_current_{shard:03d} (url TEXT PRIMARY KEY, should_crawl BOOLEAN)"))
    svc = SubmissionService(sessionmaker(bind=engine), config, str(path))
    app = create_app(str(path), service=svc)
    with TestClient(app) as client:
        yield client, svc, engine, tmp_path
    engine.dispose()


def records_by_worker(tmp_path):
    SubmissionDispatcher(yaml.safe_load((tmp_path / "ingest.yaml").read_text())).run_once()
    return {
        path.parent.parent.parent.name: read_jsonl(path)
        for path in (tmp_path / "ipc").rglob("*.jsonl")
    }


def test_submission_routes_records_and_commits_domains(setup_api):
    client, svc, engine, tmp_path = setup_api
    response = client.post("/v1/crawl-requests", json={"urls": [
        "https://www.example.com/a?b=2&a=1#fragment",
        "https://example.org/b",
        "https://special.example.com/c",
    ]})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    batches = list((tmp_path / "pending").glob("*.json"))
    assert len(batches) == 1
    assert len(json.loads(batches[0].read_text())) == 3
    assert not list((tmp_path / "ipc").rglob("*.jsonl"))
    groups = records_by_worker(tmp_path)
    assert set(groups) == {"ingestor_02", "ingestor_06", "ingestor_04"}
    with engine.connect() as conn:
        domains = {row.domain_id: row for row in conn.execute(text("SELECT * FROM domain_state"))}
    for worker, records in groups.items():
        for record in records:
            assert record["status"] == "new"
            assert "requested_at" not in record
            assert record["shard_id"] // 16 == int(worker[-2:])
            assert domains[record["domain_id"]].shard_id == record["shard_id"]
            assert record["domain_score"] == 0
            assert record["discovery_source_type"] == 0
            assert record["inlink_count_approx"] == 0
            assert record["inlink_count_external"] == 0
            assert record["discovered_from"] is None
    assert groups["ingestor_02"][0]["url"] == "https://www.example.com/a?a=1&b=2"
    assert not list(tmp_path.rglob("*.tmp"))


def test_canonical_duplicates_are_deduplicated(setup_api):
    client, _, _, tmp_path = setup_api
    response = client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/a#one", "https://example.com/a#two",
    ]})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert len(records_by_worker(tmp_path)["ingestor_02"]) == 1


def test_routes_are_defined_without_gateway_prefix(setup_api):
    client, *_ = setup_api
    paths = {route.path for route in client.app.routes}
    assert "/v1/crawl-requests" in paths
    assert all(not path.startswith("/scheduler_ingest/") for path in paths)


def test_old_generic_urls_route_is_not_exposed(setup_api):
    client, _, _, tmp_path = setup_api
    response = client.post("/v1/urls", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 404
    assert not list((tmp_path / "pending").glob("*.json"))


def test_existing_domain_is_reused_and_submissions_get_unique_files(setup_api):
    client, _, engine, tmp_path = setup_api
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO domain_state VALUES (123, 'example.com', 37, 0.9)"))
    results = [client.post("/v1/crawl-requests", json={"urls": ["https://example.com/a"]}) for _ in range(2)]
    assert all(result.status_code == 200 for result in results)
    assert all(result.json() == {"status": "ok"} for result in results)
    records_by_worker(tmp_path)
    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 2
    assert all(read_jsonl(path)[0]["domain_id"] == 123 for path in files)
    assert all(read_jsonl(path)[0]["domain_score"] == 0.9 for path in files)


@pytest.mark.parametrize("payload", [
    None, [], {}, {"urls": []}, {"urls": "https://example.com"},
    {"urls": ["https://example.com"] * 4},
    {"urls": ["https://example.com"], "shard_id": 37},
    {"urls": ["https://example.com/good", "ftp://example.com/bad"]},
    {"urls": [None]}, {"urls": [1]}, {"urls": ["/relative"]},
    {"urls": ["https://"]}, {"urls": ["https://user:pass@example.com"]},
    {"urls": ["https://example.com:99999"]},
    {"urls": ["https://example.com/a\nb"]},
    {"urls": ["https://example.com/" + "x" * 2500]},
    {"urls": ["https://example.com/" + "中" * 400]},
])
def test_invalid_batch_has_no_side_effects(setup_api, payload):
    client, _, engine, tmp_path = setup_api
    response = client.post("/v1/crawl-requests", content=json.dumps(payload), headers={"Content-Type": "application/json"})
    assert response.status_code == 400
    assert "error" in response.json()
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list((tmp_path / "pending").glob("*.json"))
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM domain_state")) == 0


@pytest.mark.parametrize("data,content_type,status", [
    ("{", "application/json", 400),
    ('{"urls": []}', "text/plain", 415),
    ("x" * 9000, "application/json", 413),
])
def test_http_errors_are_json(setup_api, data, content_type, status):
    client, *_ = setup_api
    response = client.post("/v1/crawl-requests", content=data, headers={"Content-Type": content_type})
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/json")


@pytest.mark.parametrize("error", [
    OperationalError("query", {}, psycopg2.OperationalError("secret connection string")),
    psycopg2.OperationalError("secret connection string"),
])
def test_db_failure_returns_503_without_publishing(setup_api, monkeypatch, error):
    client, _, _, tmp_path = setup_api
    monkeypatch.setattr(service_module, "load_sharding_config", Mock(
        side_effect=error,
    ))
    response = client.post("/v1/crawl-requests", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 503
    assert "secret" not in response.text
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list((tmp_path / "pending").glob("*.json"))


def test_domain_failure_rolls_back_whole_transaction(setup_api, monkeypatch):
    client, _, engine, tmp_path = setup_api
    original = service_module.DomainResolver.ensure_and_get

    def fail_second(self, domain, shard_id):
        if domain == "example.org":
            raise OperationalError("query", {}, psycopg2.OperationalError("database unavailable"))
        return original(self, domain, shard_id)

    monkeypatch.setattr(service_module.DomainResolver, "ensure_and_get", fail_second)
    response = client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 503
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list((tmp_path / "pending").glob("*.json"))
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM domain_state")) == 0


@pytest.mark.parametrize("failure", ["rename", "flush"])
def test_storage_failure_rejects_entire_multishard_batch(setup_api, monkeypatch, failure):
    client, _, _, tmp_path = setup_api
    operation = "replace" if failure == "rename" else "fsync"
    monkeypatch.setattr(service_module.os, operation, Mock(side_effect=OSError(errno.ENOSPC, "disk full")))
    response = client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 503
    assert response.json() == {"error": "Submission unavailable; no URLs accepted"}
    assert not list((tmp_path / "pending").glob("*.json"))
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list(tmp_path.rglob("*.tmp"))


def test_batch_is_published_with_one_rename_regardless_of_shards(setup_api, monkeypatch):
    client, _, _, tmp_path = setup_api
    original = service_module.os.replace
    publish = Mock(side_effect=original)
    monkeypatch.setattr(service_module.os, "replace", publish)
    response = client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 200
    assert publish.call_count == 1
    batches = list((tmp_path / "pending").glob("*.json"))
    assert len(batches) == 1
    assert len(json.loads(batches[0].read_text())) == 2


def test_delivery_failure_retains_accepted_batch_for_retry(setup_api, monkeypatch):
    client, _, _, tmp_path = setup_api
    response = client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 200
    batch = next((tmp_path / "pending").glob("*.json"))
    dispatcher = SubmissionDispatcher(yaml.safe_load((tmp_path / "ingest.yaml").read_text()))
    original_replace = service_module.os.replace
    count = 0

    def fail_second(source, target):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("disk full")
        original_replace(source, target)

    monkeypatch.setattr(dispatcher_module.os, "replace", fail_second)
    monkeypatch.setattr(dispatcher_module, "current_interval", lambda minutes: ("20261015", "0010"))
    with pytest.raises(OSError):
        dispatcher.process_file(batch)
    assert batch.exists()
    assert len(json.loads(batch.read_text())) == 2
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
    assert not list(tmp_path.rglob("*.tmp"))
    monkeypatch.setattr(dispatcher_module.os, "replace", original_replace)
    monkeypatch.setattr(dispatcher_module, "current_interval", lambda minutes: ("20261015", "0030"))
    # A fresh worker can resume an accepted batch left by a failed worker.
    SubmissionDispatcher(yaml.safe_load((tmp_path / "ingest.yaml").read_text())).run_once()
    assert not batch.exists()
    assert len(list(tmp_path.glob("ipc/ingestor_*/20261015/0030/*.jsonl"))) == 2
    groups = records_by_worker(tmp_path)
    assert set(groups) == {"ingestor_02", "ingestor_06"}


def test_dispatcher_ignores_unaccepted_temporary_files(setup_api):
    _, _, _, tmp_path = setup_api
    pending = tmp_path / "pending"
    pending.mkdir()
    (pending / "incomplete.json.tmp").write_text("incomplete")
    dispatcher = SubmissionDispatcher(yaml.safe_load((tmp_path / "ingest.yaml").read_text()))
    dispatcher.run_once()
    assert not list(tmp_path.rglob("*.jsonl"))


@pytest.mark.parametrize("error", [
    PermissionError(errno.EACCES, "secret storage path"),
    psycopg2.ProgrammingError("secret query"),
    RuntimeError("secret internal state"),
])
def test_unexpected_failure_returns_500_and_accepts_nothing(setup_api, monkeypatch, error):
    client, svc, _, tmp_path = setup_api
    monkeypatch.setattr(svc, "_submit_locked", Mock(side_effect=error))
    response = client.post("/v1/crawl-requests", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 500
    assert response.json() == {"error": "Submission failed; no URLs accepted"}
    assert not list((tmp_path / "pending").glob("*.json"))


def test_overlapping_api_requests_publish_complete_distinct_batches(setup_api):
    client, _, engine, tmp_path = setup_api
    start = Barrier(3)

    def submit(path):
        with TestClient(client.app) as concurrent_client:
            start.wait(timeout=5)
            return concurrent_client.post("/v1/crawl-requests", json={"urls": [
                f"https://example.com/{path}", f"https://example.org/{path}",
            ]}).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = [pool.submit(submit, path) for path in ("a", "b")]
        start.wait(timeout=5)
        assert [future.result(timeout=10) for future in responses] == [200, 200]
    batches = list((tmp_path / "pending").glob("*.json"))
    assert len(batches) == 2
    assert all(len(json.loads(batch.read_text())) == 2 for batch in batches)
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM domain_state")) == 2


def test_overlapping_dispatchers_do_not_deliver_same_batch_twice(setup_api, monkeypatch):
    client, _, _, tmp_path = setup_api
    assert client.post("/v1/crawl-requests", json={"urls": ["https://example.com/a"]}).status_code == 200
    batch = next((tmp_path / "pending").glob("*.json"))
    dispatcher = SubmissionDispatcher(yaml.safe_load((tmp_path / "ingest.yaml").read_text()))
    deliver = Mock(wraps=dispatcher._process_locked)
    monkeypatch.setattr(dispatcher, "_process_locked", deliver)
    start = Barrier(3)

    def dispatch():
        start.wait(timeout=5)
        dispatcher.process_file(batch)

    with ThreadPoolExecutor(max_workers=2) as pool:
        workers = [pool.submit(dispatch) for _ in range(2)]
        start.wait(timeout=5)
        for worker in workers:
            worker.result(timeout=10)
    assert deliver.call_count == 1
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
    assert not batch.exists()


def test_health_does_not_access_db(setup_api, monkeypatch):
    client, svc, *_ = setup_api
    monkeypatch.setattr(svc, "submit", Mock(side_effect=AssertionError("unexpected call")))
    assert client.get("/healthz").json() == {"status": "ok"}


def test_streamed_body_limit_rejects_before_acceptance(setup_api):
    client, _, _, tmp_path = setup_api

    def chunks():
        yield b'{"urls":["https://example.com/'
        yield b"a" * 9000
        yield b'"]}'

    response = client.post("/v1/crawl-requests", content=chunks(), headers={"Content-Type": "application/json"})
    assert response.status_code == 413
    assert not list((tmp_path / "pending").glob("*.json"))


def test_openapi_documents_gateway_prefix_and_validation_responses(setup_api):
    client, *_ = setup_api
    schema = client.get("/openapi.json").json()
    assert {"url": "/scheduler_ingest"} in schema["servers"]
    responses = schema["paths"]["/v1/crawl-requests"]["post"]["responses"]
    assert "400" in responses
    assert "422" not in responses
    assert "/scheduler_ingest/openapi.json" in client.get("/docs").text


def test_crawl_request_limits_are_loaded_from_yaml(setup_api):
    _, svc, _, tmp_path = setup_api
    config_path = tmp_path / "ingest.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["api"]["crawl_request"].update(max_urls=1, max_url_length=40)
    config_path.write_text(yaml.safe_dump(config))
    client = TestClient(create_app(str(config_path), service=svc))
    assert client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]}).status_code == 400
    assert client.post("/v1/crawl-requests", json={"urls": [
        "https://example.com/" + "a" * 30,
    ]}).status_code == 400
    assert not list((tmp_path / "pending").glob("*.json"))
    assert client.post("/v1/crawl-requests", json={"urls": ["https://example.com/a"]}).status_code == 200


@pytest.mark.parametrize("name,value", [
    ("max_urls", 0),
    ("max_url_length", 0),
    ("max_url_length", 2501),
])
def test_invalid_limit_configuration_fails_at_startup(setup_api, name, value):
    _, svc, _, tmp_path = setup_api
    config_path = tmp_path / "ingest.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["api"]["crawl_request"][name] = value
    config_path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match=f"api.crawl_request.{name}"):
        create_app(str(config_path), service=svc)

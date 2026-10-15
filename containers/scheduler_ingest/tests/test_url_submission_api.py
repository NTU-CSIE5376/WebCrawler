from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
import psycopg2
import tldextract
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.api.app import create_app
from containers.scheduler_ingest.api import service as service_module
from containers.scheduler_ingest.api.service import SubmissionService
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
        "api": {"max_urls": 3, "max_content_length": 8192},
    }
    path = tmp_path / "ingest.yaml"
    path.write_text(yaml.safe_dump(config))
    # Exercise shared sharding and YAML overrides without PostgreSQL or network.
    monkeypatch.setattr(key, "load_split_subdomains", lambda conn: {"special.example.com"})
    monkeypatch.setattr(key.tldextract, "extract", tldextract.TLDExtract(
        suffix_list_urls=(), cache_dir=None,
    ))
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE domain_state (
                domain_id INTEGER PRIMARY KEY,
                domain TEXT UNIQUE NOT NULL,
                shard_id INTEGER NOT NULL,
                domain_score FLOAT DEFAULT 0
            )
        """))
    svc = SubmissionService(sessionmaker(bind=engine), config, str(path))
    app = create_app(str(path), service=svc)
    app.config["TESTING"] = True
    yield app.test_client(), svc, engine, tmp_path
    engine.dispose()


def records_by_worker(tmp_path):
    return {
        path.parent.parent.parent.name: read_jsonl(path)
        for path in (tmp_path / "ipc").rglob("*.jsonl")
    }


def test_submission_routes_records_and_commits_domains(setup_api):
    client, svc, engine, tmp_path = setup_api
    response = client.post("/v1/urls", json={"urls": [
        "https://www.example.com/a?b=2&a=1#fragment",
        "https://example.org/b",
        "https://special.example.com/c",
    ]})
    assert response.status_code == 200
    assert response.json == {"status": "ok"}
    groups = records_by_worker(tmp_path)
    assert set(groups) == {"ingestor_02", "ingestor_06", "ingestor_04"}
    with engine.connect() as conn:
        domains = {row.domain_id: row for row in conn.execute(text("SELECT * FROM domain_state"))}
    for worker, records in groups.items():
        for record in records:
            assert record["status"] == "new"
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
    response = client.post("/v1/urls", json={"urls": [
        "https://example.com/a#one", "https://example.com/a#two",
    ]})
    assert response.status_code == 200
    assert response.json == {"status": "ok"}
    assert len(records_by_worker(tmp_path)["ingestor_02"]) == 1


@pytest.mark.parametrize("prefix", ["scheduler_ingest", "scheduler_injest"])
def test_flask_does_not_handle_gateway_prefix(setup_api, prefix):
    client, _, _, tmp_path = setup_api
    response = client.post(f"/{prefix}/v1/urls", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 404
    assert not list(tmp_path.rglob("*.jsonl"))


def test_existing_domain_is_reused_and_submissions_get_unique_files(setup_api):
    client, _, engine, tmp_path = setup_api
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO domain_state VALUES (123, 'example.com', 37, 0.9)"))
    results = [client.post("/v1/urls", json={"urls": ["https://example.com/a"]}) for _ in range(2)]
    assert all(result.status_code == 200 for result in results)
    assert all(result.json == {"status": "ok"} for result in results)
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
    response = client.post("/v1/urls", data=json.dumps(payload), content_type="application/json")
    assert response.status_code == 400
    assert "error" in response.json
    assert not list(tmp_path.rglob("*.jsonl"))
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM domain_state")) == 0


@pytest.mark.parametrize("data,content_type,status", [
    ("{", "application/json", 400),
    ('{"urls": []}', "text/plain", 415),
    ("x" * 9000, "application/json", 413),
])
def test_http_errors_are_json(setup_api, data, content_type, status):
    client, *_ = setup_api
    response = client.post("/v1/urls", data=data, content_type=content_type)
    assert response.status_code == status
    assert response.is_json


@pytest.mark.parametrize("error", [
    OperationalError("query", {}, Exception("secret connection string")),
    psycopg2.OperationalError("secret connection string"),
])
def test_db_failure_returns_503_without_publishing(setup_api, monkeypatch, error):
    client, _, _, tmp_path = setup_api
    monkeypatch.setattr(service_module, "load_sharding_config", Mock(
        side_effect=error,
    ))
    response = client.post("/v1/urls", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 503
    assert "secret" not in response.text
    assert not list(tmp_path.rglob("*.jsonl"))


def test_domain_failure_rolls_back_whole_transaction(setup_api, monkeypatch):
    client, _, engine, tmp_path = setup_api
    original = service_module.DomainResolver.ensure_and_get

    def fail_second(self, domain, shard_id):
        if domain == "example.org":
            raise OperationalError("query", {}, Exception("database unavailable"))
        return original(self, domain, shard_id)

    monkeypatch.setattr(service_module.DomainResolver, "ensure_and_get", fail_second)
    response = client.post("/v1/urls", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 503
    assert not list(tmp_path.rglob("*.jsonl"))
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM domain_state")) == 0


def test_partial_publication_is_reported_and_temporary_file_removed(setup_api, monkeypatch):
    client, _, _, tmp_path = setup_api
    original_replace = service_module.os.replace
    count = 0

    def fail_second(source, target):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError("disk full")
        original_replace(source, target)

    monkeypatch.setattr(service_module.os, "replace", fail_second)
    response = client.post("/v1/urls", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 503
    assert response.json["published_count"] == 1
    assert len(list(tmp_path.rglob("*.jsonl"))) == 1
    assert not list(tmp_path.rglob("*.tmp"))


def test_health_does_not_access_db(setup_api, monkeypatch):
    client, svc, *_ = setup_api
    monkeypatch.setattr(svc, "submit", Mock(side_effect=AssertionError("unexpected call")))
    assert client.get("/healthz").json == {"status": "ok"}

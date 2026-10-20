from __future__ import annotations

import json
import errno
import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
import psycopg2
import yaml

from containers.api.app import create_app
from containers.api import service as service_module
from containers.api import app as app_module
from containers.api.service import ManualBulkCrawlService
from libs.ipc.manual_bulk_crawl import next_batch


@pytest.fixture
def setup_api(tmp_path, monkeypatch):
    # No router or database configuration: API submission must not need either.
    config = {
        "max_urls": 3, "pending_dir": str(tmp_path / "pending"),
        "result_dir": str(tmp_path / "results"),
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"manual_bulk_crawl": config}))
    monkeypatch.setenv("API_CONFIG_PATH", str(path))
    svc = ManualBulkCrawlService(config, route_urls=lambda urls: [0] * len(urls))
    with TestClient(create_app(str(path), service=svc)) as client:
        yield client, svc, None, tmp_path


def test_submission_queues_urls_without_database_writes_or_ingestor_records(setup_api):
    client, _, _, root = setup_api
    response = client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/a#one", "https://example.com/a#two", "https://example.org/b",
    ]})
    assert response.status_code == 200
    assert len(response.json()["job_id"]) == 32
    batches = list((root / "pending").glob("*.json"))
    assert len(batches) == 1
    payload = json.loads(batches[0].read_text())
    assert payload["version"] == 1
    assert payload["max_concurrency_ratio"] == 0.5
    assert payload["result_dir"] == str(root / "results")
    assert payload["urls"] == ["https://example.com/a", "https://example.org/b"]
    assert payload["created_at"]
    assert not list(root.rglob("*.jsonl"))
    assert not list((root / "results").rglob("*.html"))


def test_default_app_uses_read_only_routing(setup_api, monkeypatch):
    _, _, _, root = setup_api
    monkeypatch.setattr(app_module, "ManualBulkCrawlRouter", lambda *args: lambda urls: [0] * len(urls))
    with TestClient(create_app(str(root / "config.yml"))) as client:
        assert client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]}).status_code == 200


def test_configured_concurrency_ratio_is_published_with_jobs(tmp_path):
    config = {"pending_dir": str(tmp_path / "pending"), "result_dir": str(tmp_path / "results"),
              "max_concurrency_ratio": 0.5}
    service = ManualBulkCrawlService(config, route_urls=lambda urls: [0] * len(urls))
    job_id = service.submit(["https://example.com/"])
    job = json.loads((service.pending_dir / f"{job_id}.json").read_text())
    assert job["max_concurrency_ratio"] == 0.5


def test_job_status_tracks_pending_running_and_completed_results(setup_api):
    client, svc, _, root = setup_api
    response = client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/a", "https://example.com/b",
    ]})
    job_id = response.json()["job_id"]
    endpoint = f"/v1/manual-bulk-crawls/{job_id}"
    status = client.get(endpoint).json()
    assert status["status"] == "pending"
    assert "result_dir" not in status
    assert status["total_urls"] == 2
    assert status["completed_urls"] == 0
    batch = next_batch(svc.pending_dir)
    try:
        assert client.get(endpoint).json()["status"] == "running"
        batch.save(0, final_url=batch.urls[0], status_code=200, body=b"<html>saved</html>")
        status = client.get(endpoint).json()
        assert status["completed_urls"] == status["succeeded_urls"] == status["html_files"] == 1
        assert "result_dir" not in status
        batch.save(1, final_url=batch.urls[1], status_code=None, error="TimeoutError")
        status = client.get(endpoint).json()
        assert status["status"] == "completed"
        assert status["completed_urls"] == 2
        assert status["succeeded_urls"] == status["failed_urls"] == 1
        assert status["created_at"] and status["started_at"] and status["completed_at"]
        assert "result_dir" not in status
        assert status["results_url"] == f"{endpoint}/results"
        assert (root / "results" / job_id / "crawler_00" / "00000.html").read_bytes() == b"<html>saved</html>"
    finally:
        batch.close()


@pytest.mark.parametrize("job_id", ["unknown", "a" * 32, "..%2Frequest"])
def test_unknown_or_invalid_job_returns_404(setup_api, job_id):
    client, *_ = setup_api
    assert client.get(f"/v1/manual-bulk-crawls/{job_id}").status_code == 404


def test_completion_is_not_reported_before_worker_progress_is_published(setup_api, monkeypatch):
    client, svc, *_ = setup_api
    job_id = client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]}).json()["job_id"]
    batch = next_batch(svc.pending_dir)
    from libs.ipc import manual_bulk_crawl as storage
    original = storage.os.replace
    def check_boundary(source, target):
        if target == batch.progress_path:
            status = client.get(f"/v1/manual-bulk-crawls/{job_id}").json()
            assert status["status"] == "running"
            assert "result_dir" not in status
        return original(source, target)
    monkeypatch.setattr(storage.os, "replace", check_boundary)
    try:
        batch.save(0, final_url=batch.urls[0], status_code=200, body=b"html")
        assert client.get(f"/v1/manual-bulk-crawls/{job_id}").json()["status"] == "completed"
    finally:
        batch.close()


def test_routes_are_defined_at_api_root(setup_api):
    client, *_ = setup_api
    paths = {route.path for route in client.app.routes}
    assert "/v1/manual-bulk-crawls" in paths
    assert all(not path.startswith("/scheduler_ingest/") for path in paths)


def test_old_generic_urls_route_is_not_exposed(setup_api):
    client, _, _, tmp_path = setup_api
    response = client.post("/v1/urls", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 404
    assert not list((tmp_path / "pending").glob("*.json"))




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
    response = client.post("/v1/manual-bulk-crawls", content=json.dumps(payload), headers={"Content-Type": "application/json"})
    assert response.status_code == 400
    assert "error" in response.json()
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list((tmp_path / "pending").glob("*.json"))


@pytest.mark.parametrize("data,content_type,status", [
    ("{", "application/json", 400),
    ('{"urls": []}', "text/plain", 415),
])
def test_http_errors_are_json(setup_api, data, content_type, status):
    client, *_ = setup_api
    response = client.post("/v1/manual-bulk-crawls", content=data, headers={"Content-Type": content_type})
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/json")






@pytest.mark.parametrize("failure", ["rename", "flush"])
def test_storage_failure_rejects_entire_multishard_batch(setup_api, monkeypatch, failure):
    client, _, _, tmp_path = setup_api
    operation = "replace" if failure == "rename" else "fsync"
    monkeypatch.setattr(service_module.os, operation, Mock(side_effect=OSError(errno.ENOSPC, "disk full")))
    response = client.post("/v1/manual-bulk-crawls", json={"urls": [
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
    response = client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]})
    assert response.status_code == 200
    assert publish.call_count == 1
    batches = list((tmp_path / "pending").glob("*.json"))
    assert len(batches) == 1
    assert len(json.loads(batches[0].read_text())["urls"]) == 2






@pytest.mark.parametrize("error", [
    PermissionError(errno.EACCES, "secret storage path"),
    RuntimeError("secret internal state"),
])
def test_unexpected_failure_returns_500_and_accepts_nothing(setup_api, monkeypatch, error):
    client, svc, _, tmp_path = setup_api
    monkeypatch.setattr(svc, "submit", Mock(side_effect=error))
    response = client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com/a"]})
    assert response.status_code == 500
    assert response.json() == {"error": "Submission failed; no URLs accepted"}
    assert not list((tmp_path / "pending").glob("*.json"))


def test_overlapping_api_requests_publish_complete_distinct_batches(setup_api):
    client, _, engine, tmp_path = setup_api
    start = Barrier(3)

    def submit(path):
        with TestClient(client.app) as concurrent_client:
            start.wait(timeout=5)
            return concurrent_client.post("/v1/manual-bulk-crawls", json={"urls": [
                f"https://example.com/{path}", f"https://example.org/{path}",
            ]}).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = [pool.submit(submit, path) for path in ("a", "b")]
        start.wait(timeout=5)
        assert [future.result(timeout=10) for future in responses] == [200, 200]
    batches = list((tmp_path / "pending").glob("*.json"))
    assert len(batches) == 2
    assert all(len(json.loads(batch.read_text())["urls"]) == 2 for batch in batches)


def test_partitions_are_invisible_until_manifest_publication(setup_api, monkeypatch):
    _, svc, _, _ = setup_api
    original = service_module.os.replace
    def check_boundary(source, target):
        if target.parent == svc.pending_dir:
            assert list((svc.pending_dir / "crawler_00").glob("*.json"))
            assert next_batch(svc.pending_dir) is None
        return original(source, target)
    monkeypatch.setattr(service_module.os, "replace", check_boundary)
    job_id = svc.submit(["https://example.com/a"])
    assert next_batch(svc.pending_dir).path.stem == job_id


def test_overlapping_submissions_stage_in_parallel(setup_api, monkeypatch):
    _, svc, _, _ = setup_api
    staged = Barrier(2)
    original = service_module.os.fsync

    def wait_for_both_submissions(descriptor):
        original(descriptor)
        staged.wait(timeout=5)

    monkeypatch.setattr(service_module.os, "fsync", wait_for_both_submissions)
    urls = ["https://example.com/a", "https://example.org/b"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(svc.submit, [url]) for url in urls]
        for future in futures:
            future.result(timeout=10)
    batches = list(svc.pending_dir.glob("*.json"))
    assert len(batches) == 2
    assert sorted(json.loads(batch.read_text())["urls"][0] for batch in batches) == urls
    assert not list(svc.pending_dir.glob("*.tmp"))




def test_health_does_not_access_db(setup_api, monkeypatch):
    client, svc, *_ = setup_api
    monkeypatch.setattr(svc, "submit", Mock(side_effect=AssertionError("unexpected call")))
    assert client.get("/healthz").json() == {"status": "ok"}


def test_health_remains_available_while_submission_waits(setup_api, monkeypatch):
    client, service, *_ = setup_api
    started, release = Event(), Event()
    original = service.submit

    def blocked_submit(*args, **kwargs):
        started.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(service, "submit", blocked_submit)
    with ThreadPoolExecutor(max_workers=2) as pool:
        submission = pool.submit(client.post, "/v1/manual-bulk-crawls",
                                 json={"urls": ["https://example.com"]})
        try:
            assert started.wait(2)
            health = pool.submit(client.get, "/healthz").result(timeout=2)
            assert health.status_code == 200
            assert health.json() == {"status": "ok"}
            assert not submission.done()
        finally:
            release.set()
        assert submission.result(timeout=5).status_code == 200



def test_old_service_prefix_is_not_exposed(setup_api):
    client, *_ = setup_api
    assert client.get("/scheduler_ingest/healthz").status_code == 404
    assert client.post("/scheduler_ingest/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]}).status_code == 404


def test_manual_bulk_crawl_limits_are_loaded_from_yaml(setup_api):
    _, svc, _, tmp_path = setup_api
    config_path = tmp_path / "config.yml"
    config = yaml.safe_load(config_path.read_text())
    config["manual_bulk_crawl"].update(max_urls=1, max_url_length=40)
    config_path.write_text(yaml.safe_dump(config))
    client = TestClient(create_app(str(tmp_path / "config.yml"), service=svc))
    assert client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/a", "https://example.org/b",
    ]}).status_code == 400
    assert client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/" + "a" * 30,
    ]}).status_code == 400
    assert not list((tmp_path / "pending").glob("*.json"))
    assert client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com/a"]}).status_code == 200


@pytest.mark.parametrize("name,value", [
    ("max_urls", 0),
    ("max_url_length", 0),
    ("max_url_length", 2501),
    ("max_concurrency_ratio", 0),
    ("max_concurrency_ratio", -0.1),
    ("max_concurrency_ratio", 1.1),
    ("max_concurrency_ratio", True),
    ("max_concurrency_ratio", "0.8"),
    ("max_concurrency_ratio", None),
    ("max_concurrency_ratio", float("nan")),
    ("max_concurrency_ratio", float("inf")),
])
def test_invalid_limit_configuration_fails_at_startup(setup_api, name, value):
    _, svc, _, tmp_path = setup_api
    config_path = tmp_path / "config.yml"
    config = yaml.safe_load(config_path.read_text())
    config["manual_bulk_crawl"][name] = value
    config_path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match=f"manual_bulk_crawl.{name}"):
        create_app(str(tmp_path / "config.yml"), service=svc)


@pytest.mark.parametrize("streamed", [False, True])
def test_api_accepts_bodies_larger_than_4_mib(setup_api, streamed):
    client, _, _, tmp_path = setup_api
    chunks = [b" " * (4 * 1024 * 1024), b'{"urls":["https://example.com/a"]}']
    content = b"".join(chunks)
    if streamed:
        options = {"content": iter(chunks)}
    else:
        options = {"content": content}
    response = client.post("/v1/manual-bulk-crawls", **options,
                           headers={"Content-Type": "application/json"})
    assert response.status_code == 200
    assert len(list((tmp_path / "pending").glob("*.json"))) == 1


@pytest.mark.parametrize("size", [0, -1, True, 1.5, "300m", None])
def test_invalid_body_limit_fails_at_startup(setup_api, size):
    _, service, _, root = setup_api
    path = root / "config.yml"
    config = yaml.safe_load(path.read_text())
    config["max_content_length"] = size
    path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match="max_content_length"):
        create_app(str(path), service=service)


@pytest.mark.parametrize("streamed", [False, True])
def test_oversized_body_returns_json_and_publishes_nothing(setup_api, streamed):
    _, service, _, root = setup_api
    path = root / "config.yml"
    config = yaml.safe_load(path.read_text())
    config["max_content_length"] = 32
    path.write_text(yaml.safe_dump(config))
    with TestClient(create_app(str(path), service=service)) as client:
        content = b'{"urls":["https://example.com/article"]}'
        response = client.post("/v1/manual-bulk-crawls",
                               content=iter([content[:20], content[20:]]) if streamed else content,
                               headers={"Content-Type": "application/json"})
    assert response.status_code == 413
    assert response.json() == {"error": "Request body too large"}
    assert not list((root / "pending").glob("*.json"))


def test_openapi_describes_submission_and_ttl(setup_api):
    client, *_ = setup_api
    schema = client.get("/openapi.json").json()
    request = schema["components"]["schemas"]["ManualBulkCrawlRequest"]
    assert request["required"] == ["urls"]
    assert request["additionalProperties"] is False
    assert request["properties"]["ttl_days"]["default"] == 14
    assert request["properties"]["ttl_days"]["minimum"] == 1
    assert request["properties"]["ttl_days"]["maximum"] == 60
    assert client.get("/docs").status_code == 200


def test_body_limit_stops_reading_chunked_upload(setup_api):
    _, service, _, root = setup_api
    path = root / "config.yml"
    config = yaml.safe_load(path.read_text())
    config["max_content_length"] = 32
    path.write_text(yaml.safe_dump(config))
    app = create_app(str(path), service=service)
    chunks = iter([b" " * 32, b" " * 32, b'{"urls":["https://example.com"]}'])
    received = []
    sent = []

    async def receive():
        chunk = next(chunks)
        received.append(chunk)
        return {"type": "http.request", "body": chunk, "more_body": True}

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
             "method": "POST", "scheme": "http", "path": "/v1/manual-bulk-crawls",
             "query_string": b"", "root_path": "",
             "headers": [(b"content-type", b"application/json")],
             "server": ("testserver", 80), "client": ("127.0.0.1", 1234)}
    asyncio.run(app(scope, receive, send))
    assert sent[0]["status"] == 413
    assert json.loads(sent[1]["body"]) == {"error": "Request body too large"}
    assert len(received) == 2
    assert not list((root / "pending").glob("*.json"))


def test_default_body_limit_preserves_300_mib(setup_api):
    client, *_ = setup_api
    assert client.app.state.max_content_length == 300 * 1024 * 1024



def test_app_routes_using_its_own_config(setup_api, monkeypatch):
    _, _, _, root = setup_api
    path = root / "config.yml"
    config = {"manual_bulk_crawl": yaml.safe_load(path.read_text())["manual_bulk_crawl"],
              "postgres": {"dsn": "postgresql://api-only"}}
    path.write_text(yaml.safe_dump(config))
    router_factory = Mock(return_value=lambda urls: [2] * len(urls))
    monkeypatch.setattr(app_module, "ManualBulkCrawlRouter", router_factory)
    with TestClient(create_app(str(path))) as client:
        response = client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]})
    assert response.status_code == 200
    router_factory.assert_called_once_with(config)
    job = json.loads(next((root / "pending").glob("*.json")).read_text())
    assert job["crawler_ids"] == [2]

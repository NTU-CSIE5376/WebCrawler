from __future__ import annotations

import json
import errno
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest.mock import Mock

import pytest
import psycopg2
import yaml

from containers.api.app import create_app
from containers.api import service as service_module
from containers.api import app as app_module
from containers.api.service import ManualBulkCrawlService
from libs.ipc.manual_bulk_crawl import queue_lock
from libs.ipc.manual_bulk_crawl import claim_next


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
    with create_app(str(path), service=svc).test_client() as client:
        yield client, svc, None, tmp_path


def test_submission_queues_urls_without_database_writes_or_ingestor_records(setup_api):
    client, _, _, root = setup_api
    response = client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/a#one", "https://example.com/a#two", "https://example.org/b",
    ]})
    assert response.status_code == 200
    assert len(response.get_json()["job_id"]) == 32
    batches = list((root / "pending").glob("*.json"))
    assert len(batches) == 1
    payload = json.loads(batches[0].read_text())
    assert payload["version"] == 1
    assert payload["result_dir"] == str(root / "results")
    assert payload["urls"] == ["https://example.com/a", "https://example.org/b"]
    assert payload["created_at"]
    assert not list(root.rglob("*.jsonl"))
    assert not (root / "results").exists()


def test_default_app_uses_read_only_routing(setup_api, monkeypatch):
    _, _, _, root = setup_api
    monkeypatch.setattr(app_module, "ManualBulkCrawlRouter", lambda *args: lambda urls: [0] * len(urls))
    with create_app(str(root / "config.yml")).test_client() as client:
        assert client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]}).status_code == 200


def test_job_status_tracks_pending_running_and_completed_results(setup_api):
    client, svc, _, root = setup_api
    response = client.post("/v1/manual-bulk-crawls", json={"urls": [
        "https://example.com/a", "https://example.com/b",
    ]})
    job_id = response.get_json()["job_id"]
    endpoint = f"/v1/manual-bulk-crawls/{job_id}"
    status = client.get(endpoint).get_json()
    assert status["status"] == "pending"
    assert "result_dir" not in status
    assert status["total_urls"] == 2
    assert status["completed_urls"] == 0
    batch = claim_next(svc.pending_dir)
    try:
        assert client.get(endpoint).get_json()["status"] == "running"
        batch.save(0, final_url=batch.urls[0], status_code=200, body=b"<html>saved</html>")
        status = client.get(endpoint).get_json()
        assert status["completed_urls"] == status["succeeded_urls"] == status["html_files"] == 1
        assert "result_dir" not in status
        batch.save(1, final_url=batch.urls[1], status_code=None, error="TimeoutError")
        status = client.get(endpoint).get_json()
        assert status["status"] == "completed"
        assert status["completed_urls"] == 2
        assert status["succeeded_urls"] == status["failed_urls"] == 1
        assert status["created_at"] and status["started_at"] and status["completed_at"]
        assert "result_dir" not in status
        assert status["results_url"] == f"{endpoint}/results"
        assert (root / "results" / job_id / "00000.html").read_bytes() == b"<html>saved</html>"
    finally:
        batch.close()


@pytest.mark.parametrize("job_id", ["unknown", "a" * 32, "..%2Frequest"])
def test_unknown_or_invalid_job_returns_404(setup_api, job_id):
    client, *_ = setup_api
    assert client.get(f"/v1/manual-bulk-crawls/{job_id}").status_code == 404


def test_completion_is_not_reported_before_result_directory_is_published(setup_api, monkeypatch):
    client, svc, *_ = setup_api
    job_id = client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]}).get_json()["job_id"]
    batch = claim_next(svc.pending_dir)
    from libs.ipc import manual_bulk_crawl as storage
    original = storage.os.replace
    def check_boundary(source, target):
        if target == batch.target:
            status = client.get(f"/v1/manual-bulk-crawls/{job_id}").get_json()
            assert status["status"] == "running"
            assert "result_dir" not in status
        return original(source, target)
    monkeypatch.setattr(storage.os, "replace", check_boundary)
    try:
        batch.save(0, final_url=batch.urls[0], status_code=200, body=b"html")
        assert client.get(f"/v1/manual-bulk-crawls/{job_id}").get_json()["status"] == "completed"
    finally:
        batch.close()


def test_routes_are_defined_at_api_root(setup_api):
    client, *_ = setup_api
    paths = {route.rule for route in client.application.url_map.iter_rules()}
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
    response = client.post("/v1/manual-bulk-crawls", data=json.dumps(payload), headers={"Content-Type": "application/json"})
    assert response.status_code == 400
    assert "error" in response.get_json()
    assert not list(tmp_path.rglob("*.jsonl"))
    assert not list((tmp_path / "pending").glob("*.json"))


@pytest.mark.parametrize("data,content_type,status", [
    ("{", "application/json", 400),
    ('{"urls": []}', "text/plain", 415),
])
def test_http_errors_are_json(setup_api, data, content_type, status):
    client, *_ = setup_api
    response = client.post("/v1/manual-bulk-crawls", data=data, headers={"Content-Type": content_type})
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
    assert response.get_json() == {"error": "Submission unavailable; no URLs accepted"}
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
    assert response.get_json() == {"error": "Submission failed; no URLs accepted"}
    assert not list((tmp_path / "pending").glob("*.json"))


def test_overlapping_api_requests_publish_complete_distinct_batches(setup_api):
    client, _, engine, tmp_path = setup_api
    start = Barrier(3)

    def submit(path):
        with client.application.test_client() as concurrent_client:
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


def test_submission_prepares_while_publication_lock_is_held(setup_api, monkeypatch):
    _, svc, engine, tmp_path = setup_api
    staged = Event()
    original = service_module.os.fsync

    def signal_staged(descriptor):
        original(descriptor)
        staged.set()

    monkeypatch.setattr(service_module.os, "fsync", signal_staged)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with queue_lock(svc.pending_dir):
            future = pool.submit(svc.submit, ["https://example.com/a"])
            assert staged.wait(timeout=5), "Preparation must not wait for the queue lock"
            assert not future.done()
            assert not list(svc.pending_dir.glob("*.json"))
            temporary = next(svc.pending_dir.glob("*.json.tmp"))
            assert len(json.loads(temporary.read_text())["urls"]) == 1
        future.result(timeout=5)
    assert len(list(svc.pending_dir.glob("*.json"))) == 1
    assert not list(svc.pending_dir.glob("*.tmp"))


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
    assert client.get("/healthz").get_json() == {"status": "ok"}



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
    client = create_app(str(tmp_path / "config.yml"), service=svc).test_client()
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
        from io import BytesIO
        options = {"input_stream": BytesIO(content), "content_length": len(content)}
    else:
        options = {"data": content}
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


def test_oversized_body_returns_json_and_publishes_nothing(setup_api):
    _, service, _, root = setup_api
    path = root / "config.yml"
    config = yaml.safe_load(path.read_text())
    config["max_content_length"] = 32
    path.write_text(yaml.safe_dump(config))
    with create_app(str(path), service=service).test_client() as client:
        response = client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com/article"]})
    assert response.status_code == 413
    assert response.get_json() == {"error": "Request body too large"}
    assert not list((root / "pending").glob("*.json"))


def test_default_body_limit_preserves_300_mib(setup_api):
    client, *_ = setup_api
    assert client.application.config["MAX_CONTENT_LENGTH"] == 300 * 1024 * 1024



def test_app_routes_using_its_own_config(setup_api, monkeypatch):
    _, _, _, root = setup_api
    path = root / "config.yml"
    config = {"manual_bulk_crawl": yaml.safe_load(path.read_text())["manual_bulk_crawl"],
              "postgres": {"dsn": "postgresql://api-only"}}
    path.write_text(yaml.safe_dump(config))
    router_factory = Mock(return_value=lambda urls: [2] * len(urls))
    monkeypatch.setattr(app_module, "ManualBulkCrawlRouter", router_factory)
    with create_app(str(path)).test_client() as client:
        response = client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]})
    assert response.status_code == 200
    router_factory.assert_called_once_with(config)
    job = json.loads(next((root / "pending").glob("*.json")).read_text())
    assert job["crawler_ids"] == [2]

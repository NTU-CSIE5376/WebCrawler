"""Remote clients can discover and download completed output through HTTP."""
import json
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
import yaml

from containers.api.app import create_app
from containers.api.service import ManualBulkCrawlService
from libs.ipc.manual_bulk_crawl import claim_next


@pytest.fixture
def job_api(tmp_path, monkeypatch):
    config = {
        "pending_dir": str(tmp_path / "pending"), "result_dir": str(tmp_path / "results"),
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"manual_bulk_crawl": config}))
    monkeypatch.setenv("API_CONFIG_PATH", str(path))
    service = ManualBulkCrawlService(config, route_urls=lambda urls: [0] * len(urls))
    with TestClient(create_app(str(path), service=service)) as client:
        yield client, service


def complete_job(service):
    job_id = service.submit(["https://example.com/a", "https://example.com/b"])
    batch = claim_next(service.pending_dir)
    try:
        batch.save(0, final_url=batch.urls[0], status_code=200,
                   body=b"<html><script>alert('saved content')</script></html>")
        batch.save(1, final_url=batch.urls[1], status_code=None, error="TimeoutError")
    finally:
        batch.close()
    return job_id


def api_get(client, url, **kwargs):
    assert url.startswith("/v1/manual-bulk-crawls/")
    return client.get(url, **kwargs)


def test_remote_client_follows_status_listing_and_download_links(job_api):
    client, service = job_api
    job_id = complete_job(service)
    status = client.get(f"/v1/manual-bulk-crawls/{job_id}").json()
    listing = api_get(client, status["results_url"])
    assert listing.status_code == 200
    files = {file["name"]: file for file in listing.json()["files"]}
    assert set(files) == {"request.json", "status.json", "00000.html", "00000.json", "00001.json"}
    assert files["00000.html"]["url"] == "https://example.com/a"
    assert files["00000.json"]["url"] == "https://example.com/a"
    assert files["00001.json"]["url"] == "https://example.com/b"
    assert files["request.json"]["url"] is None
    assert files["status.json"]["url"] is None
    html = api_get(client, files["00000.html"]["download_url"])
    assert html.content == (service.result_dir / job_id / "00000.html").read_bytes()
    assert files["00000.html"]["size_bytes"] == len(html.content)
    assert html.headers["content-disposition"] == 'attachment; filename="00000.html"'
    assert html.headers["content-type"] == "application/octet-stream"
    assert html.headers["x-content-type-options"] == "nosniff"
    metadata = api_get(client, files["00001.json"]["download_url"])
    assert json.loads(metadata.content)["error"] == "TimeoutError"
    assert client.get(f"/v1/manual-bulk-crawls/{job_id}/results/00001.html").status_code == 404


def test_download_supports_http_range_requests(job_api):
    client, service = job_api
    job_id = complete_job(service)
    response = client.get(f"/v1/manual-bulk-crawls/{job_id}/results/00000.html",
                          headers={"Range": "bytes=0-5"})
    assert response.status_code == 206
    assert response.content == b"<html>"
    assert response.headers["content-range"].startswith("bytes 0-5/")


def test_pending_and_running_jobs_do_not_expose_partial_files(job_api):
    client, service = job_api
    job_id = service.submit(["https://example.com/a", "https://example.com/b"])
    endpoint = f"/v1/manual-bulk-crawls/{job_id}/results"
    assert client.get(f"/v1/manual-bulk-crawls/{job_id}").json()["results_url"] is None
    assert client.get(endpoint).status_code == 409
    batch = claim_next(service.pending_dir)
    try:
        batch.save(0, final_url=batch.urls[0], status_code=200, body=b"partial")
        assert client.get(endpoint + "/00000.html").status_code == 409
        assert client.get(f"/v1/manual-bulk-crawls/{job_id}").json()["results_url"] is None
    finally:
        batch.close()


@pytest.mark.parametrize("filename", ["worker_00.json", "status.json.tmp", "secrets.txt", "%2E%2E%2Frequest.json"])
def test_internal_files_and_path_traversal_are_not_exposed(job_api, filename):
    client, service = job_api
    job_id = complete_job(service)
    response = client.get(f"/v1/manual-bulk-crawls/{job_id}/results/{filename}")
    assert response.status_code == 404


def test_symlinks_are_not_listed_or_downloaded(job_api, tmp_path):
    client, service = job_api
    job_id = complete_job(service)
    secret = tmp_path / "secret"
    secret.write_text("outside result storage")
    (service.result_dir / job_id / "99999.html").symlink_to(secret)
    listing = client.get(f"/v1/manual-bulk-crawls/{job_id}/results").json()
    assert "99999.html" not in [file["name"] for file in listing["files"]]
    assert client.get(f"/v1/manual-bulk-crawls/{job_id}/results/99999.html").status_code == 404


def test_unknown_jobs_and_unavailable_storage_return_json(job_api, monkeypatch):
    client, service = job_api
    assert client.get(f"/v1/manual-bulk-crawls/{'a' * 32}/results").status_code == 404
    monkeypatch.setattr(service, "result_files", Mock(side_effect=PermissionError("secret path")))
    response = client.get(f"/v1/manual-bulk-crawls/{'a' * 32}/results")
    assert response.status_code == 503
    assert response.json() == {"error": "Job results unavailable"}

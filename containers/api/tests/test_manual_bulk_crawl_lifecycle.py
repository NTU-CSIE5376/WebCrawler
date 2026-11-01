"""Retention and deletion must work while crawler workers own jobs."""
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest
from fastapi.testclient import TestClient
import yaml
from click.testing import CliRunner

from containers.api.app import create_app
from containers.api.manual_bulk_crawl_gc import main
from containers.api.service import ManualBulkCrawlService
from libs.ipc import manual_bulk_crawl as storage


@pytest.fixture
def lifecycle(tmp_path):
    config = {"pending_dir": str(tmp_path / "pending"), "result_dir": str(tmp_path / "results")}
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"manual_bulk_crawl": config}))
    service = ManualBulkCrawlService(config, route_urls=lambda urls: [0] * len(urls))
    return TestClient(create_app(str(path), service=service)), service, path


def submit(client, **options):
    return client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com/a"], **options})


@pytest.mark.parametrize("ttl", [None, 1, 60])
def test_retention_is_measured_from_submission(lifecycle, ttl):
    client, service, _ = lifecycle
    job_id = submit(client, **({} if ttl is None else {"ttl_days": ttl})).json()["job_id"]
    status = client.get(f"/v1/manual-bulk-crawls/{job_id}").json()
    assert status["ttl_days"] == (14 if ttl is None else ttl)
    assert datetime.fromisoformat(status["expires_at"]) - datetime.fromisoformat(status["created_at"]) == timedelta(days=status["ttl_days"])
    batch = storage.next_batch(service.pending_dir)
    try:
        batch.save(0, final_url=batch.urls[0], status_code=200, body=b"html")
        completed = client.get(f"/v1/manual-bulk-crawls/{job_id}").json()
        assert completed["expires_at"] == status["expires_at"]
    finally:
        batch.close()


@pytest.mark.parametrize("ttl", [0, -1, 61, True, False, "14", 14.0, None, [], {}])
def test_invalid_ttl_is_rejected_before_routing_or_writing(lifecycle, ttl):
    client, service, _ = lifecycle
    service.route_urls = lambda _: pytest.fail("invalid request must not route")
    assert submit(client, ttl_days=ttl).status_code == 400
    assert not service.pending_dir.exists()


@pytest.mark.parametrize("state", ["pending", "running", "completed"])
def test_delete_removes_all_job_data_and_late_responses_cannot_recreate_it(lifecycle, state):
    client, service, _ = lifecycle
    job_id = submit(client).json()["job_id"]
    endpoint = f"/v1/manual-bulk-crawls/{job_id}"
    batch = storage.next_batch(service.pending_dir) if state != "pending" else None
    try:
        if state == "completed":
            batch.save(0, final_url=batch.urls[0], status_code=200, body=b"html")
        assert client.delete(endpoint).status_code == 204
        assert client.get(endpoint).status_code == 404
        assert client.get(endpoint + "/results").status_code == 404
        assert client.delete(endpoint).status_code == 404
        assert storage.next_batch(service.pending_dir) is None
        if state == "running":
            with pytest.raises(storage.JobUnavailable):
                batch.save(0, final_url=batch.urls[0], status_code=200, body=b"late")
        assert not (service.result_dir / job_id).exists()
        assert not (service.result_dir / job_id).exists()
    finally:
        if batch:
            batch.close()


def test_expiration_hides_results_and_gc_preserves_live_jobs(lifecycle, monkeypatch):
    client, service, _ = lifecycle
    short = submit(client, ttl_days=1).json()["job_id"]
    long = submit(client, ttl_days=60).json()["job_id"]
    batch = storage.next_batch(service.pending_dir)
    # Complete both jobs to test completed expiration.
    while batch:
        try:
            batch.save(0, final_url=batch.urls[0], status_code=200, body=b"html")
        finally:
            batch.close()
        batch = storage.next_batch(service.pending_dir)
    deadline = datetime.fromisoformat(service.status(short)["expires_at"])

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return deadline

    monkeypatch.setattr(storage, "datetime", Clock)
    for suffix in ("", "/results", "/results/00000.html"):
        assert client.get(f"/v1/manual-bulk-crawls/{short}{suffix}").status_code == 404
    assert service.collect_expired(now=deadline - timedelta(microseconds=1)) == 0
    assert service.collect_expired(now=deadline) == 1
    assert service.collect_expired(now=deadline) == 0
    assert client.get(f"/v1/manual-bulk-crawls/{long}").status_code == 200
    assert not (service.result_dir / short).exists()


@pytest.mark.parametrize("active", [False, True])
def test_gc_expires_unfinished_jobs_and_blocks_late_writes(lifecycle, monkeypatch, active):
    client, service, _ = lifecycle
    job_id = submit(client, ttl_days=1).json()["job_id"]
    deadline = datetime.fromisoformat(service.status(job_id)["expires_at"])
    batch = storage.next_batch(service.pending_dir) if active else None

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return deadline

    monkeypatch.setattr(storage, "datetime", Clock)
    try:
        assert storage.next_batch(service.pending_dir) is None
        if batch:
            with pytest.raises(storage.JobUnavailable):
                batch.save(0, final_url=batch.urls[0], status_code=200, body=b"expired")
        assert service.collect_expired(now=deadline) == 1
        assert not (service.result_dir / job_id).exists()
    finally:
        if batch:
            batch.close()


def test_delete_does_not_wait_for_result_writes_and_blocks_late_publication(lifecycle, monkeypatch):
    client, service, _ = lifecycle
    job_id = submit(client).json()["job_id"]
    batch = storage.next_batch(service.pending_dir)
    writing, release, deleting = Event(), Event(), Event()
    original = storage.write_file

    def paused_write(path, content):
        if path.suffix == ".html":
            writing.set()
            assert release.wait(5)
        return original(path, content)

    def delete():
        deleting.set()
        service.delete(job_id)

    monkeypatch.setattr(storage, "write_file", paused_write)
    try:
        with ThreadPoolExecutor(2) as pool:
            saving = pool.submit(batch.save, 0, final_url=batch.urls[0], status_code=200, body=b"html")
            assert writing.wait(5)
            deletion = pool.submit(delete)
            assert deleting.wait(5)
            deletion.result(timeout=5)
            release.set()
            with pytest.raises(storage.JobUnavailable):
                saving.result(timeout=5)
        assert client.get(f"/v1/manual-bulk-crawls/{job_id}").status_code == 404
        assert not batch.directory.exists()
    finally:
        release.set()
        batch.close()


def test_gc_cli_collects_legacy_jobs_with_default_retention(lifecycle):
    client, service, path = lifecycle
    job_id = submit(client).json()["job_id"]
    pending = service.pending_dir / f"{job_id}.json"
    payload = json.loads(pending.read_text())
    payload.pop("ttl_days")
    payload.pop("expires_at")
    payload["created_at"] = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
    pending.write_text(json.dumps(payload))
    result = CliRunner().invoke(main, ["--config", str(path), "--once"])
    assert result.exit_code == 0, result.output
    assert not pending.exists()


def test_invalid_delete_path_cannot_remove_other_directories(lifecycle):
    client, service, _ = lifecycle
    service.result_dir.mkdir()
    sentinel = service.result_dir / "keep"
    sentinel.write_text("keep")
    assert client.delete("/v1/manual-bulk-crawls/keep").status_code == 404
    assert sentinel.read_text() == "keep"

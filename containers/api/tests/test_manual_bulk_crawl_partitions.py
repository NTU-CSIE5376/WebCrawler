"""Worker-owned partitions need no locks, including during deletion and GC."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
from threading import Event

import pytest

from containers.api.service import JobNotCompleted, ManualBulkCrawlService
from libs.ipc import manual_bulk_crawl as storage


@pytest.fixture
def service(tmp_path):
    return ManualBulkCrawlService({"pending_dir": str(tmp_path / "pending"),
                                   "result_dir": str(tmp_path / "results")},
                                  route_urls=lambda urls: list(range(len(urls))))


def test_submit_workers_status_and_delete_never_acquire_file_locks(service, monkeypatch):
    def forbidden(*args):
        pytest.fail("Manual bulk crawl must not acquire flock")
    monkeypatch.setattr(fcntl, "flock", forbidden)
    job_id = service.submit(["https://example.com/a", "https://example.org/b"])
    first = storage.next_batch(service.pending_dir, 0)
    second = storage.next_batch(service.pending_dir, 1)
    first.save(0, final_url=first.urls[0], status_code=200, body=b"first")
    assert service.status(job_id)["status"] == "running"
    with pytest.raises(JobNotCompleted):
        service.result_files(job_id)
    second.save(1, final_url=second.urls[1], status_code=200, body=b"second")
    assert service.status(job_id)["status"] == "completed"
    assert not (service.result_dir / job_id / "status.json").exists()
    files = {item["name"]: item for item in service.result_files(job_id)}
    assert files["00000.html"]["url"] == first.urls[0]
    assert files["00001.html"]["url"] == second.urls[1]
    service.delete(job_id)
    assert not list(service.pending_dir.rglob("*.json"))
    assert not (service.result_dir / job_id).exists()


def test_slow_worker_write_does_not_block_another_partition(service, monkeypatch):
    job_id = service.submit(["https://example.com/a", "https://example.org/b"])
    first = storage.next_batch(service.pending_dir, 0)
    second = storage.next_batch(service.pending_dir, 1)
    writing, release = Event(), Event()
    original = storage.write_file
    def slow_write(path, content):
        if path == first.directory / "00000.html":
            writing.set()
            assert release.wait(5)
        return original(path, content)
    monkeypatch.setattr(storage, "write_file", slow_write)
    try:
        with ThreadPoolExecutor(2) as pool:
            slow = pool.submit(first.save, 0, final_url=first.urls[0], status_code=200, body=b"slow")
            assert writing.wait(5)
            fast = pool.submit(second.save, 1, final_url=second.urls[1], status_code=200, body=b"fast")
            fast.result(timeout=2)
            status = service.status(job_id)
            assert status["completed_urls"] == 1
            assert status["status"] == "running"
            release.set()
            slow.result(timeout=5)
        assert service.status(job_id)["status"] == "completed"
    finally:
        release.set()


def test_delete_while_html_descriptor_is_open_cannot_recreate_results(service, monkeypatch):
    job_id = service.submit(["https://example.com/a"])
    batch = storage.next_batch(service.pending_dir)
    opened, release = Event(), Event()
    original = Path.open
    @contextmanager
    def paused_open(path, *args, **kwargs):
        with original(path, *args, **kwargs) as handle:
            if path == batch.directory / "00000.html":
                opened.set()
                assert release.wait(5)
            yield handle
    monkeypatch.setattr(Path, "open", paused_open)
    try:
        with ThreadPoolExecutor(1) as pool:
            saving = pool.submit(batch.save, 0, final_url=batch.urls[0], status_code=200, body=b"late")
            assert opened.wait(5)
            service.delete(job_id)
            assert not batch.directory.exists()
            release.set()
            with pytest.raises(storage.JobUnavailable):
                saving.result(timeout=5)
        with pytest.raises(FileNotFoundError):
            service.status(job_id)
        assert not (service.result_dir / job_id).exists()
    finally:
        release.set()


def test_gc_retries_cleanup_after_job_is_already_hidden(service, monkeypatch):
    job_id = service.submit(["https://example.com/a"])
    with monkeypatch.context() as patch:
        patch.setattr("containers.api.service.shutil.rmtree", lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError("busy")))
        with pytest.raises(PermissionError):
            service.delete(job_id)
    with pytest.raises(FileNotFoundError):
        service.status(job_id)
    assert storage.next_batch(service.pending_dir) is None
    assert (service.pending_dir / ".deleted" / f"{job_id}.json").exists()
    service.collect_expired()
    assert not list((service.pending_dir / ".deleted").glob("*.json"))
    assert not (service.result_dir / ".deleted" / job_id).exists()


def test_restart_cannot_change_other_workers_progress(service):
    job_id = service.submit(["https://example.com/a", "https://example.org/b"])
    first = storage.next_batch(service.pending_dir, 0)
    second = storage.next_batch(service.pending_dir, 1)
    second.save(1, final_url=second.urls[1], status_code=200, body=b"second")
    original = second.progress_path.read_bytes()
    first.close()
    resumed = storage.next_batch(service.pending_dir, 0)
    assert resumed.remaining == {0}
    assert second.progress_path.read_bytes() == original
    resumed.save(0, final_url=resumed.urls[0], status_code=200, body=b"first")
    assert service.status(job_id)["completed_urls"] == 2
    assert second.progress_path.read_bytes() == original


def test_completed_status_download_matches_listing_size(service):
    job_id = service.submit(["https://example.com/a"])
    batch = storage.next_batch(service.pending_dir)
    batch.save(0, final_url=batch.urls[0], status_code=200, body=b"html")
    entry = next(item for item in service.result_files(job_id) if item["name"] == "status.json")
    path = service.result_file(job_id, "status.json")
    assert entry["size_bytes"] == path.stat().st_size
    assert json.loads(path.read_bytes())["status"] == "completed"


def test_interrupted_submission_is_not_consumed_and_gc_removes_preparation(service):
    from datetime import datetime, timedelta, timezone
    job_id = "a" * 32
    manifest = service.pending_dir / f"{job_id}.json"
    service.pending_dir.mkdir()
    payload = {"version": 1, "urls": ["https://example.com/a"], "crawler_ids": [0],
               "result_dir": str(service.result_dir),
               "created_at": (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()}
    storage.prepare_job(manifest, payload)
    assert storage.next_batch(service.pending_dir) is None
    with pytest.raises(FileNotFoundError):
        service.status(job_id)
    assert service.collect_expired() == 1
    assert not (service.result_dir / job_id).exists()
    assert not (service.pending_dir / "crawler_00" / manifest.name).exists()


def test_failed_retry_removes_uncommitted_html_from_an_interrupted_attempt(service):
    job_id = service.submit(["https://example.com/a"])
    batch = storage.next_batch(service.pending_dir)
    (batch.directory / "00000.html").write_bytes(b"uncommitted old HTML")
    batch.close()
    resumed = storage.next_batch(service.pending_dir)
    resumed.save(0, final_url=resumed.urls[0], status_code=None, error="TimeoutError")
    assert service.status(job_id)["html_files"] == 0
    assert "00000.html" not in {file["name"] for file in service.result_files(job_id)}

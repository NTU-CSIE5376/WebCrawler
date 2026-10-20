import json
import os
from pathlib import Path
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from threading import Lock
import time

import pytest

from libs.ipc import manual_bulk_crawl as storage
from libs.ipc.manual_bulk_crawl import next_batch, prepare_job, job_status


def enqueue(root, name="batch", urls=None, crawler_ids=None):
    pending = root / "pending"
    pending.mkdir(exist_ok=True)
    path = pending / f"{name}.json"
    urls = urls or ["https://example.com/a"]
    path.write_text(json.dumps({"version": 1, "urls": urls, "crawler_ids": crawler_ids or [0] * len(urls),
                                "result_dir": str(root / "results")}))
    prepare_job(path, json.loads(path.read_text()))
    return path


def test_restart_resumes_only_unfinished_urls(tmp_path):
    path = enqueue(tmp_path, urls=["https://example.com/a", "https://example.com/b"])
    batch = next_batch(path.parent)
    batch.save(0, final_url=batch.urls[0], status_code=200, body=b"saved")
    batch.close()
    retry = next_batch(path.parent)
    assert retry.remaining == {1}
    assert retry.progress["started_at"] == batch.progress["started_at"]
    retry.save(1, final_url=retry.urls[1], status_code=None, error="TimeoutError")
    assert next_batch(path.parent) is None
    assert path.exists()  # Only API/GC removes the job manifest.
    assert (retry.directory / "00000.html").read_bytes() == b"saved"
    assert not (retry.directory / "00001.html").exists()
    status = job_status(path.stem, retry.payload, tmp_path / "results" / path.stem)
    assert status["status"] == "completed"
    assert status["succeeded_urls"] == status["failed_urls"] == 1


def test_storage_failure_retries_without_losing_url_records(tmp_path, monkeypatch):
    path = enqueue(tmp_path)
    batch = next_batch(path.parent)
    with monkeypatch.context() as patch:
        patch.setattr(storage.os, "replace", lambda *args: (_ for _ in ()).throw(OSError("disk full")))
        with pytest.raises(OSError):
            batch.save(0, final_url=batch.urls[0], status_code=200, body=b"old")
    retry = next_batch(path.parent)
    assert retry.remaining == {0}
    retry.save(0, final_url=retry.urls[0], status_code=200, body=b"new")
    assert (retry.directory / "00000.html").read_bytes() == b"new"


def test_restart_recovers_metadata_saved_before_progress_publication(tmp_path, monkeypatch):
    path = enqueue(tmp_path)
    batch = next_batch(path.parent)
    original = storage.os.replace
    def fail_progress(source, target):
        if target == batch.progress_path:
            raise OSError("interrupted")
        return original(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(storage.os, "replace", fail_progress)
        with pytest.raises(OSError):
            batch.save(0, final_url=batch.urls[0], status_code=200, body=b"saved")
    assert next_batch(path.parent) is None
    assert (batch.directory / "00000.html").read_bytes() == b"saved"
    assert json.loads(batch.progress_path.read_text())["completed_at"]


def test_temporary_files_are_never_claimed(tmp_path):
    pending = tmp_path / "pending"
    pending.mkdir()
    (pending / "incomplete.json.tmp").write_text("incomplete")
    assert next_batch(pending) is None


@pytest.mark.parametrize("legacy", [False, True])
def test_jobs_are_selected_fifo_regardless_of_uuid_order(tmp_path, legacy):
    for name, timestamp in (("f" * 32, 4070908800), ("a" * 32, 4070908801)):
        path = enqueue(tmp_path, name=name)
        if legacy:
            os.utime(path, (timestamp, timestamp))
        else:
            payload = json.loads(path.read_text())
            payload["created_at"] = f"2099-01-01T00:00:0{timestamp - 4070908800}+00:00"
            path.write_text(json.dumps(payload))
            os.utime(path, (2 * 4070908800 - timestamp,) * 2)
    pending = tmp_path / "pending"
    first = next_batch(pending)
    assert first.path.stem == "f" * 32
    second = next_batch(pending, exclude_job_ids={first.path.stem})
    assert second.path.stem == "a" * 32
    first.close()
    second.close()
    assert next_batch(pending).path.stem == "f" * 32


def test_workers_write_independent_partitions_and_api_aggregates(tmp_path):
    path = enqueue(tmp_path, urls=["https://example.com/a", "https://example.org/b"], crawler_ids=[0, 1])
    first = next_batch(path.parent, 0)
    second = next_batch(path.parent, 1)
    assert first.directory != second.directory
    assert first.remaining == {0}
    assert second.remaining == {1}
    first.save(0, final_url=first.urls[0], status_code=200, body=b"first")
    assert next_batch(path.parent, 0) is None
    assert job_status(path.stem, first.payload, tmp_path / "results" / path.stem)["status"] == "running"
    second.save(1, final_url=second.urls[1], status_code=200, body=b"second")
    status = job_status(path.stem, first.payload, tmp_path / "results" / path.stem)
    assert status["status"] == "completed"
    assert status["completed_urls"] == 2
    assert (first.directory / "00000.html").read_bytes() == b"first"
    assert (second.directory / "00001.html").read_bytes() == b"second"
    assert path.exists()
    assert not list(tmp_path.rglob("*.lock"))


def test_live_worker_crawls_only_requested_pages_and_never_emits_ingestor_results(tmp_path):
    hits = []
    active = 0
    maximum = 0
    guard = Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal active, maximum
            hits.append(self.path)
            if self.path != "/robots.txt":
                with guard:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.08)
                with guard:
                    active -= 1
            if self.path == "/robots.txt":
                body, code, mime = b"User-agent: *\nAllow: /\n", 200, "text/plain"
            elif self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/html")
                self.end_headers()
                return
            elif self.path == "/missing":
                body, code, mime = b"<html>missing</html>", 404, "text/html"
            elif self.path == "/text":
                body, code, mime = b"plain", 200, "text/plain"
            else:
                body, code, mime = b'<html><a href="/outlink">link</a></html>', 200, "text/html"
            self.send_response(code)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    # Exercise the crawler transport locally; public-host validation is tested
    # at the API boundary. No external hosts or production services are used.
    path = enqueue(tmp_path, urls=[base + part for part in ("/redirect", "/missing", "/text")])
    other = enqueue(tmp_path, name="another", urls=[base + "/other-job"])
    root = Path(__file__).resolve().parents[3]
    script = '''
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.reactor import install_reactor
install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
from crawler.spiders.spider import HtmlSpider
import tldextract
tldextract.extract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
settings = Settings()
settings.setmodule("crawler.settings")
settings.set("DOWNLOAD_DELAY", 0.1)
settings.set("RANDOMIZE_DOWNLOAD_DELAY", False)
settings.set("AUTOTHROTTLE_ENABLED", False)
settings.set("LOG_LEVEL", "ERROR")
settings.set("CLOSESPIDER_TIMEOUT", 4)
settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", 1, priority="cmdline")
settings.set("DOWNLOAD_SLOTS", {}, priority="cmdline")
settings.set("MANUAL_BULK_CRAWL_QUEUE_DIR", sys.argv[1] + "/pending")
settings.set("URL_QUEUE_TEMPLATE", sys.argv[1] + "/normal_queue/crawler_{id:02d}")
settings.set("RESULT_DIR_TEMPLATE", sys.argv[1] + "/normal_results/crawler_{id:02d}")
process = CrawlerProcess(settings)
process.crawl(HtmlSpider, crawler_id=0)
process.start()
'''
    normal = tmp_path / "normal_queue/crawler_00"
    normal.mkdir(parents=True)
    (normal / "domain_99.json").write_text(json.dumps({"urls": [base + f"/regular/{i}" for i in range(20)]}))
    try:
        result = subprocess.run([sys.executable, "-c", script, str(tmp_path)],
                                cwd=root / "containers/crawler",
                                env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}",
                                     "CRAWLER_USE_AUTOTHROTTLE": "true",
                                     "API_CONFIG_PATH": str(tmp_path / "does-not-exist.yml")},
                                capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        assert next_batch(path.parent) is None, result.stderr
        assert next_batch(other.parent) is None, result.stderr
        assert (tmp_path / "results/another/crawler_00/00000.html").read_bytes().startswith(b"<html>")
        output = tmp_path / "results/batch/crawler_00"
        assert (output / "00000.html").read_bytes().startswith(b"<html>")
        first = json.loads((output / "00000.json").read_text())
        assert first["url"] == base + "/redirect"
        assert first["final_url"] == base + "/html"
        assert json.loads((output / "00001.json").read_text())["status_code"] == 404
        assert json.loads((output / "00002.json").read_text())["error"] == "NonHTML content-type"
        assert "/outlink" not in hits
        records = [json.loads(line) for file in (tmp_path / "normal_results").rglob("*.jsonl")
                   for line in file.read_text().splitlines()]
        assert len(records) == 20
        assert all(record["url"].startswith(base + "/regular/") for record in records)
        assert all("manual_bulk_crawl_index" not in record for record in records)
        assert maximum == 1, "API and regular requests must share the same domain concurrency limit\n" + result.stderr
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("result_dir", [None, "", "relative/output", 1])
def test_job_requires_an_absolute_output_destination_from_publisher(tmp_path, result_dir):
    path = enqueue(tmp_path)
    payload = json.loads(path.read_text())
    payload["result_dir"] = result_dir
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Invalid crawl request batch"):
        next_batch(path.parent)
    assert path.exists()
    assert not list((tmp_path / "results").rglob("*.html"))

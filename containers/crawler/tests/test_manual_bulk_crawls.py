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
from libs.ipc.manual_bulk_crawl import claim_next


def enqueue(root, name="batch", urls=None, crawler_ids=None):
    pending = root / "pending"
    pending.mkdir(exist_ok=True)
    path = pending / f"{name}.json"
    urls = urls or ["https://example.com/a"]
    path.write_text(json.dumps({"version": 1, "urls": urls, "crawler_ids": crawler_ids or [0] * len(urls),
                                "result_dir": str(root / "results")}))
    return path


def test_only_one_worker_can_claim_batch_and_interrupted_owner_can_retry(tmp_path):
    path = enqueue(tmp_path)
    batch = claim_next(path.parent)
    try:
        assert claim_next(path.parent) is None
        assert path.exists()
    finally:
        batch.close()
    retry = claim_next(path.parent)
    assert retry.urls == batch.urls
    retry.close()


def test_complete_batch_is_published_atomically_with_html_and_failures(tmp_path):
    path = enqueue(tmp_path, urls=["https://example.com/a", "https://example.com/b"])
    batch = claim_next(path.parent)
    try:
        batch.save(0, final_url="https://example.com/a", status_code=200, body=b"<html>hello</html>")
        assert not batch.target.exists()
        assert path.exists()
        batch.save(1, final_url="https://example.com/b", status_code=None, error="TimeoutError")
        assert not path.exists()
        assert (batch.target / "00000.html").read_bytes() == b"<html>hello</html>"
        assert not (batch.target / "00001.html").exists()
        assert json.loads((batch.target / "00001.json").read_text())["error"] == "TimeoutError"
        assert len(json.loads((batch.target / "request.json").read_text())["urls"]) == 2
    finally:
        batch.close()


def test_storage_failure_retains_entire_batch_and_retry_replaces_partial_work(tmp_path, monkeypatch):
    path = enqueue(tmp_path)
    batch = claim_next(path.parent)
    with monkeypatch.context() as patch:
        patch.setattr(storage.os, "replace", lambda *args: (_ for _ in ()).throw(OSError("disk full")))
        with pytest.raises(OSError):
            batch.save(0, final_url=batch.urls[0], status_code=200, body=b"old")
    assert path.exists()
    assert not batch.target.exists()
    batch.close()
    retry = claim_next(path.parent)
    try:
        retry.save(0, final_url=retry.urls[0], status_code=200, body=b"new")
        assert (retry.target / "00000.html").read_bytes() == b"new"
    finally:
        retry.close()


def test_crash_after_publication_does_not_recrawl_completed_batch(tmp_path, monkeypatch):
    path = enqueue(tmp_path)
    batch = claim_next(path.parent)
    unlink = Path.unlink
    with monkeypatch.context() as patch:
        def fail_unlink(self, *args, **kwargs):
            if self == path:
                raise OSError("interrupted")
            return unlink(self, *args, **kwargs)
        patch.setattr(Path, "unlink", fail_unlink)
        with pytest.raises(OSError):
            batch.save(0, final_url=batch.urls[0], status_code=200, body=b"saved")
    batch.close()
    assert claim_next(path.parent) is None
    assert not path.exists()
    assert (batch.target / "00000.html").read_bytes() == b"saved"


def test_temporary_files_are_never_claimed(tmp_path):
    pending = tmp_path / "pending"
    pending.mkdir()
    (pending / "incomplete.json.tmp").write_text("incomplete")
    assert claim_next(pending) is None


@pytest.mark.parametrize("legacy", [False, True])
def test_jobs_are_claimed_fifo_regardless_of_uuid_order(tmp_path, legacy):
    for name, timestamp in (("f" * 32, 4070908800), ("a" * 32, 4070908801)):
        path = enqueue(tmp_path, name=name)
        if legacy:
            os.utime(path, (timestamp, timestamp))
        else:
            payload = json.loads(path.read_text())
            payload["created_at"] = f"2099-01-01T00:00:0{timestamp - 4070908800}+00:00"
            path.write_text(json.dumps(payload))
            # Creation metadata takes precedence over filesystem times.
            os.utime(path, (2 * 4070908800 - timestamp,) * 2)
    pending = tmp_path / "pending"
    first = claim_next(pending)
    try:
        assert first.path.stem == "f" * 32
        # Another claimant can take the next job while the oldest is owned.
        second = claim_next(pending)
        try:
            assert second.path.stem == "a" * 32
        finally:
            second.close()
    finally:
        first.close()
    # Releasing an unfinished older job preserves its position on retry.
    retry = claim_next(pending)
    try:
        assert retry.path.stem == "f" * 32
    finally:
        retry.close()


def test_job_waits_for_all_assigned_crawlers_before_publication(tmp_path):
    path = enqueue(tmp_path, urls=["https://example.com/a", "https://example.org/b"], crawler_ids=[0, 1])
    first = claim_next(path.parent, 0)
    second = claim_next(path.parent, 1)
    try:
        assert first.remaining == {0}
        assert second.remaining == {1}
        first.save(0, final_url=first.urls[0], status_code=200, body=b"first")
        assert path.exists()
        assert not first.target.exists()
        assert claim_next(path.parent, 0) is None
        second.save(1, final_url=second.urls[1], status_code=200, body=b"second")
        status = json.loads((second.target / "status.json").read_text())
        assert status["completed_urls"] == status["succeeded_urls"] == 2
        assert (second.target / "00000.html").read_bytes() == b"first"
        assert (second.target / "00001.html").read_bytes() == b"second"
        assert not path.exists()
    finally:
        first.close()
        second.close()


def test_retry_resumes_completed_urls_without_refetching_them(tmp_path):
    path = enqueue(tmp_path, urls=["https://example.com/a", "https://example.com/b"])
    first = claim_next(path.parent)
    first.save(0, final_url=first.urls[0], status_code=200, body=b"saved")
    started = first.progress["started_at"]
    first.close()
    retry = claim_next(path.parent)
    try:
        assert retry.remaining == {1}
        assert retry.progress["started_at"] == started
        retry.save(1, final_url=retry.urls[1], status_code=200, body=b"new")
        assert (retry.target / "00000.html").read_bytes() == b"saved"
    finally:
        retry.close()


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
        assert not path.exists(), result.stderr
        assert not other.exists(), result.stderr
        assert (tmp_path / "results/another/00000.html").read_bytes().startswith(b"<html>")
        output = tmp_path / "results/batch"
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
        claim_next(path.parent)
    assert path.exists()
    assert not (tmp_path / "results").exists()

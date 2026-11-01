"""A real crawler process consumes crawl requests next to normal work (localhost only)."""
import gzip
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread

from libs.ipc.crawl_requests import CrawlRequestPaths, atomic_write_json, batch_key

SCRIPT = '''
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
settings.setdict({
    "DOWNLOAD_DELAY": 0.05, "RANDOMIZE_DOWNLOAD_DELAY": False, "AUTOTHROTTLE_ENABLED": False,
    "LOG_LEVEL": "ERROR", "CLOSESPIDER_TIMEOUT": 6, "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    "DOWNLOAD_SLOTS": {}, "RETRY_ENABLED": False,
    "MANUAL_BULK_CRAWL_ROOT": sys.argv[1] + "/data", "MANUAL_BULK_CRAWL_POLL_S": 0.2,
    "MANUAL_BULK_CRAWL_SEGMENT_S": 1, "MANUAL_BULK_CRAWL_STATUS_S": 0.5, "MANUAL_BULK_CRAWL_MIN_FREE_GB": 0,
    "URL_QUEUE_TEMPLATE": sys.argv[1] + "/normal_queue/crawler_{id:02d}",
    "RESULT_DIR_TEMPLATE": sys.argv[1] + "/normal_results/crawler_{id:02d}",
}, priority="cmdline")
process = CrawlerProcess(settings)
process.crawl(HtmlSpider, crawler_id=0)
process.start()
'''


def test_live_crawler_writes_segments_and_shares_the_domain_slot(tmp_path):
    hits, guard = [], Lock()
    active = maximum = 0

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal active, maximum
            hits.append(self.path)
            if self.path != "/robots.txt":
                with guard:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.05)
                with guard:
                    active -= 1
            if self.path == "/robots.txt":
                body, code, mime = b"User-agent: *\nDisallow: /private\n", 200, "text/plain"
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
    paths = CrawlRequestPaths(tmp_path / "data")
    now = datetime.now(UTC)
    for request_id, items in (("a" * 32, {"r": "/redirect", "m": "/missing", "t": "/text", "p": "/private/x"}),
                              ("b" * 32, {"o": "/other-request"})):
        atomic_write_json(paths.meta(request_id), {
            "id": request_id, "tenant": "t", "name": "n", "created_at": now.isoformat(),
            "expires_at": (now + timedelta(days=1)).isoformat(), "result_quota_bytes": 10 ** 12})
        atomic_write_json(paths.inbox_file(0, request_id, batch_key("b")), {
            "request_id": request_id,
            "items": [{"id": item, "url": base + path, "priority": 0} for item, path in items.items()]})
    normal = tmp_path / "normal_queue/crawler_00"
    normal.mkdir(parents=True)
    (normal / "domain_99.json").write_text(json.dumps({"urls": [base + f"/regular/{i}" for i in range(20)]}))
    root = Path(__file__).resolve().parents[3]
    try:
        result = subprocess.run([sys.executable, "-c", SCRIPT, str(tmp_path)], cwd=root / "containers/crawler",
                                env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}",
                                     "CRAWLER_USE_AUTOTHROTTLE": "true"},
                                capture_output=True, text=True, timeout=60, check=False)
        assert result.returncode == 0, result.stderr
        output = result.stdout + result.stderr
        assert "Traceback" not in output and "_failed" not in output, output[-4000:]
        records = {}
        for path in paths.root.glob("segments/*/crawler_00/*.jsonl.gz"):
            for line in gzip.decompress(path.read_bytes()).splitlines():
                records[json.loads(line)["id"]] = json.loads(line)
        assert set(records) == {"r", "m", "t", "p", "o"}, result.stderr[-4000:]
        assert records["r"]["final_url"] == base + "/html" and records["r"]["url"] == base + "/redirect"
        assert records["r"]["html"].startswith("<html>") and records["r"]["error"] is None
        assert records["m"]["error"] == "http_404" and records["m"]["status_code"] == 404
        assert records["t"]["error"] == "non_html"
        assert records["p"]["error"] == "robots" and records["p"]["fetched_at"] is None
        assert records["o"]["error"] is None
        assert not list(paths.root.glob("segments/*/crawler_00/*.part"))
        assert not list(paths.inbox(0).glob("*.json"))
        db = sqlite3.connect(paths.frontier(0))
        assert db.execute("SELECT count(*) FROM url").fetchone() == (0,), "Published rows are forgotten"
        db.close()
        assert json.loads(paths.status(0).read_text())["crawler_id"] == 0
        assert "/outlink" not in hits
        normal_records = [json.loads(line) for file in (tmp_path / "normal_results").rglob("*.jsonl")
                          for line in file.read_text().splitlines()]
        assert len(normal_records) == 20
        assert maximum == 1, "Manual and regular requests share one domain concurrency limit"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

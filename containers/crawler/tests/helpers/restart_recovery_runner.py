"""Isolated real crawler proof. It fetches only a local HTTP server."""
import hashlib
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from scrapy.utils.reactor import install_reactor

install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

import tldextract
from crawler.spiders.spider import HtmlSpider
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings


def main():
    root = Path(sys.argv[1])
    mode = sys.argv[2]
    source = Path(__file__).resolve().parents[4]
    visits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            visits.append((self.path, time.time()))
            if mode == "redirect" and self.path == "/a":
                self.send_response(302)
                self.send_header("Location", "/final")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            body = b"<html><title>checkpoint proof</title><body>ok</body></html>"
            self.send_response(404 if mode == "failure" and self.path == "/b" else 200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    runtime = root / "restart"
    checkpoint = runtime / "proof" / "worker-02.json"
    checkpoint.parent.mkdir(parents=True)
    deadline = time.time() + 3.0
    payload = {"schema": 2, "crawler_id": 2, "pid": 2_000_000_000,
               "requests": [{"request_id": index, "method": "GET", "domain_id": 7,
                             "source_url": base + path, "current_url": base + path,
                             "location": "scheduler"} for index, path in enumerate(("/a", "/b"), 1)],
               "retry_after_deadlines_epoch": {"127.0.0.1": deadline}}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    checkpoint.write_bytes(raw)
    relatives = ("containers/crawler/crawler/spiders/spider.py",
                 "containers/crawler/crawler/restart_recovery.py",
                 "containers/crawler/config/site_policies.yml")
    manifest = {"schema": 1, "release_id": "proof",
                "source_sha256": {relative: hashlib.sha256((source / relative).read_bytes()).hexdigest() for relative in relatives},
                "workers": {"2": {"path": str(checkpoint), "sha256": hashlib.sha256(raw).hexdigest(), "old_pid": payload["pid"]}},
                "cooldown_deadlines_epoch": {"127.0.0.1": deadline}}
    active = runtime / "active.json"
    active.write_text(json.dumps(manifest))
    settings = Settings()
    settings.setmodule("crawler.settings", priority="project")
    overrides = {
        "CRAWLER_RESTART_MANIFEST": str(active),
        "URL_QUEUE_TEMPLATE": str(root / "queue" / "crawler_{id:02d}"),
        "RESULT_DIR_TEMPLATE": str(root / "results" / "crawler_{id:02d}"),
        "MANUAL_BULK_CRAWL_QUEUE_DIR": str(root / "manual"),
        "REMOTE_CONTROL_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
        "MEMUSAGE_ENABLED": False,
        "AUTOTHROTTLE_ENABLED": False,
        "DOWNLOAD_DELAY": 0,
        "RANDOMIZE_DOWNLOAD_DELAY": False,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "LOG_LEVEL": "ERROR",
    }
    for key, value in overrides.items():
        settings.set(key, value, priority="cmdline")
    # The fixture terminates when its two tasks finish. Production's continuous
    # idle/top-up behavior is covered separately and remains unchanged.
    HtmlSpider.on_idle = lambda self: None
    tldextract.extract = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
    process = CrawlerProcess(settings)
    failures = []
    crawler = process.create_crawler(HtmlSpider)
    deferred = process.crawl(crawler, crawler_id=2)
    deferred.addErrback(lambda failure: failures.append(failure.getErrorMessage()))
    process.start()
    server.shutdown()
    server.server_close()
    if failures:
        raise RuntimeError(failures)
    assert crawler.spider.restart_recovery.cooldown_middleware is not None
    records = []
    for file in (root / "results").rglob("*.jsonl"):
        records.extend(json.loads(line) for line in file.read_text().splitlines() if line)
    done = json.loads((runtime / "proof" / "worker-02.done.json").read_text())
    assert len(records) == 2 and done["completed_count"] == 2
    assert visits and min(epoch for _, epoch in visits) >= deadline - 0.02
    assert {record["status"] for record in records} == ({"ok", "fail"} if mode == "failure" else {"ok"})
    print(json.dumps({"proof_restart_recovery": True, "mode": mode, "records": len(records),
                      "visits": [path for path, _ in visits], "deadline_respected": True,
                      "completion_receipt": done["phase"]}))


if __name__ == "__main__":
    main()

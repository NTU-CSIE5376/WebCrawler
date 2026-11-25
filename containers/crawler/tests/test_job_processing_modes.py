import json
import os
from pathlib import Path
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from libs.ipc.manual_bulk_crawl import prepare_job, job_status


@pytest.mark.parametrize("mode", ["fetch", "ingest"])
def test_real_job_processing_modes(tmp_path, mode):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(404 if self.path == "/missing" else 200)
            self.send_header("Content-Type", "text/html")
            body = b'<html><title>Page title</title><a href="/child">Anchor</a></html>'
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    pending = tmp_path / "pending"
    pending.mkdir()
    manifest = pending / "job.json"
    payload = {"version": 2 if mode == "ingest" else 1, "processing_mode": mode,
               "urls": [f"http://127.0.0.1:{server.server_port}{path}" for path in ("/page", "/missing")],
               "crawler_ids": [0, 0], "result_dir": str(tmp_path / "results")}
    manifest.write_text(json.dumps(payload))
    prepare_job(manifest, payload)
    root = Path(__file__).resolve().parents[3]
    try:
        result = subprocess.run([sys.executable, str(Path(__file__).parent / "helpers/job_ingest_runner.py"), str(tmp_path)],
                                cwd=root / "containers/crawler",
                                env={**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"},
                                capture_output=True, text=True, timeout=25)
        assert result.returncode == 0, result.stdout + result.stderr
        observed = json.loads((tmp_path / "observation.json").read_text())
        assert observed["finish_reason"] == "jobs_completed", result.stdout + result.stderr
        assert observed["normal_pending"] == 7
        status = job_status("job", payload, tmp_path / "results/job")
        assert status["completed_urls"] == 2 and status["failed_urls"] == 1
        records = [json.loads(line) for p in (tmp_path / "output_00").rglob("*.jsonl")
                   for line in p.read_text().splitlines()]
        if mode == "fetch":
            assert records == [] and status["html_files"] == 2
        else:
            assert status["html_files"] == 0 and len(records) == 2
            ok = next(r for r in records if r["status"] == "ok")
            assert ok["title"] == "Page title"
            assert ok["outlinks"][0]["url"].endswith("/child")
            assert "ingest_job" not in ok
            assert next(r for r in records if r["status"] == "fail")["fail_reason"] == "HttpError 404"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

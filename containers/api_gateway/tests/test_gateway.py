"""Opt-in real Nginx tests: RUN_GATEWAY_TESTS=1 python -m pytest containers/api_gateway/tests."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import time
from uuid import uuid4

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_GATEWAY_TESTS") != "1", reason="Requires Docker and test images",
)

BACKEND = """
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.respond(200)
    def do_POST(self):
        self.respond(200)
    def respond(self, status):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0))).decode()
        data = json.dumps(dict(path=self.path, method=self.command, body=body,
                               content_type=self.headers.get('Content-Type'),
                               prefix=self.headers.get('X-Forwarded-Prefix'))).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
HTTPServer(('0.0.0.0', int(sys.argv[1])), Handler).serve_forever()
"""


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True, timeout=120).stdout


@pytest.fixture(scope="module")
def gateway():
    name = "gateway-test-" + uuid4().hex[:12]
    backend = name + "-backend"
    proxy = name + "-proxy"
    config = Path(__file__).resolve().parents[1] / "default.conf"
    # Exercise the configured route with an echo server, independent of the
    # downstream service's name, endpoints, and request schema.
    config_text = config.read_text()
    prefix = re.search(r"location (/[^\s{}]+/) \{", config_text).group(1).rstrip("/")
    backend_host, backend_port = re.search(
        r"set \$backend ([^;]+);", config_text,
    ).group(1).rsplit(":", 1)
    docker("network", "create", name)
    try:
        docker("run", "--rm", "-d", "--name", backend, "--network", name,
               "--network-alias", backend_host, "python:3.12-alpine", "python", "-u", "-c",
               BACKEND, backend_port)
        docker("run", "--rm", "-d", "--name", proxy, "--network", name,
               "--mount", f"type=bind,src={config},dst=/etc/nginx/conf.d/default.conf,readonly",
               "nginx:1.28-alpine")
        docker("exec", proxy, "nginx", "-t")
        deadline = time.monotonic() + 20
        while True:
            try:
                docker("exec", proxy, "wget", "-qO-", f"http://localhost{prefix}/ready")
                break
            except subprocess.CalledProcessError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        yield proxy, prefix
    finally:
        for container in (proxy, backend):
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        docker("network", "rm", name)


def test_post_strips_prefix_preserves_query_body_and_content_type(gateway):
    proxy, prefix = gateway
    body = json.dumps({"message": "gateway test"})
    response = docker("exec", proxy, "wget", "-qO-", "--header", "Content-Type: application/json",
                      "--post-data", body, f"http://localhost{prefix}/resources?filter=test")
    assert json.loads(response) == {
        "path": "/resources?filter=test", "method": "POST", "body": body,
        "content_type": "application/json", "prefix": prefix,
    }


def test_arbitrary_path_is_forwarded(gateway):
    proxy, prefix = gateway
    response = docker("exec", proxy, "wget", "-qO-", f"http://localhost{prefix}/ready")
    assert json.loads(response)["path"] == "/ready"


def test_unprefixed_route_is_not_exposed(gateway):
    proxy, _ = gateway
    response = subprocess.run(
        ["docker", "exec", proxy, "wget", "-S", "-O-", "http://localhost/resources"],
        capture_output=True, text=True,
    )
    assert response.returncode != 0
    assert "404" in response.stderr

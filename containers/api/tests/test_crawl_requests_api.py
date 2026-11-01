"""Crawl request API v2: auth, idempotent submission, admission, segments, GC."""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import time
from datetime import UTC, datetime, timedelta
from threading import Thread
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml
from containers.api import app as app_module
from containers.api.app import create_app
from containers.api.crawl_requests import CrawlRequestService
from fastapi.testclient import TestClient
from libs.ipc.crawl_requests import (
    CrawlRequestPaths,
    atomic_write_json,
    batch_key,
    segment_name,
)
from sqlalchemy.exc import OperationalError

AUTH = {"Authorization": "Bearer token-a"}
OTHER = {"Authorization": "Bearer token-b"}
FREE = SimpleNamespace(free=10 ** 15)


def crawl_config(base, **overrides):
    return {
        "root": str(base / "data"), "tokens_file": str(base / "tokens.yml"),
        "max_urls": 5, "max_url_length": 2500, "max_result_quota_gb": 100,
        "default_expires_days": 30, "max_expires_days": 60, "max_inbox_files": 2,
        "min_free_gb": 300, "max_tenant_queued_urls": 100, "retry_after_s": 7,
        "route_cache_ttl_s": 86400, "long_poll_max_s": 60, "long_poll_interval_s": 0.02,
        "top_hosts": 2, "gc_interval_sec": 3600, "gc_grace_days": 3, **overrides,
    }


@pytest.fixture
def api(tmp_path):
    config = crawl_config(tmp_path)
    (tmp_path / "tokens.yml").write_text(yaml.safe_dump({"token-a": "alice", "token-b": "bob"}))
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"crawl_requests": config}))
    routes = {"example.com": 0, "example.org": 3}
    router = Mock(side_effect=lambda urls: [routes.get(url.split("/")[2], 1) for url in urls])
    disk = Mock(return_value=FREE)
    service = CrawlRequestService(config, route_urls=router, disk_usage=disk)
    with TestClient(create_app(str(path), service=service)) as client:
        yield SimpleNamespace(client=client, service=service, router=router, disk=disk,
                              paths=CrawlRequestPaths(config["root"]), root=tmp_path, config_path=path)


def create(api, headers=AUTH, **body):
    response = api.client.post("/v1/crawl-requests", json={"name": "isel", **body}, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()["request_id"]


def post_urls(api, request_id, urls, batch_id="b1", headers=AUTH):
    return api.client.post(f"/v1/crawl-requests/{request_id}/urls",
                           json={"batch_id": batch_id, "urls": urls}, headers=headers)


def item(item_id, url, priority=0):
    return {"id": item_id, "url": url, "priority": priority}


def inbox_files(api):
    return sorted(api.paths.root.glob("inbox/crawler_*/*.json"))


def write_segment(api, request_id, crawler_id, seq, records=({"id": "a"},)):
    path = api.paths.segments(request_id, crawler_id) / segment_name(crawler_id, seq)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in records).encode()))
    return path


def snapshot(api, crawler_id, requests, updated_at=None):
    atomic_write_json(api.paths.status(crawler_id), {
        "crawler_id": crawler_id,
        "updated_at": (updated_at or datetime.now(UTC)).isoformat(),
        "requests": requests,
    })


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer nope"}, {"Authorization": "token-a"},
                                     {"Authorization": "Basic token-a"}])
def test_missing_or_unknown_token_is_401(api, headers):
    response = api.client.post("/v1/crawl-requests", json={"name": "x"}, headers=headers)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert api.client.get("/v1/crawl-requests", headers=headers).status_code == 401
    assert not list(api.paths.root.rglob("*.json"))


def test_other_tenants_request_is_404(api):
    request_id = create(api)
    for method, path in (("get", ""), ("post", "/seal"), ("delete", ""), ("get", "/segments")):
        response = getattr(api.client, method)(f"/v1/crawl-requests/{request_id}{path}", headers=OTHER)
        assert response.status_code == 404, path
    assert post_urls(api, request_id, [item("a", "https://example.com/")], headers=OTHER).status_code == 404
    assert api.client.get("/v1/crawl-requests", headers=OTHER).json() == {"requests": []}
    assert api.client.get("/v1/crawl-requests/" + "0" * 32, headers=AUTH).status_code == 404
    assert api.client.get("/v1/crawl-requests/../etc", headers=AUTH).status_code == 404


def test_create_defaults_and_caps_quota_and_expiry(api):
    response = api.client.post("/v1/crawl-requests", json={"name": "isel"}, headers=AUTH)
    assert response.status_code == 201
    body = response.json()
    assert set(body) == {"request_id", "name", "result_quota_gb", "expires_at"}
    assert body["result_quota_gb"] == 100
    expires = datetime.fromisoformat(body["expires_at"]) - datetime.now(UTC)
    assert timedelta(days=29, hours=23) < expires <= timedelta(days=30)
    meta = json.loads(api.paths.meta(body["request_id"]).read_text())
    assert meta["tenant"] == "alice" and meta["result_quota_bytes"] == 100 * 10 ** 9

    capped = api.client.post("/v1/crawl-requests", headers=AUTH,
                             json={"name": "x", "result_quota_gb": 500, "expires_days": 90}).json()
    assert capped["result_quota_gb"] == 100
    assert datetime.fromisoformat(capped["expires_at"]) - datetime.now(UTC) > timedelta(days=59)
    small = api.client.post("/v1/crawl-requests", headers=AUTH,
                            json={"name": "x", "result_quota_gb": 0.5, "expires_days": 1}).json()
    assert small["result_quota_gb"] == 0.5


@pytest.mark.parametrize("body", [{}, {"name": ""}, {"name": 1}, {"name": "x", "tenant": "bob"},
                                  {"name": "x", "result_quota_gb": 0}, {"name": "x", "expires_days": 0},
                                  {"name": "x", "expires_days": 1.5}])
def test_create_rejects_malformed_body(api, body):
    assert api.client.post("/v1/crawl-requests", json=body, headers=AUTH).status_code == 400


def test_submit_routes_canonical_urls_and_skips_bad_items(api):
    request_id = create(api)
    response = post_urls(api, request_id, [
        item("a", "HTTPS://Example.COM/x?b=2&a=1#frag", 5),
        item("b", "ftp://example.com/bad"),
        item("a", "https://example.com/again"),
        item("c", "https://example.org/y", -3),
        item("d", None),
    ])
    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "skipped": [
        {"index": 1, "id": "b", "reason": "invalid"},
        {"index": 2, "id": "a", "reason": "duplicate"},
        {"index": 4, "id": "d", "reason": "invalid"},
    ]}
    key = batch_key("b1")
    assert json.loads(api.paths.inbox_file(0, request_id, key).read_text()) == {
        "request_id": request_id, "items": [{"id": "a", "url": "https://example.com/x?a=1&b=2", "priority": 5}]}
    assert json.loads(api.paths.inbox_file(3, request_id, key).read_text())["items"] == [
        {"id": "c", "url": "https://example.org/y", "priority": -3}]
    assert len(inbox_files(api)) == 2


def test_same_batch_id_returns_the_first_response_and_writes_once(api):
    request_id = create(api)
    first = post_urls(api, request_id, [item("a", "https://example.com/1"), item("b", "bad")])
    [inbox] = inbox_files(api)
    inbox.unlink()  # The crawler consumed it.
    again = post_urls(api, request_id, [item("z", "https://example.org/other")])
    assert again.status_code == 202
    assert again.json() == first.json() == {"accepted": 1, "skipped": [{"index": 1, "id": "b", "reason": "invalid"}]}
    assert not inbox_files(api)
    assert api.router.call_count == 1
    api.client.post(f"/v1/crawl-requests/{request_id}/seal", headers=AUTH)
    assert post_urls(api, request_id, [item("a", "https://example.com/1")]).json() == first.json()


def test_batch_with_only_skipped_items_accepts_nothing_without_routing(api):
    request_id = create(api)
    response = post_urls(api, request_id, [item("a", "nope")])
    assert response.json() == {"accepted": 0, "skipped": [{"index": 0, "id": "a", "reason": "invalid"}]}
    api.router.assert_not_called()
    assert not inbox_files(api)


@pytest.mark.parametrize("payload", [
    {"batch_id": "b", "urls": []},
    {"batch_id": "b", "urls": [item(str(i), f"https://example.com/{i}") for i in range(6)]},
    {"batch_id": "", "urls": [item("a", "https://example.com/")]},
    {"urls": [item("a", "https://example.com/")]},
    {"batch_id": "b", "urls": [{"url": "https://example.com/", "priority": 0}]},
    {"batch_id": "b", "urls": [{"id": 1, "url": "https://example.com/", "priority": 0}]},
    {"batch_id": "b", "urls": [{"id": "a", "url": "https://example.com/", "priority": "1"}]},
    {"batch_id": "b", "urls": [{"id": "a", "url": "https://example.com/", "priority": 2 ** 63}]},
    {"batch_id": "b", "urls": [{"id": "a", "url": "https://example.com/"}]},
    {"batch_id": "b", "urls": "https://example.com/"},
])
def test_malformed_batch_is_400_without_side_effects(api, payload):
    request_id = create(api)
    response = api.client.post(f"/v1/crawl-requests/{request_id}/urls", json=payload, headers=AUTH)
    assert response.status_code == 400
    assert "error" in response.json()
    assert not inbox_files(api)
    assert not list(api.paths.root.glob("requests/*/batches/*"))


@pytest.mark.parametrize("data,content_type,status", [
    ("{", "application/json", 400),
    ('{"name": "x"}', "text/plain", 415),
])
def test_http_errors_are_json(api, data, content_type, status):
    response = api.client.post("/v1/crawl-requests", content=data,
                               headers={**AUTH, "Content-Type": content_type})
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/json")


@pytest.mark.parametrize("close", ["seal", "cancel", "expire"])
def test_closed_request_rejects_new_batches_with_409(api, close):
    request_id = create(api)
    if close == "seal":
        assert api.client.post(f"/v1/crawl-requests/{request_id}/seal", headers=AUTH).json() == {"sealed": True}
    elif close == "cancel":
        response = api.client.delete(f"/v1/crawl-requests/{request_id}", headers=AUTH)
        assert response.status_code == 202 and response.json() == {"cancelled": True}
    else:
        meta = json.loads(api.paths.meta(request_id).read_text())
        meta["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        atomic_write_json(api.paths.meta(request_id), meta)
    assert post_urls(api, request_id, [item("a", "https://example.com/")]).status_code == 409
    assert not inbox_files(api)


def test_routing_failure_is_503_and_writes_nothing(api):
    request_id = create(api)
    api.router.side_effect = OperationalError("SELECT", {}, Exception("db down"))
    response = post_urls(api, request_id, [item("a", "https://example.com/")])
    assert response.status_code == 503
    assert response.headers["retry-after"] == "7"
    assert not inbox_files(api)
    assert not list(api.paths.root.glob("requests/*/batches/*"))
    api.router.side_effect = lambda urls: [0] * len(urls)
    assert post_urls(api, request_id, [item("a", "https://example.com/")]).status_code == 202
    assert len(inbox_files(api)) == 1


def test_low_disk_is_503(api):
    request_id = create(api)
    api.disk.return_value = SimpleNamespace(free=299 * 10 ** 9)
    response = post_urls(api, request_id, [item("a", "https://example.com/")])
    assert response.status_code == 503 and response.headers["retry-after"] == "7"
    assert not inbox_files(api)


def test_inbox_backlog_of_a_target_crawler_is_503(api):
    request_id = create(api)
    for batch in ("x", "y", "z"):
        assert post_urls(api, request_id, [item(batch, "https://example.org/" + batch)],
                         batch_id=batch).status_code == 202
    # Crawler 3 holds three unconsumed files (limit 2); crawler 0 is unaffected.
    assert post_urls(api, request_id, [item("o", "https://example.org/o")], batch_id="o").status_code == 503
    assert post_urls(api, request_id, [item("c", "https://example.com/c")], batch_id="c").status_code == 202


def test_tenant_queued_urls_over_limit_is_503(api):
    mine, theirs = create(api), create(api, headers=OTHER)
    snapshot(api, 0, {theirs: {"queued": 500, "leased": 0, "top_hosts": []}})
    assert post_urls(api, mine, [item("a", "https://example.com/")]).status_code == 202
    snapshot(api, 0, {mine: {"queued": 60, "leased": 39, "top_hosts": []}})
    assert post_urls(api, mine, [item("b", "https://example.com/b")], batch_id="b2").status_code == 202
    # Two URLs are pending in inbox files: 60 + 39 + 1 + 1 > 100.
    assert post_urls(api, mine, [item("c", "https://example.com/c")], batch_id="b3").status_code == 503
    assert post_urls(api, theirs, [item("c", "https://example.com/c")], headers=OTHER).status_code == 202


def test_status_sums_crawler_snapshots_and_lists_unacked_segments(api):
    request_id = create(api)
    old = datetime.now(UTC) - timedelta(seconds=30)
    snapshot(api, 0, {request_id: {"queued": 5, "leased": 2, "top_hosts": [
        {"host": "a.example", "remaining": 4, "done_1h": 1}, {"host": "b.example", "remaining": 1, "done_1h": 9}]}},
        updated_at=old)
    snapshot(api, 1, {request_id: {"queued": 3, "leased": 1, "top_hosts": [
        {"host": "c.example", "remaining": 3, "done_1h": 0}]}})
    write_segment(api, request_id, 0, 1)
    (api.paths.segments(request_id, 0) / (segment_name(0, 2) + ".part")).write_bytes(b"open")
    status = api.client.get(f"/v1/crawl-requests/{request_id}", headers=AUTH).json()
    size = (api.paths.segments(request_id, 0) / segment_name(0, 1)).stat().st_size
    assert status == {
        "request_id": request_id, "name": "isel", "created_at": status["created_at"],
        "expires_at": status["expires_at"], "sealed": False, "cancelled": False,
        "queued": 8, "leased": 3, "unacked_segments": 1, "unacked_bytes": size,
        "top_hosts": [{"host": "a.example", "remaining": 4, "done_1h": 1},
                      {"host": "c.example", "remaining": 3, "done_1h": 0}],
        "updated_at": old.isoformat(),
    }
    listed = api.client.get("/v1/crawl-requests", headers=AUTH).json()["requests"]
    assert [entry["request_id"] for entry in listed] == [request_id]


def test_segments_are_listed_oldest_first_downloaded_and_acked(api):
    request_id = create(api)
    newer = write_segment(api, request_id, 0, 7, [{"id": "n"}])
    older = write_segment(api, request_id, 5, 3, [{"id": "o"}])
    os.utime(older, (time.time() - 60,) * 2)
    (api.paths.segments(request_id, 0) / (segment_name(0, 8) + ".part")).write_bytes(b"open")
    base = f"/v1/crawl-requests/{request_id}/segments"
    listed = api.client.get(base, headers=AUTH).json()["segments"]
    assert listed == [{"name": older.name, "bytes": older.stat().st_size},
                      {"name": newer.name, "bytes": newer.stat().st_size}]
    download = api.client.get(f"{base}/{older.name}", headers=AUTH)
    assert download.status_code == 200
    assert download.headers["content-type"] == "application/gzip"
    assert json.loads(gzip.decompress(download.content)) == {"id": "o"}
    for name in (segment_name(0, 8) + ".part", "00-1.jsonl.gz", segment_name(1, 7), "..%2F..%2Ftokens.yml"):
        assert api.client.get(f"{base}/{name}", headers=AUTH).status_code == 404

    ack = f"/v1/crawl-requests/{request_id}/ack"
    response = api.client.post(ack, json={"segments": [older.name, older.name, "bogus", segment_name(9, 9)]},
                               headers=AUTH)
    assert response.json() == {"deleted": 1}
    assert not older.exists() and newer.exists()
    assert api.client.post(ack, json={"segments": [older.name]}, headers=AUTH).json() == {"deleted": 0}
    assert [s["name"] for s in api.client.get(base, headers=AUTH).json()["segments"]] == [newer.name]


def test_segments_stay_listable_and_ackable_after_delete(api):
    request_id = create(api)
    path = write_segment(api, request_id, 2, 1)
    assert api.client.delete(f"/v1/crawl-requests/{request_id}", headers=AUTH).status_code == 202
    assert api.client.get(f"/v1/crawl-requests/{request_id}", headers=AUTH).json()["cancelled"] is True
    base = f"/v1/crawl-requests/{request_id}/segments"
    assert [s["name"] for s in api.client.get(base, headers=AUTH).json()["segments"]] == [path.name]
    assert api.client.get(f"{base}/{path.name}", headers=AUTH).status_code == 200
    assert api.client.post(f"/v1/crawl-requests/{request_id}/ack", json={"segments": [path.name]},
                           headers=AUTH).json() == {"deleted": 1}


def test_long_poll_returns_when_a_segment_closes(api):
    request_id = create(api)
    base = f"/v1/crawl-requests/{request_id}/segments"
    started = time.monotonic()
    assert api.client.get(base + "?wait=0.1", headers=AUTH).json() == {"segments": []}
    assert time.monotonic() - started >= 0.1
    closer = Thread(target=lambda: (time.sleep(0.2), write_segment(api, request_id, 0, 1)))
    closer.start()
    started = time.monotonic()
    listed = api.client.get(base + "?wait=30", headers=AUTH).json()["segments"]
    closer.join()
    assert [s["name"] for s in listed] == [segment_name(0, 1)]
    assert time.monotonic() - started < 5
    assert api.client.get(base + "?wait=-1", headers=AUTH).status_code == 400


def test_gc_removes_expired_requests_after_grace(api):
    request_id = create(api)
    post_urls(api, request_id, [item("a", "https://example.com/")])
    write_segment(api, request_id, 0, 1)
    expires = datetime.fromisoformat(json.loads(api.paths.meta(request_id).read_text())["expires_at"])
    assert api.service.collect_garbage(now=expires + timedelta(days=2)) == 0
    assert api.client.get(f"/v1/crawl-requests/{request_id}/segments", headers=AUTH).json()["segments"]
    assert api.service.collect_garbage(now=expires + timedelta(days=3)) == 1
    assert not api.paths.meta(request_id).exists()
    assert not api.paths.segments(request_id).exists()
    assert not api.paths.request_dir(request_id).exists()
    assert not inbox_files(api)
    assert api.client.get(f"/v1/crawl-requests/{request_id}", headers=AUTH).status_code == 404


def test_gc_removes_cancelled_requests_only_once_drained(api):
    request_id = create(api)
    keep = create(api)
    segment = write_segment(api, request_id, 0, 1)
    api.client.delete(f"/v1/crawl-requests/{request_id}", headers=AUTH)
    os.utime(api.paths.cancelled(request_id), (time.time() - 10,) * 2)
    snapshot(api, 0, {request_id: {"queued": 0, "leased": 0, "top_hosts": []}})
    assert api.service.collect_garbage() == 0, "Unacked segment"
    segment.unlink()
    snapshot(api, 1, {request_id: {"queued": 0, "leased": 1, "top_hosts": []}})
    assert api.service.collect_garbage() == 0, "Crawler 1 still has a leased URL"
    snapshot(api, 1, {}, updated_at=datetime.now(UTC) - timedelta(seconds=60))
    assert api.service.collect_garbage() == 0, "Crawler 1's snapshot predates the cancel"
    snapshot(api, 1, {})
    (api.paths.segments(request_id, 1) / (segment_name(1, 1) + ".part")).parent.mkdir(parents=True)
    (api.paths.segments(request_id, 1) / (segment_name(1, 1) + ".part")).write_bytes(b"open")
    assert api.service.collect_garbage() == 0, "An open segment still collects cancelled records"
    (api.paths.segments(request_id, 1) / (segment_name(1, 1) + ".part")).unlink()
    assert api.service.collect_garbage() == 1
    assert not api.paths.meta(request_id).exists() and not api.paths.cancelled(request_id).exists()
    assert api.paths.meta(keep).exists()


def test_gc_removes_orphaned_directories(api):
    orphan = "f" * 32
    write_segment(api, orphan, 0, 1)
    (api.paths.request_dir(orphan) / "batches").mkdir(parents=True)
    api.service.collect_garbage()
    assert not api.paths.segments(orphan).exists() and not api.paths.request_dir(orphan).exists()


def test_gc_cli_runs_once(api):
    from click.testing import CliRunner
    from containers.api import manual_bulk_crawl_gc

    result = CliRunner().invoke(manual_bulk_crawl_gc.main, ["--config", str(api.config_path), "--once"])
    assert result.exit_code == 0, result.output


def test_health_does_not_need_a_token(api):
    assert api.client.get("/healthz").json() == {"status": "ok"}


def test_default_app_routes_with_its_own_config(api, monkeypatch):
    config = yaml.safe_load(api.config_path.read_text())
    router = Mock(return_value=lambda urls: [2] * len(urls))
    monkeypatch.setattr(app_module, "ManualBulkCrawlRouter", router)
    monkeypatch.setattr("containers.api.crawl_requests.shutil.disk_usage", lambda path: FREE)
    with TestClient(create_app(str(api.config_path))) as client:
        request_id = client.post("/v1/crawl-requests", json={"name": "x"}, headers=AUTH).json()["request_id"]
        assert post_urls(SimpleNamespace(client=client), request_id,
                         [item("a", "https://example.com/")]).status_code == 202
    router.assert_called_once_with(config)
    assert api.paths.inbox_file(2, request_id, batch_key("b1")).exists()


@pytest.mark.parametrize("tokens", ["[]", "{token: 1}", "{'': alice}"])
def test_invalid_tokens_file_fails_at_startup(api, tokens):
    (api.root / "tokens.yml").write_text(tokens)
    with pytest.raises(ValueError, match="tokens_file"):
        create_app(str(api.config_path), service=api.service)


@pytest.mark.parametrize("name,value", [("max_urls", 0), ("max_url_length", 2501), ("root", "relative"),
                                        ("default_expires_days", 61), ("retry_after_s", True)])
def test_invalid_crawl_request_configuration_fails_at_startup(tmp_path, name, value):
    with pytest.raises(ValueError, match=f"crawl_requests.{name}"):
        CrawlRequestService(crawl_config(tmp_path, **{name: value}))


@pytest.mark.parametrize("streamed", [False, True])
def test_oversized_body_returns_json_and_publishes_nothing(api, streamed):
    config = yaml.safe_load(api.config_path.read_text())
    config["max_content_length"] = 32
    api.config_path.write_text(yaml.safe_dump(config))
    request_id = create(api)
    with TestClient(create_app(str(api.config_path), service=api.service)) as client:
        content = json.dumps({"batch_id": "b", "urls": [item("a", "https://example.com/article")]}).encode()
        response = client.post(f"/v1/crawl-requests/{request_id}/urls",
                               content=iter([content[:20], content[20:]]) if streamed else content,
                               headers={**AUTH, "Content-Type": "application/json"})
    assert response.status_code == 413
    assert response.json() == {"error": "Request body too large"}
    assert not inbox_files(api)


def test_body_limit_stops_reading_chunked_upload(api):
    config = yaml.safe_load(api.config_path.read_text())
    config["max_content_length"] = 32
    api.config_path.write_text(yaml.safe_dump(config))
    app = create_app(str(api.config_path), service=api.service)
    chunks = iter([b" " * 32, b" " * 32, b'{"name":"x"}'])
    received, sent = [], []

    async def receive():
        chunk = next(chunks)
        received.append(chunk)
        return {"type": "http.request", "body": chunk, "more_body": True}

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
             "method": "POST", "scheme": "http", "path": "/v1/crawl-requests",
             "query_string": b"", "root_path": "",
             "headers": [(b"content-type", b"application/json"), (b"authorization", b"Bearer token-a")],
             "server": ("testserver", 80), "client": ("127.0.0.1", 1234)}
    asyncio.run(app(scope, receive, send))
    assert sent[0]["status"] == 413
    assert len(received) == 2


@pytest.mark.parametrize("size", [0, -1, True, 1.5, "300m", None])
def test_invalid_body_limit_fails_at_startup(api, size):
    config = yaml.safe_load(api.config_path.read_text())
    config["max_content_length"] = size
    api.config_path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match="max_content_length"):
        create_app(str(api.config_path), service=api.service)


def test_v1_routes_are_gone(api):
    assert api.client.post("/v1/manual-bulk-crawls", json={"urls": ["https://example.com"]}).status_code == 404

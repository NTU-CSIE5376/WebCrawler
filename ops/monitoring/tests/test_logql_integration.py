"""Real Loki 3 integration, opt-in; talks ONLY to an isolated loopback test server."""

import json
import os
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import pytest
from ops.monitoring import shadow
from ops.monitoring.render_alerting import LOKI_UID, build_provisioning


@pytest.fixture(scope="module")
def loki():
    base = os.environ.get("MONITORING_TEST_LOKI_URL")
    if not base:
        pytest.skip("set MONITORING_TEST_LOKI_URL to an isolated loopback Loki 3")
    parsed = urlsplit(base)
    assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1", (
        "Never send test logs to production"
    )
    assert parsed.port == 13198, "Use the dedicated monitoring test port"
    return base.rstrip("/")


def query(base, expression):
    with urlopen(
        base + "/loki/api/v1/query?" + urlencode({"query": expression}), timeout=30
    ) as response:
        result = json.load(response)
    assert result["status"] == "success", result
    return result["data"]["result"]


def rules():
    docs = build_provisioning({"expected_crawlers": ["00", "15"]})
    return {
        rule["uid"]: rule
        for group in docs["webcrawler-rules.json"]["groups"]
        for rule in group["rules"]
        if rule["data"][0]["datasourceUid"] == LOKI_UID
    }


def test_all_generated_logql_parses_and_executes(loki):
    for uid, rule in rules().items():
        try:
            query(loki, rule["data"][0]["model"]["expr"])
        except Exception as error:
            raise AssertionError(f"Loki query failed: {uid}") from error


def test_real_heartbeat_work_and_response_signals(loki):
    # Dedicated ephemeral Loki only. No URLs or source labels are required.
    now = str(time.time_ns() - 1_000_000_000)

    def stream(service, worker, event, **fields):
        return {
            "stream": {"service": service, "worker_id": str(worker), "event": event},
            "values": [[now, json.dumps({"event": event, **fields})]],
        }

    payload = {
        "streams": [
            stream("crawler", 0, "spider.heartbeat", scheduler_size=5),
            stream("crawler", 0, "request.end", status=202),
            stream("ingestor", 0, "ingest.batch_error", batch_size=100),
        ]
    }
    request = Request(
        loki + "/loki/api/v1/push",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        assert response.status == 204
    generated = rules()

    def maximum(uid):
        result = query(loki, generated[uid]["data"][0]["model"]["expr"])
        return max(float(item["value"][1]) for item in result)

    assert maximum("wc-heartbeat-0") == 0  # Alive worker.
    assert maximum("wc-heartbeat-15") == 1  # Missing worker, despite another alive.
    assert maximum("wc-no-output-0") == 1  # 202 must not count as useful success.
    assert maximum("wc-ingest-error") > 0  # Must warn even if progress later advances.
    payload = {"streams": [stream("crawler", 0, "request.end", status=200)]}
    request = Request(
        loki + "/loki/api/v1/push",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        assert response.status == 204
    assert maximum("wc-no-output-0") == 0


def test_shadow_client_queries_real_loki_at_the_requested_time(loki):
    now = time.time()
    payload = {
        "streams": [
            {
                "stream": {"event": "shadow.integration"},
                "values": [[str(int((now - 1) * 1_000_000_000)), "test fixture"]],
            }
        ]
    }
    request = Request(
        loki + "/loki/api/v1/push",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        assert response.status == 204
    client = shadow.QueryClient("http://127.0.0.1:1", loki)
    data = client.query(
        LOKI_UID,
        'sum(count_over_time({event="shadow.integration"}[2m])) or vector(0)',
        now,
    )
    assert data["resultType"] == "vector"
    assert max(float(sample["value"][1]) for sample in data["result"]) > 0

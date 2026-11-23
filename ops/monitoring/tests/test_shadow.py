from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest
from ops.monitoring import shadow
from ops.monitoring.render_alerting import PROMETHEUS_UID, _rule


def documents(*specs):
    groups = {}
    for uid, group, pending, expr in specs:
        item = groups.setdefault(
            group,
            {
                "name": "webcrawler-" + group,
                "interval": f"{shadow.GROUPS[group]}s",
                "rules": [],
            },
        )
        item["rules"].append(_rule(uid, uid, expr, pending=pending))
    return {"webcrawler-rules.json": {"groups": list(groups.values())}}


def vector(value=1, **labels):
    return {
        "resultType": "vector",
        "result": [{"metric": labels, "value": [0, str(value)]}],
    }


class FakeClient:
    def __init__(self):
        self.calls = []
        self.data = vector()
        self.error = None

    def query(self, datasource, expression, now):
        self.calls.append((datasource, expression, now))
        if self.error:
            raise self.error
        return self.data


@pytest.fixture
def journal(tmp_path):
    result = shadow.Journal(tmp_path / "private")
    yield result
    result.close()


def make(journal, specs=None, **kwargs):
    docs = documents(*(specs or [("one", "fast", "0s", "some_metric")]))
    return shadow.Evaluator(
        docs, FakeClient(), journal, clock=kwargs.pop("clock", lambda: 0), **kwargs
    )


def instance(evaluator, uid="wc-one"):
    return next(iter(evaluator.state["rules"][uid]["instances"].values()))


def test_pending_firing_recovery_and_stable_firing_count(journal):
    evaluator = make(journal, [("one", "fast", "2m", "metric")])
    assert evaluator.run_due(0) == 1
    assert instance(evaluator)["status"] == "pending"
    evaluator.run_due(60)
    assert instance(evaluator)["status"] == "pending"
    evaluator.run_due(120)
    assert instance(evaluator)["status"] == "firing"
    evaluator.run_due(180)
    assert evaluator.state["counters"]["wc-one"]["firing_episodes"] == 1
    evaluator.client.data = vector(0)
    evaluator.run_due(240)
    assert instance(evaluator)["status"] == "normal"
    assert evaluator.state["counters"]["wc-one"]["transitions"] == 3


def test_stopped_observer_report_does_not_imply_fresh_coverage(journal):
    evaluator = make(journal)
    evaluator.run_due(0)
    fresh = shadow.report(journal.directory, now=60)
    assert not fresh["observer_stale"]
    assert not fresh["rules"]["wc-one"]["sample_stale"]
    old = shadow.report(journal.directory, now=86400)
    assert old["observer_stale"]
    assert old["rules"]["wc-one"]["sample_stale"]
    assert old["rules"]["wc-one"]["last_known_instances"] == {"firing": 1}


def test_initial_firing_once_and_separate_metric_instances(journal):
    evaluator = make(journal)
    evaluator.client.data = {
        "resultType": "vector",
        "result": [
            {"metric": {"host": host}, "value": [0, "1"]} for host in ("one", "two")
        ],
    }
    evaluator.run_due(0)
    evaluator.run_due(60)
    assert len(evaluator.state["rules"]["wc-one"]["instances"]) == 2
    assert evaluator.state["counters"]["wc-one"]["firing_episodes"] == 2


def test_default_only_fast_and_heavy_groups_wait_their_interval(journal):
    specs = [
        ("one", "fast", "0s", "one"),
        ("two", "symptoms", "0s", "two"),
        ("three", "trends", "0s", "three"),
    ]
    evaluator = make(journal, specs)
    assert evaluator.run_due(0) == 1
    assert list(evaluator.rules) == ["wc-one"]
    evaluator = make(journal, specs, groups=("fast", "symptoms", "trends"))
    assert evaluator.run_due(60) == 1
    assert evaluator.run_due(300) == 2
    assert evaluator.run_due(600) == 3


def test_same_query_cached_within_round_only(journal):
    evaluator = make(
        journal, [("one", "fast", "0s", "same"), ("two", "fast", "0s", "same")]
    )
    evaluator.run_due(0)
    assert len(evaluator.client.calls) == 1
    evaluator.run_due(60)
    assert len(evaluator.client.calls) == 2


def test_restart_and_long_gap_does_not_count_unobserved_time_as_pending(journal):
    specs = [("one", "fast", "2m", "metric")]
    evaluator = make(journal, specs)
    evaluator.run_due(0)
    evaluator.run_due(60)
    resumed = make(journal, specs)
    resumed.run_due(600)
    assert instance(resumed)["status"] == "pending"
    assert instance(resumed)["since"] == 600
    resumed.run_due(660)
    resumed.run_due(720)
    assert instance(resumed)["status"] == "firing"


def test_changed_rule_does_not_inherit_old_pending_time(journal):
    evaluator = make(journal, [("one", "fast", "2m", "metric")])
    evaluator.run_due(0)
    evaluator.run_due(60)
    changed = make(
        journal, [("one", "fast", "2m", "different_metric")], clock=lambda: 120
    )
    changed.run_due(120)
    assert instance(changed)["status"] == "pending"
    assert instance(changed)["since"] == 120
    summary = shadow.report(journal.directory)
    assert summary["rules"]["wc-one"]["evaluations"] == 1
    assert summary["rules"]["wc-one"]["observed_since"] == 120
    assert summary["rules"]["wc-one"]["fingerprint"]


def test_query_errors_and_no_data_alert_immediately_and_reset_pending(journal):
    evaluator = make(journal, [("one", "fast", "10m", "metric")])
    evaluator.client.error = TimeoutError("must never record raw exception")
    evaluator.run_due(0)
    assert instance(evaluator)["status"] == "firing"
    assert instance(evaluator)["reason"] == "query_error"
    evaluator.client.error = None
    evaluator.client.data = {"resultType": "vector", "result": []}
    evaluator.run_due(60)
    assert instance(evaluator)["reason"] == "no_data"
    evaluator.client.data = vector(1)
    evaluator.run_due(120)
    assert instance(evaluator)["status"] == "pending"
    assert instance(evaluator)["since"] == 120
    assert "must never record" not in journal.path.read_text()


def test_nonfinite_and_bad_metrics_are_query_errors(journal):
    evaluator = make(journal)
    evaluator.client.data = vector("NaN")
    evaluator.run_due(0)
    assert instance(evaluator)["reason"] == "query_error"
    evaluator.client.data = vector(1, worker=42)
    evaluator.run_due(60)
    # Rule labels override metric fields just as they do in Grafana.
    assert instance(evaluator)["reason"] == "query_error"


def test_round_budget_records_unknown_and_oldest_skipped_goes_first_next_round(journal):
    mono = [0]
    evaluator = make(
        journal,
        [("one", "fast", "0s", "one"), ("two", "fast", "0s", "two")],
        monotonic=lambda: mono[0],
        round_budget_seconds=1,
    )
    original = evaluator.client.query

    def slow(*args):
        result = original(*args)
        mono[0] += 2
        return result

    evaluator.client.query = slow
    evaluator.run_due(0)
    assert evaluator.state["rules"]["wc-two"]["coverage"] == "unknown"
    assert evaluator.state["rules"]["wc-two"]["instances"] == {}
    assert evaluator.state["counters"]["wc-two"]["skipped"] == 1
    evaluator.run_due(60)
    assert evaluator.client.calls[-1][1] == "two"
    assert instance(evaluator, "wc-two")["status"] == "firing"


def test_skipped_coverage_restarts_pending_even_without_a_large_time_gap(journal):
    evaluator = make(journal, [("one", "fast", "2m", "metric")], round_budget_seconds=1)
    evaluator.run_due(0)
    ticks = iter((0, 2))
    evaluator.monotonic = lambda: next(ticks)
    evaluator.run_due(60)
    assert instance(evaluator)["status"] == "pending"
    assert evaluator.state["rules"]["wc-one"]["coverage"] == "unknown"
    evaluator.monotonic = lambda: 0
    evaluator.run_due(61)
    assert instance(evaluator)["status"] == "pending"
    assert instance(evaluator)["since"] == 61


def test_skipped_firing_instance_is_not_resolved_or_counted_as_a_new_incident(journal):
    evaluator = make(journal, round_budget_seconds=1)
    evaluator.client.data = vector(1, host="host1")
    evaluator.run_due(0)
    ticks = iter((0, 2))
    evaluator.monotonic = lambda: next(ticks)
    evaluator.run_due(60)
    unknown = shadow.report(journal.directory)["rules"]["wc-one"]
    assert unknown["coverage"] == "unknown"
    assert unknown["last_known_instances"] == {"firing": 1}
    evaluator.monotonic = lambda: 0
    evaluator.run_due(61)
    assert instance(evaluator)["status"] == "firing"
    assert evaluator.state["counters"]["wc-one"]["firing_episodes"] == 1
    events = [json.loads(line) for line in journal.path.read_text().splitlines()]
    assert not any(event.get("to") == "missing" for event in events)


def test_report_explicitly_excludes_historical_groups_not_currently_observed(journal):
    specs = [("one", "fast", "0s", "one"), ("two", "trends", "0s", "two")]
    both = make(journal, specs, groups=("fast", "trends"))
    both.run_due(0)
    both.run_due(600)
    assert "wc-two" in shadow.report(journal.directory)["rules"]
    fast_only = make(journal, specs, clock=lambda: 660)
    fast_only.run_due(660)
    summary = shadow.report(journal.directory)
    assert summary["active_groups"] == ["fast"]
    assert summary["active_rule_count"] == 1
    assert summary["excluded_historical_rules"] == ["wc-two"]
    assert set(summary["rules"]) == {"wc-one"}
    resumed = make(journal, specs, groups=("fast", "trends"), clock=lambda: 720)
    resumed.run_due(720)
    assert resumed.state["rules"]["wc-two"]["observed_since"] == 720
    assert resumed.state["counters"]["wc-two"] == {}


def test_overlap_and_two_processes_same_directory_are_rejected(journal):
    evaluator = make(journal)
    evaluator._lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            evaluator.run_due(0)
    finally:
        evaluator._lock.release()
    with pytest.raises(ValueError, match="another observer"):
        shadow.Journal(journal.directory)


def test_disappearing_series_is_missing_not_healthy(journal):
    evaluator = make(journal)
    evaluator.client.data = vector(1, host="host1")
    evaluator.run_due(0)
    evaluator.client.data = vector(0, host="host2")
    evaluator.run_due(60)
    events = [json.loads(line) for line in journal.path.read_text().splitlines()]
    assert any(row.get("to") == "missing" for row in events)


def test_journal_rotation_preserves_counters_and_report_has_no_http(tmp_path):
    with patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "MUST-NOT-BE-READ"}):
        journal = shadow.Journal(tmp_path / "private", max_bytes=1024)
        evaluator = make(journal)
        try:
            for stamp in range(0, 1200, 60):
                evaluator.run_due(stamp)
            assert (journal.directory / "journal.jsonl.2").exists()
            for path in journal.directory.glob("journal.jsonl*"):
                assert path.stat().st_size <= 1024
                assert path.stat().st_mode & 0o077 == 0
            with patch.object(
                shadow.QueryClient, "query", side_effect=AssertionError("no HTTP")
            ):
                data = shadow.report(journal.directory)
            assert data["rules"]["wc-one"]["evaluations"] == 20
            assert data["rules"]["wc-one"]["firing_episodes"] == 1
            assert "MUST-NOT" not in json.dumps(data)
        finally:
            journal.close()


def test_symlink_and_public_output_directory_rejected(tmp_path):
    public = tmp_path / "public"
    public.mkdir(mode=0o755)
    with pytest.raises(ValueError, match="private"):
        shadow.Journal(public)
    symlink = tmp_path / "link"
    symlink.symlink_to(public)
    with pytest.raises(ValueError, match="private"):
        shadow.Journal(symlink)


@pytest.mark.parametrize(
    "url",
    [
        "https://discord.com:443",
        "http://user:pass@localhost:9090",
        "http://127.0.0.1:9090/path",
        "http://127.0.0.1:9090?q=secret",
        "http://8.8.8.8:80",
        "ftp://localhost:80",
        "http://localhost",
        "http://localhost:9090#fragment",
    ],
)
def test_public_or_credentialed_endpoint_rejected(url):
    with pytest.raises(ValueError, match="private metrics"):
        shadow.base_url(url)


@pytest.fixture
def server():
    responses = {
        "body": json.dumps({"status": "success", "data": vector()}).encode(),
        "code": 200,
        "requests": [],
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            responses["requests"].append(self.path)
            self.send_response(responses["code"])
            if responses["code"] == 302:
                self.send_header("Location", "/must-not-follow")
            self.end_headers()
            self.wfile.write(responses["body"])

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}", responses
    httpd.shutdown()
    httpd.server_close()
    thread.join()


def test_http_uses_only_instant_query_get_and_ignores_proxies(server):
    url, replies = server
    with patch.dict(os.environ, {"http_proxy": "http://invalid.invalid:1"}):
        client = shadow.QueryClient(url, url)
        assert client.query(PROMETHEUS_UID, "some_metric", 123)["result"]
    path = urlsplit(replies["requests"][0])
    assert path.path == "/api/v1/query"
    assert parse_qs(path.query) == {
        "query": ["some_metric"],
        "time": ["123"],
        "timeout": ["5s"],
    }


def test_redirect_malformed_oversized_and_failed_responses_rejected(server):
    url, replies = server
    client = shadow.QueryClient(url, url)
    replies["code"] = 302
    with pytest.raises(shadow.HTTPError):
        client.query(PROMETHEUS_UID, "x", 0)
    assert len(replies["requests"]) == 1
    replies["code"] = 200
    for body in (
        b"not json",
        b"null",
        b"x" * (shadow.MAX_RESPONSE_BYTES + 1),
        b'{"status":"error"}',
        b'{"status":"success","data":{"resultType":"matrix","result":[]}}',
    ):
        replies["body"] = body
        with pytest.raises(ValueError):
            client.query(PROMETHEUS_UID, "x", 0)


def test_local_report_cli_requires_no_query_configuration(journal, capsys):
    evaluator = make(journal)
    evaluator.run_due(0)
    assert shadow.main(["--report", "--output-dir", str(journal.directory)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["rules"]["wc-one"]["evaluations"] == 1

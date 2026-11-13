"""Safety and signal-contract tests for the opt-in Grafana 11 renderer."""

from __future__ import annotations

import importlib.util
import json
import stat
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "render_alerting.py"
SPEC = importlib.util.spec_from_file_location("render_alerting", MODULE_PATH)
renderer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = renderer
SPEC.loader.exec_module(renderer)

CONFIG = {"expected_crawlers": ["00", "15"]}


def test_costly_queries_are_separated_from_fast_incident_checks():
    groups = renderer.build_provisioning(CONFIG)["webcrawler-rules.json"]["groups"]
    by_rule = {
        rule["uid"]: (group["name"], group["interval"])
        for group in groups
        for rule in group["rules"]
    }
    assert by_rule["wc-output-drop"] == ("webcrawler-trends", "600s")
    assert by_rule["wc-no-output-0"] == ("webcrawler-symptoms", "300s")
    assert by_rule["wc-no-output-15"] == ("webcrawler-symptoms", "300s")
    for uid in (
        "wc-target-down",
        "wc-ingest-error",
        "wc-heartbeat-0",
        "wc-pipeline-lag",
        "wc-loki-query-health",
        "wc-prometheus-query-health",
    ):
        assert by_rule[uid] == ("webcrawler-fast", "60s")


def webhook() -> str:
    # Deliberately synthetic; no real Discord credential is used or contacted.
    return "https://discord.com/api/" + "webhooks/123456789/" + "x" * 24


def rules(documents: dict) -> dict:
    return {
        rule["uid"]: rule
        for group in documents["webcrawler-rules.json"]["groups"]
        for rule in group["rules"]
    }


def test_default_output_is_paused_and_cannot_replace_existing_notification_policy():
    documents = renderer.build_provisioning(CONFIG)
    assert list(documents) == ["webcrawler-rules.json"]
    assert all(rule["isPaused"] for rule in rules(documents).values())
    assert all(
        "policies" not in doc and "resetPolicies" not in doc
        for doc in documents.values()
    )
    assert "contactPoints" not in documents["webcrawler-rules.json"]


def test_enable_requires_both_secret_and_acknowledged_project_route():
    with pytest.raises(ValueError, match="Enabling requires"):
        renderer.build_provisioning(CONFIG, enabled=True, routing_confirmed=True)
    with pytest.raises(ValueError, match="Enabling requires"):
        renderer.build_provisioning(CONFIG, enabled=True, webhook=webhook())
    documents = renderer.build_provisioning(
        CONFIG,
        enabled=True,
        webhook=webhook(),
        routing_confirmed=True,
        enabled_rule_uids=["wc-ingest-error", "wc-heartbeat-0"],
    )
    assert {uid for uid, rule in rules(documents).items() if not rule["isPaused"]} == {
        "wc-ingest-error",
        "wc-heartbeat-0",
    }
    contact = documents["webcrawler-contactpoints.json"]["contactPoints"][0]
    assert contact["name"] == "webcrawler-discord"
    assert contact["receivers"][0]["type"] == "discord"
    assert contact["receivers"][0]["settings"]["url"] == webhook()
    assert webhook() not in json.dumps(documents["webcrawler-rules.json"])
    message = contact["receivers"][0]["settings"]["message"]
    assert "lt $i 6" in message and "172.16.191.2:3000/alerting/list" in message
    assert "Labels.host" in message and "Labels.mountpoint" in message
    assert "Labels.stage" in message
    assert "default.message" not in message and "Annotations.description" not in message


def test_enabling_cannot_accidentally_unpause_all_rules():
    with pytest.raises(ValueError, match="explicit"):
        renderer.build_provisioning(
            CONFIG, enabled=True, webhook=webhook(), routing_confirmed=True
        )
    with pytest.raises(ValueError, match="unknown"):
        renderer.build_provisioning(
            CONFIG,
            enabled=True,
            webhook=webhook(),
            routing_confirmed=True,
            enabled_rule_uids=["wc-does-not-exist"],
        )
    with pytest.raises(ValueError, match="enable"):
        renderer.build_provisioning(CONFIG, enabled_rule_uids=["wc-ingest-error"])


@pytest.mark.parametrize(
    "value",
    [
        "http://discord.com/api/webhooks/1/" + "x" * 24,
        "https://attacker.invalid/api/webhooks/1/" + "x" * 24,
        "https://discord.com@attacker.invalid/api/webhooks/1/" + "x" * 24,
        "https://discord.com:9999/api/webhooks/1/" + "x" * 24,
        "https://discord.com/api/webhooks/1/short",
        "https://discord.com/api/webhooks/1/" + "x" * 24 + "?secret=bad",
        "https://discord.com:notaport/api/webhooks/1/" + "x" * 24,
    ],
)
def test_bad_webhook_is_rejected_without_echoing_value(value):
    with pytest.raises(ValueError) as result:
        renderer.validate_webhook(value)
    assert value not in str(result.value)


def test_grafana_models_keep_dimensions_and_have_correct_datasource_refs():
    generated = rules(renderer.build_provisioning(CONFIG))
    assert len(generated) == len(set(generated))
    for uid, rule in generated.items():
        assert len(uid) <= 40
        assert rule["condition"] == "C"
        query, reduce, threshold = rule["data"]
        assert query["datasourceUid"] in {"loki", "crawler-prometheus"}
        assert query["model"]["datasource"]["uid"] == query["datasourceUid"]
        assert query["model"]["instant"] is True
        assert reduce["model"]["expression"] == "A"
        assert reduce["model"]["type"] == "reduce"
        assert reduce["model"]["reducer"] == "last"
        # Grafana 11 rejects settings.mode="strict"; strict is the default.
        assert reduce["model"].get("settings", {}) == {}
        assert threshold["model"]["expression"] == "B"
        assert threshold["model"]["type"] == "threshold"
        assert threshold["model"]["conditions"][0]["evaluator"]["type"] == "gt"
        assert rule["noDataState"] == "Alerting"
        if uid not in {"wc-loki-query-health", "wc-prometheus-query-health"}:
            assert rule["execErrState"] == "KeepLast"
        assert rule["labels"]["project"] == "webcrawler"


def test_each_expected_worker_has_explicit_absence_rule_even_when_others_are_alive():
    generated = rules(renderer.build_provisioning(CONFIG))
    for log_worker, display in [("0", "00"), ("15", "15")]:
        rule = generated[f"wc-heartbeat-{log_worker}"]
        expression = rule["data"][0]["model"]["expr"]
        assert f'worker_id="{log_worker}"' in expression
        assert "absent_over_time(" in expression and "[2m]" in expression
        assert "or vector(0)" in expression
        # Require repeated absence after the lookback; a single evaluation must
        # not turn a brief logging interruption into a stopped-worker incident.
        assert rule["for"] == "2m"
        assert "連續" in rule["title"]
        assert "連續" in rule["annotations"]["description"]
        assert rule["labels"]["worker"] == display
    assert "wc-heartbeat-1" not in generated


def test_query_failures_raise_monitoring_alerts_instead_of_business_failures():
    generated = rules(renderer.build_provisioning(CONFIG))
    for uid, datasource in (
        ("wc-loki-query-health", "loki"),
        ("wc-prometheus-query-health", "crawler-prometheus"),
    ):
        rule = generated[uid]
        query = rule["data"][0]
        assert query["datasourceUid"] == datasource
        assert query["model"]["expr"] == "vector(0)"
        assert rule["execErrState"] == "Alerting"
        assert rule["labels"]["severity"] == "critical"
        assert rule["labels"]["service"] == "monitoring"
        assert "監控查詢失敗" in rule["title"]
        assert "不代表" in rule["annotations"]["description"]
        assert rule["isPaused"] is True


def test_output_alert_uses_completed_responses_and_ready_work_not_admission_or_ghost_pending():
    generated = rules(renderer.build_provisioning(CONFIG))
    for uid in ["wc-no-output-0", "wc-output-drop"]:
        rule = generated[uid]
        expression = rule["data"][0]["model"]["expr"]
        assert 'event="request.end"' in expression
        assert "status != 202" in expression
        assert "scheduler_size > 0" in expression
        assert "request.start" not in expression
        assert "pending >" not in expression
        assert "manual_active" not in expression and "source=" not in expression
        assert "[5m]" in expression
        assert (
            "代理指標" in rule["annotations"]["description"]
            or "不是" in rule["annotations"]["description"]
        )
    assert "[30m] offset 5m" in generated["wc-output-drop"]["data"][0]["model"]["expr"]


def test_sparse_error_events_have_zero_fallback_but_scan_failures_are_explicit():
    generated = rules(renderer.build_provisioning(CONFIG))
    ingest = generated["wc-ingest-error"]
    assert ingest["for"] == "0s" and ingest["labels"]["severity"] == "critical"
    assert "or vector(0)" in ingest["data"][0]["model"]["expr"]
    assert "不代表資料已補回" in ingest["annotations"]["description"]
    assert (
        "crawler_pipeline_scan_success"
        in generated["wc-pipeline-scan"]["data"][0]["model"]["expr"]
    )
    assert (
        "crawler_pipeline_oldest_ready_lag_seconds"
        in generated["wc-pipeline-lag"]["data"][0]["model"]["expr"]
    )


def test_files_are_private_and_existing_output_is_never_overwritten(tmp_path):
    output = tmp_path / "runtime"
    documents = renderer.build_provisioning(CONFIG, webhook=webhook())
    renderer.write_documents(output, documents)
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    for path in output.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        json.loads(path.read_text())
    marker = output / "keep-hotfix"
    marker.write_text("preserve")
    with pytest.raises(FileExistsError):
        renderer.write_documents(output, documents)
    assert marker.read_text() == "preserve"


def test_symlink_output_is_not_followed(tmp_path):
    destination = tmp_path / "live"
    destination.mkdir()
    output = tmp_path / "runtime"
    output.symlink_to(destination, target_is_directory=True)
    with pytest.raises(FileExistsError):
        renderer.write_documents(output, renderer.build_provisioning(CONFIG))
    assert not list(destination.iterdir())


def test_cli_does_not_echo_secret_in_failure_or_success(tmp_path, monkeypatch, capsys):
    config = tmp_path / "config.json"
    config.write_text(json.dumps(CONFIG))
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", webhook())
    assert (
        renderer.main(["--config", str(config), "--output", str(tmp_path / "private")])
        == 0
    )
    output = capsys.readouterr()
    assert webhook() not in output.out + output.err
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "sensitive-invalid-credential")
    assert (
        renderer.main(["--config", str(config), "--output", str(tmp_path / "other")])
        == 1
    )
    output = capsys.readouterr()
    assert "sensitive-invalid-credential" not in output.out + output.err
    assert not (tmp_path / "other").exists()


def test_cli_requires_individually_reviewed_rules_before_enabling(
    tmp_path, monkeypatch
):
    config = tmp_path / "config.json"
    config.write_text(json.dumps(CONFIG))
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", webhook())
    output = tmp_path / "selected"
    args = [
        "--config",
        str(config),
        "--output",
        str(output),
        "--enable",
        "--routing-confirmed",
    ]
    assert renderer.main(args) == 1
    assert not output.exists()
    assert renderer.main([*args, "--enable-rule", "wc-ingest-error"]) == 0
    groups = json.loads((output / "webcrawler-rules.json").read_text())["groups"]
    assert [
        rule["uid"]
        for group in groups
        for rule in group["rules"]
        if not rule["isPaused"]
    ] == ["wc-ingest-error"]


@pytest.mark.parametrize("workers", [[], ["00", "0"], [0], ["arbitrary-domain"], "00"])
def test_worker_allowlist_prevents_accidental_missing_rules_or_cardinality_growth(
    workers,
):
    with pytest.raises(ValueError):
        renderer.build_provisioning({"expected_crawlers": workers})


def test_golden_alerts_are_opt_in_and_keep_known_skip_baseline():
    assert not any(
        uid.startswith("wc-golden")
        for uid in rules(renderer.build_provisioning(CONFIG))
    )
    configured = {
        **CONFIG,
        "golden_inject": {"enabled": True, "baseline_skipped_count": 179},
    }
    generated = rules(renderer.build_provisioning(configured))
    assert {uid for uid in generated if uid.startswith("wc-golden")} == {
        "wc-golden-read",
        "wc-golden-overdue",
        "wc-golden-stalled",
        "wc-golden-skipped",
    }
    skipped = generated["wc-golden-skipped"]
    assert skipped["data"][2]["model"]["conditions"][0]["evaluator"]["params"] == [179]
    assert generated["wc-golden-overdue"]["labels"]["severity"] == "critical"
    assert (
        "crawler_golden_scan_success"
        in generated["wc-golden-read"]["data"][0]["model"]["expr"]
    )


def test_disk_forecast_has_history_and_low_capacity_guards():
    generated = rules(renderer.build_provisioning(CONFIG))
    expression = generated["wc-disk-forecast"]["data"][0]["model"]["expr"]
    assert "predict_linear(" in expression and "[6h], 86400" in expression
    assert "present_over_time(" in expression and "offset 2h" in expression
    assert "< bool 0.30" in expression
    assert expression.endswith("or vector(0)")
    assert (
        "node_vmstat_oom_kill" in generated["wc-host-oom"]["data"][0]["model"]["expr"]
    )
    assert generated["wc-host-oom"]["for"] == "0s"


def test_global_volume_rule_uses_only_expected_workers():
    expression = rules(renderer.build_provisioning(CONFIG))["wc-output-drop"]["data"][
        0
    ]["model"]["expr"]
    assert 'worker_id=~"0|15"' in expression
    assert '{service="crawler",event=' not in expression


@pytest.mark.parametrize(
    "config",
    [
        [],
        None,
        {**CONFIG, "org_id": 0},
        {**CONFIG, "golden_inject": []},
        {**CONFIG, "golden_inject": {"enabled": "false"}},
        {**CONFIG, "golden_inject": {"enabled": True, "baseline_skipped_count": -1}},
    ],
)
def test_invalid_config_is_rejected_before_output(config):
    with pytest.raises((ValueError, TypeError)):
        renderer.build_provisioning(config)


def test_ingest_event_keeps_immediate_detection_with_separate_quiet_notifications():
    from urllib.parse import parse_qs, urlparse

    docs = renderer.build_provisioning(CONFIG, webhook=webhook())
    ingest = rules(docs)["wc-ingest-error"]
    assert "[1h]" in ingest["data"][0]["model"]["expr"] and ingest["for"] == "0s"
    points = {
        p["name"]: p["receivers"][0]
        for p in docs["webcrawler-contactpoints.json"]["contactPoints"]
    }
    assert points["webcrawler-discord"]["disableResolveMessage"] is False
    assert points["webcrawler-ingestor-discord"]["disableResolveMessage"] is True
    query = parse_qs(urlparse(ingest["annotations"]["logs_url"]).query)
    pane = json.loads(query["panes"][0])["logs"]
    assert (
        pane["datasource"] == "loki"
        and pane["queries"][0]["expr"] == renderer.INGEST_ERROR_QUERY
    )
    assert (
        "StartsAt.Add -3600000000000"
        in points["webcrawler-ingestor-discord"]["settings"]["message"]
    )
    assert "annotations" not in points["webcrawler-ingestor-discord"]["settings"]


def test_ingest_route_is_scoped_and_preserves_warning_delivery_policy():
    route = json.loads(
        (MODULE_PATH.parent / "notification-route.example.json").read_text()
    )
    assert route["object_matchers"] == [["project", "=", "webcrawler"]]
    ingest, warning = route["routes"]
    assert ingest["object_matchers"] == [
        ["service", "=", "ingestor"],
        ["severity", "=", "critical"],
    ]
    assert ingest["group_interval"] == "30m" and ingest["repeat_interval"] == "2h"
    assert (
        warning["receiver"] == "webcrawler-discord"
        and warning["group_interval"] == "1h"
    )
    assert warning["repeat_interval"] == "24h"

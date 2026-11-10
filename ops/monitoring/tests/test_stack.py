"""Keep the opt-in stack isolated from production hotfixes."""

import json
import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_prometheus_targets_and_grafana_datasource_agree():
    prom = yaml.safe_load((ROOT / "prometheus.yml").read_text())
    jobs = {item["job_name"]: item for item in prom["scrape_configs"]}
    assert set(jobs) == {
        "crawler-prometheus",
        "crawler-node",
        "crawler-pipeline",
        "crawler-probe",
    }
    assert "alerting" not in prom
    assert "rule_files" not in prom
    for filename in ("nodes.json", "probes.json"):
        groups = json.loads((ROOT / "targets" / filename).read_text())
        assert groups and all(
            group["targets"] and group["labels"]["host"] for group in groups
        )
        assert len({target for group in groups for target in group["targets"]}) == sum(
            len(group["targets"]) for group in groups
        )
    datasource = yaml.safe_load((ROOT / "grafana-datasource.yml").read_text())[
        "datasources"
    ][0]
    assert datasource["uid"] == "crawler-prometheus"
    assert datasource["isDefault"] is False
    assert datasource["url"] == "http://crawler-prometheus:9090"


def test_stack_cannot_restart_business_services_or_mount_docker_socket():
    stack = yaml.safe_load((ROOT / "compose.yml").read_text())
    assert stack["name"] == "crawler-monitoring"
    assert set(stack["services"]) == {
        "crawler-prometheus",
        "crawler-pipeline",
        "crawler-blackbox",
    }
    assert stack["networks"]["crawler"]["external"] is True
    for service in stack["services"].values():
        assert "depends_on" not in service
        assert "privileged" not in service
        assert "docker.sock" not in str(service)
        assert "latest" not in service["image"]
    pipeline = stack["services"]["crawler-pipeline"]
    assert "/data/ipc:/data/ipc:ro" in pipeline["volumes"]
    assert pipeline["read_only"] is True
    assert pipeline["cap_drop"] == ["ALL"]
    assert stack["services"]["crawler-prometheus"]["ports"][0].startswith("127.0.0.1:")


def test_host_exporter_observes_host_and_excludes_virtual_filesystems():
    node = yaml.safe_load((ROOT / "compose.node.yml").read_text())["services"][
        "crawler-node"
    ]
    assert node["network_mode"] == "host"
    assert node["pid"] == "host"
    assert "/:/host:ro,rslave" in node["volumes"]
    assert "--path.rootfs=/host" in node["command"]
    assert any("HOST_METRICS_BIND:?" in flag for flag in node["command"])


def test_compose_resolves_without_loading_production_overrides():
    # `config` parses locally; it never connects to a Docker daemon.
    env = {
        **os.environ,
        "CRAWLER_DOCKER_NETWORK": "isolated-test-network",
        "MONITOR_UID": "1000",
        "MONITOR_GID": "1000",
        "HOST_METRICS_BIND": "127.0.0.1",
    }
    for filename in ("compose.yml", "compose.node.yml"):
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(ROOT / filename),
                "config",
                "--format",
                "json",
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        config = json.loads(result.stdout)
        assert all(
            "docker.sock" not in str(item) for item in config["services"].values()
        )


def test_example_worker_manifest_matches_both_consumers():
    config = json.loads((ROOT / "monitoring.example.json").read_text())
    expected = {int(worker) for worker in config["expected_crawlers"]}
    assert len(expected) == 16
    assert {stage["name"] for stage in config["stages"]} == {
        "router",
        "ingestor",
        "extractor",
    }
    for stage in config["stages"]:
        assert set(stage["workers"]) == expected
        assert stage["interval_minutes"] == 10

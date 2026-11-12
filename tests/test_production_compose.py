"""The production entry point reads main policy and scopes every mutation."""
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ("crawler", "api", "scheduler_control", "scheduler_ingest", "sitemap_patroller")
IMAGE_KEYS = {service: f"WEBCRAWLER_{service.upper()}_IMAGE" for service in SERVICES}


def test_root_compose_cannot_revive_baked_supervisor_configuration():
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    for service in SERVICES:
        config = services[service]
        assert config["command"] == [
            "/usr/bin/supervisord", "-n", "-c", "/app/ops/production/supervisord.conf"
        ]
        environment = config["environment"]
        if isinstance(environment, list):
            environment = dict(item.split("=", 1) for item in environment)
        assert environment["WEBCRAWLER_SUPERVISOR_CONFIG"] == (
            f"/app/containers/{service}/supervisord.conf"
        )
    assert services["postgres"]["profiles"] == ["legacy-postgres"]
    assert services["loki"]["profiles"] == ["infrastructure"]
    assert services["grafana"]["profiles"] == ["infrastructure"]


@pytest.fixture
def binding_file(tmp_path):
    credential_file = tmp_path / "credentials.json"
    credential_file.write_text(json.dumps({"username": "fixture-user", "password": "fixture-password"}))
    path = tmp_path / "runtime.env"
    values = {key: "sha256:" + str(index) * 64 for index, key in enumerate(IMAGE_KEYS.values(), 1)}
    values["CRAWLER_API_DB_CREDENTIALS_FILE"] = str(credential_file)
    path.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
    return path, values


@pytest.fixture
def docker_recorder(tmp_path):
    directory = tmp_path / "bin"
    directory.mkdir()
    capture = tmp_path / "docker-call.json"
    executable = directory / "docker"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json,os,sys\n"
        "from pathlib import Path\n"
        "names=['API_PORT','API_BIND_ADDRESS','MANUAL_BULK_CRAWL_DATA_DIR',"
        "'MANUAL_BULK_CRAWL_SCHEDULING_POLICY','COMPOSE_PROFILES',"
        "'WEBCRAWLER_CRAWLER_IMAGE','CRAWLER_API_DB_CREDENTIALS_FILE']\n"
        "Path(os.environ['PRODUCTION_TEST_CAPTURE']).write_text(json.dumps("
        "{'args':sys.argv[1:],'ambient':{name:os.environ.get(name) for name in names}}))\n"
    )
    executable.chmod(0o755)
    return directory, capture


def run_wrapper(binding_file, *args, root=ROOT, extra_env=None):
    env = dict(os.environ, **(extra_env or {}))
    return subprocess.run(
        [str(root / "ops/production/compose.sh"), str(binding_file), *args],
        capture_output=True, text=True, env=env, check=False,
    )


def test_default_is_read_only_and_uses_explicit_files(binding_file, docker_recorder):
    path, _ = binding_file
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}",
        "PRODUCTION_TEST_CAPTURE": str(capture),
        "API_PORT": "12345", "API_BIND_ADDRESS": "0.0.0.0",
        "MANUAL_BULK_CRAWL_DATA_DIR": "/wrong-data",
        "MANUAL_BULK_CRAWL_SCHEDULING_POLICY": "manual_first",
        "COMPOSE_PROFILES": "infrastructure,legacy-postgres",
        "WEBCRAWLER_CRAWLER_IMAGE": "unexpected:latest",
        "CRAWLER_API_DB_CREDENTIALS_FILE": "/wrong-secret",
    })
    assert result.returncode == 0, result.stderr
    record = json.loads(capture.read_text())
    assert record["args"] == [
        "compose", "--project-name", "webcrawler", "--project-directory", str(ROOT),
        "--env-file", str(path), "-f", str(ROOT / "docker-compose.yml"),
        "-f", str(ROOT / "ops/production/compose.yml"), "config",
    ]
    assert all(value is None for value in record["ambient"].values())


@pytest.mark.parametrize("operation,tail", [
    ("up", ["up", "--detach", "--no-build", "--pull", "never", "--no-deps", "--", "api"]),
    ("recreate", ["up", "--detach", "--no-build", "--pull", "never", "--no-deps", "--force-recreate", "--", "api"]),
    ("stop", ["stop", "--", "api"]),
])
def test_mutations_are_explicit_and_never_build_or_pull(binding_file, docker_recorder, operation, tail):
    path, _ = binding_file
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, operation, "api", extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "PRODUCTION_TEST_CAPTURE": str(capture),
    })
    assert result.returncode == 0, result.stderr
    assert json.loads(capture.read_text())["args"][-len(tail):] == tail


@pytest.mark.parametrize("args", [
    ("up",), ("recreate",), ("stop",), ("down",), ("build", "api"),
    ("up", "postgres"), ("up", "loki"), ("up", "grafana"),
    ("up", "crawler", "--build"), ("config", "--profiles"),
])
def test_unsafe_operations_are_rejected_before_docker(binding_file, docker_recorder, args):
    path, _ = binding_file
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, *args, extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "PRODUCTION_TEST_CAPTURE": str(capture),
    })
    assert result.returncode == 2
    assert not capture.exists()


@pytest.mark.parametrize("extra", [
    "MANUAL_BULK_CRAWL_SHARE_PERCENT=50\n", "API_PORT=12345\n",
    "GOLDEN_DISCOVERY_RANKER_V1_MODEL_KIND=v1\n",
    "WEBCRAWLER_CRAWLER_IMAGE=latest\n",
    "export UNRELATED=value\n", "CRAWLER_API_DB_CREDENTIALS_FILE=$(touch /tmp/unwanted)\n",
])
def test_binding_file_cannot_override_policy_or_execute_shell(binding_file, docker_recorder, extra):
    path, _ = binding_file
    path.write_text(path.read_text() + extra)
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "PRODUCTION_TEST_CAPTURE": str(capture),
    })
    assert result.returncode == 2
    assert not capture.exists()


@pytest.mark.parametrize("image", ["webcrawler-api:latest", "sha256:abc", "$(false)"])
def test_initial_image_binding_must_be_immutable(binding_file, docker_recorder, image):
    path, values = binding_file
    path.write_text(path.read_text().replace(values["WEBCRAWLER_API_IMAGE"], image))
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "PRODUCTION_TEST_CAPTURE": str(capture),
    })
    assert result.returncode == 2
    assert not capture.exists()


def test_missing_binding_is_not_filled_from_ambient_shell(binding_file, docker_recorder):
    path, values = binding_file
    path.write_text("\n".join(line for line in path.read_text().splitlines()
                              if not line.startswith("WEBCRAWLER_API_IMAGE=")))
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "PRODUCTION_TEST_CAPTURE": str(capture),
        "WEBCRAWLER_API_IMAGE": values["WEBCRAWLER_API_IMAGE"],
    })
    assert result.returncode == 2
    assert not capture.exists()


def test_status_without_service_names_remains_pipeline_scoped(binding_file, docker_recorder):
    path, _ = binding_file
    binary_dir, capture = docker_recorder
    result = run_wrapper(path, "ps", extra_env={
        "PATH": f"{binary_dir}:{os.environ['PATH']}", "PRODUCTION_TEST_CAPTURE": str(capture),
    })
    assert result.returncode == 0, result.stderr
    assert json.loads(capture.read_text())["args"][-7:] == ["ps", "--", *SERVICES]


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker Compose CLI is not installed")
def test_real_compose_render_uses_main_policy_and_preserves_bindings(binding_file, tmp_path):
    path, bindings = binding_file
    root = tmp_path / "checkout"
    (root / "ops/production").mkdir(parents=True)
    for filename in ("compose.sh", "compose.yml"):
        shutil.copy2(ROOT / "ops/production" / filename, root / "ops/production" / filename)
    shutil.copy2(ROOT / "docker-compose.yml", root / "docker-compose.yml")
    # These files and shell variables must not become a hidden policy layer.
    (root / "docker-compose.override.yml").write_text("services:\n  api:\n    image: unexpected:latest\n")
    (root / ".env").write_text("API_PORT=12345\nAPI_BIND_ADDRESS=0.0.0.0\n")
    result = run_wrapper(path, "config", "--format", "json", root=root, extra_env={
        "API_PORT": "23456", "API_BIND_ADDRESS": "0.0.0.0",
        "MANUAL_BULK_CRAWL_DATA_DIR": "/wrong-data",
        "COMPOSE_PROFILES": "infrastructure,legacy-postgres",
        "WEBCRAWLER_API_IMAGE": "unexpected:latest",
    })
    assert result.returncode == 0, result.stderr
    compose = json.loads(result.stdout)
    assert compose["name"] == "webcrawler"
    for service in SERVICES:
        config = compose["services"][service]
        assert config["image"] == bindings[IMAGE_KEYS[service]]
        assert config["pull_policy"] == "never"
        assert config["command"] == ["/usr/bin/supervisord", "-n", "-c", "/app/ops/production/supervisord.conf"]
        assert config["environment"]["WEBCRAWLER_SUPERVISOR_CONFIG"] == (
            f"/app/containers/{service}/supervisord.conf"
        )
        app_mount = next(mount for mount in config["volumes"] if mount["target"] == "/app")
        assert app_mount["source"] == str(root)
        assert not any(mount["target"].endswith(".conf") or mount["target"].endswith(".yml")
                       for mount in config["volumes"])
    api = compose["services"]["api"]
    assert (api["ports"][0]["host_ip"], api["ports"][0]["published"]) == ("127.0.0.1", "4000")
    assert api["cpus"] == 8
    assert compose["services"]["scheduler_ingest"]["cpus"] == 32
    assert api["environment"]["API_CONFIG_PATH"] == "/app/containers/api/config/config.yml"
    crawler = compose["services"]["crawler"]
    assert crawler["environment"]["MANUAL_BULK_CRAWL_SCHEDULING_POLICY"] == "weighted"
    assert not any("PARSER" in name for name in crawler["environment"])
    assert "ulimits" not in crawler and "stop_grace_period" not in crawler
    assert next(mount for mount in crawler["volumes"] if mount["target"] == "/data/manual_bulk_crawls")["source"] == "/data/manual_bulk_crawls"
    assert set(compose["services"]) == set(SERVICES)
    overlay = yaml.safe_load((ROOT / "ops/production/compose.yml").read_text())
    assert overlay["services"]["postgres"]["profiles"] == ["legacy-postgres"]
    assert overlay["services"]["loki"]["profiles"] == ["infrastructure"]
    assert overlay["services"]["grafana"]["profiles"] == ["infrastructure"]


@pytest.mark.parametrize("service", SERVICES)
def test_real_supervisor_parser_loads_service_from_environment(service, tmp_path, monkeypatch):
    options_module = pytest.importorskip("supervisor.options")
    root_path = tmp_path / "root.conf"
    root_path.write_text((ROOT / "ops/production/supervisord.conf").read_text())
    fragment = tmp_path / "service.conf"
    fragment.write_text((ROOT / f"containers/{service}/supervisord.conf").read_text().replace("/app", str(ROOT)))
    monkeypatch.setenv("WEBCRAWLER_SUPERVISOR_CONFIG", str(fragment))
    monkeypatch.setenv("API_CONFIG_PATH", str(ROOT / "containers/api/config/config.yml"))
    options = options_module.ServerOptions()
    options.realize(["-c", str(root_path)])
    assert options.process_group_configs
    assert options.server_configs[0]["family"] == socket.AF_UNIX
    assert options.server_configs[0]["file"] == "/tmp/supervisor.sock"
    assert options.rpcinterface_factories
    names = {process.name for group in options.process_group_configs for process in group.process_configs}
    if service == "crawler":
        assert names == {f"crawler_{index:02d}" for index in range(16)}
    elif service == "api":
        assert names == {"api", "manual_bulk_crawl_gc"}
    elif service == "sitemap_patroller":
        assert names == {"sitemap_discover", "sitemap_patrol"}


def test_supervisor_root_has_one_main_include_and_no_parser_tuning():
    config = (ROOT / "ops/production/supervisord.conf").read_text()
    assert "files=%(ENV_WEBCRAWLER_SUPERVISOR_CONFIG)s" in config
    assert "/etc/supervisor/conf.d" not in config
    assert "minfds" not in config


def test_supervisor_does_not_fall_back_when_service_binding_is_missing(tmp_path, monkeypatch):
    options_module = pytest.importorskip("supervisor.options")
    monkeypatch.delenv("WEBCRAWLER_SUPERVISOR_CONFIG", raising=False)
    root = tmp_path / "root.conf"
    root.write_text((ROOT / "ops/production/supervisord.conf").read_text())
    with pytest.raises((ValueError, SystemExit)):
        options_module.ServerOptions().realize(["-c", str(root)])

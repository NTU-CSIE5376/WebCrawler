"""Worker settings reach Uvicorn and each FastAPI worker's application factory."""
import os
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner

from containers.api import server


@pytest.mark.parametrize("config, expected", [({}, 4), ({"workers": 1}, 1), ({"workers": 7}, 7)])
def test_server_uses_yaml_workers_and_passes_config_to_children(tmp_path, monkeypatch, config, expected):
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(config))
    monkeypatch.setenv("API_CONFIG_PATH", "previous.yaml")
    execute = Mock()
    monkeypatch.setattr(server.os, "execvp", execute)

    result = CliRunner().invoke(server.main, ["--config", str(path)])

    assert result.exit_code == 0, result.output
    execute.assert_called_once_with(server.sys.executable, [
        server.sys.executable, "-m", "uvicorn", "containers.api.app:create_app", "--factory",
        "--host", "0.0.0.0", "--port", "8080", "--workers", str(expected),
        "--timeout-graceful-shutdown", "120",
    ])
    assert os.environ["API_CONFIG_PATH"] == str(path.resolve())


@pytest.mark.parametrize("workers", [0, -1, True, 1.5, "4", None])
def test_server_rejects_invalid_worker_count_before_starting(tmp_path, monkeypatch, workers):
    config = tmp_path / "config.yml"
    config.write_text(yaml.safe_dump({"workers": workers}))
    execute = Mock()
    monkeypatch.setattr(server.os, "execvp", execute)

    result = CliRunner().invoke(server.main, ["--config", str(config)])

    assert result.exit_code == 1
    assert "workers must be a positive integer" in result.output
    execute.assert_not_called()


@pytest.mark.parametrize("bind,host,port", [
    ("127.0.0.1:4000", "127.0.0.1", "4000"),
    ("[::1]:8080", "::1", "8080"),
])
def test_server_uses_configured_bind(tmp_path, monkeypatch, bind, host, port):
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"bind": bind}))
    execute = Mock()
    monkeypatch.setattr(server.os, "execvp", execute)
    result = CliRunner().invoke(server.main, ["--config", str(path)])
    assert result.exit_code == 0, result.output
    command = execute.call_args.args[1]
    assert command[command.index("--host") + 1] == host
    assert command[command.index("--port") + 1] == port


@pytest.mark.parametrize("bind", [None, "localhost", ":8080", "localhost:0", "localhost:65536"])
def test_server_rejects_invalid_bind(tmp_path, monkeypatch, bind):
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump({"bind": bind}))
    execute = Mock()
    monkeypatch.setattr(server.os, "execvp", execute)
    result = CliRunner().invoke(server.main, ["--config", str(path)])
    assert result.exit_code == 1
    assert "bind must be a host:port" in result.output
    execute.assert_not_called()

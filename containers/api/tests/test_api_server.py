"""Worker settings reach Gunicorn and each Flask worker's application factory."""
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
    execute.assert_called_once_with("gunicorn", [
        "gunicorn", "--bind", "0.0.0.0:8080", "--workers", str(expected),
        "--timeout", "150", "--graceful-timeout", "120", "--access-logfile", "-",
        "containers.api.app:create_app()",
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

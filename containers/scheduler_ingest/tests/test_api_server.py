"""Verify worker configuration reaches Uvicorn and its child applications."""
import os
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner

from containers.scheduler_ingest.api import server


@pytest.mark.parametrize("api, expected", [({}, 4), ({"workers": 1}, 1), ({"workers": 7}, 7)])
def test_server_uses_yaml_workers_and_passes_config_to_children(tmp_path, monkeypatch, api, expected):
    config = tmp_path / "custom.yaml"
    config.write_text(yaml.safe_dump({"api": api}))
    monkeypatch.setenv("INGEST_CONFIG_PATH", "previous.yaml")
    run = Mock()
    monkeypatch.setattr(server.uvicorn, "run", run)

    result = CliRunner().invoke(server.main, ["--config", str(config)])

    assert result.exit_code == 0, result.output
    run.assert_called_once_with(
        "containers.scheduler_ingest.api.app:create_app", factory=True,
        host="0.0.0.0", port=8080, workers=expected, timeout_graceful_shutdown=120,
    )
    assert os.environ["INGEST_CONFIG_PATH"] == str(config.resolve())


@pytest.mark.parametrize("workers", [0, -1, True, 1.5, "4", None])
def test_server_rejects_invalid_worker_count_before_starting(tmp_path, monkeypatch, workers):
    config = tmp_path / "ingest.yaml"
    config.write_text(yaml.safe_dump({"api": {"workers": workers}}))
    run = Mock()
    monkeypatch.setattr(server.uvicorn, "run", run)

    result = CliRunner().invoke(server.main, ["--config", str(config)])

    assert result.exit_code == 1
    assert "api.workers must be a positive integer" in result.output
    run.assert_not_called()

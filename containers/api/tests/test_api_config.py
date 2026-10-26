"""API configuration has one location independent of the working directory."""
from pathlib import Path

import pytest

from containers.api.config_loader import (
    DEFAULT_CONFIG_PATH,
    load_api_config,
    api_config_path,
)


def test_default_api_config_is_independent_of_working_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("API_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)
    assert Path(api_config_path()) == DEFAULT_CONFIG_PATH
    config = load_api_config()["manual_bulk_crawl"]
    assert {"pending_dir", "result_dir", "max_urls", "max_url_length"} <= config.keys()


def test_api_config_override(tmp_path, monkeypatch):
    path = tmp_path / "config.yml"
    path.write_text("pending_dir: /isolated/pending\nresult_dir: /isolated/results\n")
    monkeypatch.setenv("API_CONFIG_PATH", str(path))
    assert load_api_config() == {
        "pending_dir": "/isolated/pending", "result_dir": "/isolated/results",
    }


@pytest.mark.parametrize("path", ["", "   "])
def test_empty_api_config_path_fails(monkeypatch, path):
    monkeypatch.setenv("API_CONFIG_PATH", path)
    with pytest.raises(ValueError, match="API_CONFIG_PATH"):
        load_api_config()


def test_missing_api_config_file_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("API_CONFIG_PATH", str(tmp_path / "missing.yaml"))
    with pytest.raises(FileNotFoundError):
        load_api_config()


@pytest.mark.parametrize("policy", ["balanced", "manual_first", "normal_first"])
def test_scheduling_policy_environment_overrides_yaml(tmp_path, monkeypatch, policy):
    path = tmp_path / "config.yml"
    path.write_text("manual_bulk_crawl:\n  scheduling_policy: normal_first\n")
    monkeypatch.setenv("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", policy)
    assert load_api_config(path)["manual_bulk_crawl"]["scheduling_policy"] == policy


def test_scheduling_policy_yaml_and_default(tmp_path, monkeypatch):
    monkeypatch.delenv("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", raising=False)
    path = tmp_path / "config.yml"
    path.write_text("manual_bulk_crawl: {}\n")
    assert load_api_config(path)["manual_bulk_crawl"]["scheduling_policy"] == "balanced"
    path.write_text("manual_bulk_crawl:\n  scheduling_policy: manual_first\n")
    assert load_api_config(path)["manual_bulk_crawl"]["scheduling_policy"] == "manual_first"


@pytest.mark.parametrize("policy", ["unknown", "", "BALANCED"])
def test_invalid_scheduling_policy_environment_fails(tmp_path, monkeypatch, policy):
    path = tmp_path / "config.yml"
    path.write_text("manual_bulk_crawl: {}\n")
    monkeypatch.setenv("MANUAL_BULK_CRAWL_SCHEDULING_POLICY", policy)
    with pytest.raises(ValueError, match="scheduling_policy"):
        load_api_config(path)

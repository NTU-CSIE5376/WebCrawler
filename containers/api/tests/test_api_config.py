"""API configuration has one location independent of the working directory."""
import json
from pathlib import Path

import pytest
import yaml
from sqlalchemy.engine import URL

from containers.api.config_loader import (
    DEFAULT_CONFIG_PATH,
    api_config_path,
    load_api_config,
)


def test_default_api_config_is_independent_of_working_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("API_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)
    read_text = Path.read_text

    def read_with_mounted_secret(path, *args, **kwargs):
        if path == Path("/run/secrets/crawler_api_database"):
            return json.dumps({"username": "fixture-user", "password": "fixture-password"})
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_with_mounted_secret)
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


@pytest.fixture
def database_config(tmp_path):
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text(json.dumps({
        "username": "fixture-user", "password": "fixture@password:/%?",
    }))
    config = {
        "postgres": {
            "driver": "postgresql+psycopg2", "host": "172.16.191.2", "port": 5433,
            "database": "crawlerdb", "credentials_file": str(credentials_path),
        },
    }
    path = tmp_path / "api.yml"

    def save():
        path.write_text(yaml.safe_dump(config))
        return str(path)

    return config, credentials_path, save


def test_structured_database_uses_main_endpoint_and_external_credentials(database_config, monkeypatch):
    _, _, save = database_config
    monkeypatch.setenv("PGHOST", "stale-postgres")
    monkeypatch.setenv("DATABASE_URL", "postgresql://stale-postgres/obsolete")
    url = load_api_config(save())["postgres"]["dsn"]
    assert isinstance(url, URL)
    assert (url.drivername, url.host, url.port, url.database) == (
        "postgresql+psycopg2", "172.16.191.2", 5433, "crawlerdb",
    )
    assert url.username == "fixture-user"
    assert url.password == "fixture@password:/%?"


def test_router_receives_unmasked_url_without_connecting(database_config, tmp_path, monkeypatch):
    from unittest.mock import Mock

    from containers.api import routing

    config, _, save = database_config
    ingest_path = tmp_path / "ingest.yml"
    ingest_path.write_text("router: {num_shards: 16, shards_per_ingestor: 1}\n")
    config["ingest_config_path"] = str(ingest_path)
    factory = Mock()
    monkeypatch.setattr(routing, "create_engine", factory)
    routing.ManualBulkCrawlRouter(load_api_config(save()))
    url = factory.call_args.args[0]
    assert isinstance(url, URL)
    assert url.password == "fixture@password:/%?"
    assert (url.host, url.port) == ("172.16.191.2", 5433)


def test_missing_database_secret_fails_without_falling_back(database_config):
    _, credentials_path, save = database_config
    credentials_path.unlink()
    with pytest.raises(ValueError, match="credentials_file cannot be read"):
        load_api_config(save())


@pytest.mark.parametrize("contents", [
    "not JSON containing fixture-secret",
    "[]",
    '{"username":"fixture-secret"}',
    '{"username":"fixture-user","password":null}',
    '{"username":"fixture-user","password":""}',
    '{"username":"fixture-user","password":"fixture-secret","host":"stale-postgres"}',
])
def test_invalid_secret_cannot_override_endpoint_or_leak_contents(database_config, contents):
    _, credentials_path, save = database_config
    credentials_path.write_text(contents)
    with pytest.raises(ValueError) as raised:
        load_api_config(save())
    assert "credentials_file" in str(raised.value)
    assert "fixture-secret" not in str(raised.value)
    assert "stale-postgres" not in str(raised.value)


@pytest.mark.parametrize("field,value", [
    ("port", True), ("port", 0), ("port", 65536), ("port", "5433"),
    ("host", ""), ("database", ""), ("credentials_file", ""),
])
def test_invalid_endpoint_fails(database_config, field, value):
    config, _, save = database_config
    config["postgres"][field] = value
    with pytest.raises(ValueError, match="postgres"):
        load_api_config(save())


def test_explicit_legacy_dsn_is_supported_for_custom_configs(tmp_path):
    path = tmp_path / "api.yml"
    config = {"postgres": {"dsn": "postgresql://isolated-test/fixture"}}
    path.write_text(yaml.safe_dump(config))
    assert load_api_config(str(path)) == config


def test_legacy_dsn_cannot_silently_override_structured_endpoint(database_config):
    config, _, save = database_config
    config["postgres"]["dsn"] = "postgresql://stale-postgres/obsolete"
    with pytest.raises(ValueError, match="cannot be combined"):
        load_api_config(save())


def test_checked_in_database_and_compose_share_one_secret_reference():
    config = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())
    postgres = config["postgres"]
    assert (postgres["host"], postgres["port"], postgres["database"]) == (
        "172.16.191.2", 5433, "crawlerdb",
    )
    assert "dsn" not in postgres and "password" not in postgres
    assert postgres["credentials_file"] == "/run/secrets/crawler_api_database"
    compose = yaml.safe_load((DEFAULT_CONFIG_PATH.parents[3] / "docker-compose.yml").read_text())
    assert "crawler_api_database" in compose["services"]["api"]["secrets"]
    assert compose["secrets"]["crawler_api_database"]["file"] == (
        "${CRAWLER_API_DB_CREDENTIALS_FILE:-/data/secrets/webcrawler/api-database.json}"
    )

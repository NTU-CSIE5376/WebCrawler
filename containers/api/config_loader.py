"""Locate and load the API service configuration."""
import json
import os
from pathlib import Path

from sqlalchemy.engine import URL

from libs.config.loader import load_yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config/config.yml"


def api_config_path(path=None) -> str:
    value = path if path is not None else os.environ.get("API_CONFIG_PATH", str(DEFAULT_CONFIG_PATH))
    if not str(value).strip():
        raise ValueError("API_CONFIG_PATH must be a non-empty config path")
    return str(Path(value).resolve())


def _database_url(postgres: dict) -> URL | str:
    if not isinstance(postgres, dict):
        raise TypeError("postgres must be a configuration object")
    # Explicit DSNs remain supported for isolated/custom configurations. The
    # checked-in deployment uses the structured form, never an implicit DSN.
    if "dsn" in postgres:
        if set(postgres) != {"dsn"}:
            raise ValueError("postgres.dsn cannot be combined with structured database settings")
        if not isinstance(postgres["dsn"], str) or not postgres["dsn"].strip():
            raise ValueError("postgres.dsn must be non-empty")
        return postgres["dsn"]

    fields = {"driver", "host", "port", "database", "credentials_file"}
    if set(postgres) != fields:
        raise ValueError("postgres requires driver, host, port, database and credentials_file")
    for field in fields - {"port"}:
        if not isinstance(postgres[field], str) or not postgres[field].strip():
            raise ValueError(f"postgres.{field} must be non-empty")
    if type(postgres["port"]) is not int or not 1 <= postgres["port"] <= 65535:
        raise ValueError("postgres.port must be an integer between 1 and 65535")
    try:
        credentials = json.loads(Path(postgres["credentials_file"]).read_text(encoding="utf-8"))
    except OSError:
        raise ValueError("API database credentials_file cannot be read") from None
    except (ValueError, UnicodeError):
        raise ValueError("API database credentials_file must contain valid JSON") from None
    if (not isinstance(credentials, dict) or set(credentials) != {"username", "password"}
            or any(not isinstance(value, str) or not value for value in credentials.values())):
        raise ValueError("API database credentials_file requires only non-empty username and password")
    # Keep the URL object: str(URL) masks the password, while create_engine
    # accepts the object directly and correctly handles reserved characters.
    return URL.create(
        drivername=postgres["driver"], host=postgres["host"], port=postgres["port"],
        database=postgres["database"], username=credentials["username"],
        password=credentials["password"],
    )


def load_api_config(path=None) -> dict:
    config = load_yaml(api_config_path(path))
    if "postgres" in config:
        config["postgres"] = {"dsn": _database_url(config["postgres"])}
    return config

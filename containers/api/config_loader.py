"""Locate and load the API service configuration."""
import os
from pathlib import Path

from libs.config.loader import load_yaml
from libs.ipc.manual_bulk_crawl import DEFAULT_SCHEDULING_POLICY, validate_scheduling_policy


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config/config.yml"


def api_config_path(path=None) -> str:
    value = path if path is not None else os.environ.get("API_CONFIG_PATH", str(DEFAULT_CONFIG_PATH))
    if not str(value).strip():
        raise ValueError("API_CONFIG_PATH must be a non-empty config path")
    return str(Path(value).resolve())


def load_api_config(path=None) -> dict:
    config = load_yaml(api_config_path(path))
    crawl = config.get("manual_bulk_crawl", {})
    crawl["scheduling_policy"] = validate_scheduling_policy(os.environ.get(
        "MANUAL_BULK_CRAWL_SCHEDULING_POLICY", crawl.get("scheduling_policy", DEFAULT_SCHEDULING_POLICY),
    ))
    return config

"""Locate and load the API service configuration."""
import os
from pathlib import Path

from libs.config.loader import load_yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config/config.yml"


def api_config_path(path=None) -> str:
    value = path if path is not None else os.environ.get("API_CONFIG_PATH", str(DEFAULT_CONFIG_PATH))
    if not str(value).strip():
        raise ValueError("API_CONFIG_PATH must be a non-empty config path")
    return str(Path(value).resolve())


def load_api_config(path=None) -> dict:
    return load_yaml(api_config_path(path))

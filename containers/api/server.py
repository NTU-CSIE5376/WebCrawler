"""Run the Flask API with Gunicorn."""
import os
from pathlib import Path

import click

from .config_loader import DEFAULT_CONFIG_PATH, load_api_config


@click.command()
@click.option("--config", "config_path", default=str(DEFAULT_CONFIG_PATH), show_default=True,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Path to the API server YAML configuration.")
def main(config_path: Path):
    config = load_api_config(str(config_path))
    workers = config.get("workers", 4)
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise click.ClickException("workers must be a positive integer")
    os.environ["API_CONFIG_PATH"] = str(config_path.resolve())
    os.execvp("gunicorn", [
        "gunicorn", "--bind", config.get("bind", "0.0.0.0:8080"),
        "--workers", str(workers), "--timeout", "150", "--graceful-timeout", "120",
        "--access-logfile", "-", "containers.api.app:create_app()",
    ])


if __name__ == "__main__":
    main()

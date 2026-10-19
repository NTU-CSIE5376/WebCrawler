"""Run the FastAPI application with Uvicorn workers."""
import os
import sys
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
    bind = config.get("bind", "0.0.0.0:8080")
    try:
        host, port = bind.rsplit(":", 1)
        host = host.removeprefix("[").removesuffix("]")
        port = int(port)
        if not host or not 1 <= port <= 65535:
            raise ValueError
    except (AttributeError, ValueError):
        raise click.ClickException("bind must be a host:port with port between 1 and 65535") from None
    os.environ["API_CONFIG_PATH"] = str(config_path.resolve())
    os.execvp(sys.executable, [
        sys.executable, "-m", "uvicorn", "containers.api.app:create_app", "--factory",
        "--host", host, "--port", str(port), "--workers", str(workers),
        "--timeout-graceful-shutdown", "120",
    ])


if __name__ == "__main__":
    main()

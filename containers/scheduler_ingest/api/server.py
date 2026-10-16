"""Run scheduler_ingest's API using its YAML configuration."""
from __future__ import annotations

import os
from pathlib import Path

import click
import uvicorn

from libs.config.loader import load_yaml


@click.command()
@click.option("--config", "config_path", required=True,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Path to scheduler_ingest's YAML configuration.")
def main(config_path: Path):
    """Start the scheduler_ingest API server."""
    workers = load_yaml(str(config_path)).get("api", {}).get("workers", 4)
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise click.ClickException("api.workers must be a positive integer")
    # Uvicorn children create the application independently. Pass the same
    # absolute configuration path to every worker, including custom paths.
    os.environ["INGEST_CONFIG_PATH"] = str(config_path.resolve())
    uvicorn.run("containers.scheduler_ingest.api.app:create_app",
                factory=True, host="0.0.0.0", port=8080, workers=workers,
                timeout_graceful_shutdown=120)


if __name__ == "__main__":
    main()

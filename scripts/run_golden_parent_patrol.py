"""Cron entry point: one round of the golden parent patrol.

Invoked by cron (e.g. `*/10 * * * *`). Each invocation is a full,
self-contained run: read config, open DB connections, evaluate any new
metric_batches, process due parents, exit. No long-running process,
no in-memory state.

Reads the same yaml as the rest of scheduler_control (default
`containers/scheduler_control/config/control.yaml`); the patrol-specific
section is `golden_parent_patrol`. The runner is a no-op when
`golden_parent_patrol.enabled = false` so PR 4 can land deployment
config behind a kill switch.

Usage:
    uv run scripts/run_golden_parent_patrol.py [--config PATH] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

import psycopg2
import tldextract

try:
    from scripts.constants import CRAWLERDB, METRICDB, NUM_SHARDS
except ModuleNotFoundError:
    from constants import CRAWLERDB, METRICDB, NUM_SHARDS

from libs.config.loader import load_yaml
from libs.db.sharding.key import compute_shard, load_sharding_config
from libs.patrol.config import parse_patrol_config
from libs.patrol.cron_loop import run_once


SERVICE_NAME = "golden_parent_patrol"
ENV_PREFIX = "GOLDEN_PARENT_PATROL"

DEFAULT_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_control/config/control.yaml"
)
INGEST_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_ingest/config/ingest.yaml"
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(SERVICE_NAME)


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _extract_domain(url: str) -> str | None:
    e = tldextract.extract(url)
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


def _domain_to_shard(domain, overrides, split_subdomains) -> int:
    return compute_shard(domain, NUM_SHARDS, overrides, split_subdomains)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one golden parent patrol cycle.")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help=f"Path to scheduler_control yaml (default {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Connect and load config but do not modify any database row.",
    )
    args = parser.parse_args()

    raw = load_yaml(args.config)
    section: dict[str, Any] = dict(raw.get(SERVICE_NAME) or {})
    enabled = _env_bool(
        f"{ENV_PREFIX}_ENABLED", bool(section.get("enabled", False))
    )
    if not enabled:
        logger.info(
            f"{SERVICE_NAME}.disabled",
            extra={"event": f"{SERVICE_NAME}.disabled"},
        )
        return

    config = parse_patrol_config(section)

    if args.dry_run:
        logger.info(
            f"{SERVICE_NAME}.dry_run",
            extra={
                "event": f"{SERVICE_NAME}.dry_run",
                "patrol_priority": config.patrol_priority,
                "cron_batch_size": config.cron_batch_size,
                "cycle_days": config.cycle_days,
            },
        )
        return

    metric_conn = psycopg2.connect(**METRICDB)
    try:
        crawler_conn = psycopg2.connect(**CRAWLERDB)
    except Exception:
        metric_conn.close()
        raise

    try:
        # load_sharding_config takes a crawlerdb connection (not a path) since
        # split_subdomains lives in the shard_split DB table.
        overrides, split_subdomains = load_sharding_config(INGEST_CONFIG, crawler_conn)

        result = run_once(
            metric_conn=metric_conn,
            crawler_conn=crawler_conn,
            config=config,
            extract_domain=_extract_domain,
            domain_to_shard=_domain_to_shard,
            overrides=overrides,
            split_subdomains=split_subdomains,
        )
        logger.info(
            f"{SERVICE_NAME}.run_once",
            extra={
                "event": f"{SERVICE_NAME}.run_once",
                **{f"long_loop_{k}": v for k, v in result["long_loop"].items()},
                **{f"short_loop_{k}": v for k, v in result["short_loop"].items()},
            },
        )
    except Exception:
        try:
            crawler_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        metric_conn.close()
        crawler_conn.close()


if __name__ == "__main__":
    main()

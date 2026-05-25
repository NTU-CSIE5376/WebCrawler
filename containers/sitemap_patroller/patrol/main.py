"""Entry point for the patrol worker. Runs run_once() on a loop with
a configurable sleep between passes."""
from __future__ import annotations

import argparse
import logging
import time

import psycopg2

from libs.config.loader import load_yaml, require
from libs.db.sharding.key import load_sharding_config
from libs.db.sharding.router import ShardRouter
from libs.obslog import configure as configure_logging

from .service import PatrolConfig, run_once


logger = logging.getLogger("sitemap_patrol")


def _load_config(path: str) -> tuple[PatrolConfig, int]:
    raw = load_yaml(path)
    pg = require(raw, "postgres")
    dsn = str(require(pg, "dsn"))
    ingest_path = str(require(raw, "ingest_config_path"))

    # Re-use scheduler_ingest's config for the same ShardRouter wiring the
    # router uses. The sitemap patroller emits records bound for the same
    # ingestors, so the routing must agree.
    ingest_raw = load_yaml(ingest_path)
    r = require(ingest_raw, "router")

    # libs.db.sharding.key.load_sharding_config needs a psycopg2 conn to
    # read the shard_split_subdomain table.
    conn = psycopg2.connect(dsn)
    try:
        overrides, split_subdomains = load_sharding_config(ingest_path, conn)
    finally:
        conn.close()

    sharder = ShardRouter(
        num_shards=int(require(r, "num_shards")),
        shards_per_ingestor=int(require(r, "shards_per_ingestor")),
        domain_overrides=overrides,
        split_subdomains=split_subdomains,
    )

    p = require(raw, "patrol")
    cfg = PatrolConfig(
        dsn=dsn,
        ingestor_dir_template=str(require(r, "ingestor_dir_template")),
        interval_minutes=int(require(r, "interval_minutes")),
        sharder=sharder,
        due_interval_hours=int(p.get("due_interval_hours", 24)),
        batch_limit=int(p.get("batch_limit", 500)),
        global_delay_sec=float(p.get("global_delay_sec", 2.0)),
        per_domain_cooldown_sec=float(p.get("per_domain_cooldown_sec", 60.0)),
    )
    loop_interval_sec = int(p.get("loop_interval_sec", 600))
    return cfg, loop_interval_sec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--run-once", action="store_true",
                    help="Run a single pass and exit (for ad-hoc invocation via `docker exec`).")
    args = ap.parse_args()

    configure_logging(service="sitemap_patrol")
    cfg, loop_interval_sec = _load_config(args.config)

    if args.run_once:
        run_once(cfg)
        return

    while True:
        try:
            run_once(cfg)
        except Exception as e:
            logger.exception(
                "patrol.run_error",
                extra={"event": "patrol.run_error", "err": repr(e)},
            )
        time.sleep(loop_interval_sec)


if __name__ == "__main__":
    main()

"""Entry point for the discover worker. Runs run_once() on a loop with
a configurable sleep between sweeps."""
from __future__ import annotations

import argparse
import logging
import time

from libs.config.loader import load_yaml, require
from libs.obslog import configure as configure_logging

from .service import DiscoverConfig, run_once


logger = logging.getLogger("sitemap_discover")


def _load_config(path: str) -> tuple[DiscoverConfig, int]:
    raw = load_yaml(path)
    pg = require(raw, "postgres")
    d = require(raw, "discover")
    cfg = DiscoverConfig(
        dsn=str(require(pg, "dsn")),
        score_min=float(d.get("score_min", 0.95)),
        domain_limit=d.get("domain_limit"),
        global_delay_sec=float(d.get("global_delay_sec", 0.5)),
    )
    loop_interval_sec = int(d.get("loop_interval_sec", 86400))
    return cfg, loop_interval_sec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--run-once", action="store_true",
                    help="Run a single sweep and exit (for ad-hoc invocation via `docker exec`).")
    args = ap.parse_args()

    configure_logging(service="sitemap_discover")
    cfg, loop_interval_sec = _load_config(args.config)

    if args.run_once:
        run_once(cfg)
        return

    while True:
        try:
            run_once(cfg)
        except Exception as e:
            logger.exception(
                "discover.run_error",
                extra={"event": "discover.run_error", "err": repr(e)},
            )
        time.sleep(loop_interval_sec)


if __name__ == "__main__":
    main()

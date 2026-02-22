from __future__ import annotations

import argparse

from libs.config.loader import load_yaml, require
from .service import StatsAggregatorService


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)
    stats = require(raw, "stats")
    pg = require(raw, "postgres")

    stats_dir = require(stats, "stats_dir")
    bad_dir = require(stats, "bad_dir")
    postgres_dsn = require(pg, "dsn")

    svc = StatsAggregatorService(stats_dir, bad_dir, postgres_dsn)
    svc.run_forever()


if __name__ == "__main__":
    main()

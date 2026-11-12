from __future__ import annotations

import argparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.obslog import configure as configure_logging

from .service import FrontierGCConfig, FrontierGCService


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    configure_logging(service="gc")

    raw = load_yaml(args.config)
    pg = require(raw, "postgres")
    gc = require(raw, "gc")

    engine = create_engine(
        str(require(pg, "dsn")),
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=1,
        max_overflow=1,
        pool_timeout=30,
        future=True,
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
            # Single-threaded scans: the DB host is disk-constrained and
            # parallel workers' shared-memory segments can fail to allocate.
            "options": "-c max_parallel_workers_per_gather=0",
        },
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    cfg = FrontierGCConfig(
        total_shards=int(gc.get("total_shards", 256)),
        frontier_cap=int(gc.get("frontier_cap", 1_000_000)),
        yield_floor=float(gc.get("yield_floor", 0.01)),
        sample_percent=float(gc.get("sample_percent", 0.5)),
        stale_pending_days=int(gc.get("stale_pending_days", 60)),
        frozen_pending_days=int(gc.get("frozen_pending_days", 7)),
        batch_size=int(gc.get("batch_size", 5000)),
        run_hour_utc=int(gc.get("run_hour_utc", 4)),
        run_minute_utc=int(gc.get("run_minute_utc", 0)),
        check_interval_sec=int(gc.get("check_interval_sec", 3600)),
        catch_up_on_start=bool(gc.get("catch_up_on_start", True)),
    )

    svc = FrontierGCService(cfg=cfg, Session=Session)
    if args.once:
        svc.run_once()
        return
    svc.run_forever()


if __name__ == "__main__":
    main()

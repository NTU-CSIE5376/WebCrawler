from __future__ import annotations

import argparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.obslog import configure as configure_logging

from .service import CounterRolloffConfig, CounterRolloffService


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    configure_logging(service="accounting")

    raw = load_yaml(args.config)
    pg = require(raw, "postgres")
    accounting = require(raw, "accounting")

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
        },
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    cfg = CounterRolloffConfig(
        total_shards=int(accounting.get("total_shards", 256)),
        event_retention_days=int(accounting.get("event_retention_days", 90)),
        batch_size=int(accounting.get("batch_size", 1000)),
        run_hour_utc=int(accounting.get("run_hour_utc", 3)),
        run_minute_utc=int(accounting.get("run_minute_utc", 0)),
        check_interval_sec=int(accounting.get("check_interval_sec", 30)),
        catch_up_on_start=bool(accounting.get("catch_up_on_start", True)),
    )

    svc = CounterRolloffService(cfg=cfg, Session=Session)
    if args.once:
        svc.run_once()
        return
    svc.run_forever()


if __name__ == "__main__":
    main()

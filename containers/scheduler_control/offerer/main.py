from __future__ import annotations

import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require

from .service import OffererDerivation, OffererConfig, OffererService
from .selection.example_strategy import ExampleStrategy


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--offerer-id", type=int, required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)

    offerer = require(raw, "offerer")
    pg = require(raw, "postgres")

    id_start = int(offerer.get("id_start", 0))
    id_end = int(offerer.get("id_end", 0))
    offerer_id = int(args.offerer_id)

    if not (id_start <= offerer_id <= id_end):
        raise SystemExit(f"offerer-id {offerer_id} not in configured range [{id_start}, {id_end}]")

    engine = create_engine(
        str(require(pg, "dsn")),
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=2,
        max_overflow=1,
        pool_timeout=30,
        future=True,
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5
        },
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    selector = ExampleStrategy(
        Session=Session,
    )

    deriv = OffererDerivation(
        queue_dir_template=str(offerer.get("queue_dir_template", "/data/ipc/url_queue/crawler_{id:02d}")),
        total_shards=int(offerer.get("total_shards", 256)),
        shards_per_offerer=int(offerer.get("shards_per_offerer", 16)),
    )

    cfg = OffererConfig(
        offerer_id=offerer_id,
        scan_interval_sec=int(offerer.get("scan_interval_sec", 300)),
        max_domain_files=int(offerer.get("max_domain_files", 32)),
        low_watermark_domains=int(offerer.get("low_watermark_domains", 16)),
        per_domain_url_cap=int(offerer.get("per_domain_url_cap", 100)),
        stats_dir=str(offerer.get("stats_dir", "/data/ipc/stats")),
    )

    OffererService(cfg, deriv, selector).run_forever()


if __name__ == "__main__":
    main()


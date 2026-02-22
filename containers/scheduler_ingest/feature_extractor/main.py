from __future__ import annotations
import time
import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.ipc.folder_reader import Progress, FolderReader
from libs.stats.delta_writer import StatsDeltaWriter

from .service import ExtractService
from .db_ops import FeatureDB


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--extractor-id", type=int, required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)

    extractor = require(raw, "extractor")
    pg = require(raw, "postgres")

    prog = Progress(require(extractor, "progress_template").format(id=args.extractor_id))
    interval_minutes = int(extractor.get("interval_minutes", 30))
    reader = FolderReader(
        require(extractor, "result_dir_template").format(id=args.extractor_id),
        prog,
        interval_minutes
    )

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

    db = FeatureDB(Session)
    stats_dir=require(extractor, "stats_dir")
    svc = ExtractService(args.extractor_id, db, StatsDeltaWriter(stats_dir))

    while True:
        progressed = False
        for date, time_, folder in reader.iter_ready_folders():
            svc.process_folder(folder)
            prog.advance(date, time_)
            progressed = True
        if not progressed:
            time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    main()


from __future__ import annotations
import time
import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.ipc.folder_reader import Progress, FolderReader
from libs.obslog import configure as configure_logging
from libs.stats.delta_writer import StatsDeltaWriter

from .service import IngestService
from .db_ops import IngestDB


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--ingestor-id", type=int, required=True)
    args = ap.parse_args()

    raw = load_yaml(args.config)
    ingestor_id = args.ingestor_id
    configure_logging(service="ingestor", worker_id=ingestor_id)

    ingestor = require(raw, "ingestor")
    pg = require(raw, "postgres")

    prog = Progress(require(ingestor, "progress_template").format(id=ingestor_id))
    interval_minutes = int(ingestor.get("interval_minutes", 30))
    reader = FolderReader(
        require(ingestor, "result_dir_template").format(id=ingestor_id),
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

    db = IngestDB(Session)
    stats_dir=require(ingestor, "stats_dir")
    svc = IngestService(ingestor_id, db, StatsDeltaWriter(stats_dir))

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


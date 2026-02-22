from __future__ import annotations

import os
import time
from datetime import datetime, date
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.ipc.jsonio import read_json, atomic_write_json
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import apply_stats_delta


class StatsAggregatorService:
    def __init__(self, stats_dir, bad_dir, postgres_dsn):
        self.stats_dir = Path(stats_dir)
        self.bad_dir = Path(bad_dir)
        self.bad_dir.mkdir(parents=True, exist_ok=True)

        self.engine = create_engine(
            postgres_dsn,
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
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)

    def process_file(self, path: Path):
        delta = read_json(path)

        with self.Session() as session:
            try:
                apply_stats_delta(session, delta)
                session.commit()

                try:
                    path.unlink()   # delete immediately
                except FileNotFoundError:
                    pass
            except Exception as e:
                session.rollback()
                print(f"[stats] ERROR processing {path}: {e}", flush=True)

                new_path = self.bad_dir / path.name
                os.replace(path, new_path)

                # write stats_error
                with self.Session() as session2:
                    try:
                        apply_stats_delta(session2, {"counters": {"stats_error": 1, "error_count": 1}})
                        session2.commit()
                    except Exception as e:
                        session2.rollback()
                        print(f"[stats] ERROR cannot write stats_error: {e}", flush=True)

    def run_forever(self):
        while True:
            files = sorted(self.stats_dir.glob("*.json"))[:10]
            if not files:
                time.sleep(300)
                continue

            for f in files:
                self.process_file(f)

            print(f"[stats] INFO processed {len(files)} files.")


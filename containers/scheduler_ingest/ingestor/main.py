from __future__ import annotations
import time
import argparse
import logging
import os
from pathlib import Path
from typing import Any
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.ipc.folder_reader import Progress, FolderReader
from libs.obslog import configure as configure_logging
from libs.scoring.golden_discovery_runtime import GoldenDiscoveryRuntimeScorer
from libs.stats.delta_writer import StatsDeltaWriter

from .service import IngestService
from .db_ops import IngestDB


LOGGER = logging.getLogger("ingestor")
RANKER_ENV_PREFIX = "GOLDEN_DISCOVERY_RANKER_V1"


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    return float(value) if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value not in (None, "") else default


def _ranker_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return dict(raw.get("golden_discovery_ranker_v1") or {})


def _load_inline_ranker(raw: dict[str, Any]) -> GoldenDiscoveryRuntimeScorer | None:
    ranker_raw = _ranker_raw(raw)
    enabled = _env_bool(
        f"{RANKER_ENV_PREFIX}_INGEST_INLINE_ENABLED",
        bool(ranker_raw.get("ingest_inline_enabled", False)),
    )
    if not enabled:
        return None

    # Ingest inline scoring stays on the v1 model and loads it with the v1
    # loader (GoldenDiscoveryRuntimeScorer) below. Its artifact env is namespaced
    # `_INGEST_INLINE_ARTIFACT` — distinct from the background scorer's
    # `_ARTIFACT`, which may point at the v2 joblib. Sharing the bare `_ARTIFACT`
    # name would crash the ingestor (v1 loader on a v2 artifact) if that env is
    # set at a shared scope. Matches the existing `_INGEST_INLINE_*` knobs.
    artifact_path = _env_str(
        f"{RANKER_ENV_PREFIX}_INGEST_INLINE_ARTIFACT",
        str(ranker_raw.get("artifact_path", "")),
    )
    if not artifact_path or not Path(artifact_path).exists():
        raise SystemExit(f"Golden Discovery Ranker artifact not found: {artifact_path!r}")

    ranker = GoldenDiscoveryRuntimeScorer.load(artifact_path)
    LOGGER.info(
        "golden_discovery_ranker_v1.ingest_inline_loaded",
        extra={
            "event": "golden_discovery_ranker_v1.ingest_inline_loaded",
            "artifact_path": artifact_path,
            "heads": ",".join(ranker.heads),
            "model_name": ranker.metadata.get("model_name"),
            "score_version": ranker.metadata.get("score_version"),
        },
    )
    return ranker


def _inline_score_timeout_sec(raw: dict[str, Any]) -> float:
    ranker_raw = _ranker_raw(raw)
    return _env_float(
        f"{RANKER_ENV_PREFIX}_INGEST_INLINE_SCORE_TIMEOUT_SEC",
        float(ranker_raw.get("ingest_inline_score_timeout_sec", 10.0)),
    )


def _inline_score_batch_size(raw: dict[str, Any]) -> int:
    ranker_raw = _ranker_raw(raw)
    return max(
        1,
        _env_int(
            f"{RANKER_ENV_PREFIX}_INGEST_INLINE_SCORE_BATCH_SIZE",
            int(ranker_raw.get("ingest_inline_score_batch_size", 5000)),
        ),
    )


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

    db = IngestDB(
        Session,
        inline_ranker=_load_inline_ranker(raw),
        inline_score_timeout_sec=_inline_score_timeout_sec(raw),
        inline_score_batch_size=_inline_score_batch_size(raw),
    )
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

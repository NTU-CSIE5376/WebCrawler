from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.obslog import configure as configure_logging
from libs.scoring.golden_discovery_runtime import GoldenDiscoveryRuntimeScorer
from libs.scoring.golden_discovery_runtime_v2 import GoldenDiscoveryRuntimeScorerV2

from .service import GoldenDiscoveryRankerConfig, GoldenDiscoveryRankerService


SERVICE_NAME = "golden_discovery_ranker_v1"
ENV_PREFIX = "GOLDEN_DISCOVERY_RANKER_V1"

logger = logging.getLogger(SERVICE_NAME)


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value not in (None, "") else default


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--worker-id", type=int, required=True)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    configure_logging(service=SERVICE_NAME, worker_id=args.worker_id)
    raw = load_yaml(args.config)
    pg = require(raw, "postgres")
    scorer_raw: dict[str, Any] = dict(raw.get(SERVICE_NAME) or {})

    enabled = _env_bool(f"{ENV_PREFIX}_ENABLED", bool(scorer_raw.get("enabled", False)))
    if not enabled:
        logger.info(
            f"{SERVICE_NAME}.disabled",
            extra={"event": f"{SERVICE_NAME}.disabled", "worker_id": args.worker_id},
        )
        return

    artifact_path = _env_str(f"{ENV_PREFIX}_ARTIFACT", str(scorer_raw.get("artifact_path", "")))
    if not artifact_path or not Path(artifact_path).exists():
        raise SystemExit(f"Golden Discovery Ranker artifact not found: {artifact_path!r}")

    num_workers = _env_int(f"{ENV_PREFIX}_WORKERS", int(scorer_raw.get("num_workers", 1)))
    worker_id = int(args.worker_id)
    if not (0 <= worker_id < num_workers):
        raise SystemExit(f"worker-id {worker_id} not in configured range [0, {num_workers - 1}]")

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

    # model_kind selects v1 (default) or the v2 prefetch-hybrid ranker. v2
    # optionally takes a v1 artifact as the fallback scorer for domains it does
    # not cover (unknown / non-top-30); without it the v2 general head scores them.
    model_kind = _env_str(f"{ENV_PREFIX}_MODEL_KIND", str(scorer_raw.get("model_kind", "v1"))).lower()
    if model_kind == "v2":
        v1_fb_path = _env_str(
            f"{ENV_PREFIX}_V1_FALLBACK_ARTIFACT",
            str(scorer_raw.get("v1_fallback_artifact", "")),
        )
        v1_fallback = None
        if v1_fb_path and Path(v1_fb_path).exists():
            v1_fallback = GoldenDiscoveryRuntimeScorer.load(v1_fb_path).score_many
        scorer = GoldenDiscoveryRuntimeScorerV2.load(artifact_path, v1_scorer=v1_fallback)
    else:
        scorer = GoldenDiscoveryRuntimeScorer.load(artifact_path)
    logger.info(
        f"{SERVICE_NAME}.loaded",
        extra={
            "event": f"{SERVICE_NAME}.loaded",
            "worker_id": worker_id,
            "artifact_path": artifact_path,
            "heads": ",".join(scorer.heads),
            "model_name": scorer.metadata.get("model_name"),
            "score_version": scorer.metadata.get("score_version"),
        },
    )

    cfg = GoldenDiscoveryRankerConfig(
        total_shards=_env_int(f"{ENV_PREFIX}_TOTAL_SHARDS", int(scorer_raw.get("total_shards", 256))),
        num_workers=num_workers,
        worker_id=worker_id,
        batch_size=_env_int(f"{ENV_PREFIX}_BATCH_SIZE", int(scorer_raw.get("batch_size", 1000))),
        scan_interval_sec=_env_int(
            f"{ENV_PREFIX}_SCAN_INTERVAL_SEC",
            int(scorer_raw.get("scan_interval_sec", 60)),
        ),
        max_batches_per_shard=_env_int(
            f"{ENV_PREFIX}_MAX_BATCHES_PER_SHARD",
            int(scorer_raw.get("max_batches_per_shard", 4)),
        ),
        domain_priority_steering_enabled=_env_bool(
            f"{ENV_PREFIX}_DOMAIN_PRIORITY_STEERING_ENABLED",
            bool(scorer_raw.get("domain_priority_steering_enabled", False)),
        ),
        rescore_ttl_days=_env_int(
            f"{ENV_PREFIX}_RESCORE_TTL_DAYS",
            int(scorer_raw.get("rescore_ttl_days", 5)),
        ),
        min_age_days=_env_int(
            f"{ENV_PREFIX}_MIN_AGE_DAYS",
            int(scorer_raw.get("min_age_days", 3)),
        ),
    )

    svc = GoldenDiscoveryRankerService(cfg=cfg, Session=Session, scorer=scorer)
    if args.once:
        svc.run_once()
        return
    svc.run_forever()


if __name__ == "__main__":
    main()

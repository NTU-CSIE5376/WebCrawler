"""Ingestion filtering and inline-scoring configuration."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from libs.scoring.golden_discovery_runtime import GoldenDiscoveryRuntimeScorer
from .trap_filter import DEFAULT_NORMALIZE_PARAMS, TrapFilter, TrapFilterConfig


LOGGER = logging.getLogger("ingestor")
RANKER_ENV_PREFIX = "GOLDEN_DISCOVERY_RANKER_V1"
TRAP_LOG_DIR_TEMPLATE = "/data/ipc/trap_filter/ingestor_{id:02d}"


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


def _load_trap_filter(raw: dict[str, Any], ingestor_id: int) -> TrapFilter | None:
    """Build the trap filter from the optional `trap_filter` block.

    Every key defaults to the reviewed rollout (#105), so a host that keeps its
    own ingest.yaml without the block runs the filter after a pull + restart.
    `trap_filter: {enabled: false}` is the kill switch. The startup log states
    which way it went and where the setting came from.
    """
    source = "yaml" if "trap_filter" in raw else "default"
    tf = dict(raw.get("trap_filter") or {})
    if not tf.get("enabled", True):
        LOGGER.info(
            "ingest.trap_filter_disabled",
            extra={"event": "ingest.trap_filter_disabled", "config_source": source},
        )
        return None
    log_dir_template = tf.get("log_dir_template", TRAP_LOG_DIR_TEMPLATE)
    cfg = TrapFilterConfig(
        enabled=True,
        normalize=bool(tf.get("normalize", True)),
        normalize_params=frozenset(
            str(p).lower() for p in tf.get("normalize_params", DEFAULT_NORMALIZE_PARAMS)
        ),
        shadow_normalize=bool(tf.get("shadow_normalize", True)),
        drop=bool(tf.get("drop", True)),
        log_dir=log_dir_template.format(id=ingestor_id) if log_dir_template else None,
        audit_batch_size=int(tf.get("audit_batch_size", TrapFilterConfig.audit_batch_size)),
        audit_max_buffer_bytes=int(tf.get("audit_max_buffer_bytes", TrapFilterConfig.audit_max_buffer_bytes)),
        audit_flush_interval_seconds=float(tf.get("audit_flush_interval_seconds", TrapFilterConfig.audit_flush_interval_seconds)),
    )
    LOGGER.info(
        "ingest.trap_filter_enabled",
        extra={"event": "ingest.trap_filter_enabled", "config_source": source,
               "normalize": cfg.normalize,
               "normalize_params": ",".join(sorted(cfg.normalize_params)),
               "shadow_normalize": cfg.shadow_normalize, "drop": cfg.drop, "drop_rules": ",".join(sorted(cfg.drop_rules)),
               "log_dir": cfg.log_dir, "audit_batch_size": cfg.audit_batch_size,
               "audit_max_buffer_bytes": cfg.audit_max_buffer_bytes,
               "audit_flush_interval_seconds": cfg.audit_flush_interval_seconds},
    )
    return TrapFilter(cfg)

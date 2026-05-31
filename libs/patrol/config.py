"""Load and resolve patrol cron configuration from yaml.

The cron is a stateless script invoked by cron(8); it loads this config
on every run. Keeping the schema in one place (this module) means the
cadence math (libs.patrol.cadence) and the cron loop never see raw
yaml — they take a typed PatrolConfig.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from libs.patrol.cadence import BUCKET_ORDER, CadencePolicy


@dataclass(frozen=True)
class PatrolConfig:
    """Fully resolved patrol policy for one cron run."""

    cadence_policy: CadencePolicy
    patrol_priority: float
    grace_period_seconds: int
    cron_batch_size: int
    cycle_days: int


def _require(d: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in d:
        raise ValueError(f"Missing required config key {where}.{key}")
    return d[key]


def _optional_int(raw: Mapping[str, Any], key: str, default: int) -> int:
    """Like raw.get(key, default) but also coerces yaml `null` to default."""
    v = raw.get(key)
    return int(default if v is None else v)


def _optional_float(raw: Mapping[str, Any], key: str, default: float) -> float:
    v = raw.get(key)
    return float(default if v is None else v)


def _validate_intervals(intervals: Mapping[str, int]) -> None:
    """Every bucket the cadence state machine knows about must have an
    interval — otherwise interval_seconds() raises at the worst moment
    (inside the cron loop). Surface the gap at config parse time."""
    missing = [b for b in BUCKET_ORDER if b not in intervals]
    if missing:
        raise ValueError(
            f"cadence_buckets missing required bucket(s) {missing}; "
            f"BUCKET_ORDER = {list(BUCKET_ORDER)}"
        )
    for name, sec in intervals.items():
        if sec <= 0:
            raise ValueError(
                f"cadence_buckets.{name} must be positive seconds, got {sec}"
            )


def _validate_thresholds(
    *,
    promote_threshold: int,
    demote_threshold: int,
    miss_demote: int,
    miss_retire: int,
) -> None:
    """Threshold consistency. The retire-vs-demote ordering is the subtle
    one: cadence.long_loop_transition() checks retire first, so if
    miss_retire <= miss_demote the demote branch is unreachable and the
    parent retires on its first qualifying miss."""
    for name, v in (
        ("bucket_transitions.promote_threshold", promote_threshold),
        ("bucket_transitions.demote_threshold", demote_threshold),
        ("retire_policy.consecutive_miss_batches_demote", miss_demote),
        ("retire_policy.consecutive_miss_batches_retire", miss_retire),
    ):
        if v <= 0:
            raise ValueError(f"{name} must be positive, got {v}")
    if miss_retire <= miss_demote:
        raise ValueError(
            f"retire_policy.consecutive_miss_batches_retire ({miss_retire}) must "
            f"be > consecutive_miss_batches_demote ({miss_demote}); "
            f"otherwise retire fires before demote can run."
        )


def _validate_optional_positives(cfg: "PatrolConfig") -> None:
    if cfg.patrol_priority <= 0:
        raise ValueError(
            f"patrol_priority must be positive, got {cfg.patrol_priority}"
        )
    if cfg.grace_period_seconds < 0:
        raise ValueError(
            f"grace_period_seconds must be >= 0, got {cfg.grace_period_seconds}"
        )
    if cfg.cron_batch_size <= 0:
        raise ValueError(
            f"cron_batch_size must be positive, got {cfg.cron_batch_size}"
        )
    if cfg.cycle_days <= 0:
        raise ValueError(f"cycle_days must be positive, got {cfg.cycle_days}")


def parse_patrol_config(raw: Mapping[str, Any]) -> PatrolConfig:
    """Parse the `golden_parent_patrol` block of the yaml config.

    Required structure:

      golden_parent_patrol:
        enabled: bool                # not parsed here; the runner checks it
        patrol_priority: 1.0
        cadence_buckets:
          medium: 21600
          slow: 86400
          trial: 172800
          cold: 604800
        bucket_transitions:
          promote_threshold: 5
          demote_threshold: 2
        retire_policy:
          consecutive_miss_batches_demote: 1
          consecutive_miss_batches_retire: 2
        grace_period_seconds: 900
        cron_batch_size: 1000
        cycle_days: 14

    Missing optional keys raise ValueError so a misconfigured yaml fails
    fast in the cron's first run instead of silently behaving wrong.
    """
    intervals_raw = _require(raw, "cadence_buckets", "golden_parent_patrol")
    intervals = {str(k): int(v) for k, v in intervals_raw.items()}
    _validate_intervals(intervals)

    transitions = _require(raw, "bucket_transitions", "golden_parent_patrol")
    promote_threshold = int(_require(transitions, "promote_threshold", "bucket_transitions"))
    demote_threshold = int(_require(transitions, "demote_threshold", "bucket_transitions"))

    retire = _require(raw, "retire_policy", "golden_parent_patrol")
    miss_demote = int(
        _require(retire, "consecutive_miss_batches_demote", "retire_policy")
    )
    miss_retire = int(
        _require(retire, "consecutive_miss_batches_retire", "retire_policy")
    )
    _validate_thresholds(
        promote_threshold=promote_threshold,
        demote_threshold=demote_threshold,
        miss_demote=miss_demote,
        miss_retire=miss_retire,
    )

    cadence_policy = CadencePolicy(
        intervals_sec=intervals,
        promote_new_url_threshold=promote_threshold,
        demote_no_new_url_threshold=demote_threshold,
        retire_consecutive_miss_batches=miss_retire,
        demote_on_consecutive_miss_batches=miss_demote,
    )

    cfg = PatrolConfig(
        cadence_policy=cadence_policy,
        patrol_priority=_optional_float(raw, "patrol_priority", 1.0),
        grace_period_seconds=_optional_int(raw, "grace_period_seconds", 900),
        cron_batch_size=_optional_int(raw, "cron_batch_size", 1000),
        cycle_days=_optional_int(raw, "cycle_days", 14),
    )
    _validate_optional_positives(cfg)
    return cfg

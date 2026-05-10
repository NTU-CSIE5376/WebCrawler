"""Load and resolve patrol cron configuration from yaml.

The cron is a stateless script invoked by cron(8); it loads this config
on every run. Keeping the schema in one place (this module) means the
cadence math (libs.patrol.cadence) and the cron loop never see raw
yaml — they take a typed PatrolConfig.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from libs.patrol.cadence import CadencePolicy


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


def parse_patrol_config(raw: Mapping[str, Any]) -> PatrolConfig:
    """Parse the `golden_parent_patrol` block of the yaml config.

    Required structure:

      golden_parent_patrol:
        enabled: bool                # not parsed here; the runner checks it
        patrol_priority: 1.0
        cadence_buckets:
          fast: 3600
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

    cadence_policy = CadencePolicy(
        intervals_sec=intervals,
        promote_new_url_threshold=promote_threshold,
        demote_no_new_url_threshold=demote_threshold,
        retire_consecutive_miss_batches=miss_retire,
        demote_on_consecutive_miss_batches=miss_demote,
    )

    return PatrolConfig(
        cadence_policy=cadence_policy,
        patrol_priority=float(raw.get("patrol_priority", 1.0)),
        grace_period_seconds=int(raw.get("grace_period_seconds", 900)),
        cron_batch_size=int(raw.get("cron_batch_size", 1000)),
        cycle_days=int(raw.get("cycle_days", 14)),
    )

"""Cadence-bucket state machine for golden parent patrol.

Pure functions, no IO. Two loops feed in here:

  * Short loop, every patrol cycle: short_loop_transition() looks at the
    new-URL-count from the most recent fetch and decides whether to
    promote (more new URLs than expected → fetch more often) or demote
    (no new URLs for several cycles → fetch less often). One step at a
    time, with hysteresis on demotion to absorb single-cycle noise.

  * Long loop, every new golden batch: long_loop_transition() looks at
    whether the parent first-discovered any URLs in the closing batch
    and decides whether to demote (one miss → step down) or retire
    (two consecutive misses → out).

The bucket set, thresholds, and interval values are config-driven; only
the *order* (medium → cold) is intrinsic. Adding a bucket means adding it
to the policy and updating BUCKET_ORDER.
"""
from __future__ import annotations

from dataclasses import dataclass


# Ordered fastest → slowest. Promotion moves left, demotion moves right.
# When a bucket sits at the end of the array, further promotion / demotion
# in that direction is a no-op. "medium" is the fastest bucket (6h) —
# faster (1h) was dropped in review to avoid hammering anti-bot WAFs on
# the productive parent pages.
BUCKET_ORDER: tuple[str, ...] = ("medium", "slow", "trial", "cold")


@dataclass(frozen=True)
class CadencePolicy:
    """Resolved config for one cron run.

    Built by libs.patrol.config from the patrol yaml; this dataclass keeps
    the cadence math fully decoupled from yaml parsing so it can be unit
    tested without touching disk.
    """

    intervals_sec: dict[str, int]
    promote_new_url_threshold: int
    demote_no_new_url_threshold: int
    retire_consecutive_miss_batches: int
    demote_on_consecutive_miss_batches: int


def interval_seconds(bucket: str, policy: CadencePolicy) -> int:
    if bucket not in policy.intervals_sec:
        raise ValueError(
            f"unknown cadence bucket {bucket!r}; "
            f"policy has {list(policy.intervals_sec)}"
        )
    return policy.intervals_sec[bucket]


def promote_bucket(bucket: str) -> str:
    """Move one step toward the fastest bucket. No-op if already at the fastest."""
    if bucket not in BUCKET_ORDER:
        return bucket
    idx = BUCKET_ORDER.index(bucket)
    return BUCKET_ORDER[max(0, idx - 1)]


def demote_bucket(bucket: str) -> str:
    """Move one step toward `cold`. No-op if already at slowest."""
    if bucket not in BUCKET_ORDER:
        return bucket
    idx = BUCKET_ORDER.index(bucket)
    return BUCKET_ORDER[min(len(BUCKET_ORDER) - 1, idx + 1)]


@dataclass(frozen=True)
class ShortLoopOutcome:
    new_bucket: str
    new_consecutive_no_new_url: int


def short_loop_transition(
    *,
    current_bucket: str,
    consecutive_no_new_url: int,
    new_url_count: int,
    policy: CadencePolicy,
) -> ShortLoopOutcome:
    """Decide whether to promote, demote, or hold the cadence bucket.

    Rules:
      * new_url_count >= promote_threshold → promote one step, reset the
        no-new-url counter.
      * new_url_count == 0 → increment the no-new-url counter; if the
        counter has hit demote_threshold, demote one step and reset the
        counter (so the next demotion is a fresh streak).
      * 0 < new_url_count < promote_threshold → hold; reset counter.

    The hysteresis (demote only after `demote_no_new_url_threshold`
    consecutive zeros) prevents flapping when a parent has occasional
    quiet cycles.
    """
    if new_url_count >= policy.promote_new_url_threshold:
        return ShortLoopOutcome(
            new_bucket=promote_bucket(current_bucket),
            new_consecutive_no_new_url=0,
        )
    if new_url_count == 0:
        next_consec = consecutive_no_new_url + 1
        if next_consec >= policy.demote_no_new_url_threshold:
            return ShortLoopOutcome(
                new_bucket=demote_bucket(current_bucket),
                new_consecutive_no_new_url=0,
            )
        return ShortLoopOutcome(
            new_bucket=current_bucket,
            new_consecutive_no_new_url=next_consec,
        )
    return ShortLoopOutcome(
        new_bucket=current_bucket,
        new_consecutive_no_new_url=0,
    )


@dataclass(frozen=True)
class LongLoopOutcome:
    new_bucket: str
    new_status: str
    new_consecutive_miss_batches: int


def long_loop_transition(
    *,
    current_bucket: str,
    current_status: str,
    consecutive_miss_batches: int,
    cycle_hits: int,
    policy: CadencePolicy,
) -> LongLoopOutcome:
    """Apply long-loop (per-batch) result to bucket / status / miss counter.

    cycle_hits is the count of golden child URLs first-discovered by this
    parent within the closing batch's cycle window.

      * cycle_hits >= 1 → keep bucket / status, reset miss counter.
      * cycle_hits == 0 and the new miss-streak hits the retire
        threshold → status = retired (cron stops scheduling it; cadence
        bucket is left alone for analytics).
      * cycle_hits == 0 and the new miss-streak hits the demote-only
        threshold → demote one bucket, hold status.
      * Otherwise (cycle_hits == 0, but streak below thresholds) → hold
        bucket and status; just bump the counter.
    """
    if cycle_hits >= 1:
        return LongLoopOutcome(
            new_bucket=current_bucket,
            new_status=current_status,
            new_consecutive_miss_batches=0,
        )

    next_consec = consecutive_miss_batches + 1
    if next_consec >= policy.retire_consecutive_miss_batches:
        return LongLoopOutcome(
            new_bucket=current_bucket,
            new_status="retired",
            new_consecutive_miss_batches=next_consec,
        )
    if next_consec >= policy.demote_on_consecutive_miss_batches:
        return LongLoopOutcome(
            new_bucket=demote_bucket(current_bucket),
            new_status=current_status,
            new_consecutive_miss_batches=next_consec,
        )
    return LongLoopOutcome(
        new_bucket=current_bucket,
        new_status=current_status,
        new_consecutive_miss_batches=next_consec,
    )

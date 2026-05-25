from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker


logger = logging.getLogger("gc")


@dataclass(frozen=True)
class FrontierGCConfig:
    total_shards: int

    # Freeze a domain once it holds > frontier_cap pending URLs AND its fetch
    # yield is < yield_floor. sample_percent finds candidates before exact
    # verification. See docs/06 for the tuning rationale.
    frontier_cap: int
    yield_floor: float
    sample_percent: float

    # Evict never-scheduled frontier rows older than these many days
    # (frozen domains drain faster), in trickle batches.
    stale_pending_days: int
    frozen_pending_days: int
    batch_size: int

    # Daily UTC schedule, mirroring the accounting roll-off.
    run_hour_utc: int
    run_minute_utc: int
    check_interval_sec: int
    catch_up_on_start: bool


class FrontierGCService:
    """Bounds the crawl frontier so no single eTLD+1 accumulates an unbounded
    backlog of never-fetched URLs. Each daily pass, per shard: refresh
    `domain_state.discovery_frozen` from current pending/yield, then evict
    stale never-scheduled rows in trickle batches. Does not touch
    `url_state_history`.
    """

    def __init__(self, cfg: FrontierGCConfig, Session: sessionmaker):
        self.cfg = cfg
        self.Session = Session
        self._last_run_for_day: date | None = None

    @staticmethod
    def _tcur(shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    # --- freeze refresh -------------------------------------------------

    def _candidate_domains(self, shard_id: int) -> set[int]:
        """Domains worth an exact re-check: those that look oversized in a cheap
        sample (estimated pending over half the cap, leaving noise headroom),
        plus every currently frozen domain so drained ones can be unfrozen."""
        tcur = self._tcur(shard_id)
        sql = text(f"""
            WITH s AS (
                SELECT domain_id, count(*) AS c
                FROM {tcur} TABLESAMPLE SYSTEM (:pct)
                WHERE should_crawl = TRUE
                GROUP BY domain_id
            )
            SELECT domain_id FROM s
            WHERE c::float / (:pct / 100.0) > (:cap / 2.0)
        """)
        with self.Session() as sess:
            sampled = {
                r[0]
                for r in sess.execute(
                    sql, {"pct": self.cfg.sample_percent, "cap": self.cfg.frontier_cap}
                ).fetchall()
            }
            frozen = {
                r[0]
                for r in sess.execute(
                    text(
                        "SELECT domain_id FROM domain_state "
                        "WHERE shard_id = :s AND discovery_frozen = TRUE"
                    ),
                    {"s": shard_id},
                ).fetchall()
            }
        return sampled | frozen

    def _refresh_freeze(self, shard_id: int) -> dict[str, int]:
        tcur = self._tcur(shard_id)
        froze = unfroze = 0
        for domain_id in self._candidate_domains(shard_id):
            with self.Session() as sess:
                # Single index scan on (domain_id) returns all three counts.
                row = sess.execute(
                    text(f"""
                        SELECT
                            count(*) AS total,
                            count(*) FILTER (WHERE should_crawl) AS pending,
                            count(*) FILTER (WHERE last_fetch_ok IS NOT NULL) AS ok
                        FROM {tcur} WHERE domain_id = :d
                    """),
                    {"d": domain_id},
                ).first()
                total, pending, ok = int(row.total), int(row.pending), int(row.ok)
                yield_rate = ok / total if total else 0.0
                should_freeze = (
                    pending > self.cfg.frontier_cap and yield_rate < self.cfg.yield_floor
                )
                changed = sess.execute(
                    text(
                        "UPDATE domain_state SET discovery_frozen = :f "
                        "WHERE domain_id = :d AND discovery_frozen IS DISTINCT FROM :f"
                    ),
                    {"f": should_freeze, "d": domain_id},
                ).rowcount
                sess.commit()
            if changed:
                if should_freeze:
                    froze += 1
                    logger.info(
                        "gc.freeze",
                        extra={"event": "gc.freeze", "shard_id": shard_id,
                               "domain_id": domain_id, "pending": pending,
                               "yield": round(yield_rate, 5)},
                    )
                else:
                    unfroze += 1
        return {"froze": froze, "unfroze": unfroze}

    # --- eviction -------------------------------------------------------

    def _evict(self, shard_id: int, where: str, params: dict) -> int:
        """Delete one trickle batch matching `where`, return rows removed."""
        tcur = self._tcur(shard_id)
        sql = text(f"""
            WITH picked AS (
                SELECT ctid FROM {tcur}
                WHERE {where}
                ORDER BY first_seen
                LIMIT :batch
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {tcur} WHERE ctid IN (SELECT ctid FROM picked)
        """)
        params = {**params, "batch": self.cfg.batch_size}
        with self.Session() as sess:
            n = sess.execute(sql, params).rowcount
            sess.commit()
        return n

    def _evict_shard(self, shard_id: int) -> dict[str, int]:
        out = {"frozen_evicted": 0, "stale_evicted": 0}

        # Frozen-domain frontier: drain aggressively. Uses the domain_id index.
        frozen_where = (
            "domain_id IN (SELECT domain_id FROM domain_state "
            "WHERE shard_id = :s AND discovery_frozen = TRUE) "
            "AND should_crawl = TRUE "
            "AND first_seen < now() - make_interval(days => :days)"
        )
        while True:
            n = self._evict(shard_id, frozen_where,
                            {"s": shard_id, "days": self.cfg.frozen_pending_days})
            out["frozen_evicted"] += n
            if n == 0:
                break

        # General stale frontier the ranker never scored. Uses the partial
        # index idx_..._golden_discovery_v1_unscored(first_seen).
        stale_where = (
            "should_crawl = TRUE AND url_score_updated_at IS NULL "
            "AND first_seen < now() - make_interval(days => :days)"
        )
        while True:
            n = self._evict(shard_id, stale_where, {"days": self.cfg.stale_pending_days})
            out["stale_evicted"] += n
            if n == 0:
                break
        return out

    # --- run loop -------------------------------------------------------

    def run_once(self) -> None:
        totals = {"froze": 0, "unfroze": 0, "frozen_evicted": 0, "stale_evicted": 0}
        for shard_id in range(self.cfg.total_shards):
            f = self._refresh_freeze(shard_id)
            e = self._evict_shard(shard_id)
            for k, v in {**f, **e}.items():
                totals[k] += v
        logger.info("gc.run_once", extra={"event": "gc.run_once", **totals})

    def _scheduled_today_utc(self, now_utc: datetime) -> datetime:
        run_t = dt_time(hour=self.cfg.run_hour_utc, minute=self.cfg.run_minute_utc)
        return datetime.combine(now_utc.date(), run_t, tzinfo=timezone.utc)

    def run_forever(self) -> None:
        while True:
            now_utc = datetime.now(timezone.utc)
            scheduled_today = self._scheduled_today_utc(now_utc)
            should_run = False
            if self.cfg.catch_up_on_start and self._last_run_for_day is None:
                if now_utc >= scheduled_today:
                    should_run = True
            elif now_utc >= scheduled_today and self._last_run_for_day != now_utc.date():
                should_run = True

            if should_run:
                try:
                    self.run_once()
                    self._last_run_for_day = now_utc.date()
                except Exception as e:
                    logger.error("gc.run_error",
                                 extra={"event": "gc.run_error", "error": str(e)})
            time.sleep(self.cfg.check_interval_sec)

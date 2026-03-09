from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker


@dataclass(frozen=True)
class CounterRolloffConfig:
    total_shards: int
    event_retention_days: int
    batch_size: int
    run_hour_utc: int
    run_minute_utc: int
    check_interval_sec: int
    catch_up_on_start: bool


class CounterRolloffService:
    def __init__(self, cfg: CounterRolloffConfig, Session: sessionmaker):
        self.cfg = cfg
        self.Session = Session
        self._last_run_for_day: date | None = None

    @staticmethod
    def _tcur(shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    @staticmethod
    def _this(shard_id: int) -> str:
        return f"url_state_history_{shard_id:03d}"

    @staticmethod
    def _tevt(shard_id: int) -> str:
        return f"url_event_counter_{shard_id:03d}"

    def _scheduled_today_utc(self, now_utc: datetime) -> datetime:
        run_t = dt_time(hour=self.cfg.run_hour_utc, minute=self.cfg.run_minute_utc)
        return datetime.combine(now_utc.date(), run_t, tzinfo=timezone.utc)

    def _process_batch(self, shard_id: int, cutoff_date: date) -> dict[str, int]:
        tcur = self._tcur(shard_id)
        thst = self._this(shard_id)
        tevt = self._tevt(shard_id)

        sql = text(
            f"""
            WITH picked AS (
                SELECT
                    e.url,
                    e.event_date,
                    e.num_scheduled,
                    e.num_fetch_ok,
                    e.num_fetch_fail,
                    e.num_content_update
                FROM {tevt} e
                WHERE e.accounted = TRUE
                  AND e.event_date <= :cutoff_date
                ORDER BY e.event_date, e.url
                FOR UPDATE SKIP LOCKED
                LIMIT :batch_size
            ),
            lockable AS (
                SELECT c.url
                FROM {tcur} c
                WHERE c.url IN (SELECT DISTINCT p.url FROM picked p)
                FOR UPDATE OF c SKIP LOCKED
            ),
            processable AS (
                SELECT p.*
                FROM picked p
                JOIN lockable l ON l.url = p.url
            ),
            processable_by_url AS (
                SELECT
                    p.url,
                    SUM(p.num_scheduled) AS num_scheduled,
                    SUM(p.num_fetch_ok) AS num_fetch_ok,
                    SUM(p.num_fetch_fail) AS num_fetch_fail,
                    SUM(p.num_content_update) AS num_content_update
                FROM processable p
                GROUP BY p.url
            ),
            updated AS (
                UPDATE {tcur} c
                SET
                    num_scheduled_90d = GREATEST(0, c.num_scheduled_90d - pbu.num_scheduled),
                    num_fetch_ok_90d = GREATEST(0, c.num_fetch_ok_90d - pbu.num_fetch_ok),
                    num_fetch_fail_90d = GREATEST(0, c.num_fetch_fail_90d - pbu.num_fetch_fail),
                    num_content_update_90d = GREATEST(0, c.num_content_update_90d - pbu.num_content_update)
                FROM processable_by_url pbu
                WHERE c.url = pbu.url
                RETURNING
                    c.url,
                    c.domain_id,
                    c.first_seen,
                    c.last_scheduled,
                    c.last_fetch_ok,
                    c.last_content_update,
                    c.num_scheduled_90d,
                    c.num_fetch_ok_90d,
                    c.num_fetch_fail_90d,
                    c.num_content_update_90d,
                    c.num_consecutive_fail,
                    c.last_fail_reason,
                    c.content_hash,
                    c.should_crawl,
                    c.url_score,
                    c.domain_score
            ),
            inserted_hist AS (
                INSERT INTO {thst} (
                    url,
                    domain_id,
                    first_seen,
                    last_scheduled,
                    last_fetch_ok,
                    last_content_update,
                    num_scheduled_90d,
                    num_fetch_ok_90d,
                    num_fetch_fail_90d,
                    num_content_update_90d,
                    num_consecutive_fail,
                    last_fail_reason,
                    content_hash,
                    should_crawl,
                    url_score,
                    domain_score
                )
                SELECT
                    u.url,
                    u.domain_id,
                    u.first_seen,
                    u.last_scheduled,
                    u.last_fetch_ok,
                    u.last_content_update,
                    u.num_scheduled_90d,
                    u.num_fetch_ok_90d,
                    u.num_fetch_fail_90d,
                    u.num_content_update_90d,
                    u.num_consecutive_fail,
                    u.last_fail_reason,
                    u.content_hash,
                    u.should_crawl,
                    u.url_score,
                    u.domain_score
                FROM updated u
                RETURNING 1
            ),
            missing AS (
                SELECT p.url, p.event_date
                FROM picked p
                LEFT JOIN {tcur} c ON c.url = p.url
                WHERE c.url IS NULL
            ),
            marked AS (
                UPDATE {tevt} e
                SET accounted = FALSE
                FROM (
                    SELECT p.url, p.event_date
                    FROM processable p
                    UNION ALL
                    SELECT m.url, m.event_date
                    FROM missing m
                ) done
                WHERE e.url = done.url
                  AND e.event_date = done.event_date
                RETURNING 1
            )
            SELECT
                (SELECT COUNT(*) FROM picked) AS picked_count,
                (SELECT COUNT(*) FROM processable) AS processed_count,
                (SELECT COUNT(*) FROM missing) AS missing_count,
                (SELECT COUNT(*) FROM inserted_hist) AS history_count,
                (SELECT COUNT(*) FROM marked) AS marked_count
            """
        )

        with self.Session() as sess:
            row = sess.execute(
                sql, {"cutoff_date": cutoff_date, "batch_size": self.cfg.batch_size}
            ).first()
            sess.commit()

        out = dict(row._mapping)
        return {k: int(v) for k, v in out.items()}

    def run_once(self) -> None:
        cutoff_date = datetime.now(timezone.utc).date() - timedelta(
            days=self.cfg.event_retention_days
        )
        totals = {
            "picked_count": 0,
            "processed_count": 0,
            "missing_count": 0,
            "history_count": 0,
            "marked_count": 0,
            "stalled_batches": 0,
        }

        for shard_id in range(self.cfg.total_shards):
            while True:
                stats = self._process_batch(shard_id=shard_id, cutoff_date=cutoff_date)
                for key in (
                    "picked_count",
                    "processed_count",
                    "missing_count",
                    "history_count",
                    "marked_count",
                ):
                    totals[key] += stats[key]

                if stats["picked_count"] == 0:
                    break

                if stats["marked_count"] == 0:
                    totals["stalled_batches"] += 1
                    break

        print(
            f"[accounting] done cutoff={cutoff_date.isoformat()} totals={totals}",
            flush=True,
        )

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
                    print(f"[accounting] ERROR: {e}", flush=True)

            time.sleep(self.cfg.check_interval_sec)

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from sqlalchemy import text, insert
from sqlalchemy.orm import sessionmaker

from libs.db import url_state_history_table

@dataclass
class IngestResult:
    new_link: bool = False
    domain_id: int | None = None
    is_ok: bool = False
    is_upd: bool = False
    fail_reason: str | None = None


BATCH_SIZE = 500


class IngestDB:
    """
    Updates:
      - url_state_current_{shard}
      - url_state_history_{shard}
      - url_event_counter_{shard}
    Insert newly discovered links

    Returns counters for stats aggregation.
    """
    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _tcur(self, shard_id: int) -> str:
        return f"url_state_current_{shard_id:03d}"

    def _this(self, shard_id: int) -> str:
        return f"url_state_history_{shard_id:03d}"

    def _tevt(self, shard_id: int) -> str:
        return f"url_event_counter_{shard_id:03d}"

    def _process_one_result(self, sess, rec: dict) -> IngestResult:
        url = rec["url"]
        shard_id = int(rec["shard_id"])
        domain_id = int(rec["domain_id"])
        status = rec["status"]
        fetched_at = datetime.fromisoformat(rec["fetched_at"]) if rec.get("fetched_at") else datetime.now(timezone.utc)
        fail_reason = rec.get("fail_reason")
        content_hash = rec.get("content_hash")

        tcur = self._tcur(shard_id)

        is_ok = status == "ok"
        if is_ok and content_hash is not None:
            old = sess.execute(
                text(f"SELECT content_hash FROM {tcur} WHERE url = :url"),
                {"url": url},
            ).first()
            old_hash = old.content_hash if old else None
            is_upd = content_hash != old_hash
        else:
            is_upd = False

        row = sess.execute(
            text(f"""
            INSERT INTO {tcur} (
              url, domain_id,
              last_fetch_ok, last_content_update,
              num_fetch_ok_90d, num_fetch_fail_90d, num_content_update_90d,
              num_consecutive_fail, last_fail_reason,
              content_hash, should_crawl
            )
            VALUES (
              :url, :domain_id,
              CASE WHEN :is_ok THEN :fetched_at ELSE NULL END,
              CASE WHEN :is_upd THEN :fetched_at ELSE NULL END,
              :inc_ok, :inc_fail, :inc_upd,
              CASE WHEN :is_ok THEN 0 ELSE 1 END,
              CASE WHEN :is_ok THEN NULL ELSE :fail_reason END,
              :content_hash,
              FALSE
            )
            ON CONFLICT (url) DO UPDATE SET
              last_fetch_ok = COALESCE(EXCLUDED.last_fetch_ok, {tcur}.last_fetch_ok),
              last_content_update = COALESCE(EXCLUDED.last_content_update, {tcur}.last_content_update),
              num_fetch_ok_90d = {tcur}.num_fetch_ok_90d + EXCLUDED.num_fetch_ok_90d,
              num_fetch_fail_90d = {tcur}.num_fetch_fail_90d + EXCLUDED.num_fetch_fail_90d,
              num_content_update_90d = {tcur}.num_content_update_90d + EXCLUDED.num_content_update_90d,
              num_consecutive_fail = CASE
                WHEN :is_ok THEN 0
                ELSE {tcur}.num_consecutive_fail + 1
              END,
              last_fail_reason = EXCLUDED.last_fail_reason,
              content_hash = CASE
                WHEN :is_upd THEN EXCLUDED.content_hash
                ELSE {tcur}.content_hash
              END,
              should_crawl = FALSE
            RETURNING
              *, (xmax = 0) AS inserted;
            """),
            {
                "url": url,
                "domain_id": domain_id,
                "fetched_at": fetched_at,
                "is_ok": is_ok,
                "is_upd": is_upd,
                "inc_ok": int(is_ok),
                "inc_fail": 1 - int(is_ok),
                "inc_upd": int(is_upd),
                "fail_reason": fail_reason,
                "content_hash": content_hash,
            },
        ).first()

        row_dict = dict(row._mapping)
        inserted = row_dict.pop('inserted')

        sess.execute(insert(url_state_history_table(shard_id)).values(**row_dict))

        tevt = self._tevt(shard_id)
        sess.execute(
            text(f"""
            INSERT INTO {tevt} (
              url, event_date,
              num_fetch_ok, num_fetch_fail, num_content_update,
              accounted
            )
            VALUES (
              :url, CURRENT_DATE,
              :ok, :fail, :upd,
              TRUE
            )
            ON CONFLICT (url, event_date) DO UPDATE SET
              num_fetch_ok = {tevt}.num_fetch_ok + EXCLUDED.num_fetch_ok,
              num_fetch_fail = {tevt}.num_fetch_fail + EXCLUDED.num_fetch_fail,
              num_content_update = {tevt}.num_content_update + EXCLUDED.num_content_update
            """),
            {"url": url, "ok": int(is_ok), "fail": 1 - int(is_ok), "upd": int(is_upd)},
        )

        return IngestResult(
            new_link=bool(inserted),
            domain_id=domain_id,
            is_ok=is_ok,
            is_upd=is_upd,
            fail_reason=fail_reason,
        )

    def _process_one_link(self, sess, rec: dict) -> bool:
        url = rec["url"]
        shard_id = int(rec["shard_id"])
        domain_id = int(rec["domain_id"])
        domain_score = float(rec.get("domain_score", 0.0))

        tcur = self._tcur(shard_id)
        th = self._this(shard_id)

        inserted_url = sess.execute(
            text(f"""
            INSERT INTO {tcur} (url, domain_id, domain_score)
            VALUES (:url, :domain_id, :domain_score)
            ON CONFLICT (url) DO NOTHING
            RETURNING url;
            """),
            {"url": url, "domain_id": domain_id, "domain_score": domain_score},
        ).scalar_one_or_none()

        if inserted_url is not None:
            sess.execute(
                text(f"""
                INSERT INTO {th} (url, domain_id, domain_score)
                VALUES (:url, :domain_id, :domain_score)
                RETURNING 1;
                """),
                {"url": url, "domain_id": domain_id, "domain_score": domain_score},
            )

        return inserted_url is not None

    def process_batch(self, recs: list[dict]) -> list[IngestResult | bool]:
        """Process a batch of records in a single transaction.

        Returns a list matching input order. Each element is either:
          - IngestResult for crawl result records
          - bool for new link records (True if newly inserted)
          - None if the record raised an error
        """
        results: list[IngestResult | bool | None] = []
        with self.Session() as sess:
            try:
                for i, rec in enumerate(recs):
                    try:
                        if rec.get("status") == "new":
                            results.append(self._process_one_link(sess, rec))
                        else:
                            results.append(self._process_one_result(sess, rec))
                    except Exception:
                        sess.rollback()
                        raise
                sess.commit()
            except Exception:
                sess.rollback()
                raise
        return results

    # Keep single-record methods for backward compatibility.
    def process_result(self, rec: dict) -> IngestResult:
        return self.process_batch([rec])[0]

    def process_link(self, rec: dict) -> bool:
        return self.process_batch([rec])[0]



from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from sqlalchemy import text, insert
from sqlalchemy.orm import sessionmaker


class FeatureDB:
    """
    Updates:
      - content_feature_current_{shard}
      - content_feature_history_{shard}
    """
    def __init__(self, Session: sessionmaker):
        self.Session = Session

    def _tcur(self, shard_id: int) -> str:
        return f"content_feature_current_{shard_id:03d}"

    def _this(self, shard_id: int) -> str:
        return f"content_feature_history_{shard_id:03d}"

    def process(self, rec: dict) -> None:
        url = rec["url"]
        shard_id = rec["shard_id"]
        domain_id = rec["domain_id"]
        fetched_at = rec["fetched_at"]
        content_length = rec["content_length"]
        content_hash = rec["content_hash"]
        num_links = rec["num_links"]

        with self.Session() as sess:
            try:
                sess.execute(
                    text(f"""
                    INSERT INTO {self._tcur(shard_id)} (
                      url, domain_id, fetched_at,
                      content_length, content_hash,
                      num_links
                    )
                    VALUES (
                      :url, :domain_id, :fetched_at,
                      :content_length, :content_hash,
                      :num_links
                    )
                    ON CONFLICT (url) DO UPDATE SET
                      fetched_at = EXCLUDED.fetched_at,
                      content_length = EXCLUDED.content_length,
                      content_hash = EXCLUDED.content_hash,
                      num_links = EXCLUDED.num_links
                      ;
                    """),
                    {
                        "url": url,
                        "domain_id": domain_id,
                        "fetched_at": fetched_at,
                        "content_length": content_length,
                        "content_hash": content_hash,
                        "num_links": num_links
                    },
                )

                sess.execute(
                    text(f"""
                    INSERT INTO {self._this(shard_id)} (
                      url, domain_id, fetched_at,
                      content_length, content_hash,
                      num_links
                    )
                    VALUES (
                      :url, :domain_id, :fetched_at,
                      :content_length, :content_hash,
                      :num_links
                    )
                    ;
                    """),
                    {
                        "url": url,
                        "domain_id": domain_id,
                        "fetched_at": fetched_at,
                        "content_length": content_length,
                        "content_hash": content_hash,
                        "num_links": num_links
                    },
                )
                sess.commit()

            except Exception as e:
                sess.rollback()
                raise e


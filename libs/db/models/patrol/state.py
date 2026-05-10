from __future__ import annotations

from sqlalchemy import (
    ARRAY,
    Column,
    DateTime,
    Integer,
    SmallInteger,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from libs.db.base import Base


class GoldenParentPatrolState(Base):
    """Single global table tracking parent pages re-visited by the golden
    parent patrol cron.

    Distinct from `url_state_current_*`: that table is the URL frontier
    (what the crawler should fetch). This table is the patrol watchlist
    (which parents are worth re-fetching, how often, and how they have
    been performing).

    Children discovered from a patrolled parent flow through the normal
    ingest path into `url_state_current_*` with the parent URL written
    to `discovered_from`; this row only stores patrol lifecycle state.
    """

    __tablename__ = "golden_parent_patrol_state"

    # Identity
    parent_key = Column(String, primary_key=True)
    fetch_url = Column(String, nullable=False)
    aliases = Column(ARRAY(String), nullable=False, server_default=text("'{}'::text[]"))
    parent_domain = Column(String, nullable=False)
    shard_id = Column(SmallInteger, nullable=False)

    # Source tagging — informs initial cadence policy.
    # Values: 'live_observed', 'wat_exact', 'manual'
    source_type = Column(String, nullable=False)
    first_enrolled_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Lifecycle.
    # status values: 'trial', 'active', 'cooling', 'retired'
    # cadence_bucket values: 'fast', 'medium', 'slow', 'trial', 'cold'
    status = Column(String, nullable=False, server_default=text("'trial'"))
    cadence_bucket = Column(String, nullable=False, server_default=text("'trial'"))

    # Cron timestamps. last_patrol_at == when cron last wrote should_crawl=TRUE.
    # last_observed_fetch_at == url_state_current.last_fetch_ok the last time
    # we synced; used to detect "crawler has actually fetched since we asked".
    last_patrol_at = Column(DateTime(timezone=True))
    next_patrol_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    last_observed_fetch_at = Column(DateTime(timezone=True))

    # Short-loop signals.
    lifetime_golden_child_count = Column(
        Integer, nullable=False, server_default=text("0")
    )
    last_seen_new_url_count = Column(Integer, nullable=False, server_default=text("0"))
    consecutive_no_new_url = Column(Integer, nullable=False, server_default=text("0"))

    # Long-loop signals.
    last_eval_batch_id = Column(Integer)
    consecutive_miss_batches = Column(
        Integer, nullable=False, server_default=text("0")
    )

    # Failure backstop. Increments when the URL we set should_crawl=TRUE on
    # comes back as a fetch failure; retire after N consecutive fails so
    # broken fetch_url choices self-heal.
    consecutive_fetch_fail = Column(
        Integer, nullable=False, server_default=text("0")
    )

    # Audit.
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    metadata_json = Column(JSONB)

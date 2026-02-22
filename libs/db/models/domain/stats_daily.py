from sqlalchemy import Column, Integer, Date
from sqlalchemy.dialects.postgresql import JSONB
from libs.db.base import Base


class DomainStatsDaily(Base):
    __tablename__ = "domain_stats_daily"

    domain_id = Column(Integer, primary_key=True)
    event_date = Column(Date, primary_key=True)

    shard_id = Column(Integer, nullable=False)

    num_scheduled = Column(Integer, default=0)
    num_fetch_ok = Column(Integer, default=0)
    num_fetch_fail = Column(Integer, default=0)
    num_content_update = Column(Integer, default=0)

    fail_reasons = Column(JSONB, default=dict)


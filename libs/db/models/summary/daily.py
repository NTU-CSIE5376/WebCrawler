from sqlalchemy import Column, Integer, Date
from sqlalchemy.dialects.postgresql import JSONB
from libs.db.base import Base


class SummaryDaily(Base):
    __tablename__ = "summary_daily"

    event_date = Column(Date, primary_key=True)

    new_links = Column(Integer, default=0)
    num_scheduled = Column(Integer, default=0)
    num_fetch_ok = Column(Integer, default=0)
    num_fetch_fail = Column(Integer, default=0)
    num_content_update = Column(Integer, default=0)

    fail_reasons = Column(JSONB, default=dict)

    error_count = Column(Integer, default=0)
    offer_error = Column(Integer, default=0)
    route_error = Column(Integer, default=0)
    ingest_error = Column(Integer, default=0)
    stats_error = Column(Integer, default=0)
    extract_error = Column(Integer, default=0)


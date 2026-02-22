from __future__ import annotations

from datetime import datetime
from sqlalchemy import Boolean, Column, Date, Integer, String

from libs.db.base import Base

class UrlEventCounter(Base):
    __abstract__ = True

    url = Column(String, primary_key=True)
    event_date = Column(Date, primary_key=True)

    num_scheduled = Column(Integer, default=0)
    num_fetch_ok = Column(Integer, default=0)
    num_fetch_fail = Column(Integer, default=0)
    num_content_update = Column(Integer, default=0)

    accounted = Column(Boolean, default=True)


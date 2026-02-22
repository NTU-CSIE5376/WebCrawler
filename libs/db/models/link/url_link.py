from __future__ import annotations

from sqlalchemy import Column, String, DateTime, Computed
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import BYTEA
from libs.db.base import Base


class UrlLink(Base):
    __tablename__ = "url_link"

    src_url = Column(String, primary_key=True)
    dst_url = Column(String, primary_key=True)
    anchor_hash = Column(BYTEA, Computed("decode(md5(anchor_text), 'hex')", persisted=True), primary_key=True)

    anchor_text = Column(String)

    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_seen = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


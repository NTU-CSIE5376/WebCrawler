from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    SmallInteger,
    String,
    text
)
from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy.sql import func


class UrlStateMixin:
    domain_id = Column(BigInteger, nullable=False)

    # Scheduler timestamps.
    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_scheduled = Column(DateTime(timezone=True))
    last_fetch_ok = Column(DateTime(timezone=True))
    last_content_update = Column(DateTime(timezone=True))

    # 90-day event counter
    num_scheduled_90d = Column(Integer, default=0)
    num_fetch_ok_90d = Column(Integer, default=0)
    num_fetch_fail_90d = Column(Integer, default=0)
    num_content_update_90d = Column(Integer, default=0)

    num_consecutive_fail = Column(Integer, default=0)
    last_fail_reason = Column(String)

    content_hash = Column(String)

    should_crawl = Column(Boolean, default=True)

    # Priority signals
    url_score = Column(Float, default=0.0)
    domain_score = Column(Float, default=0.0)
    parent_page_score = Column(Float)
    inlink_count_approx = Column(Integer, nullable=False, server_default=text("0"))
    inlink_count_external = Column(Integer, nullable=False, server_default=text("0"))

    # Discovery metadata.
    discovery_source_type = Column(SmallInteger, nullable=False, server_default=text("0"))
    anchor_text = Column(String)
    robots_bits = Column(SmallInteger, nullable=False, server_default=text("0"))

    # Page metadata: <title> trimmed to 500 chars, NULL on fail / non-HTML.
    title = Column(String)
    hreflang_count = Column(Integer)

    # HTTP response metadata used by recrawl scheduling.
    last_modified = Column(DateTime(timezone=True))
    etag = Column(String)
    cache_control = Column(String)
    is_redirect = Column(Boolean)
    redirect_hop_count = Column(SmallInteger)

from __future__ import annotations

from sqlalchemy import Column, DateTime, String
from libs.db.base import Base
from .mixins import UrlStateMixin


class UrlStateCurrent(
    Base,
    UrlStateMixin
):
    """
    Logical ORM definition for url_state_current_XXX.
    """
    __abstract__ = True

    url = Column(String, primary_key=True)
    # Latest explicit API request; prevents delayed results from clearing a
    # request that has not yet been claimed by an offerer.
    last_crawl_request_at = Column(DateTime(timezone=True))


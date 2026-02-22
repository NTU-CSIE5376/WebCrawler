from sqlalchemy import Column, BigInteger, DateTime, String
from sqlalchemy.orm import declared_attr
from sqlalchemy.sql import func

from libs.db.base import Base
from .mixins import UrlStateMixin


class UrlStateHistory(
    Base,
    UrlStateMixin
):
    """
    Logical ORM for url_state_history_XXX.
    """
    __abstract__ = True

    snapshot_id = Column(BigInteger, primary_key=True)
    snapshot_at = Column(DateTime(timezone=True), server_default=func.now())

    url = Column(String, nullable=False)


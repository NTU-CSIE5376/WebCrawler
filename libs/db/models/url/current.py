from __future__ import annotations

from sqlalchemy import Column, String
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


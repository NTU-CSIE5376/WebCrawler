from functools import lru_cache
from typing import Type

from libs.db.base import Base
from libs.db.models.url.current import UrlStateCurrent
from libs.db.models.url.history import UrlStateHistory
from libs.db.models.url.counter import UrlEventCounter


@lru_cache(maxsize=None)
def url_state_current_table(shard_id: int) -> Type[UrlStateCurrent]:
    """
    Return ORM class bound to table `url_state_current_XXX`.

    Cache is REQUIRED because:
    - SQLAlchemy Base cannot register two classes with same table name
    - repeated calls must return the same class object
    """
    if shard_id < 0:
        raise ValueError("invalid shard_id")

    return type(
        f"UrlStateCurrent_{shard_id:03d}",
        (UrlStateCurrent,),
        {
            "__tablename__": f"url_state_current_{shard_id:03d}",
        },
    )


@lru_cache(maxsize=None)
def url_state_history_table(shard_id: int) -> Type[UrlStateHistory]:
    """
    ORM for url_state_history_XXX.
    """
    if shard_id < 0:
        raise ValueError("invalid shard_id")

    return type(
        f"UrlStateHistory_{shard_id:03d}",
        (UrlStateHistory,),
        {
            "__tablename__": f"url_state_history_{shard_id:03d}",
        },
    )

@lru_cache(maxsize=None)
def url_event_counter_table(shard_id: int) -> Type[UrlEventCounter]:
    """
    ORM for url_event_counter_XXX.
    """
    if shard_id < 0:
        raise ValueError("invalid shard_id")

    return type(
        f"UrlEventCounter_{shard_id:03d}",
        (UrlEventCounter,),
        {
            "__tablename__": f"url_event_counter_{shard_id:03d}",
        },
    )

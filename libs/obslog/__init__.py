from __future__ import annotations

import logging
import sys
from typing import Any

from pythonjsonlogger import jsonlogger


_CONFIGURED = False


class _ContextFilter(logging.Filter):
    def __init__(self, service: str, worker_id: Any, shard: Any):
        super().__init__()
        self.service = service
        self.worker_id = worker_id
        self.shard = shard

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self.service
        if self.worker_id is not None:
            record.worker_id = self.worker_id
        if self.shard is not None:
            record.shard = self.shard
        return True


def configure(
    service: str,
    worker_id: int | str | None = None,
    shard: int | None = None,
    level: int = logging.INFO,
) -> None:
    global _CONFIGURED

    root = logging.getLogger()
    if not _CONFIGURED:
        for h in list(root.handlers):
            root.removeHandler(h)

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            jsonlogger.JsonFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                rename_fields={"asctime": "ts", "levelname": "level", "name": "logger"},
                json_ensure_ascii=False,
            )
        )
        root.addHandler(handler)
        root.setLevel(level)

        logging.getLogger("scrapy").setLevel(logging.WARNING)
        logging.getLogger("scrapy.core.engine").setLevel(logging.INFO)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

        _CONFIGURED = True

    ctx = _ContextFilter(service=service, worker_id=worker_id, shard=shard)
    for h in root.handlers:
        for existing in list(h.filters):
            if isinstance(existing, _ContextFilter):
                h.removeFilter(existing)
        h.addFilter(ctx)

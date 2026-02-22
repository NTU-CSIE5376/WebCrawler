from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.ipc.jsonio import atomic_write_json

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class StatsDeltaWriter:
    def __init__(self, stats_dir: str):
        self.stats_dir = Path(stats_dir)

    def write(self, source: str, **kwargs) -> str:
        """
        stats delta example:
        {
          "generated_at": "2026-01-01T01:01:01+00:00",
          "source": "offerer",
          "counters": {
            "new_links": 80
          },
          "domains": {
            33: {
              "num_fetch_fail": 120,
              "fail_reasons": {
                "HttpError 404": 20
              }
            },
            41: {
              "num_fetch_ok": 200
            }
          }
        }
        """
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        name = f"{int(time.time())}_{uuid.uuid4()}.json"
        path = str(self.stats_dir / name)

        payload: dict[str, Any] = {
            "generated_at": now_iso(),
            "source": source,
            **kwargs
        }
        atomic_write_json(path, payload)
        return path


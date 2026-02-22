from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def atomic_write_json(path: str, payload: Any) -> None:
    """
    Atomic write: write <name>.tmp then os.replace to final path.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    tmp = p.with_suffix(p.suffix + ".tmp")
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        #os.fsync(f.fileno())

    os.replace(tmp, p)


def read_json(path: Path) -> Any:
    with open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))

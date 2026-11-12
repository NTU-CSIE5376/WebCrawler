"""Durable UTC completion dates for the daily maintenance services."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from pathlib import Path


class DailyStateError(ValueError):
    """State cannot safely be used; messages never include file contents."""


def load_last_success_day(
    path: str | None, *, service: str, today_utc: date,
) -> date | None:
    if path is None:
        return None
    target = Path(path)
    try:
        if target.is_symlink():
            raise DailyStateError("Daily completion state must not be a symlink")
        raw = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError):
        raise DailyStateError(f"Cannot read {service} daily completion state") from None
    try:
        payload = json.loads(raw)
        if (not isinstance(payload, dict)
                or set(payload) != {"version", "service", "last_success_day"}
                or type(payload["version"]) is not int or payload["version"] != 1
                or payload["service"] != service
                or not isinstance(payload["last_success_day"], str)):
            raise ValueError("Unexpected state schema")
        day = date.fromisoformat(payload["last_success_day"])
        if day.isoformat() != payload["last_success_day"]:
            raise ValueError("Noncanonical date")
    except (ValueError, TypeError):
        raise DailyStateError(f"Invalid {service} daily completion state") from None
    if day > today_utc:
        raise DailyStateError(f"{service} daily completion state is ahead of today's UTC date")
    return day


def save_last_success_day(path: str | None, *, service: str, day_utc: date) -> None:
    if path is None:
        return
    if type(day_utc) is not date:
        raise DailyStateError("Daily completion state requires a UTC date")
    target = Path(path)
    temporary: str | None = None
    try:
        if target.is_symlink():
            raise DailyStateError("Daily completion state must not be a symlink")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": 1, "service": service, "last_success_day": day_utc.isoformat()}
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=target.parent,
            prefix=f".{target.name}.", delete=False,
        ) as stream:
            temporary = stream.name
            json.dump(payload, stream, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:
        raise DailyStateError(f"Cannot persist {service} daily completion state") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

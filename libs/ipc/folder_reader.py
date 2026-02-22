from __future__ import annotations
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Iterable, Tuple


def parse_date_time(date: str, time: str) -> datetime:
    return datetime.strptime(date + time, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)

def current_interval(interval_minutes: int) -> Tuple[str, str]:
    '''
    Return (date, time) for current time interval
    '''
    now = datetime.now(timezone.utc)
    minute = (now.minute // interval_minutes) * interval_minutes
    bucket = now.replace(minute=minute)

    return bucket.strftime("%Y%m%d"), bucket.strftime("%H%M")

def interval_ready(date: str, time: str, interval_minutes: int) -> bool:
    start = parse_date_time(date, time)
    # Wait twice as long for greater robustness.
    end = start + timedelta(minutes=interval_minutes*2)
    now = datetime.now(timezone.utc)
    return now >= end

class Progress:
    def __init__(self, path: str):
        self.path = Path(path)
        self.state = self._load()

    def _load(self):
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {"date": None, "time": None}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.state))

    def seen(self, date: str, time: str) -> bool:
        if self.state["date"] is None:
            return False
        return (date, time) <= (self.state["date"], self.state["time"])

    def advance(self, date: str, time: str):
        self.state["date"] = date
        self.state["time"] = time
        self.save()

class FolderReader:
    '''
    List {base_dir}/{date}/{time}/*.json in order. Track progress.
    Folders become available only after a specified time interval following their creation.
    '''
    def __init__(
        self,
        base_dir: str,
        progress: Progress,
        interval_minutes: int,
    ):
        self.base = Path(base_dir)
        self.progress = progress
        self.interval_minutes = interval_minutes

    def iter_ready_folders(self) -> Iterable[Tuple[str, str, Path]]:
        if not self.base.exists():
            return

        for date_dir in sorted(self.base.iterdir()):
            if not date_dir.is_dir():
                continue
            date = date_dir.name
            for time_dir in sorted(date_dir.iterdir()):
                if not time_dir.is_dir():
                    continue
                time = time_dir.name
                if self.progress.seen(date, time):
                    continue
                if not interval_ready(date, time, self.interval_minutes):
                    return
                yield date, time, time_dir


from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from libs.ipc.jsonio import append_jsonl
from libs.ipc.folder_reader import current_interval


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class JsonPipeline:
    """
    Writes each item into:
      {base_dir}/{date}/{time}/{uuid}.json

    base_dir = RESULT_DIR_TEMPLATE.format(id=crawler_id)

    date = YYYYMMDD
    time = HHMM (floored to interval)
    """
    def __init__(self, result_dir_template: str, interval_minutes: int):
        self.result_dir_template = result_dir_template
        self.interval_minutes = interval_minutes
        self.base_dir = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings

        tmpl = settings.get("RESULT_DIR_TEMPLATE")
        interval_minutes = int(settings.getint("INTERVAL_MINUTES", 5))

        pipe = cls(tmpl, interval_minutes)
        return pipe

    def open_spider(self, spider):
        self.base_dir = Path(self.result_dir_template.format(id=spider.crawler_id))
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def process_item(self, item, spider):
        if self.base_dir is None:
            raise RuntimeError("Pipeline not initialized (spider_opened not called)")

        date, t = current_interval(self.interval_minutes)

        out_dir = self.base_dir / date / t
        out_dir.mkdir(parents=True, exist_ok=True)

        path = out_dir / f"{datetime.now(timezone.utc).strftime('%H%M')}.jsonl"

        rec = {
            "url": item.get("url"),
            "domain": item.get("domain"),
            "fetched_at": _now_iso(),
            "status": "ok" if item.get("content") else "fail",
            "fail_reason": item.get("fail_reason"),
            "content": item.get("content"),
            "outlinks": item.get("outlinks", []),
            "title": item.get("title"),
            "hreflang_count": item.get("hreflang_count"),
            "last_modified": item.get("last_modified"),
            "etag": item.get("etag"),
            "cache_control": item.get("cache_control"),
            "is_redirect": item.get("is_redirect"),
            "redirect_hop_count": item.get("redirect_hop_count"),
        }

        append_jsonl(path, rec)
        return item

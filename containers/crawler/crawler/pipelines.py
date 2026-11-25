from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from crawler.json_writer import JsonWriterPool

from libs.ipc.jsonio import append_jsonl
from libs.ipc.folder_reader import current_interval


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class JsonPipeline:
    """
    Writes each item as a JSONL record, acknowledging it only after persistence.

    base_dir = RESULT_DIR_TEMPLATE.format(id=crawler_id)

    date = YYYYMMDD
    time = HHMM (floored to interval)
    """
    def __init__(self, result_dir_template: str, interval_minutes: int, *, writer_processes=0,
                 on_broken=None, batch_size=8, batch_bytes=4 * 1024**2,
                 flush_interval=0.05):
        if writer_processes not in (0, 1):
            raise ValueError("JSON_WRITER_PROCESSES must be 0 or 1 to preserve record order")
        self.result_dir_template = result_dir_template
        self.interval_minutes = interval_minutes
        self.base_dir = None
        self.writer_processes = writer_processes
        self.stats = None
        self._pool = None
        self._pending = 0
        self._closing = False
        self._on_broken = on_broken
        self._batch_options = dict(batch_size=batch_size, batch_bytes=batch_bytes,
                                   flush_interval=flush_interval)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings

        tmpl = settings.get("RESULT_DIR_TEMPLATE")
        interval_minutes = int(settings.getint("INTERVAL_MINUTES", 5))

        pipe = cls(tmpl, interval_minutes,
                   writer_processes=settings.getint("JSON_WRITER_PROCESSES", 0),
                   on_broken=lambda: crawler.spider._writer_pool_broken(),
                   batch_size=settings.getint("JSON_WRITER_BATCH_SIZE", 8),
                   batch_bytes=settings.getint("JSON_WRITER_BATCH_BYTES", 4 * 1024**2),
                   flush_interval=settings.getfloat("JSON_WRITER_FLUSH_INTERVAL_MS", 50) / 1000)
        pipe.stats = crawler.stats
        return pipe

    def open_spider(self, spider):
        self.base_dir = Path(self.result_dir_template.format(id=spider.crawler_id))
        self.base_dir.mkdir(parents=True, exist_ok=True)
        if self.writer_processes:
            self._pool = JsonWriterPool(on_broken=self._on_broken, stats=self.stats,
                                        **self._batch_options)

    def process_item(self, item, spider):
        if self.base_dir is None:
            raise RuntimeError("Pipeline not initialized (spider_opened not called)")
        if self._closing:
            raise RuntimeError("JSON writer is closing")

        date, t = current_interval(self.interval_minutes)

        out_dir = self.base_dir / date / t
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
            "has_json_ld": item.get("has_json_ld"),
            "last_modified": item.get("last_modified"),
            "etag": item.get("etag"),
            "cache_control": item.get("cache_control"),
            "is_redirect": item.get("is_redirect"),
            "redirect_hop_count": item.get("redirect_hop_count"),
        }

        if not self.writer_processes:
            append_jsonl(path, rec)
            self._record_written(spider, rec)
            return item

        work = self._pool.write(path, rec)
        self._pending += 1
        if self.stats is not None:
            self.stats.set_value("json_writer/pending", self._pending)
        result = Deferred()

        def finished(value):
            self._pending -= 1
            failed = isinstance(value, Failure)
            if self.stats is not None:
                self.stats.set_value("json_writer/pending", self._pending)
                self.stats.inc_value("json_writer/errors" if failed else "json_writer/written")
            if not failed:
                self._record_written(spider, rec)
            # Cancellation of the caller must not cancel the internal write or
            # release its admission permit before the writer actually finishes.
            if not result.called:
                if failed:
                    result.errback(value)
                else:
                    result.callback(item)
            return None

        work.addBoth(finished)
        return result

    @staticmethod
    def _record_written(spider, record):
        # Preserve the production heartbeat counters on the reactor, after the
        # child's acknowledgement rather than before handing work to the pool.
        spider.items_written = getattr(spider, "items_written", 0) + 1
        if record["status"] == "ok":
            spider.items_ok = getattr(spider, "items_ok", 0) + 1

    def close_spider(self, spider):
        self._closing = True
        return self._pool.close() if self._pool is not None else None

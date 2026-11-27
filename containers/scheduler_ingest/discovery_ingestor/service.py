from __future__ import annotations

from collections import Counter, OrderedDict, defaultdict
import logging
from pathlib import Path
import sys
import time

from libs.ipc.jsonio import read_json, read_jsonl
from ..ingestor.db_ops import IngestDB, MAX_URL_LEN, _prefer_higher_parent, _prefer_higher_priority
from ..ingestor.service import _record_size

LOGGER = logging.getLogger("discovery_ingestor")
_METADATA = ("anchor_text", "discovered_from", "parent_page_score", "discovery_source_type",
             "url_priority", "source", "inlink_count_approx", "inlink_count_external")


class DiscoveryIngestor:
    def __init__(self, db, stats, checkpoint, trap_filter=None, distinct_url_limit=64000,
                 max_buffer_bytes=2 * 1024**3, idle_seconds=600, max_age_seconds=600,
                 clock=time.monotonic):
        if min(distinct_url_limit, max_buffer_bytes, idle_seconds, max_age_seconds) <= 0:
            raise ValueError("discovery buffer limits and timers must be positive")
        self.db, self.stats, self.checkpoint = db, stats, checkpoint
        self.trap_filter, self.clock = trap_filter, clock
        self.limit, self.max_bytes = distinct_url_limit, max_buffer_bytes
        self.idle_seconds, self.max_age_seconds = idle_seconds, max_age_seconds
        self.buffers = defaultdict(dict)
        self.bytes = Counter()
        self.inputs = Counter()
        self.buffered_bytes = 0
        self.filter_cache = OrderedDict()
        self.last_input_at = self.oldest_pending_at = None
        state = checkpoint.progress.state
        self.last_scanned = (state["date"], state["time"]) if state["date"] else None

    def seen(self, date, bucket):
        return self.last_scanned is not None and (date, bucket) <= self.last_scanned

    def _checkpoint_if_empty(self):
        if not self.buffers:
            self.oldest_pending_at = None
            if self.last_scanned:
                self.checkpoint.advance(*self.last_scanned)

    def _normalize(self, record):
        url = record["url"]
        if self.trap_filter is None or url in self.buffers.get(int(record["shard_id"]), {}):
            # URL-only rules already accepted this canonical URL. Avoid
            # filtering hot duplicates again even if the small LRU evicts it.
            return record
        if url in self.filter_cache:
            new_url = self.filter_cache[url]
            self.filter_cache.move_to_end(url)
        else:
            kept, _ = self.trap_filter.apply([record])
            new_url = kept[0]["url"] if kept else None
            self.filter_cache[url] = new_url
            if len(self.filter_cache) > 65536:
                self.filter_cache.popitem(last=False)
        return {**record, "url": new_url} if new_url is not None else None

    def add(self, record):
        self.db.validate(record)
        self.last_input_at = self.clock()
        if not record.get("url"):
            return
        record = self._normalize(record)
        if record is None or len(record["url"]) > MAX_URL_LEN:
            return
        sid, url = int(record["shard_id"]), record["url"]
        buf = self.buffers[sid]
        existing = buf.get(url)
        if existing is None:
            # Use the existing aggregation policy, including default counters.
            existing = IngestDB.aggregate_links([record])[0]
            buf[url] = existing
            growth = _record_size(existing)
        else:
            before = sum(sys.getsizeof(existing.get(k)) for k in _METADATA)
            existing["inlink_count_approx"] += int(record.get("inlink_count_approx", 1))
            existing["inlink_count_external"] += int(record.get("inlink_count_external", 0))
            if not existing.get("anchor_text") and record.get("anchor_text"):
                existing["anchor_text"] = record["anchor_text"]
            _prefer_higher_parent(existing, record)
            _prefer_higher_priority(existing, record)
            growth = sum(sys.getsizeof(existing.get(k)) for k in _METADATA) - before
        self.bytes[sid] += growth
        self.buffered_bytes += growth
        self.inputs[sid] += 1
        if self.oldest_pending_at is None:
            self.oldest_pending_at = self.clock()
        if len(buf) >= self.limit:
            self.flush_shard(sid, "count")
        while self.buffered_bytes >= self.max_bytes and self.buffers:
            self.flush_shard(max(self.bytes, key=self.bytes.get), "bytes")

    def flush_shard(self, sid, reason):
        records = list(self.buffers[sid].values())
        started = self.clock()
        # Do not discard a failed batch or advance its checkpoint. The process
        # exits and the durable source folders remain available for recovery.
        try:
            results = self.db.process_batch(records)
        except Exception:
            LOGGER.exception("discovery_ingest.batch_error", extra={"event": "discovery_ingest.batch_error",
                             "shard_id": sid, "records": len(records), "reason": reason})
            raise
        self.stats.write(source="ingestor", counters={"new_links": sum(r is True for r in results)}, domains={})
        LOGGER.info("discovery_ingest.batch_done", extra={"event": "discovery_ingest.batch_done",
                    "shard_id": sid, "records": len(records), "input_records": self.inputs[sid],
                    "duration_s": round(self.clock() - started, 3), "reason": reason})
        self.buffered_bytes -= self.bytes.pop(sid)
        self.inputs.pop(sid)
        self.buffers.pop(sid)
        self._checkpoint_if_empty()

    def flush_all(self, reason):
        for sid in sorted(self.buffers):
            self.flush_shard(sid, reason)
        self._checkpoint_if_empty()

    def flush_if_due(self):
        if self.oldest_pending_at is None:
            return
        now = self.clock()
        if now - self.last_input_at >= self.idle_seconds:
            self.flush_all("idle")
        elif now - self.oldest_pending_at >= self.max_age_seconds:
            # An idle-only timer never fires under continuous traffic. Bound
            # checkpoint delay and replay volume even for low-cardinality URLs.
            self.flush_all("age")

    def process_folder(self, folder: Path, date, bucket):
        started = self.clock()
        records = 0
        for path in folder.iterdir():
            if not path.is_file() or path.suffix not in {".json", ".jsonl"}:
                continue
            source = [read_json(path)] if path.suffix == ".json" else read_jsonl(path)
            for record in source:
                if record.get("status") != "new":
                    continue
                self.add(record)
                records += 1
                if records % 1024 == 0:
                    self.flush_if_due()
        self.last_scanned = (date, bucket)
        self._checkpoint_if_empty()
        LOGGER.info("discovery_ingest.folder_scanned", extra={"event": "discovery_ingest.folder_scanned",
                    "folder": str(folder), "input_records": records,
                    "buffered_distinct_urls": sum(map(len, self.buffers.values())),
                    "buffered_bytes": self.buffered_bytes, "duration_s": round(self.clock()-started, 3)})

    def close(self):
        try:
            self.flush_all("shutdown")
        finally:
            if self.trap_filter is not None:
                self.trap_filter.close()

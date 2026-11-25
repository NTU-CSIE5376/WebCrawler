from __future__ import annotations

from collections import defaultdict
import logging
from pathlib import Path
import time

from libs.ipc.jsonio import read_json, read_jsonl
from ..ingestor.db_ops import BATCH_SIZE
from ..ingestor.service import IngestService, _record_size

LOGGER = logging.getLogger("content_ingestor")


class ContentIngestor:
    def __init__(self, db, stats, checkpoint, max_buffer_bytes=64 * 1024**2, batch_size=BATCH_SIZE):
        if min(max_buffer_bytes, batch_size) <= 0:
            raise ValueError("content buffer limits must be positive")
        self.db, self.stats, self.checkpoint = db, stats, checkpoint
        self.max_bytes, self.batch_size = max_buffer_bytes, batch_size

    def seen(self, date, bucket):
        return self.checkpoint.progress.seen(date, bucket)

    def process_folder(self, folder: Path, date, bucket):
        started = time.monotonic()
        buffers = defaultdict(list)
        sizes = defaultdict(int)
        total_bytes = 0
        counters, domains = defaultdict(int), {}
        records = 0

        def flush(sid):
            nonlocal total_bytes
            batch = buffers[sid]
            tick = time.monotonic()
            for result in self.db.process_batch(batch):
                IngestService._accumulate_result(self, result, counters, domains)
            LOGGER.info("content_ingest.batch_done", extra={"event": "content_ingest.batch_done",
                        "shard_id": sid, "records": len(batch),
                        "duration_s": round(time.monotonic()-tick, 3)})
            total_bytes -= sizes.pop(sid)
            buffers.pop(sid)

        for path in folder.iterdir():
            if not path.is_file() or path.suffix not in {".json", ".jsonl"}:
                continue
            source = [read_json(path)] if path.suffix == ".json" else read_jsonl(path)
            for record in source:
                if record.get("status") == "new":
                    continue
                sid = self.db.validate(record)
                # Router already computed content_hash. Neither the body nor
                # outlinks is consumed by the URL/history/counter SQL paths;
                # the extractor independently reads the original source file.
                record = {key: value for key, value in record.items()
                          if key not in {"content", "outlinks"}}
                size = _record_size(record)
                buffers[sid].append(record)
                sizes[sid] += size
                total_bytes += size
                records += 1
                if len(buffers[sid]) >= self.batch_size:
                    flush(sid)
                while total_bytes >= self.max_bytes and buffers:
                    flush(max(sizes, key=sizes.get))
        for sid in sorted(buffers):
            flush(sid)
        domains.pop(None, None)
        self.stats.write(source="ingestor", counters=counters, domains=domains)
        self.checkpoint.advance(date, bucket)
        LOGGER.info("ingest.folder_done", extra={"event": "ingest.folder_done",
                    "component": "content_ingestor", "num_fetch_ok": counters.get("num_fetch_ok", 0),
                    "num_fetch_fail": counters.get("num_fetch_fail", 0),
                    "num_content_update": counters.get("num_content_update", 0),
                    "folder": str(folder), "records": records,
                    "duration_s": round(time.monotonic()-started, 3)})

    def flush_if_due(self):
        pass  # Folder tails commit before the content checkpoint advances.

    def close(self):
        pass

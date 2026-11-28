from __future__ import annotations

from collections import defaultdict
import logging
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.compute as pc

from libs.ipc.parquet import read_parquet_batches, routed_files
from ..libs.db_ops import BATCH_SIZE

LOGGER = logging.getLogger("content_ingestor")


def _accumulate_result(result, counters, domains):
    if isinstance(result, bool):
        if result:
            counters["new_links"] += 1
        return

    if not result:
        return

    domains.setdefault(result.domain_id, defaultdict(int))

    if result.new_link:
        counters["new_links"] += 1
    if result.is_ok:
        counters["num_fetch_ok"] += 1
        domains[result.domain_id]["num_fetch_ok"] += 1
    else:
        counters["num_fetch_fail"] += 1
        domains[result.domain_id]["num_fetch_fail"] += 1
    if result.is_upd:
        counters["num_content_update"] += 1
        domains[result.domain_id]["num_content_update"] += 1
    if result.fail_reason:
        counters.setdefault("fail_reasons", defaultdict(int))[result.fail_reason] += 1
        domains[result.domain_id].setdefault("fail_reasons", defaultdict(int))[result.fail_reason] += 1


class ContentIngestor:
    def __init__(self, db, stats, checkpoint, max_buffer_bytes=64 * 1024**2, batch_size=BATCH_SIZE):
        if min(max_buffer_bytes, batch_size) <= 0:
            raise ValueError("content buffer limits must be positive")
        self.db, self.stats, self.checkpoint = db, stats, checkpoint
        self.max_bytes, self.batch_size = max_buffer_bytes, batch_size
        self.materialize_seconds = 0.0

    def seen(self, date, bucket):
        return self.checkpoint.progress.seen(date, bucket)

    def process_folder(self, folder: Path, date, bucket):
        started = time.monotonic()
        buffers = defaultdict(list)
        sizes = defaultdict(int)
        counts = defaultdict(int)
        total_bytes = 0
        counters, domains = defaultdict(int), {}
        records = 0

        def flush(sid):
            nonlocal total_bytes
            # Python rows exist only for the current database transaction.
            materialize_started = time.perf_counter()
            batch = pa.Table.from_batches(buffers[sid]).to_pylist()
            materialize_s = time.perf_counter()-materialize_started
            self.materialize_seconds += materialize_s
            tick = time.monotonic()
            for result in self.db.process_batch(batch):
                _accumulate_result(result, counters, domains)
            LOGGER.info("content_ingest.batch_done", extra={"event": "content_ingest.batch_done",
                        "shard_id": sid, "records": len(batch),
                        "row_materialize_s": round(materialize_s, 6),
                        "duration_s": round(time.monotonic()-tick, 3)})
            total_bytes -= sizes.pop(sid)
            buffers.pop(sid)
            counts.pop(sid)

        def append(sid, chunk, rows, size):
            nonlocal total_bytes, records
            buffers[sid].append(chunk)
            sizes[sid] += size
            counts[sid] += rows
            total_bytes += size
            records += rows
            if counts[sid] >= self.batch_size:
                flush(sid)
            while total_bytes >= self.max_bytes and buffers:
                flush(max(sizes, key=sizes.get))

        for path in routed_files(folder, 'content'):
            for batch in read_parquet_batches(path, 'content', metadata_only=True,
                                              batch_records=self.batch_size):
                if pc.any(pc.is_null(batch.column('shard_id'))).as_py():
                    raise ValueError('content batch has a null shard_id')
                if pc.any(pc.equal(batch.column('status'), 'new')).as_py():
                    raise ValueError('discovery record in content batch')
                for sid in pc.unique(batch.column('shard_id')).to_pylist():
                    shard = batch.filter(pc.equal(batch.column('shard_id'), sid))
                    self.db.validate(shard.slice(0, 1).to_pylist()[0])
                    offset = 0
                    while offset < shard.num_rows:
                        length = min(shard.num_rows-offset, self.batch_size-counts[sid])
                        chunk = shard.slice(offset, length)
                        # Charge retained buffers, including sliced parents.
                        append(sid, chunk, length, chunk.get_total_buffer_size())
                        offset += length
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

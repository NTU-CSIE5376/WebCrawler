from __future__ import annotations
import logging
import sys
import time
from pathlib import Path
from collections import defaultdict

from libs.ipc.jsonio import read_json, read_jsonl
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import IngestDB, BATCH_SIZE
from .trap_filter import TrapFilter


logger = logging.getLogger("ingestor")
MAX_BUFFER_BYTES = 64 * 1024 * 1024


def _record_size(value):
    """Conservative decoded JSON size, including nested content/outlinks.

    Shared keys/scalars are counted repeatedly, so the budget is an estimate
    rather than an exact RSS measurement. JSON records contain no cycles.
    """
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(_record_size(k) + _record_size(v) for k, v in value.items())
    elif isinstance(value, list):
        size += sum(_record_size(v) for v in value)
    return size


class IngestService:
    def __init__(
        self,
        ingestor_id: int,
        db: IngestDB,
        stats: StatsDeltaWriter,
        trap_filter: TrapFilter | None = None,
        max_buffer_bytes: int = MAX_BUFFER_BYTES,
        discovery_batch_size: int | None = None,
    ):
        if max_buffer_bytes <= 0:
            raise ValueError("max_buffer_bytes must be positive")
        self.ingestor_id = ingestor_id
        self.db = db
        self.stats = stats
        self.trap_filter = trap_filter
        self.max_buffer_bytes = max_buffer_bytes
        self.discovery_batch_size = BATCH_SIZE if discovery_batch_size is None else discovery_batch_size
        if self.discovery_batch_size <= 0:
            raise ValueError("discovery_batch_size must be positive")

    def _accumulate_result(self, result, counters, domains):
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

    def process_folder(self, folder: Path):
        logger.info(
            "ingest.folder_start",
            extra={"event": "ingest.folder_start", "folder": str(folder)},
        )
        started = time.monotonic()
        counters = defaultdict(int)
        domains = {}
        # Snapshot filenames only so both passes use the same input files.
        files = [f for f in folder.iterdir() if f.is_file() and f.suffix in {".json", ".jsonl"}]
        trap_dropped = url_normalized = url_would_normalize = 0

        def flush(batch, discovery):
            nonlocal trap_dropped, url_normalized, url_would_normalize
            input_records = len(batch)
            if discovery:
                # Dedup only within the bounded batch. Repeated URLs in later
                # batches are handled by the database's existing UPSERT.
                batch = self.db.aggregate_links(batch)
                if self.trap_filter is not None:
                    batch, fstats = self.trap_filter.apply(batch)
                    trap_dropped += fstats.dropped
                    url_normalized += fstats.normalized
                    url_would_normalize += fstats.would_normalize
                    if fstats.normalized:
                        batch = self.db.aggregate_links(batch)
            if not batch:
                return
            batch_started = time.monotonic()
            try:
                for result in self.db.process_batch(batch):
                    self._accumulate_result(result, counters, domains)
                logger.info(
                    "ingest.batch_done",
                    extra={"event": "ingest.batch_done",
                           "batch_kind": "discovery" if discovery else "result",
                           "input_records": input_records, "records": len(batch),
                           "duration_s": round(time.monotonic() - batch_started, 3)},
                )
            except Exception as e:
                logger.error(
                    "ingest.batch_error",
                    extra={
                        "event": "ingest.batch_error",
                        "error": str(e),
                        "batch_size": len(batch),
                    },
                )
                counters["error_count"] += len(batch)
                counters["ingest_error"] += len(batch)

        # Stream twice to finish all discoveries before any crawl results,
        # without retaining results during discovery. Enforce the record limit
        # for each kind per shard and a shared byte budget across all shards. A
        # single oversized record is processed immediately. Flush tails before
        # the next pass/folder.
        for discovery in (True, False):
            batch_limit = self.discovery_batch_size if discovery else BATCH_SIZE
            recs_by_shard = defaultdict(list)
            bytes_by_shard = defaultdict(int)
            buffered_bytes = 0
            for f in files:
                recs = [read_json(f)] if f.suffix == ".json" else read_jsonl(f)
                for rec in recs:
                    if (rec.get("status") == "new") != discovery:
                        continue
                    shard_id = int(rec["shard_id"])
                    recs_by_shard[shard_id].append(rec)
                    size = _record_size(rec)
                    bytes_by_shard[shard_id] += size
                    buffered_bytes += size
                    if len(recs_by_shard[shard_id]) >= batch_limit:
                        buffered_bytes -= bytes_by_shard.pop(shard_id)
                        flush(recs_by_shard.pop(shard_id), discovery)
                    while buffered_bytes >= self.max_buffer_bytes:
                        largest = max(bytes_by_shard, key=bytes_by_shard.get)
                        buffered_bytes -= bytes_by_shard.pop(largest)
                        flush(recs_by_shard.pop(largest), discovery)
            for shard_id in list(recs_by_shard):
                flush(recs_by_shard.pop(shard_id), discovery)

        domains.pop(None, None)
        self.stats.write(
            source="ingestor",
            counters=counters,
            domains=domains,
        )
        logger.info(
            "ingest.folder_done",
            extra={
                "event": "ingest.folder_done",
                "folder": str(folder),
                "file_cnt": len(files),
                "num_fetch_ok": counters.get("num_fetch_ok", 0),
                "num_fetch_fail": counters.get("num_fetch_fail", 0),
                "num_content_update": counters.get("num_content_update", 0),
                "new_links": counters.get("new_links", 0),
                "trap_dropped": trap_dropped,
                "url_normalized": url_normalized,
                "url_would_normalize": url_would_normalize,
                "duration_s": round(time.monotonic() - started, 1),
                "discovery_batch_size": self.discovery_batch_size,
            },
        )


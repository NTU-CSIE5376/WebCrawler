"""Bounded, immutable Parquet batches shared by router output consumers."""
from collections import defaultdict
import math
import os
from pathlib import Path
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .routed_schema import CONTENT_METADATA_COLUMNS, ROUTED_SCHEMAS


def routed_columns():
    return {kind: {name: [] for name in schema.names}
            for kind, schema in ROUTED_SCHEMAS.items()}


class ParquetBatchWriter:
    """Buffer Arrow batches across destinations and publish only closed files.

    Counts and retained Arrow buffers are bounded globally. A single oversized
    record is written alone. Flush tails before checkpointing a source folder.
    """
    def __init__(self, producer_id, *, batch_records=16384, max_buffer_bytes=64*1024**2,
                 flush_seconds=1.0, clock=time.monotonic):
        if min(batch_records, max_buffer_bytes, flush_seconds) <= 0 or not math.isfinite(flush_seconds):
            raise ValueError('Parquet batch limits must be positive')
        self.producer_id = producer_id
        self.batch_records, self.max_bytes = batch_records, max_buffer_bytes
        self.flush_seconds, self.clock = flush_seconds, clock
        self.buffers = defaultdict(list)
        self.sizes = defaultdict(int)
        self.counts = defaultdict(int)
        self.buffered_bytes = 0
        self.started_at = None

    def append_batch(self, folder, kind, batch):
        if not batch.schema.equals(ROUTED_SCHEMAS[kind], check_metadata=False):
            raise ValueError('batch schema does not match its routed stream')
        if self.started_at is not None and self.clock()-self.started_at >= self.flush_seconds:
            self.flush()
        key = (Path(folder), kind)
        offset = 0
        while offset < batch.num_rows:
            rows = min(batch.num_rows-offset, self.batch_records-self.counts.get(key, 0))
            chunk = batch.slice(offset, rows)
            # Slicing can retain an entire large parent allocation. Copy only
            # when necessary to split a batch larger than the shared budget.
            while chunk.get_total_buffer_size() > self.max_bytes and chunk.num_rows > 1:
                rows = max(1, rows//2)
                chunk = batch.slice(offset, rows).take(pa.array(range(rows), type=pa.int64()))
            size = chunk.get_total_buffer_size()
            while self.buffers and self.buffered_bytes+size > self.max_bytes:
                self._flush(max(self.sizes, key=self.sizes.get))
            self.buffers[key].append(chunk)
            self.counts[key] += chunk.num_rows
            self.sizes[key] += size
            self.buffered_bytes += size
            if self.started_at is None:
                self.started_at = self.clock()
            if self.counts[key] >= self.batch_records or self.buffered_bytes >= self.max_bytes:
                self._flush(key)
            offset += chunk.num_rows

    def _flush(self, key):
        folder, kind = key
        # This is a zero-copy assembly of already typed Arrow batches.
        table = pa.Table.from_batches(self.buffers[key], schema=ROUTED_SCHEMAS[kind])
        folder.mkdir(parents=True, exist_ok=True)
        name = f'{kind}-{time.time_ns():020d}-router{self.producer_id:02d}-{uuid4().hex}.parquet'
        target = folder/name
        temporary = target.with_suffix('.parquet.tmp')
        try:
            pq.write_table(table, temporary, compression='snappy', row_group_size=self.batch_records)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        self.buffered_bytes -= self.sizes.pop(key)
        del self.buffers[key]
        del self.counts[key]
        if not self.buffers:
            self.started_at = None

    def flush(self):
        for key in list(self.buffers):
            self._flush(key)


def routed_files(folder, kind):
    """Read closed routed files in publication order; reject incompatible input."""
    paths = [path for path in Path(folder).iterdir() if path.is_file()]
    if any(path.suffix in {'.json', '.jsonl'} for path in paths):
        raise ValueError(f'expected Parquet router output: {folder}')
    yield from sorted(path for path in paths
                      if path.suffix == '.parquet' and path.name.startswith(kind+'-'))


def read_parquet_batches(path, kind, *, metadata_only=False, batch_records=16384):
    if batch_records <= 0:
        raise ValueError('Parquet batch records must be positive')
    columns = CONTENT_METADATA_COLUMNS if kind == 'content' and metadata_only else None
    with pq.ParquetFile(path) as source:
        if not source.schema_arrow.equals(ROUTED_SCHEMAS[kind], check_metadata=False):
            raise ValueError(f'incompatible {kind} Parquet schema: {path}')
        yield from source.iter_batches(batch_size=batch_records, columns=columns, use_threads=False)


def read_routed_records(folder, kind, *, metadata_only=False):
    """Materialize records only for consumers that require scalar fields."""
    rows = 64 if kind == 'content' and not metadata_only else 16384
    for path in routed_files(folder, kind):
        for batch in read_parquet_batches(path, kind, metadata_only=metadata_only, batch_records=rows):
            yield from batch.to_pylist()

from __future__ import annotations

import logging
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.compute as pc

from libs.ipc.parquet import read_parquet_batches, routed_files
from ..libs.db_ops import MAX_URL_LEN
from .aggregation import aggregate_discoveries

LOGGER = logging.getLogger("discovery_ingestor")
FILTER_CACHE_CAPACITY = 65536
_FILTER_CACHE_SCHEMA = pa.schema([
    ("url",pa.string()), ("normalized_url",pa.string()), ("last_seen",pa.int64()),
])


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
        self.buffers = {}  # One compact Arrow table per shard, never URL dictionaries.
        self.buffered_bytes = 0
        self.materialize_seconds = 0.0
        self.filter_cache = pa.Table.from_batches([],schema=_FILTER_CACHE_SCHEMA)
        self.filter_cache_sequence = 0
        self.candidate_seconds = self.aggregation_seconds = 0.0
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

    def _filtered(self, table):
        table = table.filter(pc.fill_null(pc.not_equal(table['url'], ''), False))
        if table.num_rows == 0:
            return table
        if self.trap_filter is not None and self.trap_filter.cfg.enabled:
            # Group once globally: accepted URLs and cache hits never become
            # Python metadata dictionaries. Only misses reach the policy parser.
            indexed = table.append_column('__row',pa.array(range(table.num_rows),type=pa.int64()))
            first = indexed.group_by('url',use_threads=False).aggregate([('__row','min'),('__row','max')])
            first = first.sort_by([('__row_min','ascending')])
            candidates = table.select(['url','domain_id','shard_id','discovered_from']).take(first['__row_min'])
            urls = candidates['url']
            buffered = pa.chunked_array([chunk for pending in self.buffers.values()
                                        for chunk in pending['url'].chunks],type=pa.string())
            accepted = pc.is_in(urls,value_set=buffered)
            cache_index = pc.index_in(urls,value_set=self.filter_cache['url'])
            cached = pc.take(self.filter_cache['normalized_url'],cache_index)
            missing = pc.and_(pc.invert(accepted),pc.is_null(cache_index))
            misses = candidates.filter(missing)
            decisions = self.trap_filter.evaluate_urls(misses['url'])
            new_urls = pc.if_else(decisions['dropped'],None,decisions['normalized_url'])
            miss_index = pc.index_in(urls,value_set=misses['url'])
            normalized = pc.if_else(accepted,urls,pc.if_else(pc.is_valid(cache_index),cached,pc.take(new_urls,miss_index)))

            # A prior normalization may already have accepted a later raw
            # canonical URL. Avoid a redundant audit/cache action in that case.
            eligible = pc.fill_null(pc.less_equal(pc.utf8_length(normalized),MAX_URL_LEN),False)
            ancestor = pc.index_in(urls,value_set=pc.if_else(eligible,normalized,None))
            position = pa.array(range(candidates.num_rows),type=pa.int32())
            local_accepted = pc.fill_null(pc.less(ancestor,position),False)
            audit_active = pc.invert(pc.take(local_accepted,pc.index_in(misses['url'],value_set=urls)))
            self.trap_filter.record_decisions(misses.filter(audit_active),decisions.filter(audit_active))
            touches = pc.add(first['__row_max'],self.filter_cache_sequence)
            entries = pa.table({'url':urls,'normalized_url':normalized,'last_seen':touches})
            self._update_filter_cache(entries.filter(pc.invert(pc.or_(accepted,local_accepted))))
            self.filter_cache_sequence += table.num_rows
            inverse = pc.index_in(table['url'],value_set=urls)
            table = table.set_column(table.schema.get_field_index('url'),'url',pc.take(normalized,inverse))
        return table.filter(pc.fill_null(pc.less_equal(pc.utf8_length(table['url']),MAX_URL_LEN),False))

    def _update_filter_cache(self, entries):
        if not entries.num_rows:
            return
        old_index = pc.index_in(self.filter_cache['url'],value_set=entries['url'])
        touched = pc.coalesce(pc.take(entries['last_seen'],old_index),self.filter_cache['last_seen'])
        old = self.filter_cache.set_column(2,'last_seen',touched)
        fresh = entries.filter(pc.is_null(pc.index_in(entries['url'],value_set=self.filter_cache['url'])))
        self.filter_cache = pa.concat_tables([old,fresh]).sort_by([('last_seen','descending')]).slice(0,FILTER_CACHE_CAPACITY)

    def add_batch(self, batch):
        table = pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
        if not table.num_rows:
            return
        if table['shard_id'].null_count or pc.any(pc.fill_null(pc.not_equal(table['status'], 'new'), True)).as_py():
            raise ValueError('invalid discovery shard or status')
        self.last_input_at = self.clock()
        shards = pc.unique(table['shard_id']).to_pylist()
        for sid in shards:
            self.db.validate({'shard_id':sid,'status':'new'})
        started = time.perf_counter()
        table = self._filtered(table)
        self.candidate_seconds += time.perf_counter()-started
        for sid in shards:
            incoming = table.filter(pc.equal(table['shard_id'],sid))
            if incoming.num_rows == 0:
                continue
            incoming = incoming.append_column('__inputs', pa.repeat(pa.scalar(1, type=pa.int64()), incoming.num_rows))
            previous = self.buffers.get(sid)
            combined = pa.concat_tables([previous, incoming]) if previous is not None else incoming
            started = time.perf_counter()
            self.buffers[sid] = aggregate_discoveries(combined)
            self.aggregation_seconds += time.perf_counter()-started
            self._measure_buffers()
            if self.oldest_pending_at is None:
                self.oldest_pending_at = self.clock()
            while sid in self.buffers and self.buffers[sid].num_rows >= self.limit:
                self.flush_shard(sid, 'count', self.limit)
            while self.buffered_bytes >= self.max_bytes and self.buffers:
                largest = max(self.buffers, key=lambda key:self.buffers[key].get_total_buffer_size())
                self.flush_shard(largest, 'bytes')

    def _measure_buffers(self):
        self.buffered_bytes = sum(table.get_total_buffer_size() for table in self.buffers.values())

    def flush_shard(self, sid, reason, rows=None):
        table = self.buffers[sid]
        batch = table.slice(0, rows) if rows is not None else table
        materialize_started = time.perf_counter()
        records = batch.drop(['__inputs']).to_pylist()
        materialize_s = time.perf_counter()-materialize_started
        self.materialize_seconds += materialize_s
        started = self.clock()
        try:
            results = self.db.process_batch(records)
        except Exception:
            LOGGER.exception('discovery_ingest.batch_error', extra={'event':'discovery_ingest.batch_error',
                             'shard_id':sid, 'records':len(records), 'reason':reason})
            raise
        self.stats.write(source='ingestor', counters={'new_links':sum(r is True for r in results)}, domains={})
        LOGGER.info('discovery_ingest.batch_done', extra={'event':'discovery_ingest.batch_done',
                    'shard_id':sid, 'records':len(records), 'input_records':pc.sum(batch['__inputs']).as_py(),
                    'row_materialize_s':round(materialize_s, 6),
                    'duration_s':round(self.clock()-started, 3), 'reason':reason})
        if batch.num_rows == table.num_rows:
            del self.buffers[sid]
        else:
            self.buffers[sid] = table.slice(batch.num_rows)
        self._measure_buffers()
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
        for path in routed_files(folder, 'discovery'):
            for batch in read_parquet_batches(path, 'discovery'):
                self.add_batch(batch)
                records += batch.num_rows
                self.flush_if_due()
        self.last_scanned = (date, bucket)
        self._checkpoint_if_empty()
        LOGGER.info('discovery_ingest.folder_scanned', extra={'event':'discovery_ingest.folder_scanned',
                    'folder':str(folder), 'input_records':records,
                    'buffered_distinct_urls':sum(table.num_rows for table in self.buffers.values()),
                    'buffered_bytes':self.buffered_bytes, 'duration_s':round(self.clock()-started, 3)})

    def close(self):
        try:
            self.flush_all('shutdown')
        finally:
            if self.trap_filter is not None:
                self.trap_filter.close()

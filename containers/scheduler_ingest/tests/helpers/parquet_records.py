"""Construct canonical typed router output for ingestion tests."""
import pyarrow as pa

from libs.ipc.new_link_record import build_new_link_record
from libs.ipc.parquet import ParquetBatchWriter, routed_columns
from libs.ipc.routed_schema import CONTENT_SCHEMA, ROUTED_SCHEMAS


def link(url='https://example.test/a', sid=0, **kwargs):
    values = build_new_link_record(url=url, shard_id=sid, domain_id=1, domain_score=.5,
                                  discovered_from=None, discovery_source_type=1)
    values.update(kwargs)
    return values


def columns(records):
    streams = routed_columns()
    for record in records:
        kind = 'discovery' if record.get('status') == 'new' else 'content'
        values = link() if kind == 'discovery' else dict.fromkeys(CONTENT_SCHEMA.names)
        values.update({key:value for key,value in record.items() if key in values})
        for name, column in streams[kind].items():
            column.append(values[name])
    return {kind: pa.RecordBatch.from_pydict(data, schema=ROUTED_SCHEMAS[kind])
            for kind, data in streams.items()}


def add(service, *records):
    service.add_batch(columns(records)['discovery'])


def write_records(folder, records, **kwargs):
    sink = ParquetBatchWriter(0, **kwargs)
    for kind, batch in columns(records).items():
        if batch.num_rows:
            sink.append_batch(folder, kind, batch)
    sink.flush()
    return sink

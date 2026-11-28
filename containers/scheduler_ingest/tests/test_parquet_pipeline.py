import copy
from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from containers.scheduler_ingest.ingestor.content_ingestor.service import ContentIngestor
from containers.scheduler_ingest.ingestor.discovery_ingestor.service import DiscoveryIngestor
from containers.scheduler_ingest.feature_extractor.service import ExtractService
from containers.scheduler_ingest.ingestor.libs.trap_filter import TrapFilter, TrapFilterConfig
from containers.scheduler_ingest.ingestor.libs.worker import Checkpoint
from containers.scheduler_ingest.router.service import RouterOutputError, RouterService
from containers.scheduler_ingest.tests.test_router_service import router, _crawl_folder, _record, _output_lines
from libs.ipc.new_link_record import build_new_link_record
from libs.ipc.parquet import ParquetBatchWriter, read_parquet_batches, read_routed_records
from libs.ipc.routed_schema import CONTENT_SCHEMA, DISCOVERY_SCHEMA
from containers.scheduler_ingest.tests.helpers.parquet_records import columns, write_records


def link(i=0, **kwargs):
    values = dict(url=f'https://example.test/{i}', shard_id=0, domain_id=1, domain_score=.5,
                  discovered_from=None, discovery_source_type=1)
    values.update(kwargs)
    return build_new_link_record(**values)


def content(i=0, **kwargs):
    values = dict.fromkeys(CONTENT_SCHEMA.names)
    values.update(url=f'https://example.test/{i}', status='ok', shard_id=0, domain_id=1,
                  domain_score=.5, fetched_at='2026-11-28T00:00:00+00:00',
                  content='<html>中文😀</html>', outlinks=[], content_hash=str(i), title=str(i))
    values.update(kwargs)
    return values


def writer(folder, records, **kwargs):
    return write_records(folder, records, **kwargs)


def checkpoint(root, kind):
    return Checkpoint(root/f'{kind}.json', root/'legacy.json', root/'peer.json')


def database():
    db = MagicMock()
    db.validate.side_effect = lambda r: int(r['shard_id'])
    output = []
    db.process_batch.side_effect = lambda records: output.extend(copy.deepcopy(records)) or []
    return db, output


def test_writer_keeps_types_streams_and_oversized_singletons_bounded(tmp_path):
    sink = ParquetBatchWriter(2, batch_records=2, max_buffer_bytes=2048)
    records = [link(1), content(2, content='x'*20000), link(3), content(4)]
    for record in records:
        kind = 'discovery' if record['status']=='new' else 'content'
        sink.append_batch(tmp_path, kind, columns([record])[kind])
        assert sink.buffered_bytes <= 2048
    sink.flush()
    assert sink.buffered_bytes == 0 and not sink.buffers
    assert list(read_routed_records(tmp_path, 'discovery')) == [records[0], records[2]]
    assert list(read_routed_records(tmp_path, 'content')) == [records[1], records[3]]
    assert not list(tmp_path.glob('*.tmp'))


def test_elapsed_flush_and_folder_tail_do_not_wait_for_full_batch(tmp_path):
    now = [0.0]
    sink = ParquetBatchWriter(0, clock=lambda:now[0], flush_seconds=1)
    sink.append_batch(tmp_path, 'discovery', columns([link(1)])['discovery'])
    assert not list(tmp_path.glob('*.parquet'))
    now[0] = 1
    sink.append_batch(tmp_path, 'discovery', columns([link(2)])['discovery'])
    assert list(read_routed_records(tmp_path, 'discovery')) == [link(1)]
    sink.flush()
    assert list(read_routed_records(tmp_path, 'discovery')) == [link(1), link(2)]


def test_failed_parquet_publication_retains_buffer_and_hides_partial_file(tmp_path, monkeypatch):
    sink = ParquetBatchWriter(0)
    sink.append_batch(tmp_path, 'discovery', columns([link()])['discovery'])
    def fail(table, path, **kwargs):
        Path(path).write_bytes(b'incomplete')
        raise OSError('disk full')
    monkeypatch.setattr(pq, 'write_table', fail)
    with pytest.raises(OSError, match='disk full'):
        sink.flush()
    assert sink.buffered_bytes > 0
    assert not list(tmp_path.iterdir())


def test_router_parquet_output_preserves_records_and_publishes_tails(router):
    original, _, root = router
    records = [_record('https://known.example/1', ['https://known.example/a', 'https://other.example/b'])]
    original.process_folder(_crawl_folder(root, 'expected', records))
    expected = _output_lines(root)
    cfg = replace(original.cfg, ingestor_dir_template=str(root/'parquet_{id:02d}'))
    parquet = RouterService(cfg)
    parquet.process_folder(_crawl_folder(root, 'parquet', records))
    actual = []
    for folder in root.glob('parquet_*/*/*'):
        for kind in ('discovery', 'content'):
            actual.extend(read_routed_records(folder, kind))
    assert sorted(actual, key=lambda r:(r['status'],r['url'])) == sorted(expected, key=lambda r:(r['status'],r['url']))
    assert not parquet.parquet_writer.buffers


def test_router_publication_error_propagates_instead_of_returning_success(router, monkeypatch):
    original, _, root = router
    parquet = RouterService(replace(original.cfg, output_batch_records=1))
    monkeypatch.setattr(parquet.parquet_writer, '_flush', MagicMock(side_effect=OSError('disk full')))
    with pytest.raises(RouterOutputError):
        parquet.process_folder(_crawl_folder(root, 'failure', [_record('https://known.example/', [])]))


def test_both_ingestors_preserve_counters_and_per_shard_result_order(tmp_path):
    records = [link(1), content(1), link(1, inlink_count_approx=3, anchor_text='first'),
               content(2, shard_id=1), content(1, status='fail', content_hash=None, title='failed'),
               link(3, shard_id=1), content(1, content_hash='updated', title='updated')]
    folder = tmp_path/'input'
    writer(folder, records, batch_records=2)
    outputs = {}
    for kind, cls in [('discovery', DiscoveryIngestor), ('content', ContentIngestor)]:
        db, output = database()
        ck = checkpoint(tmp_path/'progress', kind)
        service = cls(db, MagicMock(), ck)
        service.process_folder(folder, '20261128', '0000')
        service.close()
        assert ck.progress.seen('20261128', '0000')
        outputs[kind] = output
    assert [r['status'] for r in outputs['content'] if r['shard_id']==0] == ['ok','fail','ok']
    first = next(r for r in outputs['discovery'] if r['url']==link(1)['url'])
    assert first['inlink_count_approx']==4 and first['anchor_text']=='first'


def test_content_reads_projected_columns_and_bounds_db_batches(tmp_path, monkeypatch):
    writer(tmp_path/'input', [content(i, content='x'*100000, outlinks=[{'url':'https://other.test/','domain_id':2,'anchor':'a'}]) for i in range(5)])
    from containers.scheduler_ingest.ingestor.content_ingestor import service as module
    real = module.read_parquet_batches
    def observed(*args, **kwargs):
        assert kwargs['metadata_only'] is True
        for batch in real(*args, **kwargs):
            assert 'content' not in batch.schema.names and 'outlinks' not in batch.schema.names
            yield batch
    monkeypatch.setattr(module, 'read_parquet_batches', observed)
    db, output = database()
    service = ContentIngestor(db, MagicMock(), checkpoint(tmp_path, 'content'), batch_size=2, max_buffer_bytes=1)
    service.process_folder(tmp_path/'input', '20261128', '0000')
    assert [r['url'] for r in output] == [content(i)['url'] for i in range(5)]
    assert all(len(call.args[0]) <= 2 for call in db.process_batch.call_args_list)


@pytest.mark.parametrize('kind,cls', [('discovery', DiscoveryIngestor), ('content', ContentIngestor)])
def test_failed_database_batch_does_not_checkpoint_parquet(tmp_path, kind, cls):
    writer(tmp_path/'input', [link(), content()])
    db, _ = database()
    db.process_batch.side_effect = RuntimeError('write failed')
    ck = checkpoint(tmp_path, kind)
    service = cls(db, MagicMock(), ck)
    with pytest.raises(RuntimeError, match='write failed'):
        service.process_folder(tmp_path/'input', '20261128', '0000')
        service.close()
    assert ck.progress.state['date'] is None
    assert list((tmp_path/'input').glob('*.parquet'))


def test_extractor_reads_full_parquet_content_and_ignores_discovery(tmp_path):
    record = content(1, outlinks=[{'url':'https://other.test/','domain_id':2,'anchor':'a'}])
    writer(tmp_path, [link(), record])
    db = MagicMock()
    ExtractService(0, db, MagicMock()).process_folder(tmp_path)
    assert db.process.call_count == 1
    features = db.process.call_args.args[0]
    assert features['content_length'] == len(record['content'])
    assert features['num_links'] == 1


def test_corrupt_schema_is_rejected_and_unpublished_files_are_ignored(tmp_path):
    (tmp_path/'content-pending.parquet.tmp').write_bytes(b'incomplete')
    assert list(read_routed_records(tmp_path, 'content')) == []
    pq.write_table(pa.table({'url':['wrong schema']}), tmp_path/'content-bad.parquet')
    with pytest.raises(ValueError, match='incompatible content'):
        list(read_parquet_batches(tmp_path/'content-bad.parquet', 'content'))

"""Split ingestion streams Arrow batches with bounded, independent buffers."""
from collections import Counter
from unittest.mock import MagicMock

import pytest

from containers.scheduler_ingest.ingestor.discovery_ingestor import service as discovery_module
from containers.scheduler_ingest.ingestor.discovery_ingestor.service import DiscoveryIngestor
from containers.scheduler_ingest.ingestor.content_ingestor import service as content_module
from containers.scheduler_ingest.ingestor.content_ingestor.service import ContentIngestor
from containers.scheduler_ingest.tests.helpers.parquet_records import columns, link, write_records
from containers.scheduler_ingest.ingestor.libs.trap_filter import TrapFilter, TrapFilterConfig
from containers.scheduler_ingest.ingestor.libs.worker import Checkpoint


def checkpoint(root, kind):
    return Checkpoint(root/f'{kind}.json', root/'legacy.json', root/'peer.json')


def database():
    db = MagicMock()
    db.validate.side_effect = lambda row: int(row['shard_id'])
    db.process_batch.return_value = []
    return db


def test_discovery_writes_full_batches_before_input_is_exhausted(tmp_path, monkeypatch):
    consumed, writes = [], []
    def batches(*args, **kwargs):
        for i in range(40):
            consumed.append(i)
            yield columns([link(f'https://example.test/{i}', sid=i % 2)])['discovery']
    monkeypatch.setattr(discovery_module, 'routed_files', lambda *args: [tmp_path/'input.parquet'])
    monkeypatch.setattr(discovery_module, 'read_parquet_batches', batches)
    db = database()
    db.process_batch.side_effect = lambda batch: writes.append((len(consumed), len(batch))) or []
    service = DiscoveryIngestor(db, MagicMock(), checkpoint(tmp_path, 'discovery'), distinct_url_limit=4)
    service.process_folder(tmp_path, '20261128', '0000')
    service.close()
    assert writes[0] == (7, 4)
    assert all(size == 4 for _, size in writes)
    assert len(writes) == 10 and len(consumed) == 40


def test_duplicates_across_batches_preserve_additive_inlinks(tmp_path):
    shared = 'https://example.test/shared'
    records = [link(shared, inlink_count_approx=2), link('https://example.test/other'),
               link(shared, inlink_count_approx=3), link(shared, inlink_count_approx=4)]
    write_records(tmp_path/'input', records, batch_records=1)
    counts, sizes = Counter(), []
    db, stats = database(), MagicMock()
    def upsert(batch):
        sizes.append(len(batch))
        results = []
        for rec in batch:
            results.append(rec['url'] not in counts)
            counts[rec['url']] += rec['inlink_count_approx']
        return results
    db.process_batch.side_effect = upsert
    service = DiscoveryIngestor(db, stats, checkpoint(tmp_path, 'discovery'), distinct_url_limit=2)
    service.process_folder(tmp_path/'input', '20261128', '0000')
    service.close()
    assert sizes == [2, 1]
    assert counts[shared] == 9
    assert sum(call.kwargs['counters'].get('new_links', 0) for call in stats.write.call_args_list) == 2


def test_filter_normalizes_duplicates_across_input_batches(tmp_path):
    write_records(tmp_path/'input', [link(f'https://example.test/a?srsltid={i}') for i in range(3)] +
                  [link('https://example.test/x/x/x/')], batch_records=1)
    db = database()
    service = DiscoveryIngestor(db, MagicMock(), checkpoint(tmp_path, 'discovery'),
                               distinct_url_limit=2, trap_filter=TrapFilter(TrapFilterConfig()))
    service.process_folder(tmp_path/'input', '20261128', '0000')
    service.close()
    batch = db.process_batch.call_args.args[0]
    assert len(batch) == 1 and batch[0]['url'] == 'https://example.test/a'
    assert batch[0]['inlink_count_approx'] == 3


def test_failed_discovery_batch_retains_checkpoint_for_retry(tmp_path):
    write_records(tmp_path/'input', [link(f'https://example.test/{i}') for i in range(5)])
    db, ck = database(), checkpoint(tmp_path, 'discovery')
    db.process_batch.side_effect = RuntimeError('write failed')
    service = DiscoveryIngestor(db, MagicMock(), ck, distinct_url_limit=2)
    with pytest.raises(RuntimeError, match='write failed'):
        service.process_folder(tmp_path/'input', '20261128', '0000')
    assert ck.progress.state['date'] is None
    assert db.process_batch.call_count == 1
    assert service.buffers  # The failed batch is retained; no silent skipping.


def test_content_byte_budget_writes_before_input_is_exhausted(tmp_path, monkeypatch):
    consumed, writes = [], []
    def batches(*args, **kwargs):
        for i in range(20):
            consumed.append(i)
            batch = columns([dict(url=f'https://example.test/{i}', status='ok',
                                  shard_id=i % 4, title='x'*10000)])['content']
            yield batch.select([n for n in batch.schema.names if n not in {'content', 'outlinks'}])
    monkeypatch.setattr(content_module, 'routed_files', lambda *args: [tmp_path/'input.parquet'])
    monkeypatch.setattr(content_module, 'read_parquet_batches', batches)
    db = database()
    db.process_batch.side_effect = lambda batch: writes.append((len(consumed), len(batch))) or []
    service = ContentIngestor(db, MagicMock(), checkpoint(tmp_path, 'content'), max_buffer_bytes=25000)
    service.process_folder(tmp_path, '20261128', '0000')
    assert writes[0][0] <= 3
    assert sum(size for _, size in writes) == 20
    assert max(size for _, size in writes) < 2000


def test_single_oversized_content_record_is_processed(tmp_path):
    write_records(tmp_path/'input', [dict(url='https://example.test/large', status='ok',
                                        shard_id=0, title='x'*10000)])
    db = database()
    ContentIngestor(db, MagicMock(), checkpoint(tmp_path, 'content'), max_buffer_bytes=1000).process_folder(
        tmp_path/'input', '20261128', '0000')
    assert len(db.process_batch.call_args.args[0]) == 1


def test_discovery_byte_budget_flushes_before_count_limit(tmp_path):
    write_records(tmp_path/'input', [link(f'https://example.test/{i}', anchor_text='x'*10000)
                                    for i in range(5)], batch_records=1)
    db, sizes = database(), []
    db.process_batch.side_effect = lambda batch: sizes.append(len(batch)) or []
    service = DiscoveryIngestor(db, MagicMock(), checkpoint(tmp_path, 'discovery'),
                               max_buffer_bytes=25000, distinct_url_limit=4000)
    service.process_folder(tmp_path/'input', '20261128', '0000')
    service.close()
    assert sizes == [3, 2]


@pytest.mark.parametrize('size', [0, -1])
def test_invalid_discovery_batch_size_is_rejected(tmp_path, size):
    with pytest.raises(ValueError, match='discovery buffer limits'):
        DiscoveryIngestor(database(), MagicMock(), checkpoint(tmp_path, 'discovery'), distinct_url_limit=size)


@pytest.mark.parametrize('records', [[], [link('https://example.test/x/x/x/')]])
def test_empty_or_filtered_input_never_writes_empty_batch(tmp_path, records):
    folder = tmp_path/'input'
    folder.mkdir()
    write_records(folder, records)
    db = database()
    service = DiscoveryIngestor(db, MagicMock(), checkpoint(tmp_path, 'discovery'),
                               trap_filter=TrapFilter(TrapFilterConfig()))
    service.process_folder(folder, '20261128', '0000')
    service.close()
    db.process_batch.assert_not_called()

"""Policy and memory-bound equivalence for the streaming CPU fast paths."""
import copy
from datetime import datetime, timezone
import random

import pytest

import pyarrow as pa
from containers.scheduler_ingest.ingestor.discovery_ingestor.aggregation import aggregate_discoveries
from containers.scheduler_ingest.tests.helpers.parquet_records import columns
from containers.scheduler_ingest.ingestor.libs.db_ops import IngestDB
from containers.scheduler_ingest.ingestor.libs.trap_filter import TrapFilter, TrapFilterConfig, read_audit_log
from containers.scheduler_ingest.tests.test_split_ingestors import link, setup


NOW = datetime(2026, 11, 28, 4, tzinfo=timezone.utc)
URLS = [
    'https://example.test/clean',
    'https://example.test/a?srsltid=跟踪&id=1',
    'https://example.test/a?utm_source=x',
    'https://example.test/a?srsltid=x&utm_source=y',
    'https://example.test/x/x/x/?srsltid=a',
    'https://example.test/a;jsessionid=abc?x=1',
    'https://[invalid',
    '',
]


@pytest.mark.parametrize('options', [
    {}, {'enabled': False}, {'normalize': False}, {'shadow_normalize': False},
    {'drop': False}, {'normalize_params': frozenset({'srsltid', 'jsessionid'})},
])
def test_streaming_filter_preserves_batch_policy_and_audit(tmp_path, options):
    records = [link(url, domain_id=3, discovered_from='https://example.test/parent') for url in URLS]
    original = copy.deepcopy(records)
    filters = [TrapFilter(TrapFilterConfig(log_dir=str(tmp_path/name), **options), now=lambda: NOW)
               for name in ('batch', 'stream')]
    try:
        expected, _ = filters[0].apply(records)
        actual = [kept for record in records if (kept := filters[1].apply_one(record)) is not None]
    finally:
        for trap in filters:
            trap.close()
    assert actual == expected
    assert records == original
    assert list(read_audit_log(tmp_path/'batch')) == list(read_audit_log(tmp_path/'stream'))


def test_clean_streaming_filter_does_not_format_a_timestamp():
    def unexpected_clock():
        raise AssertionError('a clean URL needs no audit timestamp')
    trap = TrapFilter(TrapFilterConfig(), now=unexpected_clock)
    record = link()
    assert trap.apply_one(record) is record


def test_columnar_aggregation_preserves_metadata_and_compaction_order():
    rng = random.Random(16)
    records = [link(f'https://example.test/{rng.randrange(12)}', sid=0,
                    inlink_count_approx=rng.choice([1,3,10000]),
                    inlink_count_external=rng.choice([0,1,20000]),
                    anchor_text=rng.choice([None,'','first','中文']),
                    parent_page_score=rng.choice([None,.1,.5,1.0]),
                    url_priority=rng.choice([None,0.,1.,2.]),
                    discovered_from=f'parent/{i}', discovery_source_type=i%3,
                    source=i%5) for i in range(600)]
    expected = []
    for sid in (0,1):
        expected.extend(IngestDB.aggregate_links([r for r in records if r['shard_id']==sid]))
    previous = None
    for start in range(0,len(records),37):
        table = pa.Table.from_batches([columns(records[start:start+37])['discovery']])
        table = table.append_column('__inputs',pa.repeat(pa.scalar(1,type=pa.int64()),table.num_rows))
        previous = aggregate_discoveries(pa.concat_tables([previous,table]) if previous is not None else table)
    actual = previous.drop(['__inputs']).to_pylist()
    assert sorted(actual,key=lambda r:(r['shard_id'],r['url'])) == sorted(expected,key=lambda r:(r['shard_id'],r['url']))
    assert sum(previous['__inputs'].to_pylist()) == len(records)


def test_columnar_counter_overflow_fails_instead_of_wrapping():
    table = pa.Table.from_batches([columns([link(inlink_count_approx=2**63-1),link()])['discovery']])
    table = table.append_column('__inputs',pa.array([1,1],type=pa.int64()))
    with pytest.raises(pa.ArrowInvalid):
        aggregate_discoveries(table)


def test_filter_cache_distinguishes_rejection_and_bounds_recency(tmp_path,monkeypatch):
    import pyarrow.compute as pc
    from containers.scheduler_ingest.ingestor.discovery_ingestor import service as module
    monkeypatch.setattr(module,'FILTER_CACHE_CAPACITY',2)
    service,_,_,_=setup(tmp_path)
    service._update_filter_cache(pa.table({'url':['a','b'],'normalized_url':['A',None],
                                         'last_seen':pa.array([0,1],type=pa.int64())}))
    def lookup(urls):
        index=pc.index_in(pa.array(urls),value_set=service.filter_cache['url'])
        return index,pc.take(service.filter_cache['normalized_url'],index)
    index,urls=lookup(['a','b','c'])
    assert index[1].as_py() is not None and index[2].as_py() is None
    assert urls.to_pylist()==['A',None,None]
    service._update_filter_cache(pa.table({'url':['a','c'],'normalized_url':['A','C'],
                                         'last_seen':pa.array([3,2],type=pa.int64())}))
    index,urls=lookup(['a','b','c'])
    assert index[1].as_py() is None
    assert urls.to_pylist()==['A',None,'C']
    assert service.filter_cache.num_rows==2

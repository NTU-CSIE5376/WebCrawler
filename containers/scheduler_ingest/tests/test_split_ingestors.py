import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from containers.scheduler_ingest.ingestor.discovery_ingestor.service import DiscoveryIngestor
from containers.scheduler_ingest.ingestor.content_ingestor.service import ContentIngestor
from containers.scheduler_ingest.ingestor.libs.trap_filter import TrapFilter, TrapFilterConfig
from containers.scheduler_ingest.ingestor.libs.worker import Checkpoint, ShardWriter, exclusive
from libs.ipc.folder_reader import Progress
from scripts.cleanup_crawl_result import discover_stages


from containers.scheduler_ingest.tests.helpers.parquet_records import add, link, write_records
from libs.ipc.parquet import read_routed_records


def setup(tmp_path, **kwargs):
    time = [0.0]
    db = MagicMock()
    db.validate.side_effect = lambda r: int(r['shard_id'])
    batches = []
    db.process_batch.side_effect = lambda records: batches.append([dict(r) for r in records]) or [False]*len(records)
    ck = Checkpoint(tmp_path/'discovery.json', tmp_path/'legacy.json', tmp_path/'content.json')
    service = DiscoveryIngestor(db, MagicMock(), ck, clock=lambda:time[0], **kwargs)
    return service, batches, ck, time


def folder(tmp_path, name, records):
    path = tmp_path/name
    path.mkdir()
    write_records(path, records)
    return path


def test_unique_limit_aggregates_counts_provenance_priority_and_anchor(tmp_path):
    svc, batches, ck, clock = setup(tmp_path, distinct_url_limit=2)
    add(svc, link(inlink_count_approx=3, parent_page_score=.1, discovered_from='old'))
    add(svc, link(inlink_count_approx=4, inlink_count_external=1, anchor_text='first',
                 parent_page_score=.5, discovered_from='best', url_priority=1.5, source=4))
    add(svc, link(anchor_text='second', parent_page_score=.2, discovered_from='other',url_priority=1.2))
    assert not batches  # Three inputs, still just one distinct URL.
    add(svc, link('https://example.test/b'))
    assert len(batches)==1 and len(batches[0])==2
    a=batches[0][0]
    assert a['inlink_count_approx']==8 and a['inlink_count_external']==1
    assert (a['anchor_text'],a['discovered_from'],a['parent_page_score'],a['url_priority'])==('first','best',.5,1.5)
    assert svc.buffered_bytes==0


def test_threshold_is_per_shard_and_memory_budget_is_shared(tmp_path):
    svc,batches,_,_=setup(tmp_path,distinct_url_limit=2,max_buffer_bytes=10**6)
    add(svc, link(sid=0));add(svc, link(sid=1))
    assert not batches
    add(svc, link('https://example.test/b',sid=0))
    assert [r['shard_id'] for r in batches[0]]==[0,0]
    assert list(svc.buffers)==[1]
    tiny,batches,_,_=setup(tmp_path/'tiny',max_buffer_bytes=1)
    add(tiny, link(anchor_text='x'*10000))
    assert len(batches)==1 and tiny.buffered_bytes==0


def test_tails_span_folders_checkpoint_only_after_commit(tmp_path):
    svc,batches,ck,clock=setup(tmp_path)
    for name,bucket,n in [('one','0000',2),('two','0010',3)]:
        svc.process_folder(folder(tmp_path,name,[link() for _ in range(n)]),'20261125',bucket)
    assert not batches and ck.progress.state['date'] is None
    assert svc.seen('20261125','0010')
    clock[0]=599;svc.flush_if_due();assert not batches
    clock[0]=600;svc.flush_if_due()
    assert len(batches)==1 and batches[0][0]['inlink_count_approx']==5
    assert ck.progress.seen('20261125','0010')


def test_continuous_duplicate_traffic_cannot_hold_checkpoint_forever(tmp_path):
    svc,batches,ck,clock=setup(tmp_path)
    svc.process_folder(folder(tmp_path,'one',[link()]),'20261125','0000')
    clock[0]=599;add(svc, link());svc.flush_if_due();assert not batches
    clock[0]=600;add(svc, link());svc.flush_if_due()
    assert batches[0][0]['inlink_count_approx']==3
    assert ck.progress.seen('20261125','0000')


def test_failures_keep_input_checkpoint_and_buffer(tmp_path):
    svc,_,ck,_=setup(tmp_path)
    svc.process_folder(folder(tmp_path,'one',[link()]),'20261125','0000')
    svc.db.process_batch.side_effect=RuntimeError('write failed')
    with pytest.raises(RuntimeError,match='write failed'):svc.close()
    assert ck.progress.state['date'] is None
    assert svc.buffers[0]['inlink_count_approx'].to_pylist()==[1]
    assert list((tmp_path/"one").glob("*.parquet"))  # Durable input is retained.


def test_filter_normalizes_before_aggregation_and_never_filters_results(tmp_path):
    svc,batches,_,_=setup(tmp_path,trap_filter=TrapFilter(TrapFilterConfig()))
    svc.process_folder(folder(tmp_path,'one',[
        link('https://example.test/a?srsltid=1'),link('https://example.test/a?srsltid=2'),
        link('https://example.test/a?srsltid=1'),link('https://example.test/x/x/x/'),
        dict(url='https://example.test/result',status='ok',shard_id=0)]),'20261125','0000')
    svc.close()
    assert len(batches[0])==1 and batches[0][0]['inlink_count_approx']==3


def test_content_does_not_wait_for_discovery_and_preserves_result_order(tmp_path):
    db=MagicMock();db.validate.side_effect=lambda r:r['shard_id'];batches=[]
    db.process_batch.side_effect=lambda records:batches.append(list(records)) or []
    ck=Checkpoint(tmp_path/'content.json',tmp_path/'legacy.json',tmp_path/'discovery.json')
    svc=ContentIngestor(db,MagicMock(),ck,batch_size=2)
    results=[dict(url='https://example.test/a',status='ok',shard_id=0,title=str(i)) for i in range(3)]
    svc.process_folder(folder(tmp_path,'one',[link(),*results]),'20261125','0000')
    assert [len(b) for b in batches]==[2,1]
    assert [r['title'] for b in batches for r in b]==['0','1','2']
    assert ck.progress.seen('20261125','0000')
    assert not (tmp_path/'legacy.json').exists()  # Discovery has not committed.


def test_content_failure_does_not_checkpoint(tmp_path):
    db=MagicMock();db.validate.return_value=0;db.process_batch.side_effect=RuntimeError('write failed')
    ck=Checkpoint(tmp_path/'content.json',tmp_path/'legacy.json',tmp_path/'discovery.json')
    svc=ContentIngestor(db,MagicMock(),ck)
    with pytest.raises(RuntimeError):
        svc.process_folder(folder(tmp_path,'one',[dict(url='x',status='fail',shard_id=0)]),'20261125','0000')
    assert ck.progress.state['date'] is None


def test_migration_seeds_legacy_and_combined_progress_tracks_slowest(tmp_path):
    Progress(str(tmp_path/'legacy.json')).advance('20261125','0000')
    a=Checkpoint(tmp_path/'a.json',tmp_path/'legacy.json',tmp_path/'b.json')
    b=Checkpoint(tmp_path/'b.json',tmp_path/'legacy.json',tmp_path/'a.json')
    assert a.progress.seen('20261125','0000') and b.progress.seen('20261125','0000')
    a.advance('20261125','0020');b.advance('20261125','0010')
    assert Progress(str(tmp_path/'legacy.json')).state==dict(date='20261125',time='0010')


def test_shard_writer_validates_ownership_kind_and_single_shard(tmp_path):
    db=MagicMock();w=ShardWriter(db,0,16,tmp_path,True)
    with pytest.raises(ValueError,match='not owned'):w.process_batch([link(sid=16)])
    with pytest.raises(ValueError,match='wrong ingestion'):w.process_batch([dict(status='ok',shard_id=0)])
    with pytest.raises(ValueError,match='exactly one'):w.process_batch([link(sid=0),link(sid=1)])
    db.process_batch.assert_not_called()
    w.process_batch([link('z'),link('a')])
    assert [r['url'] for r in db.process_batch.call_args.args[0]]==['a','z']


def test_duplicate_component_cannot_take_same_worker_lock(tmp_path):
    with exclusive(tmp_path/'worker.lock',nonblocking=True):
        with pytest.raises(BlockingIOError):
            with exclusive(tmp_path/'worker.lock',nonblocking=True):pass


def test_cleanup_waits_for_both_split_consumers_and_extractor(tmp_path):
    stream=str(tmp_path/'ingestor_{id:02d}')
    (tmp_path/'ingestor_00').mkdir()
    raw={'router':dict(crawler_dir_template=str(tmp_path/'crawler_{id:02d}'),progress_template='router_{id}.json'),
         'extractor':dict(result_dir_template=stream,progress_template='extractor_{id}.json')}
    for kind in ('discovery_ingestor','content_ingestor'):
        raw[kind]=dict(result_dir_template=stream,progress_template=kind+'_{id}.json')
    stage,=discover_stages(raw)
    assert len(stage.progress_files)==3
    assert any('discovery_ingestor' in str(p) for p in stage.progress_files)


def test_content_buffer_excludes_unused_html_and_outlinks(tmp_path):
    db=MagicMock();db.validate.side_effect=lambda r:r['shard_id'];batches=[]
    db.process_batch.side_effect=lambda records:batches.append(list(records)) or []
    ck=Checkpoint(tmp_path/'content.json',tmp_path/'legacy.json',tmp_path/'discovery.json')
    svc=ContentIngestor(db,MagicMock(),ck,max_buffer_bytes=20000,batch_size=2000)
    records=[dict(url=f'https://example.test/{i}',status='ok',shard_id=0,
                  content='x'*100000,outlinks=[dict(url='https://other.test/')]*100,
                  content_hash='hash',title='title') for i in range(10)]
    path=folder(tmp_path,'large',records)
    svc.process_folder(path,'20261125','0000')
    assert [len(batch) for batch in batches]==[10]
    assert all('content' not in r and 'outlinks' not in r for r in batches[0])
    assert all(r['content_hash']=='hash' and r['title']=='title' for r in batches[0])
    assert list(read_routed_records(path, 'content'))[0]['content']=='x'*100000

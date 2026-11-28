"""Real SQL checks in an isolated schema; never uses the production DSN."""
import os
import uuid

import psycopg2
from psycopg2 import sql
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from containers.scheduler_ingest.ingestor.libs.db_ops import IngestDB, _HIST_COLS


@pytest.fixture
def db():
    dsn = os.environ.get('INGESTOR_TEST_DSN')
    if not dsn:
        pytest.skip('INGESTOR_TEST_DSN is required for isolated PostgreSQL tests')
    schema = 'ingestor_test_' + uuid.uuid4().hex
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    engine = create_engine(dsn.replace('postgresql://','postgresql+psycopg2://',1),
                           connect_args={'options':f'-csearch_path={schema}'})
    dates={'first_seen','last_scheduled','last_fetch_ok','last_content_update','last_modified','url_score_updated_at'}
    floats={'url_score','domain_score','parent_page_score'}
    bools={'should_crawl','has_json_ld','is_redirect'}
    ints={'domain_id','hreflang_count','redirect_hop_count','source','discovery_source_type','robots_bits'}
    columns=[]
    for name in _HIST_COLS:
        typ='TIMESTAMPTZ' if name in dates else 'DOUBLE PRECISION' if name in floats else 'BOOLEAN' if name in bools else 'BIGINT' if name in ints or name.startswith('num_') or name.startswith('inlink_') else 'TEXT'
        default=' DEFAULT CURRENT_TIMESTAMP' if name=='first_seen' else ' DEFAULT 0' if name.startswith('num_') or name.startswith('inlink_') else ''
        columns.append(f'{name} {typ}{default}')
    with engine.begin() as c:
        c.execute(text('CREATE TABLE domain_state (domain_id BIGINT PRIMARY KEY, discovery_frozen BOOLEAN DEFAULT FALSE,domain_fail_count INTEGER DEFAULT 0,crawl_paused_until TIMESTAMPTZ)'))
        c.execute(text('INSERT INTO domain_state (domain_id) VALUES (1)'))
        c.execute(text('CREATE TABLE url_state_current_000 ('+', '.join(columns)+', PRIMARY KEY(url))'))
        c.execute(text('CREATE TABLE url_state_history_000 (snapshot_id BIGSERIAL PRIMARY KEY,snapshot_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,'+', '.join(columns)+')'))
        c.execute(text('CREATE TABLE url_event_counter_000 (url TEXT,event_date DATE,num_fetch_ok INTEGER DEFAULT 0,num_fetch_fail INTEGER DEFAULT 0,num_content_update INTEGER DEFAULT 0,accounted BOOLEAN,PRIMARY KEY(url,event_date))'))
    store=IngestDB(sessionmaker(bind=engine,autoflush=False))
    try:
        yield store,engine
    finally:
        engine.dispose()
        with conn.cursor() as cur:
            cur.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        conn.close()


@pytest.mark.parametrize('content_first',[False,True])
def test_both_arrival_orders_preserve_fetch_state_and_discovery_metadata(db,content_first):
    store,engine=db
    link=dict(url='https://example.test/a',status='new',shard_id=0,domain_id=1,domain_score=.7,
              parent_page_score=.9,discovered_from='https://parent.test/',anchor_text='first',
              inlink_count_approx=7,inlink_count_external=3,discovery_source_type=1)
    result=dict(url=link['url'],status='ok',shard_id=0,domain_id=1,domain_score=.7,content_hash='hash',title='title')
    timings=[]
    store.timing_observer=lambda stage,seconds:timings.append((stage,seconds))
    for rec in ([result,link] if content_first else [link,result]):
        store.process_batch([rec])
    with engine.connect() as c:
        row=c.execute(text('SELECT domain_score,url_score,source,content_hash,num_fetch_ok_90d,num_content_update_90d,inlink_count_approx,inlink_count_external,anchor_text,parent_page_score,discovered_from,should_crawl FROM url_state_current_000')).one()
        assert tuple(row)==(.7,0.,0,'hash',1,1,7,3,'first',.9,'https://parent.test/',False)
        assert c.scalar(text('SELECT num_fetch_ok FROM url_event_counter_000'))==1
    stages={stage for stage,_ in timings}
    assert {'discovery_upsert','content_upsert','history_insert','discovery_lookup','result_hash_lookup','commit'} <= stages

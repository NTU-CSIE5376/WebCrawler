from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
import time
from concurrent.futures import Future

import pytest

SPEC = importlib.util.spec_from_file_location('capture', Path(__file__).parents[1] / 'yb_deadlock_capture.py')
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)
U1 = 'ba99f472-dded-4b39-87d7-dcba23b462bf'
U2 = '89aebf79-95ac-4621-b805-004cbd452bb4'
ERROR = b'2026-11-13 05:07:46.932 UTC [123] ERROR: deadlock detected\n'
DETAIL = f'2026-11-13 05:07:46.932 UTC [123] DETAIL: {U1}->{U2}->{U1}; key https://private.example/password\n'.encode()


def test_parser_handles_split_error_detail_without_retaining_payload():
    parser = c.DeadlockParser()
    assert parser.feed(ERROR[:17]) == []
    assert parser.feed(ERROR[17:] + DETAIL[:31]) == []
    event, = parser.feed(DETAIL[31:])
    assert event['pid'] == 123
    assert event['transaction_ids'] == [U1, U2]
    assert 'private' not in json.dumps(event) and 'password' not in json.dumps(event)
    assert len(parser.buffer) == 0 and not parser.pending


def test_statement_text_cannot_forge_deadlock_and_pid_only_event_is_preserved():
    parser = c.DeadlockParser()
    assert not parser.feed(b'2026-11-13 05:07:46.932 UTC [123] STATEMENT: SELECT \'deadlock detected\'\n')
    assert not parser.feed(ERROR)
    event, = parser.feed(b'2026-11-13 05:07:46.932 UTC [123] STATEMENT: secret SQL\n')
    assert event['pid'] == 123 and event['transaction_ids'] == []
    assert 'SQL' not in json.dumps(event)


def test_oversized_statement_line_is_bounded_and_next_event_survives():
    parser = c.DeadlockParser()
    for _ in range(100):
        parser.feed(b'x' * 4096)
        assert len(parser.buffer) <= 8192
    events = parser.feed(b'\n' + ERROR + DETAIL)
    assert len(events) == 1 and parser.truncated >= 1


def test_private_row_projection_removes_keys_queries_and_unsafe_application_name():
    row = {'pid': 123, 'transaction_id': U1, 'query_id': 999, 'blocked_by': [U2],
           'query': "UPDATE url_state_current_242 SET x='secret'", 'hash_cols': ['https://private'],
           'range_cols': ['private'], 'application_name': 'https://private', 'mode': ['StrongWrite']}
    got, = c.safe_rows([row])
    assert 'query' not in got and 'hash_cols' not in got and 'range_cols' not in got
    assert got['application_name'] == '<redacted>' and got['blocked_by'] == [U2]
    assert 'https://' not in json.dumps(got)
    assert len(c.safe_rows([row] * 300)) == 256


class Cursor:
    description = [('pid',), ('query_id',)]
    def __init__(self): self.sql = []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params): self.sql.append((sql, params))
    def fetchall(self): return [(123, 456)]


class Connection:
    closed = False
    autocommit = False
    def __init__(self): self.cur = Cursor()
    def cursor(self): return self.cur
    def close(self): self.closed = True


def test_connection_is_readonly_bounded_and_reuses_session():
    seen = []
    con = Connection()
    def connect(**kwargs): seen.append(kwargs); return con
    db = c.Database('172.16.191.3', connect=connect)
    assert db.query(c.ACTIVITY_SQL) == [{'pid': 123, 'query_id': 456}]
    db.query(c.ACTIVITY_SQL)
    assert len(seen) == 1 and con.autocommit
    options = seen[0]['options']
    for value in ['default_transaction_read_only=on', 'statement_timeout=250ms',
                  'yb_enable_memory_tracking=off', 'yb_enable_pg_locks=on']:
        assert value in options
    assert seen[0]['application_name'] == 'wc-deadlock-capture'
    db.close(); assert con.closed


def test_output_budget_stops_without_deleting_previous_evidence_and_status_has_reserve(tmp_path):
    output = c.Output(tmp_path, budget=20000)
    output.write('first.json', {'safe': 'x' * 3000})
    with pytest.raises(c.OutputLimit): output.write('second.json', {'huge': 'x' * 10000})
    assert (tmp_path / 'first.json').exists() and not (tmp_path / 'second.json').exists()
    output.write('status.json', {'output_limit': True}, status=True)
    assert (tmp_path.stat().st_mode & 0o777) == 0o700
    assert ((tmp_path / 'first.json').stat().st_mode & 0o777) == 0o600


def test_output_rejects_escape_and_symlink(tmp_path):
    output = c.Output(tmp_path / 'output')
    with pytest.raises(ValueError): output.write('../bad.json', {})
    (output.root / 'secret.json').symlink_to(tmp_path / 'elsewhere')
    with pytest.raises(ValueError): output.write('secret.json', {})


class FakeDatabase:
    def __init__(self, host, timeout=250): self.host, self.timeout, self.closed = host, timeout, False
    def query(self, sql, params=()):
        if sql == c.LOCK_SQL:
            return [{'pid': 123, 'transaction_id': params[0], 'blocked_by': [U2], 'hash_cols': ['https://secret']}]
        return [{'pid': 123, 'query_id': 77, 'transaction_id': U1, 'query': "SELECT 'secret'", 'client_port': 5555}]
    def close(self): self.closed = True


class ImmediatePool:
    def submit(self, fn, *args):
        future = Future()
        try: future.set_result(fn(*args))
        except BaseException as e: future.set_exception(e)
        return future
    def shutdown(self, **kwargs): pass


def collector(tmp_path, clock):
    capture = c.Capture(['node'], c.Output(tmp_path), database_factory=FakeDatabase, clock=lambda: clock[0])
    capture.pool.shutdown(); capture.pool = ImmediatePool(); capture.running = True
    return capture


def test_fake_database_and_pg_log_capture_before_after_metadata_without_urls(tmp_path):
    now = [1.0]; capture = collector(tmp_path, now)
    capture.tick()
    for event in capture.parser.feed(ERROR + DETAIL): capture.events.put_nowait(event)
    now[0] = 2; capture.tick()
    assert len(capture.pending) == 1
    now[0] = 13; capture.tick()
    paths = list(tmp_path.glob('event-*.json')); assert len(paths) == 1
    text = paths[0].read_text(); event = json.loads(text)
    assert event['before'] and event['post_window_complete'] and event['locks']
    assert 'https://' not in text and 'secret' not in text and 'hash_cols' not in text and '"query"' not in text
    assert event['event']['transaction_ids'] == [U1, U2]


def test_failed_queries_back_off_from_completion_and_health_reports_failure(tmp_path):
    now = [1.0]; capture = collector(tmp_path, now)
    class Failed(FakeDatabase):
        def query(self, *args): now[0] += 2; raise RuntimeError('https://password')
    capture.databases['node'] = Failed('node')
    capture.sample('node', 1)
    assert capture.state['node']['next'] >= 5
    capture.status(); status = json.loads((tmp_path / 'status.json').read_text())
    assert not status['healthy'] and status['nodes']['node']['errors'] == 1
    assert 'password' not in (tmp_path / 'status.json').read_text()


def test_ring_and_oversized_result_are_bounded_and_visible(tmp_path):
    now = [1.0]; capture = collector(tmp_path, now)
    class Many(FakeDatabase):
        def query(self, *args): return [{'pid': i} for i in range(300)]
    capture.databases['node'] = Many('node')
    capture.tick()
    assert capture.state['node']['capped'] == 1 and capture.metrics['ash_capped'] == 1
    assert all(len(x['rows']) == 256 for x in capture.ring)
    for i in range(1000): capture.ring.append({'at': time.time(), 'rows': []})
    assert len(capture.ring) == 512


def test_incident_cooldown_is_visible(tmp_path):
    now = [1.0]; capture = collector(tmp_path, now)
    capture.events.put({'pid': 123, 'transaction_ids': [U1, U2]})
    capture.events.put({'pid': 124, 'transaction_ids': [U1, U2]})
    capture.tick()
    assert capture.metrics['incidents'] == 1 and capture.metrics['cooldown_drops'] == 1


def test_log_reader_has_open_stdin_and_eof_is_failure(tmp_path, monkeypatch):
    capture = c.Capture(['node'], c.Output(tmp_path), [sys.executable, '-c', 'pass'])
    capture.tail_logs()
    assert capture.tail.stdin is not None and capture.metrics['tail_eof']
    capture.tail.stdin.close(); capture.tail.wait(timeout=3); capture.pool.shutdown()


def test_duration_shutdown_flushes_incomplete_incident_and_closes_databases(tmp_path):
    now = [0.0]; capture = collector(tmp_path, now)
    capture.duration = 1
    capture.events.put({'pid': 123, 'transaction_ids': [U1, U2]})
    class StopSoon:
        def __init__(self): self.value = False
        def is_set(self): return self.value
        def set(self): self.value = True
        def wait(self, t): now[0] += 2
    capture.stop = StopSoon()
    capture.run()
    event, = [json.loads(p.read_text()) for p in tmp_path.glob('event-*.json')]
    assert not event['post_window_complete']
    assert capture.databases['node'].closed
    assert not json.loads((tmp_path / 'status.json').read_text())['running']


def test_timestamp_prefix_cannot_carry_urls_or_sql_literals():
    parser = c.DeadlockParser()
    bad = b'2026-11-13 05:07:46.932 https://private [123] ERROR: deadlock detected\n'
    assert not parser.feed(bad + DETAIL)
    assert not parser.pending


def test_health_tracks_current_cap_and_staleness_without_hiding_history(tmp_path):
    now = [1.0]; capture = collector(tmp_path, now)
    class Many(FakeDatabase):
        def query(self, *args): return [{'pid': i} for i in range(300)]
    capture.databases['node'] = Many('node'); capture.sample('node', now[0]);capture.status()
    assert not json.loads((tmp_path/'status.json').read_text())['healthy']
    now[0] = 3;capture.databases['node'] = FakeDatabase('node');capture.sample('node', now[0]);capture.status()
    status = json.loads((tmp_path/'status.json').read_text())
    assert status['healthy'] and status['nodes']['node']['capped'] == 1
    assert not status['nodes']['node']['current_capped']
    now[0] = 10;capture.status()
    assert not json.loads((tmp_path/'status.json').read_text())['healthy']


def test_error_state_keeps_sqlstate_but_not_database_error_text(tmp_path):
    now = [1.0];capture = collector(tmp_path, now)
    class Error(Exception): pgcode = '57014'
    class Failed(FakeDatabase):
        def query(self, *args): raise Error('https://private password=secret')
    capture.databases['node'] = Failed('node');capture.sample('node', now[0]);capture.status()
    text=(tmp_path/'status.json').read_text();status=json.loads(text)
    assert status['nodes']['node']['sqlstate']=='57014'
    assert 'password' not in text and 'https://' not in text
    assert 'blocked_by::text[] AS blocked_by' in c.LOCK_SQL


def test_output_has_single_owner_and_releases_lock_on_close(tmp_path):
    first = c.Output(tmp_path)
    with pytest.raises(RuntimeError, match='already owned'):
        c.Output(tmp_path)
    first.close()
    second = c.Output(tmp_path); second.close()


def test_literal_sql_percent_is_not_interpolated_as_empty_parameters():
    con = Connection()
    db = c.Database('test', connect=lambda **kw: con)
    db.query(c.ACTIVITY_SQL)
    assert con.cur.sql[-1][1] is None  # psycopg2 treats () as interpolation.
    db.query(c.LOCK_SQL, (U1,))
    assert con.cur.sql[-1][1] == (U1,)
    db.close()


def test_metadata_only_mode_avoids_uuid_rpc_only_for_its_own_connection():
    seen=[]
    con=Connection()
    def connect(**kwargs):
        seen.append(kwargs)
        return con
    db=c.Database('secondary',connect=connect,uuid_info=False)
    db.query(c.ACTIVITY_SQL)
    assert 'yb_enable_pg_locks=off' in seen[0]['options']
    assert 'default_transaction_read_only=on' in seen[0]['options']
    db.close()


def test_canceled_autocommit_sample_reuses_a_verified_idle_session():
    class Canceled(Exception):
        pgcode='57014'
    con=Connection()
    con.get_transaction_status=lambda: 0
    seen=[]
    def connect(**kw):
        seen.append(kw)
        return con
    real=con.cur.execute
    calls=[]
    def execute(sql,params):
        calls.append(sql)
        if len(calls)==1:raise Canceled()
        return real(sql,params)
    con.cur.execute=execute
    db=c.Database('node',connect=connect)
    with pytest.raises(Canceled):db.query(c.ACTIVITY_SQL)
    assert not con.closed
    assert db.query(c.ACTIVITY_SQL)==[{'pid':123,'query_id':456}]
    assert len(seen)==1
    db.close()


@pytest.mark.parametrize('query',[
    "SELECT url FROM url_state_current_001 WHERE url='https://private.example/a'",
    'SELECT url FROM url_state_current_001 -- confidential comment',
    'SELECT url FROM url_state_current_001 /* confidential comment */',
    'SELECT $tag$private$tag$ FROM url_state_current_001',
    'ALTER TABLE url_state_current_001 ADD COLUMN x int',
])
def test_normalized_shape_gate_rejects_payloads_and_unrelated_commands(query):
    assert c.safe_statement_shape(query) is None


def test_live_shapes_use_only_exact_incident_transactions_without_db_lookup(tmp_path):
    class Shapes:
        def __init__(self,*args,**kw):self.calls=[]
        def query(self,sql,params=None):
            self.calls.append((sql,params))
            return [{'query_id':'9223372036854770000','statement_shape':'SELECT url FROM url_state_current_242 WHERE url=$1'},
                    {'query_id':'9','statement_shape':"SELECT url FROM url_state_current_242 WHERE url='private'"}]
        def close(self):pass
    run=c.Capture(['node'],c.Output(tmp_path),database_factory=Shapes)
    snapshots=[{'node':'node','kind':'activity','rows':[{'transaction_id':U1,'query_id':'9223372036854770000','statement_prefix':'SELECT url FROM url_state_current_242 WHERE url=$1'},
                                                      {'transaction_id':U2,'query_id':'9','statement_prefix':"SELECT url FROM url_state_current_242 WHERE url='private'"},
                                                      {'transaction_id':'unrelated','query_id':'10'}]}]
    got=run.statement_shapes({'transaction_ids':[U1,U2]},snapshots)
    assert run.databases['node'].calls==[]
    assert got[0]['statement_shape'].endswith('url=?')
    assert got[1]['statement_shape'] is None and got[1]['omitted_for_privacy']
    assert 'private' not in json.dumps(got)
    run.pool.shutdown();run.output.close()


def test_live_prefix_projection_removes_numeric_values_preserving_identifiers():
    row={'pid':1,'statement_prefix':'SELECT url FROM url_state_current_242 WHERE x=123 AND y=1.25e-4 AND z=$9'}
    got,=c.safe_rows([row])
    assert got['statement_prefix']=='SELECT url FROM url_state_current_242 WHERE x=? AND y=? AND z=?'
    assert got['statement_prefix_partial'] is True

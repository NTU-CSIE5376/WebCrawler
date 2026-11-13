import importlib.util
import json
from pathlib import Path
import socket

import pytest

SPEC = importlib.util.spec_from_file_location('waitqueue_probe', Path(__file__).parents[1] / 'yb_waitqueue_probe.py')
p = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(p)
NODE = '172.16.191.3'
TABLET = '658b02a149d6434cb1773365fee53211'
U1 = 'ba99f472-dded-4b39-87d7-dcba23b462bf'
U2 = '89aebf79-95ac-4621-b805-004cbd452bb4'
MAP = [{'node': NODE, 'tablet_id': TABLET, 'relation_name': 'url_state_current_223'}]


def html(rows='', single='', blockers='', headers=None, tablet=TABLET):
    headers = headers or ['WaiterId', 'RequestId', 'PgSessionReqVersion', 'BlockerId']
    return (f'<html><h1>Waiters for Tablet {tablet}</h1><h2>Txn Waiters:</h2><table><tr>'
      + ''.join(f'<th>{x}</th>' for x in headers) + '</tr>' + rows
      + '</table><h2>Single Shard Waiters:</h2><table><tr><th>Index</th><th>BlockerId</th></tr>'
      + single + '</table><h2>Blockers:</h2><table><tr><th>BlockerId</th><th>Status</th></tr>'
      + blockers + '</table></html>').encode()


def row(a=U1, b=U2, request='42', version='7'):
    return f'<tr><td>|{a}</td><td>|{request}</td><td>|{version}</td><td>|{b}</td></tr>'


class Socket:
    def __init__(self): self.timeouts = []
    def settimeout(self, value): self.timeouts.append(value)


class Response:
    status = 200
    def __init__(self, body): self.body = body; self.offset = 0
    def read1(self, size):
        got = self.body[self.offset:self.offset + size]; self.offset += len(got); return got
    def close(self): pass


class Connection:
    calls = []
    body = html()
    status = 200
    error = None
    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = Socket(); self.__class__.calls.append(self)
    def connect(self):
        if self.error: raise self.error('https://secret/password')
    def request(self, method, path, headers): self.method, self.path = method, path
    def getresponse(self):
        response = Response(self.body); response.status = self.status; return response
    def close(self): pass


@pytest.fixture
def transport():
    class Fake(Connection):
        calls = []; body = html(); status = 200; error = None
    return Fake


def probe(transport, table_map=MAP):
    return p.WaitQueueProbe(table_map, connection_factory=transport)


def frame(prefix=TABLET[:15], node=NODE, count=1):
    return {'node': node, 'rows': [{'tablet_prefix': prefix} for _ in range(count)]}


def test_recognized_empty_schema_is_only_true_empty(transport):
    got, = probe(transport).sample([frame()])
    assert got['empty'] and got['edges'] == [] and got['error_type'] is None
    assert got['node'] == NODE and got['tablet_id'] == TABLET and got['kind'] == 'waitqueue'
    assert transport.calls[0].path == '/waitqueue?id=' + TABLET
    assert transport.calls[0].port == 9000 and transport.calls[0].timeout == .15


def test_real_column_headers_map_uuid_edges_and_integer_request(transport):
    transport.body = html(row())
    got, = probe(transport).sample([frame()])
    assert got['edges'] == [{'waiter_transaction_id': U1, 'blocker_transaction_id': U2,
                             'request_id': 42, 'pg_session_request_version': 7}]
    assert not got['empty'] and not got['truncated']


def test_reordered_columns_and_html_entities_do_not_reverse_edge(transport):
    transport.body = html(f'<tr><td>&#124;{U2}</td><td>42</td><td>{U1.replace("-", "&#45;")}</td><td>nullopt</td></tr>',
                          headers=['BlockerId', 'RequestId', 'WaiterId', 'PgSessionReqVersion'])
    got, = probe(transport).sample([frame()])
    assert got['edges'][0]['waiter_transaction_id'] == U1
    assert got['edges'][0]['blocker_transaction_id'] == U2
    assert got['edges'][0]['pg_session_request_version'] is None


def test_single_shard_waiters_and_blockers_do_not_become_fake_uuid_edges(transport):
    transport.body = html(single=f'<tr><td>|0</td><td>|{U2}</td></tr>',
                          blockers=f'<tr><td>|{U2}</td><td>secret SQL/URL https://secret</td></tr>')
    got, = probe(transport).sample([frame()])
    assert not got['empty'] and not got['edges'] and got['single_shard_waiter_count'] == 1
    assert got['blocker_transaction_ids'] == [U2]
    assert 'secret' not in json.dumps(got) and 'https://' not in json.dumps(got)


@pytest.mark.parametrize('body', [b'<html>oops</html>', b'<table><tr><th>WaiterId</th><th>BlockerId</th></tr></table>', html(tablet='a' * 32)])
def test_unknown_or_wrong_tablet_never_means_healthy_empty(transport, body):
    transport.body = body
    got, = probe(transport).sample([frame()])
    assert not got['empty'] and got['error_type'] is not None


def test_bad_row_is_explicit_without_saving_sensitive_cells(transport):
    transport.body = html(row(a='https://secret?password=x'))
    got, = probe(transport).sample([frame()])
    assert got['error_type'] == 'ParseError' and not got['empty']
    assert 'secret' not in json.dumps(got)


def test_body_limit_keeps_verified_partial_edges_but_marks_truncation(transport):
    transport.body = html(row()) + b'x' * 70000
    got, = probe(transport).sample([frame()])
    assert got['truncated'] and not got['empty'] and got['error_type'] == 'ResponseTooLarge'
    assert len(got['edges']) == 1


def test_edge_cap_and_uuid_cap_are_visible(transport):
    transport.body = html(''.join(row(request=str(i)) for i in range(70)),
                          blockers=''.join(f'<tr><td>{i:08x}-0000-0000-0000-000000000000</td><td>pending</td></tr>' for i in range(260)))
    got, = probe(transport).sample([frame()])
    assert len(got['edges']) <= 64 and got['truncated'] and not got['empty']
    ids = set(got['blocker_transaction_ids'])
    for e in got['edges']: ids.update([e['waiter_transaction_id'], e['blocker_transaction_id']])
    assert len(ids) <= 256


@pytest.mark.parametrize('status', [301,302,307,308])
def test_redirects_are_rejected_not_followed(transport, status):
    transport.status = status
    got, = probe(transport).sample([frame()])
    assert got['error_type'] == 'RedirectError' and not got['empty']
    assert len(transport.calls) == 1


def test_timeout_only_records_exception_type(transport):
    transport.error = socket.timeout
    got, = probe(transport).sample([frame()])
    assert got['error_type'] == 'TimeoutError' and not got['empty']
    assert 'secret' not in json.dumps(got)


def test_top_three_are_deduplicated_globally_and_not_redirected_by_proxy_env(transport, monkeypatch):
    monkeypatch.setenv('HTTP_PROXY','http://evil.example')
    maps=[{'node':NODE,'tablet_id':f'{i:032x}','relation_name':'table_'+str(i)} for i in range(4)]
    subject=probe(transport,maps)
    frames=[frame(m['tablet_id'][:15],count=i+1) for i,m in enumerate(maps)]
    # Distinct full IDs here intentionally share prefixes; they must be rejected.
    got=subject.sample(frames)
    assert not transport.calls and all(x['error_type']=='AmbiguousTabletPrefix' for x in got)
    maps=[{'node':NODE,'tablet_id':f'{i:x}'*32,'relation_name':'table_'+str(i)} for i in range(1,5)]
    transport.body=html(tablet=maps[-1]['tablet_id'])
    got=probe(transport,maps).sample([frame(m['tablet_id'][:15],count=i+1) for i,m in enumerate(maps)])
    assert len(transport.calls)==3 and len(got)==3
    assert transport.calls[0].host==NODE and transport.calls[0].path.endswith(maps[-1]['tablet_id'])


def test_mapping_is_node_scoped_and_invalid_frames_cannot_request_arbitrary_url(transport):
    subject=probe(transport)
    got=subject.sample([frame(node='http://evil'),frame(prefix='../../secret')])
    assert not transport.calls and all(not x['empty'] for x in got)
    assert 'evil' not in json.dumps(got) and 'secret' not in json.dumps(got)
    got=subject.sample([frame(node='172.16.191.4')])
    assert got[0]['error_type']=='UnknownTabletPrefix' and not transport.calls


def test_ambiguous_prefix_never_picks_first_tablet(transport):
    maps=MAP+[{'node':NODE,'tablet_id':TABLET[:15]+'a'*17,'relation_name':'other'}]
    got,=probe(transport,maps).sample([frame()])
    assert got['error_type']=='AmbiguousTabletPrefix' and got['tablet_id'] is None
    assert not transport.calls


def test_edges_from_wrong_tablet_are_never_attributed_to_requested_id(transport):
    transport.body = html(row(), tablet='a' * 32)
    got, = probe(transport).sample([frame()])
    assert got['error_type'] == 'UnknownStructure' and not got['edges']
    assert not got['blocker_transaction_ids']


def test_shutting_down_empty_tables_are_not_reported_healthy_empty(transport):
    transport.body = html().replace(b'<h2>Txn', b'Shutting down...<h2>Txn')
    got, = probe(transport).sample([frame()])
    assert got['error_type'] == 'WaitQueueShuttingDown' and not got['empty']


def test_malformed_frame_shapes_remain_visible_without_network(transport):
    got = probe(transport).sample([None, {'node': [], 'rows': [{}]}, {'node': NODE, 'rows': [None]}])
    assert got and all(x['error_type'] == 'InvalidFrame' for x in got)
    assert not transport.calls


def test_connection_deadline_is_updated_for_each_read(transport):
    class Clock:
        def __init__(self): self.now=0
        def __call__(self): self.now += .01; return self.now
    subject=p.WaitQueueProbe(MAP, connection_factory=transport, clock=Clock())
    got,=subject.sample([frame()])
    assert got['empty']
    times=transport.calls[0].sock.timeouts
    assert all(0 < x <= .15 for x in times)
    assert times == sorted(times, reverse=True)


def test_total_deadline_expiry_is_explicit(transport):
    class Clock:
        def __init__(self): self.now=0
        def __call__(self): self.now += .08; return self.now
    got,=p.WaitQueueProbe(MAP,connection_factory=transport,clock=Clock()).sample([frame()])
    assert got['error_type']=='TimeoutError' and not got['empty']


def test_unknown_high_volume_prefixes_do_not_starve_known_tablets(transport):
    maps=[{'node':NODE,'tablet_id':str(i)*32,'relation_name':'table_'+str(i)} for i in range(1,4)]
    frames=[frame('a'*15,count=100),frame('b'*15,count=100),frame('c'*15,count=100)]
    frames.extend(frame(m['tablet_id'][:15]) for m in maps)
    got=probe(transport,maps).sample(frames)
    assert len(transport.calls)==3
    assert {r['tablet_id'] for r in got}=={m['tablet_id'] for m in maps}


def test_unknown_structure_remains_visible_after_known_selection_when_room(transport):
    got=probe(transport).sample([frame('a'*15,count=100),frame()])
    assert got[0]['tablet_id']==TABLET
    assert got[1]['error_type']=='UnknownTabletPrefix'

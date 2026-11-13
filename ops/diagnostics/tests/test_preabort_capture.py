"""Pre-error wait snapshots must survive errors without unbounded probing."""
import importlib.util
import json
from pathlib import Path
from concurrent.futures import Future

SPEC = importlib.util.spec_from_file_location('preabort_capture', Path(__file__).parents[1] / 'yb_deadlock_capture.py')
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)
U1 = 'ba99f472-dded-4b39-87d7-dcba23b462bf'
U2 = '89aebf79-95ac-4621-b805-004cbd452bb4'


class ImmediatePool:
    def submit(self, fn, *args):
        future = Future()
        try: future.set_result(fn(*args))
        except Exception as exc: future.set_exception(exc)
        return future
    def shutdown(self, **kwargs): pass


class Database:
    def __init__(self, node, timeout=250): self.calls = []
    def query(self, sql, params=None):
        self.calls.append((sql, params))
        if sql == c.PREWAIT_SQL:
            return [{'tablet_prefix': 'a' * 15, 'pid': 12}]
        if sql == c.LOCK_SQL:
            return [{'transaction_id': params[0], 'granted': False, 'blocked_by': [U2],
                     'hash_cols': ['https://private.example']}]
        return []
    def close(self): pass


class Probe:
    def sample(self, frames):
        assert frames[0]['rows'][0]['tablet_prefix'] == 'a' * 15
        return [{'at': 1, 'node': 'node', 'kind': 'waitqueue', 'tablet_id': 'a' * 32,
                 'edges': [{'waiter_transaction_id': U1, 'blocker_transaction_id': U2}],
                 'empty': False, 'latency_ms': 2, 'error_type': None}]


def run(tmp_path, clock, probe=None):
    capture = c.Capture(['node'], c.Output(tmp_path), database_factory=Database,
                        clock=lambda: clock[0], wait_probe=probe)
    capture.pool.shutdown(); capture.pool = ImmediatePool()
    if capture.wait_pool:
        capture.wait_pool.shutdown(); capture.wait_pool = ImmediatePool()
    capture.running = True
    return capture


def test_disabled_does_not_add_queries_or_threads(tmp_path):
    now = [1.0]; capture = run(tmp_path, now)
    capture.tick()
    assert capture.wait_pool is None
    assert all(sql != c.PREWAIT_SQL for sql, _ in capture.databases['node'].calls)
    capture.output.close()


def test_live_graph_and_targeted_locks_are_saved_before_error(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    capture.tick(); now[0] = 2; capture.tick()
    assert any(frame['kind'] == 'waitqueue' for frame in capture.ring)
    locks = next(frame for frame in capture.ring if frame['kind'] == 'live_locks')
    assert locks['source'] == 'live_waitqueue'
    assert len(locks['transactions']) == 2
    assert 'hash_cols' not in json.dumps(locks) and 'https://' not in json.dumps(locks)
    capture.events.put({'pid': 12, 'transaction_ids': [U1, U2]})
    now[0] = 3; capture.tick(); now[0] = 14; capture.tick()
    incident, = list(tmp_path.glob('event-*.json'))
    payload = json.loads(incident.read_text())
    assert any(frame['kind'] == 'live_locks' for frame in payload['before'])
    assert capture.wait_state['live_lock_probes'] == 1
    capture.output.close()


def test_busy_probe_is_never_queued_again(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    capture.wait_future = Future()
    for value in range(1, 8):
        now[0] = value; capture.sample_waits(value)
    assert capture.wait_state['busy_skips'] == 7
    assert not capture.databases['node'].calls
    capture.output.close()


def test_delayed_pre_error_graph_is_not_lost_between_windows(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    future = Future()
    capture.wait_future = future
    capture.events.put({'pid': 12, 'transaction_ids': [U1, U2]})
    capture.tick()
    start = capture.pending[0]['started_at']
    now[0] = 12; capture.tick()
    assert not list(tmp_path.glob('event-*.json'))  # Wait for the in-flight pre-error snapshot.
    future.set_result([{'at': start - .1, 'node': 'node', 'kind': 'waitqueue',
                        'edges': [{'waiter_transaction_id': U1, 'blocker_transaction_id': U2}],
                        'empty': False, 'latency_ms': 3}])
    now[0] = 13; capture.tick()
    incident, = list(tmp_path.glob('event-*.json'))
    payload = json.loads(incident.read_text())
    graph = next(frame for frame in payload['before'] if frame['kind'] == 'waitqueue')
    assert graph['edges'][0]['waiter_transaction_id'] == U1
    assert all(frame['at'] >= start for frame in payload['after'])
    capture.output.close()


def test_pre_wait_error_backs_off_without_disabling_activity(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    original = capture.databases['node'].query
    def fail(sql, params=None):
        if sql == c.PREWAIT_SQL: raise RuntimeError('private SQL or URL')
        return original(sql, params)
    capture.databases['node'].query = fail
    capture.tick(); capture.status()
    status = json.loads((tmp_path/'status.json').read_text())
    assert status['wait_probe']['ash_errors'] == 1
    assert status['wait_probe']['backoff'] == 2
    assert status['nodes']['node']['samples'] == 1
    assert not status['healthy'] and 'private' not in json.dumps(status)
    capture.output.close()


def test_wait_query_uses_only_tserver_aux_and_clips_rows():
    assert "wait_event_component='TServer'" in c.PREWAIT_SQL
    assert "interval '2 seconds'" in c.PREWAIT_SQL
    assert 'LIMIT 65' in c.PREWAIT_SQL
    assert c.ACTIVITY_SQL.index("THEN 'offerer'") < c.ACTIVITY_SQL.index("THEN 'score_update'")
    assert 'md5(json_build_array(hash_cols,range_cols)::text)' in c.LOCK_SQL
    assert 'subtransaction_id' in c.LOCK_SQL


def test_future_failure_is_unhealthy_and_not_hidden_by_ash_success(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    future = Future(); future.set_exception(RuntimeError('private diagnostic detail'))
    capture.wait_future = future
    capture.sample_waits(now[0]); capture.status()
    assert capture.wait_state['last_http_error'] == 1
    assert capture.wait_state['http_backoff'] == 2
    now[0] = 3; capture.sample_waits(now[0]); capture.status()
    status = json.loads((tmp_path/'status.json').read_text())
    assert not status['healthy'] and 'private' not in json.dumps(status)
    capture.output.close()


def test_http_failures_keep_exponential_backoff_across_successful_ash_reads(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    class BadProbe:
        def sample(self, frames):
            return [{'at': 1, 'kind': 'waitqueue', 'edges': [], 'error_type': 'TimeoutError'}]
    capture.wait_probe = BadProbe()
    capture.sample_waits(1)
    for expected in [2, 4, 8, 16, 30]:
        now[0] += 1
        capture.sample_waits(now[0])
        assert capture.wait_state['http_backoff'] == expected
        now[0] = capture.next_wait
        capture.sample_waits(now[0])
        assert capture.wait_state['backoff'] == 1  # Successful ASH, independent HTTP backoff.
    capture.output.close()


def test_stop_after_one_waits_for_complete_saved_incident(tmp_path):
    now = [1.0]; capture = run(tmp_path, now, Probe())
    capture.stop_after_incidents = 1
    capture.events.put({'pid': 12, 'transaction_ids': [U1, U2]})
    capture.tick()
    assert not capture.stop.is_set() and not capture.metrics['saved_incidents']
    now[0] = 12; capture.tick()
    assert capture.stop.is_set() and capture.metrics['saved_incidents'] == 1
    assert len(list(tmp_path.glob('event-*.json'))) == 1
    capture.output.close()

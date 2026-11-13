"""Bounded, read-only YB deadlock evidence capture; never log raw SQL or keys."""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import threading
import time
import uuid

SESSION_OPTIONS = (
    "-c default_transaction_read_only=on -c statement_timeout={timeout}ms "
    "-c lock_timeout=100ms -c yb_enable_memory_tracking=off "
    "-c yb_enable_pg_locks={lock_info} -c yb_locks_max_transactions=4 "
    "-c yb_locks_txn_locks_per_tablet=5"
)
ACTIVITY_SQL = """
SELECT pid, query_id::text AS query_id, yb_backend_xid::text AS transaction_id,
 application_name, client_addr::text AS client_addr, client_port, state,
 wait_event_type, wait_event, extract(epoch FROM xact_start)::float8 AS xact_start,
 CASE WHEN query ILIKE '%INSERT INTO%url_state_current_%' AND query ILIKE '%last_fetch_ok%'
      THEN 'ingest_results'
      WHEN query ILIKE '%INSERT INTO%url_state_current_%' THEN 'ingest_links'
      WHEN query ILIKE '%FOR UPDATE%' AND query ILIKE '%inlink_count_approx%'
      THEN 'score_select'
      WHEN query ILIKE '%UPDATE%url_state_current_%' AND query ILIKE '%url_score_updated_at%'
       AND query NOT ILIKE '%num_fetch_ok_90d%' THEN 'score_update'
      WHEN query ILIKE '%num_scheduled_90d%' AND query ILIKE '%FOR UPDATE%' THEN 'offerer'
      WHEN query ILIKE '%DELETE FROM%url_state_current_%' THEN 'delete_current'
      ELSE 'other' END AS statement_kind,
 substring(query FROM 'url_state_current_[0-9]{3}') AS relation_name,
 split_part(query,chr(39),1) AS statement_prefix
FROM pg_stat_get_activity(NULL)
WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
 AND backend_type='client backend' AND COALESCE(application_name,'') <> 'wc-deadlock-capture'
 AND (state <> 'idle' OR xact_start IS NOT NULL)
ORDER BY pid LIMIT 257
"""
ASH_SQL = """
SELECT extract(epoch FROM sample_time)::float8 AS sample_time, pid, query_id::text AS query_id,
 root_request_id::text AS root_request_id, top_level_node_id::text AS node,
 wait_event_component, wait_event_class, wait_event, wait_event_type
FROM yb_active_session_history(now()-interval '12 seconds',now())
WHERE wait_event ILIKE '%conflict%' OR wait_event ILIKE '%waitqueue%'
 OR wait_event_type='Lock' OR wait_event_class ILIKE '%lock%'
ORDER BY sample_time DESC LIMIT 257
"""
LOCK_SQL = """
SELECT pid,node,tablet_id,relation::text AS relation_oid,
 transaction_id::text AS transaction_id,blocked_by::text[] AS blocked_by,granted,mode,waitstart::text
FROM yb_lock_status(NULL,%s::uuid) LIMIT 257
"""
ALLOWED = {
    'pid', 'query_id', 'transaction_id', 'application_name', 'client_addr', 'client_port',
    'state', 'wait_event_type', 'wait_event', 'xact_start', 'statement_kind', 'relation_name',
    'sample_time', 'root_request_id', 'node', 'wait_event_component', 'wait_event_class',
    'tablet_id', 'relation_oid', 'blocked_by', 'granted', 'mode', 'waitstart',
}
UUID_RE = re.compile(r'\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b')
HEADER = re.compile(r'^(\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:[.,]\d{1,9})?(?: [A-Za-z0-9:+-]{1,16})?)\s+\[(\d+)\]\s+(ERROR|DETAIL|STATEMENT|CONTEXT):\s*(.*)$')


def safe_value(value):
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (list, tuple)):
        return [safe_value(x) for x in value[:32]]
    if isinstance(value, (uuid.UUID, datetime)):
        value = str(value)
    if not isinstance(value, str):
        return '<redacted>'
    if any(x in value.lower() for x in ['://', 'password', 'secret', 'token=']) or "'" in value or len(value) > 128:
        return '<redacted>'
    return value


def safe_rows(rows):
    result = []
    for row in rows[:256]:
        projected = {k: safe_value(v) for k, v in row.items() if k in ALLOWED}
        if 'statement_prefix' in row:
            projected['statement_prefix'] = safe_statement_shape(row['statement_prefix'])
            projected['statement_prefix_partial'] = True
        result.append(projected)
    return result


def safe_statement_shape(text):
    """Accept only relevant normalized DML with no literal payloads."""
    if not isinstance(text, str) or len(text) > 16384:
        return None
    if not re.match(r'^\s*(SELECT|INSERT|UPDATE|WITH)\b', text, re.I):
        return None
    if not re.search(r'\burl_state_current_[0-9]{3}\b', text):
        return None
    if ("'" in text or '--' in text or '/*' in text
            or re.search(r'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$', text)
            or any(s in text.lower() for s in ['://', 'password', 'secret', 'token='])):
        return None
    # Server-side clipping stops before the first quoted value. Remove numeric
    # literals too; identifiers such as url_state_current_242 remain intact.
    return re.sub(r'\$[0-9]+|(?<![A-Za-z0-9_])(?:0x[0-9a-fA-F]+|(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)(?![A-Za-z0-9_])', '?', text)


class Database:
    def __init__(self, host, timeout=250, connect=None, uuid_info=True):
        self.host, self.timeout, self.connect, self.connection = host, timeout, connect, None
        self.uuid_info = uuid_info

    def query(self, sql, params=None):
        if self.connection is None or self.connection.closed:
            connect = self.connect
            if connect is None:
                import psycopg2
                connect = psycopg2.connect
            self.connection = connect(host=self.host, port=5433, dbname='crawlerdb', user='yugabyte',
                connect_timeout=2, application_name='wc-deadlock-capture',
                options=SESSION_OPTIONS.format(timeout=self.timeout, lock_info='on' if self.uuid_info else 'off'), keepalives=1,
                keepalives_idle=5, keepalives_interval=2, keepalives_count=2, tcp_user_timeout=5000)
            self.connection.autocommit = True
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql, params)
                names = [x[0] for x in cur.description]
                rows = [dict(zip(names, row)) for row in cur.fetchall()]
            return rows
        except Exception as exc:
            # A canceled autocommit metadata statement is already rolled back.
            # Keep the usable session and its warmed catalog caches; reconnecting
            # after every cold-query timeout can otherwise prevent any sample.
            idle_after_cancel = (
                getattr(exc, 'pgcode', None) == '57014'
                and not self.connection.closed
                and getattr(self.connection, 'get_transaction_status', lambda: None)() == 0
            )
            if not idle_after_cancel:
                self.close()
            raise

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class DeadlockParser:
    """Retain only time/PID/UUIDs; never retain source SQL or error text."""
    def __init__(self):
        self.pending = {}
        self.buffer = bytearray()
        self.discard = False
        self.truncated = 0

    def feed(self, chunk):
        events = []
        for part in chunk.splitlines(keepends=True):
            ended = part.endswith(b'\n')
            if not self.discard:
                remaining = 8192 - len(self.buffer)
                self.buffer.extend(part[:remaining])
                if len(part) > remaining:
                    self.discard = True
                    self.truncated += 1
            if ended:
                events.extend(self.line(self.buffer.decode('utf-8', errors='replace')))
                self.buffer.clear()
                self.discard = False
        return events

    def line(self, line):
        match = HEADER.match(line.rstrip('\r\n'))
        if not match:
            return []
        timestamp, pid, level, text = match.groups()
        events = []
        if level == 'ERROR' and 'deadlock detected' in text.lower():
            self.pending[pid] = {'timestamp': timestamp.strip(), 'pid': int(pid), 'transaction_ids': []}
            while len(self.pending) > 64:
                self.pending.pop(next(iter(self.pending)))
        event = self.pending.get(pid)
        if event is not None and level in {'ERROR', 'DETAIL'}:
            event['transaction_ids'] = list(dict.fromkeys(event['transaction_ids'] + UUID_RE.findall(text)))[:4]
            if len(event['transaction_ids']) >= 2:
                events.append(self.pending.pop(pid))
        elif event is not None and level == 'STATEMENT':
            events.append(self.pending.pop(pid))
        return events


class OutputLimit(Exception):
    pass


class Output:
    def __init__(self, root, budget=128 * 1024**2):
        self.root = Path(root)
        if self.root.is_symlink():
            raise ValueError('Output must not be a symlink')
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.root.chmod(0o700)
        self.owner_fd = os.open(self.root/'.capture.lock', os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.owner_fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            os.close(self.owner_fd)
            raise RuntimeError('Capture output already owned') from None
        self.budget = budget
        self.used = sum(p.stat().st_size for p in self.root.iterdir() if p.is_file())

    def close(self):
        if self.owner_fd is not None:
            os.close(self.owner_fd); self.owner_fd = None

    def write(self, name, value, status=False):
        data = (json.dumps(value, separators=(',', ':'), default=str) + '\n').encode()
        target = self.root / name
        if target.parent != self.root or target.is_symlink():
            raise ValueError('Unsafe output path')
        old_size = target.stat().st_size if target.exists() else 0
        limit = self.budget if status else self.budget - 16384
        if self.used - old_size + len(data) > limit:
            raise OutputLimit()
        temporary = self.root / ('.tmp-' + uuid.uuid4().hex)
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
        os.replace(temporary, target)
        self.used += len(data) - old_size


class Capture:
    def __init__(self, nodes, output, log_command=None, interval=1.0, duration=21600,
                 database_factory=Database, clock=time.monotonic):
        self.nodes, self.output, self.log_command = nodes, output, log_command
        self.interval, self.duration, self.factory, self.clock = interval, duration, database_factory, clock
        self.databases = {n: database_factory(n) for n in nodes}
        self.ring = deque(maxlen=512)
        self.events = queue.Queue(maxsize=32)
        self.pending = []
        self.state = {n: {'samples': 0, 'errors': 0, 'slow': 0, 'backoff': interval, 'next': 0,
                         'last_success': None, 'last_error': None, 'latency_ms': None, 'capped': 0, 'current_capped': False} for n in nodes}
        self.parser = DeadlockParser()
        self.stop = threading.Event()
        self.tail = None
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.metrics = {'incidents': 0, 'cooldown_drops': 0, 'queue_drops': 0,
                        'tail_eof': False, 'output_limit': False, 'ash_errors': 0, 'ash_capped': 0}
        self.running = False
        self.metadata_only_nodes = []
        self.last_incident = -float('inf')
        self.last_ash = self.last_status = -float('inf')

    def tail_logs(self):
        try:
            self.tail = subprocess.Popen(self.log_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            while not self.stop.is_set():
                chunk = os.read(self.tail.stdout.fileno(), 8192)
                if not chunk:
                    self.metrics['tail_eof'] = not self.stop.is_set()
                    return
                for event in self.parser.feed(chunk):
                    try:
                        self.events.put_nowait(event)
                    except queue.Full:
                        self.metrics['queue_drops'] += 1
        except Exception:
            self.metrics['tail_eof'] = True

    def locks(self, event):
        db = self.factory(self.nodes[0], timeout=1000)
        result = []
        try:
            for transaction in event['transaction_ids'][:4]:
                transaction = str(uuid.UUID(transaction))
                try:
                    rows = db.query(LOCK_SQL, (transaction,))
                    result.append({'transaction_id': transaction, 'rows': safe_rows(rows), 'capped': len(rows) > 256})
                except Exception as exc:
                    result.append({'transaction_id': transaction, 'error_type': type(exc).__name__})
        finally:
            db.close()
        return result

    def sample(self, node, now):
        state = self.state[node]
        if now < state['next']:
            return
        started = self.clock()
        try:
            rows = self.databases[node].query(ACTIVITY_SQL)
            elapsed = (self.clock() - started) * 1000
            state.update(samples=state['samples'] + 1, last_success=now, latency_ms=round(elapsed, 3))
            state['current_capped'] = len(rows) > 256
            state['capped'] += int(state['current_capped'])
            if elapsed > 100:
                state['slow'] += 1
                state['backoff'] = min(30, max(self.interval, state['backoff'] * 2))
            else:
                state['backoff'] = self.interval
            self.ring.append({'at': time.time(), 'node': node, 'kind': 'activity', 'rows': safe_rows(rows)})
        except Exception as exc:
            code = getattr(exc, 'pgcode', None)
            code = code if isinstance(code, str) and re.fullmatch(r'[0-9A-Z]{5}', code) else None
            state.update(errors=state['errors'] + 1, last_error=now, error_type=type(exc).__name__, sqlstate=code)
            state['backoff'] = min(30, max(2, state['backoff'] * 2))
        state['next'] = self.clock() + state['backoff']

    def statement_shapes(self, event, snapshots):
        # Preserve safe live prefixes before query IDs can leave statistics.
        # This path performs no pg_stat_statements text-file scan or DB query.
        transactions = set(event['transaction_ids'])
        found = {}
        for snapshot in reversed(snapshots):
            if snapshot.get('kind') != 'activity':
                continue
            for row in snapshot['rows']:
                value = str(row.get('query_id') or '')
                if row.get('transaction_id') not in transactions or not re.fullmatch(r'-?[0-9]{1,20}', value):
                    continue
                number = int(value)
                if not number or not -(2**63) <= number < 2**63:
                    continue
                key = (snapshot['node'], value)
                if key in found or sum(k[0] == key[0] for k in found) >= 8:
                    continue
                shape = safe_statement_shape(row.get('statement_prefix'))
                found[key] = {'node': key[0], 'query_id': value, 'statement_shape': shape,
                              'statement_shape_truncated': True, 'source': 'live_prefix',
                              'omitted_for_privacy': shape is None}
        return list(found.values())

    def tick(self):
        now = self.clock()
        for node in self.nodes:
            self.sample(node, now)
        if now - self.last_ash >= 10:
            self.last_ash = now
            for node, db in self.databases.items():
                if self.state[node]['last_error'] is not None and self.state[node]['last_error'] > (self.state[node]['last_success'] or -1):
                    continue
                try:
                    rows = db.query(ASH_SQL)
                    self.metrics['ash_capped'] += int(len(rows) > 256)
                    self.ring.append({'at': time.time(), 'node': node, 'kind': 'ash', 'rows': safe_rows(rows)})
                except Exception:
                    self.metrics['ash_errors'] += 1
        cutoff = time.time() - 60
        while self.ring and self.ring[0]['at'] < cutoff:
            self.ring.popleft()
        while not self.events.empty():
            event = self.events.get_nowait()
            if now - self.last_incident < 60:
                self.metrics['cooldown_drops'] += 1
                continue
            self.last_incident = now
            self.metrics['incidents'] += 1
            event_id = f'event-{time.time_ns()}'
            self.pending.append({'name': event_id, 'event': event, 'started_at': time.time(), 'before': list(self.ring),
                                 'end': now + 10, 'future': self.pool.submit(self.locks, event)})
        for event in list(self.pending):
            if now < event['end'] or not event['future'].done():
                continue
            payload = {k: v for k, v in event.items() if k not in {'future', 'end', 'name'}}
            payload.update(after=[s for s in self.ring if s['at'] >= event['started_at']],
                           post_window_complete=True, locks=event['future'].result())
            payload['normalized_statements'] = self.statement_shapes(
                event['event'], event['before'] + payload['after'])
            try:
                self.output.write(event['name'] + '.json', payload)
            except OutputLimit:
                self.metrics['output_limit'] = True
                self.output.write('budget-trigger.json', {'event': event['event'],
                    'reason': 'Evidence budget exhausted; full incident not persisted'}, status=True)
                self.stop.set()
            self.pending.remove(event)
        if now - self.last_status >= 30:
            self.last_status = now
            self.status()

    def status(self):
        healthy = all(s['last_success'] is not None and (s['last_error'] is None or s['last_success'] > s['last_error'])
                      for s in self.state.values()) and all(s['backoff'] == self.interval and not s['current_capped'] and self.clock()-s['last_success'] <= max(5, 3*self.interval) for s in self.state.values()) and not self.metrics['tail_eof'] and not self.metrics['output_limit'] and not self.metrics['queue_drops'] and self.running
        self.output.write('status.json', {'at': time.time(), 'healthy': healthy, 'nodes': self.state,
            'metrics': self.metrics, 'tail_line_truncations': self.parser.truncated,
            'running': self.running, 'metadata_only_nodes': self.metadata_only_nodes,
            'sample_ages_seconds': {n: round(self.clock()-s['last_success'], 2) if s['last_success'] is not None else None for n,s in self.state.items()}, 'ring_snapshots': len(self.ring), 'pending_events': len(self.pending), 'bytes': self.output.used}, status=True)

    def run(self):
        self.running = True
        if self.log_command:
            threading.Thread(target=self.tail_logs, daemon=True).start()
        deadline = self.clock() + self.duration
        try:
            while not self.stop.is_set() and self.clock() < deadline:
                self.tick()
                if self.metrics['tail_eof']:
                    self.stop.set()
                self.stop.wait(.1)
        except OutputLimit:
            self.metrics['output_limit'] = True
        finally:
            self.stop.set()
            self.running = False
            if self.tail is not None:
                if self.tail.stdin is not None:
                    self.tail.stdin.close()
                try:
                    self.tail.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.tail.terminate()
                    try:
                        self.tail.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        self.tail.kill()
            self.pool.shutdown(wait=True)
            for event in list(self.pending):
                payload = {k: v for k, v in event.items() if k not in {'future', 'end', 'name'}}
                payload.update(after=[s for s in self.ring if s['at'] >= event['started_at']],
                               post_window_complete=False, locks=event['future'].result())
                payload['normalized_statements'] = self.statement_shapes(
                    event['event'], event['before'] + payload['after'])
                try:
                    self.output.write(event['name']+'.json', payload)
                except OutputLimit:
                    self.metrics['output_limit'] = True
            self.pending.clear()
            for db in self.databases.values():
                db.close()
            try:
                self.status()
            finally:
                self.output.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes', nargs='+', default=['172.16.191.3', '172.16.191.2', '172.16.191.4'])
    parser.add_argument('--output', required=True)
    parser.add_argument('--log-command-json', help='Exact argv JSON; executed without a shell')
    parser.add_argument('--duration-seconds', type=float, default=21600)
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--max-bytes', type=int, default=128 * 1024**2)
    parser.add_argument('--metadata-only-node', action='append', default=[],
                        help='Skip activity UUID RPC here; PID/query ID/ASH remain available')
    args = parser.parse_args()
    if not 1 <= args.interval <= 60 or not 1 <= args.duration_seconds <= 86400:
        parser.error('Interval/duration outside bounded range')
    if not 32768 <= args.max_bytes <= 128 * 1024**2 or not 1 <= len(set(args.nodes)) <= 3:
        parser.error('Output/connection budget outside bounded range')
    metadata_only = set(args.metadata_only_node)
    if not metadata_only.issubset(args.nodes) or args.nodes[0] in metadata_only:
        parser.error('Metadata-only nodes must be sampled secondary nodes')
    command = json.loads(args.log_command_json) if args.log_command_json else None
    if command is not None and (not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command)):
        parser.error('log command must be a nonempty argv list')
    def factory(host, timeout=250):
        return Database(host, timeout=timeout, uuid_info=host not in metadata_only)

    capture = Capture(args.nodes, Output(args.output, args.max_bytes), command, args.interval,
                      args.duration_seconds, database_factory=factory)
    capture.metadata_only_nodes = sorted(metadata_only)
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, lambda *_: capture.stop.set())
    capture.run()


if __name__ == '__main__':
    main()

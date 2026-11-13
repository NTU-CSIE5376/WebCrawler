"""Read at most three allowlisted YB tablet wait queues; return no HTML or keys."""
from collections import Counter, defaultdict
import http.client
from html.parser import HTMLParser
import re
import time
import uuid

NODES = frozenset({'172.16.191.2', '172.16.191.3', '172.16.191.4'})
TABLET = re.compile(r'[0-9a-f]{32}\Z')
PREFIX = re.compile(r'[0-9a-f]{15}\Z')
RELATION = re.compile(r'[A-Za-z_][A-Za-z0-9_.]{0,159}\Z')
MAX_BYTES, MAX_EDGES, MAX_UUIDS = 65536, 64, 256
HEADERS = {
    'txn waiters:': {'waiterid', 'requestid', 'pgsessionreqversion', 'blockerid'},
    'single shard waiters:': {'index', 'blockerid'},
    'blockers:': {'blockerid', 'status'},
}


class ParseError(Exception): pass
class UnknownStructure(Exception): pass
class RedirectError(Exception): pass
class ResponseTooLarge(Exception): pass


class _Tables(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.heading = ''; self.heading_tag = None; self.heading_text = []
        self.table = None; self.row = None; self.cell = None
        self.tables = []; self.titles = []; self.broken = False; self.ignored = 0; self.shutting_down = False

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style'}: self.ignored += 1
        if self.ignored: return
        if tag in {'h1', 'h2'}:
            self.heading_tag = tag; self.heading_text = []
        elif tag == 'table':
            if self.table is not None: self.broken = True
            else: self.table = {'heading': self.heading, 'rows': []}
        elif tag == 'tr' and self.table is not None:
            if self.row is not None: self.broken = True
            self.row = []
        elif tag in {'th', 'td'} and self.row is not None:
            if self.cell is not None: self.broken = True
            self.cell = {'kind': tag, 'text': []}

    def handle_data(self, data):
        if self.ignored: return
        if data.strip() == 'Shutting down...': self.shutting_down = True
        if self.heading_tag: self.heading_text.append(data)
        if self.cell is not None: self.cell['text'].append(data)

    def handle_endtag(self, tag):
        if tag in {'script', 'style'} and self.ignored: self.ignored -= 1; return
        if self.ignored: return
        if tag in {'h1', 'h2'} and self.heading_tag == tag:
            text = ' '.join(''.join(self.heading_text).split())
            if tag == 'h1': self.titles.append(text)
            else: self.heading = text.lower()
            self.heading_tag = None
        elif tag in {'td', 'th'} and self.cell is not None:
            if self.cell['kind'] != tag: self.broken = True
            text = ''.join(self.cell['text']).strip().removeprefix('|').strip()
            self.row.append((self.cell['kind'], text)); self.cell = None
        elif tag == 'tr' and self.row is not None:
            if self.cell is not None: self.broken = True
            self.table['rows'].append(self.row); self.row = None; self.cell = None
        elif tag == 'table' and self.table is not None:
            if self.row is not None: self.broken = True
            self.tables.append(self.table); self.table = None


def _integer(value, optional=False):
    if optional and value.lower() in {'nullopt', 'none', 'null', 'undefined', ''}: return None
    if not re.fullmatch(r'-?[0-9]{1,20}', value): raise ParseError()
    number = int(value)
    if not -(2**63) <= number < 2**64: raise ParseError()
    return number


def _uuid(value):
    if not re.fullmatch(r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}', value): raise ParseError()
    return str(uuid.UUID(value))


def parse_waitqueue(body, tablet_id, truncated=False):
    parser = _Tables()
    result = {'edges': [], 'blocker_transaction_ids': [], 'single_shard_waiter_count': 0,
              'empty': False, 'truncated': truncated, 'error_type': None}
    try:
        parser.feed(body.decode('utf-8', errors='strict')); parser.close()
    except (ValueError, UnicodeError):
        result['error_type'] = 'ParseError'; return result
    if parser.table is not None and truncated: parser.tables.append(parser.table)
    recognized = {}; invalid = parser.broken; count = 0; ids = set(); blockers = set(); seen_edges = set()
    title_ok = parser.titles == ['Waiters for Tablet ' + tablet_id]
    if not title_ok:
        result['error_type'] = 'UnknownStructure'; return result
    for table in parser.tables:
        heading = table['heading']
        if heading not in HEADERS: continue
        rows = table['rows']
        if heading in recognized or not rows or any(kind != 'th' for kind, _ in rows[0]): invalid = True; continue
        headers = [text.lower() for _, text in rows[0]]
        if set(headers) != HEADERS[heading] or len(headers) != len(set(headers)): invalid = True; continue
        recognized[heading] = True
        for row in rows[1:]:
            count += 1
            if len(row) != len(headers) or any(kind != 'td' for kind, _ in row): invalid = True; continue
            fields = dict(zip(headers, [text for _, text in row]))
            try:
                blocker = _uuid(fields['blockerid'])
                waiter = _uuid(fields['waiterid']) if heading == 'txn waiters:' else None
                additional = {blocker} | ({waiter} if waiter else set())
                if len(ids | additional) > MAX_UUIDS: result['truncated'] = True; continue
                ids.update(additional); blockers.add(blocker)
                if heading == 'txn waiters:':
                    edge = {'waiter_transaction_id': waiter, 'blocker_transaction_id': blocker,
                            'request_id': _integer(fields['requestid']),
                            'pg_session_request_version': _integer(fields['pgsessionreqversion'], optional=True)}
                    key = tuple(edge.values())
                    if key not in seen_edges:
                        if len(result['edges']) >= MAX_EDGES: result['truncated'] = True
                        else: result['edges'].append(edge); seen_edges.add(key)
                elif heading == 'single shard waiters:':
                    _integer(fields['index']); result['single_shard_waiter_count'] += 1
            except ParseError:
                invalid = True
    result['blocker_transaction_ids'] = sorted(blockers)
    if not title_ok or set(recognized) != set(HEADERS): result['error_type'] = 'UnknownStructure'
    elif invalid or parser.table is not None or parser.row is not None: result['error_type'] = 'ParseError'
    if truncated: result['error_type'] = 'ResponseTooLarge'
    elif parser.shutting_down: result['error_type'] = 'WaitQueueShuttingDown'
    result['empty'] = bool(title_ok and set(recognized) == set(HEADERS) and not invalid
                           and not result['truncated'] and result['error_type'] is None and count == 0)
    return result


class WaitQueueProbe:
    def __init__(self, tablet_map, connection_factory=None, clock=time.monotonic):
        self.factory = connection_factory or http.client.HTTPConnection
        self.clock = clock; self.lookup = defaultdict(set)
        for entry in tablet_map:
            if not isinstance(entry, dict): raise ValueError('Invalid tablet mapping')
            node, tablet_id, relation = entry.get('node'), entry.get('tablet_id'), entry.get('relation_name')
            if not isinstance(node, str) or node not in NODES or not isinstance(tablet_id, str) or not TABLET.fullmatch(tablet_id.lower()):
                raise ValueError('Invalid allowlisted tablet mapping')
            if not isinstance(relation, str) or not RELATION.fullmatch(relation):
                raise ValueError('Unsafe relation name')
            tablet_id = tablet_id.lower()
            self.lookup[(node, tablet_id[:15])].add((tablet_id, relation))

    def _get(self, node, tablet_id):
        deadline = self.clock() + .15
        connection = self.factory(node, 9000, timeout=.15)
        response = None
        def remaining():
            value = deadline-self.clock()
            if value <= 0: raise TimeoutError()
            return value
        try:
            connection.connect(); sock = connection.sock; sock.settimeout(remaining())
            connection.request('GET', '/waitqueue?id='+tablet_id,
                               headers={'Connection': 'close', 'Accept': 'text/html'})
            sock.settimeout(remaining()); response = connection.getresponse()
            if 300 <= response.status < 400: raise RedirectError()
            if response.status != 200: raise ConnectionError()
            data = bytearray()
            while len(data) <= MAX_BYTES:
                sock.settimeout(remaining())
                chunk = response.read1(min(8192, MAX_BYTES+1-len(data)))
                if not chunk: break
                data.extend(chunk)
            remaining()
            return bytes(data[:MAX_BYTES]), len(data) > MAX_BYTES
        finally:
            if response is not None: response.close()
            connection.close()

    def sample(self, frames):
        candidates = Counter()
        for frame in frames:
            if not isinstance(frame, dict) or not isinstance(frame.get('rows', []), (list, tuple)):
                candidates[(None, None)] += 1; continue
            node = frame.get('node')
            for row in frame.get('rows', []):
                if not isinstance(row, dict):
                    candidates[(None, None)] += 1; continue
                prefix = row.get('tablet_prefix')
                if not isinstance(node, str) or node not in NODES: candidates[(None, None)] += 1
                elif not isinstance(prefix, str) or not PREFIX.fullmatch(prefix.lower()): candidates[(node, None)] += 1
                else: candidates[(node, prefix.lower())] += 1
        snapshots = []
        def priority(key):
            matches = self.lookup.get(key, set())
            rank = 0 if len(matches) == 1 else 1 if matches else 2
            if key[0] is None or key[1] is None: rank = 3
            return rank, -candidates[key], str(key[0]), str(key[1])
        ordered = sorted(candidates, key=priority)[:3]
        for node, prefix in ordered:
            started = self.clock()
            snapshot = {'node': node, 'tablet_id': None, 'relation_name': None, 'kind': 'waitqueue',
                        'at': time.time(), 'edges': [], 'empty': False, 'latency_ms': 0,
                        'truncated': False, 'error_type': None}
            matches = self.lookup.get((node, prefix), set())
            if node is None or prefix is None: snapshot['error_type'] = 'InvalidFrame'
            elif not matches: snapshot['error_type'] = 'UnknownTabletPrefix'
            elif len(matches) != 1: snapshot['error_type'] = 'AmbiguousTabletPrefix'
            else:
                tablet_id, relation = next(iter(matches)); snapshot.update(tablet_id=tablet_id, relation_name=relation)
                try:
                    body, truncated = self._get(node, tablet_id)
                    snapshot.update(parse_waitqueue(body, tablet_id, truncated))
                except Exception as error:
                    snapshot['error_type'] = type(error).__name__
            snapshot['latency_ms'] = round((self.clock()-started)*1000, 3)
            snapshots.append(snapshot)
        return snapshots

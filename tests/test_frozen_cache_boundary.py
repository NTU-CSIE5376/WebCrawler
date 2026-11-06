"""Offline cache rollover regressions at the real configured capacity."""
from unittest.mock import patch

from containers.scheduler_ingest.ingestor import db_ops
from containers.scheduler_ingest.ingestor.db_ops import IngestDB

NOW = 1000.0
MAX = db_ops.FROZEN_DOMAIN_CACHE_MAX
TTL = db_ops.FROZEN_DOMAIN_CACHE_TTL_SEC


class Cursor:
    def __init__(self, frozen=()):
        self.frozen = set(frozen)
        self.queries = []

    def execute(self, sql, params):
        self.queries.append(set(params[0]))
        self.rows = [(did,) for did in params[0] if did in self.frozen]

    def fetchall(self):
        return self.rows


def full_cache(size=MAX):
    db = IngestDB(Session=None)
    db._frozen_cache = {did: (did % 2 == 0, NOW) for did in range(size)}
    return db


def lookup(db, cur, ids):
    with patch.object(db_ops.time, 'monotonic', return_value=NOW):
        return db._frozen_domains(cur, set(ids))


def test_real_capacity_mixed_fresh_and_new_ids_remain_defined():
    db, cur = full_cache(), Cursor(frozen={0, MAX})
    assert lookup(db, cur, {0, 1, MAX}) == {0, MAX}
    assert len(cur.queries) == 1
    assert len(db._frozen_cache) <= MAX
    assert {0, 1, MAX} <= db._frozen_cache.keys()


def test_expired_existing_id_is_replacement_not_cache_growth():
    db, cur = full_cache(), Cursor(frozen=set())
    db._frozen_cache[2] = (True, NOW - TTL)
    assert lookup(db, cur, {0, 2}) == {0}
    assert cur.queries == [{2}]
    assert len(db._frozen_cache) == MAX
    assert db._frozen_cache[2] == (False, NOW)


def test_all_fresh_at_capacity_keeps_cache_and_avoids_database():
    db, cur = full_cache(), Cursor()
    assert lookup(db, cur, {0, 1}) == {0}
    assert cur.queries == []
    assert len(db._frozen_cache) == MAX


def test_growing_to_exact_capacity_does_not_evict_fresh_ids():
    db, cur = full_cache(MAX - 1), Cursor(frozen={MAX - 1})
    assert lookup(db, cur, {0, MAX - 1}) == {0, MAX - 1}
    assert cur.queries == [{MAX - 1}]
    assert len(db._frozen_cache) == MAX
    assert 2 in db._frozen_cache


def test_refreshing_expired_entries_does_not_evict_unrelated_keys():
    db, cur = full_cache(), Cursor(frozen={1})
    db._frozen_cache[0] = (True, NOW - TTL)
    db._frozen_cache[1] = (False, NOW - TTL)
    assert lookup(db, cur, {0, 1}) == {1}
    assert cur.queries == [{0, 1}]
    assert len(db._frozen_cache) == MAX


def test_empty_input_does_not_invalidate_full_cache():
    db, cur = full_cache(), Cursor()
    assert lookup(db, cur, set()) == set()
    assert cur.queries == []
    assert len(db._frozen_cache) == MAX

"""Check scripts/oneoff_low_score_pipeline.py (DELETE -> VACUUM -> REINDEX CONCURRENTLY with a
disk guard) against a real local PostgreSQL, in a throwaway schema. Free space is faked.

    LOW_SCORE_PIPELINE_LOCAL_DB_SMOKE_DSN=postgresql://postgres@127.0.0.1:5440/crawlsim \\
        python -m pytest tests/test_low_score_pipeline_local_db_smoke.py

Refuses any DSN that is not a local socket or localhost.
"""
from __future__ import annotations

import os
import pathlib
import tempfile
import time
import unittest
from types import SimpleNamespace

import psycopg2

from scripts import oneoff_delete_low_score as L
from scripts import oneoff_low_score_pipeline as P
from tests.test_low_score_delete_local_db_smoke import _assert_local

DSN_ENV = "LOW_SCORE_PIPELINE_LOCAL_DB_SMOKE_DSN"
T = 0.025
GOLDEN = ["https://g.com/golden-low"]
G = 2**30


class FakeDisk:
    """disk_usage() stand-in: returns free values from a script, then the last one forever."""

    def __init__(self, *free):
        self.free_values, self.calls = list(free), 0

    def __call__(self, _path):
        self.calls += 1
        v = self.free_values[0] if len(self.free_values) == 1 else self.free_values.pop(0)
        return SimpleNamespace(free=v)


@unittest.skipUnless(os.environ.get(DSN_ENV), f"set {DSN_ENV} to a local PostgreSQL")
class LowScorePipelineSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ[DSN_ENV]
        _assert_local(cls.dsn)
        cls.schema = f"low_score_pipeline_smoke_{os.getpid()}_{int(time.time())}"
        with psycopg2.connect(cls.dsn) as c, c.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {cls.schema}")
        cls.conn = psycopg2.connect(cls.dsn, options=f"-csearch_path={cls.schema}")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        with psycopg2.connect(cls.dsn) as c, c.cursor() as cur:
            cur.execute(f"DROP SCHEMA {cls.schema} CASCADE")

    def connect(self):
        conn = psycopg2.connect(self.dsn, options=f"-csearch_path={self.schema}")
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
        conn.commit()
        return conn

    def setUp(self):
        L._stop = False
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS url_state_current_000")
            cur.execute("""CREATE TABLE url_state_current_000 (
                url VARCHAR PRIMARY KEY, should_crawl BOOLEAN NOT NULL,
                source SMALLINT NOT NULL DEFAULT 0, url_score DOUBLE PRECISION, domain_id BIGINT,
                last_scheduled TIMESTAMPTZ, last_fetch_ok TIMESTAMPTZ) WITH (fillfactor = 80)""")
            cur.execute("CREATE INDEX idx_url_state_current_000_domain ON url_state_current_000 (domain_id)")
            cur.execute("CREATE INDEX idx_url_state_current_000_score ON url_state_current_000 "
                        "(domain_id, url_score DESC NULLS LAST) WHERE should_crawl")
            # 20k rows, two thirds of them at or below T: enough pages for the indexes to shrink on rebuild.
            # Random insert order, like prod, so the PK is not perfectly packed to begin with.
            cur.execute("""INSERT INTO url_state_current_000 (url, should_crawl, source, url_score, domain_id)
                           SELECT 'https://d' || (g % 97) || '.com/' || md5(g::text) || '/' || repeat('p', g % 40),
                                  TRUE, 0, (g % 3) * 0.02, g % 97
                           FROM generate_series(1, 20000) g ORDER BY random()""")
            cur.execute("""INSERT INTO url_state_current_000 (url, should_crawl, source, url_score, last_scheduled,
                                                              last_fetch_ok) VALUES
                           ('https://a.com/fetched', FALSE, 0, 0.001, now(), now()),
                           ('https://a.com/golden1', TRUE, 1, 0.001, NULL, NULL),
                           ('https://a.com/scheduled', TRUE, 0, 0.001, now(), NULL),
                           (%s, TRUE, 0, 0.001, NULL, NULL)""", (GOLDEN[0],))
        self.conn.commit()
        self.state_dir = pathlib.Path(tempfile.mkdtemp())

    def pipeline(self, disk, **kw):
        guard = P.DiskGuard("/data", min_free=60 * G, resume_free=80 * G, abort_free=30 * G,
                            poll=0.01, max_wait=kw.pop("max_wait", 60), usage=disk, sleep=lambda s: None)
        return P.Pipeline(self.connect, guard, self.state_dir, T, {0: GOLDEN}, batch_pages=16,
                          reindex_min_bytes=0, retry_sleep=0.1, watch_interval=0.05, **kw)

    def q(self, sql, *args):
        with self.conn.cursor() as cur:
            cur.execute(sql, args)
            r = cur.fetchall()
        self.conn.commit()
        return r

    def index_sizes(self):
        return dict(self.q("""SELECT i.relname, pg_relation_size(i.oid) FROM pg_index x
                              JOIN pg_class i ON i.oid = x.indexrelid
                              WHERE x.indrelid = 'url_state_current_000'::regclass"""))

    def test_full_pipeline_deletes_vacuums_and_shrinks_indexes(self):
        expected = self.q("SELECT count(*) FROM url_state_current_000 WHERE url_score <= %s AND should_crawl "
                          "AND source = 0 AND last_scheduled IS NULL AND last_fetch_ok IS NULL AND url <> %s",
                          T, GOLDEN[0])[0][0]
        before = self.index_sizes()
        self.pipeline(FakeDisk(150 * G)).run([0])

        st = L.ShardState(self.state_dir, 0, T)
        self.assertEqual(st.d["stage"], "done")
        self.assertEqual(st.d["deleted"], expected)
        self.assertEqual(sorted(st.d["reindexed"]), sorted(before))
        left = {r[0] for r in self.q("SELECT url FROM url_state_current_000")}
        for kept in ("https://a.com/fetched", "https://a.com/golden1", "https://a.com/scheduled", GOLDEN[0]):
            self.assertIn(kept, left)
        self.assertEqual(self.q("SELECT count(*) FROM url_state_current_000 WHERE url_score <= %s AND "
                                "should_crawl AND source = 0 AND last_scheduled IS NULL AND url <> %s",
                                T, GOLDEN[0])[0][0], 0)
        self.assertEqual(self.q("SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid = "
                                "'url_state_current_000'::regclass")[0][0], 0)
        after = self.index_sizes()
        self.assertEqual(set(after), set(before))                      # same names, nothing invalid left
        self.assertTrue(all(v for (v,) in self.q("SELECT indisvalid FROM pg_index WHERE indrelid = "
                                                 "'url_state_current_000'::regclass")))
        self.assertLess(sum(after.values()), sum(before.values()))
        steps = (self.state_dir / "report.csv").read_text()
        self.assertIn("delete", steps)
        self.assertIn("vacuum", steps)
        self.assertIn("reindex:url_state_current_000_pkey", steps)

    def test_low_disk_pauses_then_resumes(self):
        # 50G < min 60G -> pause; 70G is not yet resume (80G); 90G resumes
        disk = FakeDisk(50 * G, 70 * G, 90 * G, 150 * G)
        self.pipeline(disk).run([0])
        self.assertEqual(L.ShardState(self.state_dir, 0, T).d["stage"], "done")
        self.assertGreaterEqual(disk.calls, 4)

    def test_disk_that_never_recovers_stops_with_resumable_state(self):
        with self.assertRaises(SystemExit):
            self.pipeline(FakeDisk(50 * G), max_wait=0.05).run([0])
        st = L.ShardState(self.state_dir, 0, T)
        self.assertEqual(st.d["stage"], "delete")
        self.assertEqual(st.d["deleted"], 0)
        self.pipeline(FakeDisk(150 * G)).run([0])                     # resume
        self.assertEqual(L.ShardState(self.state_dir, 0, T).d["stage"], "done")

    def test_reindex_waits_for_room_for_the_index(self):
        # Enough for DELETE/VACUUM, but free - index size < min-free only when sizes are huge:
        # check the arithmetic directly on a guard.
        disk = FakeDisk(65 * G, 65 * G, 100 * G)
        guard = P.DiskGuard("/data", 60 * G, 80 * G, 30 * G, poll=0.01, usage=disk, sleep=lambda s: None)
        guard.wait(need=10 * G, what="REINDEX x")                      # 65-10 < 60 -> waits for 90
        self.assertEqual(disk.calls, 3)

    def test_watchdog_cancels_long_statement_on_low_disk(self):
        pipe = self.pipeline(FakeDisk(150 * G, 20 * G))
        conn = pipe._autocommit_conn()
        secs, cancelled = pipe._long(conn, "SELECT pg_sleep(30)")
        conn.close()
        self.assertTrue(cancelled)
        self.assertLess(secs, 10)

    def test_leftover_invalid_ccnew_index_is_dropped(self):
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            with self.assertRaises(psycopg2.errors.UniqueViolation):   # domain_id repeats: leaves an invalid index
                cur.execute("CREATE UNIQUE INDEX CONCURRENTLY url_state_current_000_pkey_ccnew "
                            "ON url_state_current_000 (domain_id)")
        self.conn.autocommit = False
        self.assertIn("url_state_current_000_pkey_ccnew", self.index_sizes())
        self.pipeline(FakeDisk(150 * G)).run([0])
        self.assertNotIn("url_state_current_000_pkey_ccnew", self.index_sizes())
        self.assertEqual(L.ShardState(self.state_dir, 0, T).d["stage"], "done")

    def test_stop_and_resume_and_other_threshold_refused(self):
        pipe = self.pipeline(FakeDisk(150 * G), max_rows=50)
        pipe.run([0])
        st = L.ShardState(self.state_dir, 0, T)
        self.assertEqual(st.d["stage"], "delete")
        self.assertFalse(st.d["done"])
        self.pipeline(FakeDisk(150 * G)).run([0])
        self.assertEqual(L.ShardState(self.state_dir, 0, T).d["stage"], "done")
        with self.assertRaises(SystemExit):
            L.ShardState(self.state_dir, 0, 0.03)


if __name__ == "__main__":
    unittest.main()

"""Check scripts/oneoff_delete_low_score.py's DELETE against a real local PostgreSQL.

Runs delete_shard on a throwaway schema: only never-scheduled, never-fetched
source=0 pending rows at or below the threshold that are not golden URLs may go;
a paused run resumes to the same result; resuming with another threshold is refused.

    LOW_SCORE_DELETE_LOCAL_DB_SMOKE_DSN=postgresql://postgres@/postgres?host=/path/to/socket \\
        python -m pytest tests/test_low_score_delete_local_db_smoke.py

Refuses any DSN that is not a local socket or localhost.
"""
from __future__ import annotations

import os
import pathlib
import tempfile
import time
import unittest
from urllib.parse import parse_qs, urlsplit

import psycopg2

from scripts import oneoff_delete_low_score as L

DSN_ENV = "LOW_SCORE_DELETE_LOCAL_DB_SMOKE_DSN"
T = 0.025
GOLDEN = ["https://g.com/golden-low", "https://g.com/golden-high"]


def _assert_local(dsn: str) -> None:
    parts = urlsplit(dsn)
    host = parts.hostname or parse_qs(parts.query).get("host", [""])[0]
    if not (host in ("", "localhost", "127.0.0.1", "::1") or host.startswith("/")):
        raise RuntimeError(f"refusing non-local smoke-test DSN host {host!r}")


@unittest.skipUnless(os.environ.get(DSN_ENV), f"set {DSN_ENV} to a local PostgreSQL")
class LowScoreDeleteSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ[DSN_ENV]
        _assert_local(cls.dsn)
        cls.schema = f"low_score_delete_smoke_{os.getpid()}_{int(time.time())}"
        with psycopg2.connect(cls.dsn) as c, c.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {cls.schema}")
        cls.conn = psycopg2.connect(cls.dsn, options=f"-csearch_path={cls.schema}")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        with psycopg2.connect(cls.dsn) as c, c.cursor() as cur:
            cur.execute(f"DROP SCHEMA {cls.schema} CASCADE")

    def setUp(self):
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS url_state_current_000")
            cur.execute("""CREATE TABLE url_state_current_000 (
                url VARCHAR PRIMARY KEY, should_crawl BOOLEAN NOT NULL,
                source SMALLINT NOT NULL DEFAULT 0, url_score DOUBLE PRECISION, pad TEXT,
                last_scheduled TIMESTAMPTZ, last_fetch_ok TIMESTAMPTZ)""")
            rows = [
                ("https://a.com/low", True, 0, 0.001),          # delete
                ("https://a.com/at-t", True, 0, T),              # delete (<= T)
                ("https://a.com/high", True, 0, 0.5),
                ("https://a.com/null", True, 0, None),
                ("https://a.com/fetched", False, 0, 0.001),
                ("https://a.com/golden1", True, 1, 0.001),       # injected golden, model score
                ("https://a.com/golden1-3", True, 1, 3.0),
                ("https://a.com/pageview", True, 2, 0.001),
                ("https://a.com/patrol", True, 3, 0.001),
                (GOLDEN[0], True, 0, 0.001),                     # organic golden
                (GOLDEN[1], True, 0, 0.9),
            ]
            # pending again after an earlier schedule / fetch: kept
            seen = [("https://a.com/scheduled", "now()", "NULL"), ("https://a.com/fetched-ok", "NULL", "now()")]
            cur.executemany("INSERT INTO url_state_current_000 VALUES (%s,%s,%s,%s, "
                            "(SELECT string_agg(md5(random()::text || n), '') FROM generate_series(1, 60) n))",
                            rows)
            for url, sched, ok in seen:
                cur.execute(f"INSERT INTO url_state_current_000 (url, should_crawl, source, url_score, "
                            f"last_scheduled, last_fetch_ok) VALUES (%s, TRUE, 0, 0.001, {sched}, {ok})", (url,))
            # incompressible ~1.9 KB pad (under the TOAST threshold) puts ~4 rows on a heap page,
            # so the filler spans many pages and batching crosses page boundaries
            cur.execute("INSERT INTO url_state_current_000 "
                        "SELECT 'https://f.com/' || g, TRUE, 0, (g % 100) / 1000.0, "
                        "(SELECT string_agg(md5(random()::text || g || n), '') FROM generate_series(1, 60) n) "
                        "FROM generate_series(1, 400) g")
        self.conn.commit()
        self.state_dir = pathlib.Path(tempfile.mkdtemp())

    def _urls(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT url FROM url_state_current_000")
            urls = {r[0] for r in cur.fetchall()}
        self.conn.commit()
        return urls

    def _run(self, budget=float("inf"), threshold=T):
        st = L.ShardState(self.state_dir, 0, threshold)
        L.delete_shard(self.conn, "url_state_current_000", threshold, GOLDEN, st,
                       batch_pages=8, budget=[budget])
        return st

    def _expected_deleted(self):
        filler = {f"https://f.com/{g}" for g in range(1, 401) if (g % 100) / 1000.0 <= T}
        return filler | {"https://a.com/low", "https://a.com/at-t"}

    def test_deletes_only_low_score_natural_non_golden(self):
        before = self._urls()
        st = self._run()
        after = self._urls()
        gone = before - after
        self.assertEqual(gone, self._expected_deleted())
        self.assertTrue(st.d["done"])
        self.assertEqual(st.d["deleted"], len(gone))
        for kept in ("https://a.com/golden1", "https://a.com/pageview", "https://a.com/patrol",
                     "https://a.com/fetched", "https://a.com/null", "https://a.com/scheduled",
                     "https://a.com/fetched-ok", GOLDEN[0]):
            self.assertIn(kept, after)

    def test_pause_and_resume_gives_same_result(self):
        before = self._urls()
        st = self._run(budget=5)
        self.assertFalse(st.d["done"])
        self.assertGreater(len(before - self._urls()), 0)
        st = self._run()
        self.assertTrue(st.d["done"])
        gone = before - self._urls()
        self.assertEqual(gone, self._expected_deleted())
        self.assertEqual(st.d["deleted"], len(gone))

    def test_resume_with_other_threshold_is_refused(self):
        self._run(budget=5)
        with self.assertRaises(SystemExit):
            L.ShardState(self.state_dir, 0, 0.03)


if __name__ == "__main__":
    unittest.main()

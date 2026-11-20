"""Opt-in PostgreSQL/Yugabyte tests in a private, disposable schema.

Set RANKER_CLAIM_TEST_DSN. No production tables are read or written.
"""
import os
import threading
import time
import unittest
import uuid
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig, GoldenDiscoveryRankerService,
)


@unittest.skipUnless(os.environ.get("RANKER_CLAIM_TEST_DSN"), "set RANKER_CLAIM_TEST_DSN")
class RankerClaimDatabaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ["RANKER_CLAIM_TEST_DSN"]
        cls.schema = "ranker_claim_test_" + uuid.uuid4().hex[:16]
        cls.admin = psycopg2.connect(cls.dsn)
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
            cls.addClassCleanup(cls._remove_schema)
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(cls.schema)))
            cur.execute("SET statement_timeout='30s'")
            cur.execute("""CREATE TABLE domain_state (
                domain_id BIGINT PRIMARY KEY, shard_id INT, domain_score DOUBLE PRECISION)""")
            cur.execute("""CREATE TABLE url_state_current_000 (
                url TEXT PRIMARY KEY, domain_id BIGINT, should_crawl BOOLEAN,
                first_seen TIMESTAMPTZ, url_score DOUBLE PRECISION,
                url_score_updated_at TIMESTAMPTZ, inlink_count_approx BIGINT,
                inlink_count_external BIGINT, anchor_text TEXT)""")
            # Correctness fixtures do not need a secondary index. Production
            # index use is checked separately with a non-executing EXPLAIN.
            cur.execute("SELECT to_regprocedure('yb_lock_status(oid,uuid)') IS NOT NULL")
            cls.yugabyte = cur.fetchone()[0]

    @classmethod
    def _remove_schema(cls):
        try:
            assert cls.schema.startswith("ranker_claim_test_")
            with cls.admin.cursor() as cur:
                cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))
        finally:
            cls.admin.close()

    def setUp(self):
        self.connections = []
        self.reset()

    def tearDown(self):
        for conn in self.connections:
            conn.rollback()
            conn.close()

    def reset(self, domains=4, per_domain=12):
        self.per_domain = per_domain
        now = datetime.now(timezone.utc)
        with self.admin.cursor() as cur:
            cur.execute("DELETE FROM url_state_current_000")
            cur.execute("DELETE FROM domain_state")
            execute_values(cur, "INSERT INTO domain_state VALUES %s", [(d, 0, float(domains-d+1)) for d in range(1, domains+1)])
            rows = [(self.url(d,i), d, True, now-timedelta(days=20), .2,
                     now-timedelta(days=10)+timedelta(seconds=i), i+1, i%2, f"anchor-{d}-{i}")
                    for d in range(1, domains+1) for i in range(per_domain)]
            execute_values(cur, "INSERT INTO url_state_current_000 VALUES %s", rows, page_size=1000)

    @staticmethod
    def url(domain, i):
        return f"https://claim-test.invalid/{domain}/{i:05d}"

    def connection(self):
        conn = psycopg2.connect(self.dsn)
        conn.set_session(isolation_level="READ COMMITTED", autocommit=True)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
            cur.execute("SET statement_timeout='30s'")
        conn.autocommit = False
        self.connections.append(conn)
        return conn

    def ranker(self, batch=3, **kw):
        return GoldenDiscoveryRankerService(GoldenDiscoveryRankerConfig(
            total_shards=1, num_workers=2, worker_id=0, batch_size=batch,
            scan_interval_sec=60, max_batches_per_shard=1,
            domain_priority_steering_enabled=True, **kw), Session=None, scorer=None)

    def claim(self, conn, batch=3, **kw):
        with conn.cursor() as cur:
            return self.ranker(batch, **kw)._claim_steering_batch(cur, 0)

    def legacy_claim(self, conn, batch):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.url, u.inlink_count_approx, u.inlink_count_external, u.anchor_text
                FROM domain_state d CROSS JOIN LATERAL (
                    SELECT url,inlink_count_approx,inlink_count_external,anchor_text,url_score_updated_at
                    FROM url_state_current_000 u
                    WHERE u.domain_id=d.domain_id AND u.should_crawl=TRUE
                      AND u.first_seen<NOW()-INTERVAL '3 days'
                      AND (u.url_score_updated_at IS NULL OR u.url_score_updated_at<NOW()-INTERVAL '5 days')
                      AND (url_score IS NULL OR url_score<=1.0)
                    ORDER BY u.url_score_updated_at ASC NULLS FIRST LIMIT %s
                    FOR UPDATE OF u SKIP LOCKED
                ) u WHERE d.shard_id=0 AND d.domain_score>0
                ORDER BY d.domain_score DESC,u.url_score_updated_at ASC NULLS FIRST LIMIT %s
            """, (batch,batch))
            return cur.fetchall()

    def explicit_lock_count(self, conn):
        if not self.yugabyte:
            return None
        with conn.cursor() as cur:
            cur.execute("SELECT yb_backend_xid FROM pg_stat_activity WHERE pid=pg_backend_pid()")
            txn = cur.fetchone()[0]
        with self.admin.cursor() as cur:
            cur.execute("SET yb_locks_txn_locks_per_tablet=20000")
            cur.execute("""SELECT count(DISTINCT(tablet_id,hash_cols,range_cols))
                FROM yb_lock_status('url_state_current_000'::regclass::oid,%s::uuid)
                WHERE locktype='row' AND is_explicit AND granted""", (txn,))
            return cur.fetchone()[0]

    def test_same_priority_and_no_locks_on_discarded_candidates(self):
        a, probe = self.connection(), self.connection()
        old = self.legacy_claim(a,3)
        with probe.cursor() as cur:
            with self.assertRaises(psycopg2.Error) as caught:
                cur.execute("SELECT url FROM url_state_current_000 WHERE url=%s FOR UPDATE NOWAIT", (self.url(2,0),))
        self.assertEqual(caught.exception.pgcode,"55P03")
        probe.rollback(); a.rollback()
        new = self.claim(a)
        self.assertEqual(old,new)
        self.assertEqual([r[0] for r in new],[self.url(1,i) for i in range(3)])
        with probe.cursor() as cur:
            cur.execute("SELECT url FROM url_state_current_000 WHERE url=%s FOR UPDATE NOWAIT", (self.url(2,0),))
            self.assertEqual(cur.fetchone()[0],self.url(2,0))

    def test_workers_return_disjoint_partial_batches_without_refilling(self):
        a,b = self.connection(),self.connection()
        first,second = self.claim(a,1),self.claim(b)
        self.assertEqual([r[0] for r in first],[self.url(1,0)])
        self.assertEqual([r[0] for r in second],[self.url(1,i) for i in range(1,3)])
        self.assertFalse({r[0] for r in first}&{r[0] for r in second})

    def test_fully_locked_candidates_return_empty_and_can_be_revisited(self):
        blocker,worker = self.connection(),self.connection()
        with blocker.cursor() as cur:
            cur.execute("SELECT url FROM url_state_current_000 WHERE url=ANY(%s) FOR UPDATE", ([self.url(1,i) for i in range(6)],))
        self.assertEqual(self.claim(worker),[])
        if self.yugabyte:
            self.assertEqual(self.explicit_lock_count(worker),0)
        blocker.rollback()
        self.assertEqual([r[0] for r in self.claim(worker)],[self.url(1,i) for i in range(3)])

    def test_age_ttl_pending_and_manual_priority_guards(self):
        with self.admin.cursor() as cur:
            cur.execute("UPDATE url_state_current_000 SET first_seen=NOW() WHERE url=%s",(self.url(1,0),))
            cur.execute("UPDATE url_state_current_000 SET url_score_updated_at=NOW() WHERE url=%s",(self.url(1,1),))
            cur.execute("UPDATE url_state_current_000 SET should_crawl=FALSE WHERE url=%s",(self.url(1,2),))
            cur.execute("UPDATE url_state_current_000 SET url_score=3.0 WHERE url=%s",(self.url(1,3),))
            cur.execute("UPDATE domain_state SET domain_score=0 WHERE domain_id=2")
            cur.execute("UPDATE domain_state SET shard_id=1 WHERE domain_id=3")
        rows=self.claim(self.connection(),100)
        expected={self.url(1,i) for i in range(4,12)}|{self.url(4,i) for i in range(12)}
        self.assertEqual({r[0] for r in rows},expected)

    def test_write_once_does_not_rescore_stale_rows(self):
        with self.admin.cursor() as cur:
            cur.execute("UPDATE url_state_current_000 SET url_score_updated_at=NULL WHERE url=%s",(self.url(1,4),))
        self.assertEqual([r[0] for r in self.claim(self.connection(),rescore_ttl_days=0)],[self.url(1,4)])

    def test_rechecks_changes_between_discovery_and_claim(self):
        owner=self
        for update in ("url_score=3.0","should_crawl=FALSE","url_score_updated_at=NOW()","first_seen=NOW()"):
            with self.subTest(update=update):
                self.reset()
                conn=self.connection()
                with conn.cursor() as cursor:
                    class RaceCursor:
                        changed=False
                        def execute(self,query,params):
                            if "FOR UPDATE OF u" in query and not self.changed:
                                with owner.admin.cursor() as cur:
                                    cur.execute("UPDATE url_state_current_000 SET "+update+" WHERE url=%s",(owner.url(1,0),))
                                self.changed=True
                            cursor.execute(query,params)
                        def fetchall(self): return cursor.fetchall()
                    rows=self.ranker()._claim_steering_batch(RaceCursor(),0)
                self.assertEqual([r[0] for r in rows],[self.url(1,i) for i in range(1,3)])
                conn.rollback()

    def test_changes_during_claim_recheck_eligibility_and_release_locks_on_rollback(self):
        owner=self
        conn=self.connection()
        errors=[]
        def update_after_snapshot():
            try:
                time.sleep(.05)
                with owner.admin.cursor() as cur:
                    cur.execute("UPDATE url_state_current_000 SET url_score=3.0 WHERE url=%s",(owner.url(1,0),))
            except Exception as exc:
                errors.append(exc)
        with conn.cursor() as cursor:
            class RaceCursor:
                changed=False
                def execute(self,query,params):
                    if "FOR UPDATE OF u" in query and not self.changed:
                        self.changed=True
                        thread=threading.Thread(target=update_after_snapshot)
                        # Delay the first claim after its statement snapshot.
                        # The scalar subquery runs once, before rows are locked.
                        query=query.replace("u.anchor_text\n","u.anchor_text || (SELECT pg_sleep(.3))::text\n",1)
                        thread.start()
                        try:
                            cursor.execute(query,params)
                        finally:
                            thread.join()
                    else:
                        cursor.execute(query,params)
                def fetchall(self): return cursor.fetchall()
            rows=self.ranker()._claim_steering_batch(RaceCursor(),0)
        self.assertEqual(errors,[])
        self.assertEqual([r[0] for r in rows],[self.url(1,i) for i in range(1,3)])
        probe=self.connection()
        if self.yugabyte:
            self.assertEqual(self.explicit_lock_count(conn),2)
            with probe.cursor() as cur:
                cur.execute("SELECT url FROM url_state_current_000 WHERE url=%s FOR UPDATE NOWAIT",(self.url(1,0),))
                self.assertEqual(cur.fetchone()[0],self.url(1,0))
        else:
            # PostgreSQL retains the lock on a row that matched its snapshot,
            # even if a concurrent update makes it fail the eligibility recheck.
            # https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE
            with probe.cursor() as cur:
                with self.assertRaises(psycopg2.errors.LockNotAvailable):
                    cur.execute("SELECT url FROM url_state_current_000 WHERE url=%s FOR UPDATE NOWAIT",(self.url(1,0),))
        probe.rollback();conn.rollback()
        # Both engines release all candidate locks when the transaction ends.
        with probe.cursor() as cur:
            cur.execute("SELECT url FROM url_state_current_000 WHERE url=ANY(%s) FOR UPDATE NOWAIT",([self.url(1,i) for i in range(3)],))
            self.assertEqual(len(cur.fetchall()),3)

    def test_full_scoring_transaction_updates_only_claimed_rows(self):
        class Scorer:
            def score_many_rows(self,rows):
                assert len(rows)==3 and rows[0]["anchor_text"]=="anchor-1-0"
                return [.4]*len(rows)
        dsn=self.dsn.replace("postgresql://","postgresql+psycopg2://",1)
        engine=create_engine(dsn,connect_args={"options":f"-c search_path={self.schema} -c statement_timeout=30000"})
        try:
            svc=self.ranker();svc.Session=sessionmaker(bind=engine);svc.scorer=Scorer()
            self.assertEqual(svc._score_batch(0),3)
            with self.admin.cursor() as cur:
                cur.execute("SELECT count(*) FROM url_state_current_000 WHERE url_score=.4")
                self.assertEqual(cur.fetchone()[0],3)
        finally:
            engine.dispose()

    def test_partial_scoring_batch_commits_and_releases_its_locks(self):
        owner=self
        scored=[]
        class Scorer:
            def score_many_rows(self,rows):
                scored.append([r["url"] for r in rows])
                return [.4]*len(rows)
        blocker=self.connection()
        with blocker.cursor() as cur:
            cur.execute("SELECT url FROM url_state_current_000 WHERE url=%s FOR UPDATE",(self.url(1,0),))
        engine=create_engine(self.dsn.replace("postgresql://","postgresql+psycopg2://",1),
            connect_args={"options":f"-c search_path={self.schema} -c statement_timeout=30000"})
        try:
            svc=self.ranker();svc.Session=sessionmaker(bind=engine);svc.scorer=Scorer()
            self.assertEqual(svc._score_batch(0),2)
            self.assertEqual(scored,[[owner.url(1,i) for i in (1,2)]])
            with self.admin.cursor() as cur:
                cur.execute("SELECT url FROM url_state_current_000 WHERE url_score=.4 ORDER BY url")
                self.assertEqual([r[0] for r in cur.fetchall()],scored[0])
            probe=self.connection()
            with probe.cursor() as cur:
                cur.execute("SELECT url FROM url_state_current_000 WHERE url=ANY(%s) FOR UPDATE NOWAIT",(scored[0],))
                self.assertEqual(len(cur.fetchall()),2)
            probe.rollback();blocker.rollback()
            self.assertEqual(svc._score_batch(0),3)
            self.assertEqual(scored[1],[owner.url(1,i) for i in (0,3,4)])
        finally:
            engine.dispose()

    def test_statement_timeout_rolls_back_and_releases_claims(self):
        class Scorer:
            def score_many(self,urls): return [.4]*len(urls)
        engine=create_engine(self.dsn.replace("postgresql://","postgresql+psycopg2://",1),
            connect_args={"options":f"-c search_path={self.schema} -c statement_timeout=30000"},
            pool_size=1,max_overflow=0)
        try:
            svc=self.ranker(statement_timeout_sec=1);svc.Session=sessionmaker(bind=engine);svc.scorer=Scorer()
            def stalled_update(cur,*args,**kwargs):
                cur.execute("SHOW statement_timeout")
                self.assertEqual(cur.fetchone()[0],"1s")
                cur.execute("SELECT pg_sleep(2)")
            with patch("containers.scheduler_control.scorer.service.execute_values",stalled_update):
                with self.assertRaises(psycopg2.errors.QueryCanceled):
                    svc._score_batch(0)
            with self.admin.cursor() as cur:
                cur.execute("SELECT count(*) FROM url_state_current_000 WHERE url_score=.4")
                self.assertEqual(cur.fetchone()[0],0)
            with self.connection().cursor() as cur:
                cur.execute("SELECT url FROM url_state_current_000 WHERE url=ANY(%s) FOR UPDATE NOWAIT",([self.url(1,i) for i in range(3)],))
                self.assertEqual(len(cur.fetchall()),3)
            # SET LOCAL must not leak into another transaction on the pooled connection.
            with engine.connect() as conn:
                self.assertEqual(conn.exec_driver_sql("SHOW statement_timeout").scalar(),"30s")
        finally:
            engine.dispose()

    def test_model_failure_rolls_back_and_releases_claims(self):
        class Scorer:
            def score_many(self, urls):
                raise RuntimeError("test model failure")
        dsn=self.dsn.replace("postgresql://","postgresql+psycopg2://",1)
        engine=create_engine(dsn,connect_args={"options":f"-c search_path={self.schema} -c statement_timeout=30000"})
        try:
            svc=self.ranker();svc.Session=sessionmaker(bind=engine);svc.scorer=Scorer()
            with self.assertRaisesRegex(RuntimeError,"test model failure"):
                svc._score_batch(0)
            with self.connection().cursor() as cur:
                cur.execute("SELECT url FROM url_state_current_000 WHERE url=ANY(%s) FOR UPDATE NOWAIT",([self.url(1,i) for i in range(3)],))
                self.assertEqual(len(cur.fetchall()),3)
        finally:
            engine.dispose()

    @unittest.skipUnless(os.environ.get("RANKER_CLAIM_TEST_LARGE"),"set RANKER_CLAIM_TEST_LARGE for 1000-row lock comparison")
    def test_thousand_row_lock_comparison(self):
        self.reset(domains=8,per_domain=2000)
        conn=self.connection()
        old=self.legacy_claim(conn,1000)
        old_locks=self.explicit_lock_count(conn);conn.rollback()
        new=self.claim(conn,1000)
        new_locks=self.explicit_lock_count(conn)
        self.assertEqual(old,new)
        self.assertEqual(len(new),1000)
        if self.yugabyte:
            self.assertEqual(old_locks,8000)
            self.assertEqual(new_locks,1000)

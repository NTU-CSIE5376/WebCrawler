"""When a fetch result is written under a different URL than the row that was
scheduled (HTTP redirect, or a row stored under a non-canonical string), the
scheduled row must also learn the outcome. Otherwise it keeps
last_fetch_ok = NULL forever and golden coverage counts it as never crawled."""
from __future__ import annotations

import os
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scrapy.http import HtmlResponse, Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure

# The spider imports its package as `crawler` (scrapy project root).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "containers" / "crawler"))
from crawler.spiders.spider import HtmlSpider  # noqa: E402
from containers.scheduler_ingest.ingestor import db_ops as ingest_db_ops
from containers.scheduler_ingest.ingestor.db_ops import STATUS_REQUESTED, IngestDB
from containers.scheduler_ingest.router.service import (
    RouterService,
    build_requested_record,
)
from libs.db.sharding.router import ShardRouter


# ---------------------------------------------------------------- spider ----

def _spider() -> HtmlSpider:
    spider = HtmlSpider(crawler_id=0)
    spider.link_extractor = LinkExtractor(canonicalize=True)
    spider._finish_owned_request = lambda **kwargs: None
    return spider


class SpiderRequestedUrlTest(unittest.TestCase):
    def test_parse_after_redirect_keeps_scheduled_url(self):
        req = Request(
            "https://www.example.com/a/",
            meta={"source_url": "http://example.com/a", "_track_domain_id": 42, "redirect_times": 1},
        )
        resp = HtmlResponse(
            url=req.url,
            body=b"<html><title>t</title></html>",
            headers={"Content-Type": "text/html"},
            request=req,
        )
        (item,) = list(_spider().parse(resp))
        self.assertEqual(item["url"], "https://www.example.com/a/")
        self.assertEqual(item["requested_url"], "http://example.com/a")
        self.assertEqual(item["requested_domain_id"], 42)
        self.assertTrue(item["is_redirect"])

    def test_non_html_result_also_carries_scheduled_url(self):
        req = Request("https://example.com/f.pdf", meta={"source_url": "https://example.com/f.pdf", "_track_domain_id": 1})
        resp = HtmlResponse(url=req.url, body=b"%PDF", headers={"Content-Type": "application/pdf"}, request=req)
        (item,) = list(_spider().parse(resp))
        self.assertEqual(item["fail_reason"], "NonHTML content-type")
        self.assertEqual(item["requested_url"], "https://example.com/f.pdf")

    def test_errback_carries_scheduled_url(self):
        req = Request(
            "https://example.com/b",
            meta={"source_url": "https://example.com/b?y=2&x=1", "_track_domain_id": 7},
        )
        resp = HtmlResponse(url=req.url, status=403, body=b"", request=req)
        failure = Failure(HttpError(resp))
        failure.request = req  # scrapy attaches the request before calling errback
        (item,) = list(_spider().errback(failure))
        self.assertEqual(item["fail_reason"], "HttpError 403")
        self.assertEqual(item["requested_url"], "https://example.com/b?y=2&x=1")
        self.assertEqual(item["requested_domain_id"], 7)


# ---------------------------------------------------------------- router ----

class _FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _FakeSess:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return _FakeResult(self.row)


def _router() -> RouterService:
    svc = RouterService.__new__(RouterService)  # skip engine / stats setup
    svc.sharder = ShardRouter(num_shards=256, shards_per_ingestor=16, domain_overrides={})
    return svc


class RouterRequestedRowTest(unittest.TestCase):
    def test_same_url_emits_nothing(self):
        sess = _FakeSess(SimpleNamespace(shard_id=5))
        rec = {"url": "https://example.com/", "requested_url": "https://example.com/", "requested_domain_id": 3}
        self.assertIsNone(_router()._requested_row(sess, rec, {}))
        self.assertEqual(sess.calls, [])

    def test_missing_requested_url_emits_nothing(self):
        # results written by a spider that predates requested_url
        self.assertIsNone(_router()._requested_row(_FakeSess(None), {"url": "https://example.com/"}, {}))

    def test_shard_comes_from_scheduled_rows_domain_id(self):
        sess = _FakeSess(SimpleNamespace(shard_id=5))
        cache: dict[int, int] = {}
        rec = {"url": "https://www.example.com/a/", "requested_url": "http://example.com/a", "requested_domain_id": 3}
        self.assertEqual(_router()._requested_row(sess, rec, cache), ("http://example.com/a", 5))
        self.assertIn("FROM domain_state WHERE domain_id", sess.calls[0][0])
        self.assertEqual(sess.calls[0][1], {"d": 3})
        # second record for the same domain uses the cache
        _router()._requested_row(sess, rec, cache)
        self.assertEqual(len(sess.calls), 1)

    def test_unknown_domain_id_falls_back_to_host_routing(self):
        router = _router()
        rec = {"url": "https://www.example.com/a/", "requested_url": "http://example.com/a", "requested_domain_id": 0}
        url, shard = router._requested_row(_FakeSess(None), rec, {})
        self.assertEqual(shard, router.sharder.domain_to_shard("example.com"))

    def test_record_shape(self):
        rec = {
            "url": "https://www.example.com/a/",
            "status": "ok",
            "fetched_at": "2026-09-01T00:00:00+00:00",
            "fail_reason": None,
            "is_redirect": True,
            "redirect_hop_count": 2,
            "requested_domain_id": 3,
        }
        out = build_requested_record(rec, "http://example.com/a", 5)
        self.assertEqual(out["status"], STATUS_REQUESTED)
        self.assertEqual(out["url"], "http://example.com/a")
        self.assertEqual(out["shard_id"], 5)
        self.assertEqual(out["result_status"], "ok")
        self.assertEqual(out["final_url"], "https://www.example.com/a/")


# -------------------------------------------------------------- ingestor ----

def _requested(url, result_status="ok", fetched_at="2026-09-01T00:00:00+00:00", fail_reason=None, shard_id=3):
    return {
        "status": STATUS_REQUESTED,
        "url": url,
        "shard_id": shard_id,
        "domain_id": 7,
        "fetched_at": fetched_at,
        "result_status": result_status,
        "fail_reason": fail_reason,
        "final_url": url + "/final",
        "is_redirect": True,
        "redirect_hop_count": 1,
    }


class IngestRequestedRowsTest(unittest.TestCase):
    def test_update_only_and_dedup_prefers_ok_then_latest(self):
        db = IngestDB(Session=None)
        captured = {}

        def fake_execute_values(cur, sql, rows, template=None, page_size=None, fetch=False):
            captured.update(sql=sql, rows=rows, template=template)

        items = [
            (0, _requested("u1", "fail", "2026-09-01T02:00:00+00:00", "HttpError 403")),
            (1, _requested("u1", "ok", "2026-09-01T01:00:00+00:00")),
            (2, _requested("u2", "fail", "2026-09-01T01:00:00+00:00", "TimeoutError")),
            (3, _requested("u2", "fail", "2026-09-01T03:00:00+00:00", "HttpError 404")),
        ]
        with patch.object(ingest_db_ops, "execute_values", fake_execute_values):
            db._bulk_requested(cur=None, shard_id=3, items=items)

        sql = captured["sql"]
        self.assertIn("UPDATE url_state_current_003", sql)
        self.assertNotIn("INSERT", sql)
        self.assertNotIn("num_fetch", sql)  # counters stay with the result row
        rows = {r[0]: r for r in captured["rows"]}
        self.assertEqual(set(rows), {"u1", "u2"})
        self.assertEqual(rows["u1"][1], datetime(2026, 9, 1, 1, tzinfo=timezone.utc))
        self.assertIsNone(rows["u1"][2])
        self.assertIsNone(rows["u2"][1])
        self.assertEqual(rows["u2"][2], "HttpError 404")
        self.assertIn("::timestamptz", captured["template"])

    def test_process_batch_routes_requested_records_and_returns_none(self):
        calls = {"results": [], "requested": []}

        class _Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _Sess:
            def connection(self):
                return SimpleNamespace(connection=SimpleNamespace(cursor=lambda: _Cur()))

        class _Begin:
            def __enter__(self):
                return _Sess()

            def __exit__(self, *a):
                return False

        db = IngestDB(Session=SimpleNamespace(begin=lambda: _Begin()))
        db._bulk_results = lambda cur, sid, items: calls["results"].append(items) or []
        db._bulk_requested = lambda cur, sid, items: calls["requested"].append((sid, items))

        recs = [
            {"status": "ok", "url": "https://www.example.com/a/", "shard_id": 1, "domain_id": 1},
            _requested("http://example.com/a", shard_id=9),
        ]
        out = db.process_batch(recs)
        self.assertEqual(out, [None, None])
        self.assertEqual(len(calls["results"]), 1)
        self.assertEqual(calls["requested"][0][0], 9)
        self.assertEqual(calls["requested"][0][1][0][1]["url"], "http://example.com/a")


@unittest.skipUnless(
    os.environ.get("INGESTOR_LOCAL_DB_SMOKE_DSN"),
    "set INGESTOR_LOCAL_DB_SMOKE_DSN to run the PostgreSQL smoke test",
)
class IngestRequestedRowsLocalDBTest(unittest.TestCase):
    def test_scheduled_row_gets_outcome_against_local_postgres(self):
        import psycopg2
        from sqlalchemy import create_engine, event
        from sqlalchemy.orm import sessionmaker

        dsn = os.environ["INGESTOR_LOCAL_DB_SMOKE_DSN"]
        schema = f"ingest_requested_smoke_{os.getpid()}_{int(time.time())}"
        admin = psycopg2.connect(dsn)
        admin.autocommit = True
        try:
            with admin.cursor() as cur:
                cur.execute(f"CREATE SCHEMA {schema}")
                cur.execute(
                    f"""
                    CREATE TABLE {schema}.url_state_current_003 (
                        url TEXT PRIMARY KEY,
                        domain_id BIGINT NOT NULL,
                        last_fetch_ok TIMESTAMPTZ,
                        num_fetch_ok_90d INTEGER DEFAULT 0,
                        num_fetch_fail_90d INTEGER DEFAULT 0,
                        last_fail_reason VARCHAR,
                        should_crawl BOOLEAN DEFAULT FALSE,
                        is_redirect BOOLEAN,
                        redirect_hop_count SMALLINT
                    )
                    """
                )
                cur.execute(
                    f"INSERT INTO {schema}.url_state_current_003 (url, domain_id, last_fail_reason) "
                    "VALUES ('http://example.com/a', 7, NULL), ('http://example.com/b', 7, 'old')"
                )

            engine = create_engine(
                "postgresql+psycopg2://" + dsn.split("://", 1)[1]
                if dsn.startswith("postgresql://") else dsn
            )

            @event.listens_for(engine, "connect")
            def _search_path(dbapi_conn, _):
                with dbapi_conn.cursor() as c:
                    c.execute(f"SET search_path TO {schema}")

            db = IngestDB(Session=sessionmaker(bind=engine))
            db.process_batch([
                _requested("http://example.com/a", "ok", "2026-09-01T01:00:00+00:00"),
                _requested("http://example.com/b", "fail", "2026-09-01T01:00:00+00:00", None),
                _requested("http://example.com/missing", "ok"),
            ])

            with admin.cursor() as cur:
                cur.execute(
                    f"SELECT url, last_fetch_ok, last_fail_reason, is_redirect, redirect_hop_count, "
                    f"num_fetch_ok_90d, num_fetch_fail_90d FROM {schema}.url_state_current_003 ORDER BY url"
                )
                a, b = cur.fetchall()
            engine.dispose()

            self.assertEqual(a[1], datetime(2026, 9, 1, 1, tzinfo=timezone.utc))
            self.assertIsNone(a[2])
            self.assertEqual((a[3], a[4]), (True, 1))
            self.assertEqual((a[5], a[6]), (0, 0))  # counters untouched
            self.assertIsNone(b[1])
            self.assertIsNone(b[2])  # fail with no reason clears the stale one, like the result path
        finally:
            with admin.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            admin.close()


if __name__ == "__main__":
    unittest.main()

"""End-to-end integration smoke test for the golden parent patrol.

Disabled by default; gated on the `GOLDEN_PARENT_PATROL_LOCAL_DB_SMOKE_DSN`
env var pointing at a Postgres the test can freely DROP / CREATE tables in.
Mirrors the convention of `test_golden_discovery_local_db_smoke.py`.

Builds a minimal schema, seeds parents and goldens across multiple shards
(critical: a parent's children land on shards keyed by the *child* URL's
domain, so the test must populate cross-shard data), and runs:

    migration -> backfill -> short loop x2 -> long loop

asserting each step's outcome. Together these exercise the bulk of the
patrol cron's IO surface against a real Postgres, including the
all-shards UNION ALL in count_new_urls_since.
"""
from __future__ import annotations

import os
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import tldextract

from libs.db.sharding.key import compute_shard, load_sharding_config
from libs.patrol.cadence import CadencePolicy
from libs.patrol.config import PatrolConfig
from libs.patrol.cron_loop import (
    SOURCE_GOLDEN_PARENT_PATROL,
    run_long_loop_evaluation,
    run_short_loop,
)


WEBCRAWLER = Path(__file__).resolve().parents[1]
INGEST_CONFIG = WEBCRAWLER / "containers/scheduler_ingest/config/ingest.yaml"

NUM_SHARDS = 256


def _extract_domain(url):
    e = tldextract.extract(url)
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


def _domain_to_shard(domain, overrides, splits):
    return compute_shard(domain, NUM_SHARDS, overrides, splits)


@unittest.skipUnless(
    os.environ.get("GOLDEN_PARENT_PATROL_LOCAL_DB_SMOKE_DSN"),
    "set GOLDEN_PARENT_PATROL_LOCAL_DB_SMOKE_DSN (e.g. "
    "'postgresql://crawler:crawler@127.0.0.1:5432/patrol_smoke') to run",
)
class GoldenParentPatrolLocalDBSmokeTest(unittest.TestCase):
    """Each phase asserts a specific behaviour:

    * Step 1-2 — migration creates the patrol_state table + 2 indexes.
    * Step 3   — seed: 2 productive parents, 1 control (zero golden), 4
                 goldens spread across two child shards.
    * Step 4   — backfill: aggregates correctly (espn=3, wiki=1), no
                 row for the zero-golden control.
    * Step 5   — backfill is idempotent on rerun.
    * Step 6   — short loop fresh path: enqueues into url_state_current
                 with source=3 / url_score=1.0 / should_crawl=TRUE.
    * Step 7   — short loop transitioned path: discovers 3 new child
                 URLs across multiple shards (proves the all-shards
                 UNION ALL works), promotes cadence trial -> slow.
    * Step 8   — long loop: backdates first_enrolled_at so the cycle
                 window covers the parents, evaluates the seeded batch,
                 verifies cycle_hits credit lifetime_count and reset
                 miss_streak.
    """

    def setUp(self):
        dsn = os.environ["GOLDEN_PARENT_PATROL_LOCAL_DB_SMOKE_DSN"]
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True

        # load_sharding_config takes a crawlerdb connection (not a path)
        # because split_subdomains lives in the shard_split DB table, not
        # in a yaml file.
        overrides, splits = load_sharding_config(INGEST_CONFIG, self.conn)
        self.overrides = overrides
        self.splits = splits

        # Suffix tables with our schema name to keep the test self-contained
        # if someone reruns it without dropping the DB. Each invocation gets
        # a fresh schema and SET search_path so the patrol scripts see
        # exactly the tables we created.
        self.schema = f"patrol_smoke_{os.getpid()}_{int(time.time())}"
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {self.schema}")
            cur.execute(f"SET search_path TO {self.schema}, public")
        self.conn.autocommit = False

    def tearDown(self):
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
        self.conn.close()

    def _build_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}, public")
            cur.execute(
                """
                CREATE TABLE domain_state (
                    domain_id BIGSERIAL PRIMARY KEY,
                    domain TEXT NOT NULL UNIQUE,
                    shard_id INTEGER NOT NULL,
                    domain_score DOUBLE PRECISION DEFAULT 0.0,
                    domain_fail_count INTEGER NOT NULL DEFAULT 0,
                    crawl_paused_until TIMESTAMPTZ
                )
                """
            )
            for sid in range(NUM_SHARDS):
                cur.execute(
                    f"""
                    CREATE TABLE url_state_current_{sid:03d} (
                        url TEXT PRIMARY KEY,
                        domain_id BIGINT,
                        domain_score DOUBLE PRECISION DEFAULT 0.0,
                        first_seen TIMESTAMPTZ DEFAULT NOW(),
                        last_scheduled TIMESTAMPTZ,
                        last_fetch_ok TIMESTAMPTZ,
                        should_crawl BOOLEAN DEFAULT TRUE,
                        url_score DOUBLE PRECISION DEFAULT 0.0,
                        url_score_updated_at TIMESTAMPTZ,
                        source SMALLINT NOT NULL DEFAULT 0,
                        discovered_from VARCHAR
                    )
                    """
                )
            cur.execute(
                """
                CREATE TABLE metric_batches (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "CREATE TABLE metric_queries ("
                "  id SERIAL PRIMARY KEY,"
                "  batch_id INTEGER REFERENCES metric_batches(id))"
            )
            cur.execute(
                "CREATE TABLE metric_url ("
                "  id SERIAL PRIMARY KEY,"
                "  query_id INTEGER REFERENCES metric_queries(id),"
                "  url TEXT)"
            )
        self.conn.commit()

    def test_end_to_end(self):
        self._build_schema()

        # Step 2: run patrol migration.
        from scripts import migrate_add_golden_parent_patrol as migration

        with self.conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}, public")
        migration.create_table(self.conn, dry_run=False)
        self.conn.autocommit = True
        migration.create_indexes(self.conn, dry_run=False)
        with self.conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}, public")
            cur.execute("SELECT to_regclass('golden_parent_patrol_state')")
            self.assertIsNotNone(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM pg_indexes WHERE schemaname = %s "
                "AND tablename = 'golden_parent_patrol_state'",
                (self.schema,),
            )
            self.assertEqual(cur.fetchone()[0], 3)  # pkey + due + domain

        # Step 3: seed parents and goldens.
        now = datetime.now(timezone.utc)
        espn_raw = "https://www.espn-test.com/scoreboard"
        wiki_raw = "http://wiki-test.com/article"
        # `blog-test.com` parent has 0 goldens — control: must NOT enroll.
        blog_raw = "https://blog-test.com/post"
        espn_shard = _domain_to_shard("espn-test.com", self.overrides, self.splits)
        wiki_shard = _domain_to_shard("wiki-test.com", self.overrides, self.splits)
        blog_shard = _domain_to_shard("blog-test.com", self.overrides, self.splits)

        with self.conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}, public")
            for d, sid in [
                ("espn-test.com", espn_shard),
                ("wiki-test.com", wiki_shard),
                ("blog-test.com", blog_shard),
            ]:
                cur.execute(
                    "INSERT INTO domain_state (domain, shard_id, domain_score) "
                    "VALUES (%s, %s, %s)",
                    (d, sid, 0.5),
                )

            for sid, url, dom in [
                (espn_shard, espn_raw, "espn-test.com"),
                (wiki_shard, wiki_raw, "wiki-test.com"),
                (blog_shard, blog_raw, "blog-test.com"),
            ]:
                cur.execute(
                    f"INSERT INTO url_state_current_{sid:03d} "
                    "(url, domain_id, domain_score, last_fetch_ok, should_crawl, source)"
                    " VALUES (%s, (SELECT domain_id FROM domain_state WHERE domain = %s), "
                    "0.5, %s, FALSE, 0)",
                    (url, dom, now - timedelta(days=15)),
                )

            # 3 espn-discovered goldens (children land on whatever shard
            # the child's domain hashes to; that's the cross-shard case
            # the cron's all-shards UNION ALL exists for).
            for i, child in enumerate(
                [
                    "https://nba.com/game1",
                    "https://nba.com/game2",
                    "https://nba.com/news1",
                ]
            ):
                child_dom = _extract_domain(child)
                child_shard = _domain_to_shard(child_dom, self.overrides, self.splits)
                cur.execute(
                    "INSERT INTO domain_state (domain, shard_id) VALUES (%s, %s) "
                    "ON CONFLICT (domain) DO NOTHING",
                    (child_dom, child_shard),
                )
                cur.execute(
                    f"INSERT INTO url_state_current_{child_shard:03d} "
                    "(url, domain_id, source, discovered_from, first_seen) "
                    "VALUES (%s, (SELECT domain_id FROM domain_state WHERE domain = %s),"
                    " 1, %s, %s)",
                    (child, child_dom, espn_raw, now - timedelta(days=10 + i)),
                )

            # 1 wiki-discovered golden, also on nba.com (different
            # parent, same child domain — exercises shared-shard).
            wiki_child = "https://nba.com/wiki-discovered"
            wch_dom = _extract_domain(wiki_child)
            wch_shard = _domain_to_shard(wch_dom, self.overrides, self.splits)
            cur.execute(
                f"INSERT INTO url_state_current_{wch_shard:03d} "
                "(url, domain_id, source, discovered_from, first_seen) "
                "VALUES (%s, (SELECT domain_id FROM domain_state WHERE domain = %s),"
                " 1, %s, %s)",
                (wiki_child, wch_dom, wiki_raw, now - timedelta(days=12)),
            )
        self.conn.commit()

        # Step 4: live backfill.
        from scripts.backfill_golden_parent_patrol import (
            aggregate_by_parent_key,
            fetch_max_metric_batch_id,
            scan_shard_for_live_parents,
            upsert,
        )

        last_eval = fetch_max_metric_batch_id(self.conn)
        self.assertEqual(last_eval, 0, "no batches yet, bootstrap should be 0")

        all_rows = []
        for sid in range(NUM_SHARDS):
            all_rows.extend(scan_shard_for_live_parents(self.conn, sid))

        aggs = aggregate_by_parent_key(all_rows, self.overrides, self.splits)
        self.assertEqual(len(aggs), 2, f"expected 2 parents, got {list(aggs)}")

        upserted = upsert(self.conn, aggs, last_eval, dry_run=False)
        self.conn.commit()

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT parent_key, source_type, lifetime_golden_child_count, "
                "status, cadence_bucket FROM golden_parent_patrol_state "
                "ORDER BY parent_key"
            )
            rows = {r[0]: r for r in cur.fetchall()}

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows["https://espn-test.com/scoreboard"][2], 3)
        self.assertEqual(rows["https://wiki-test.com/article"][2], 1)
        self.assertNotIn("https://blog-test.com/post", rows, "blog parent had 0 goldens; must not enroll")
        self.assertTrue(all(r[1] == "live_observed" for r in rows.values()))

        # Step 5: rerun is idempotent.
        upsert(self.conn, aggs, last_eval, dry_run=False)
        self.conn.commit()
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM golden_parent_patrol_state")
            self.assertEqual(cur.fetchone()[0], 2)

        # Step 6: short loop, fresh path.
        cfg = PatrolConfig(
            cadence_policy=CadencePolicy(
                intervals_sec={
                    "medium": 21600,
                    "slow": 86400,
                    "trial": 172800,
                    "cold": 604800,
                },
                promote_new_url_threshold=2,
                demote_no_new_url_threshold=2,
                retire_consecutive_miss_batches=2,
                demote_on_consecutive_miss_batches=1,
            ),
            patrol_priority=1.0,
            grace_period_seconds=900,
            cron_batch_size=10,
            cycle_days=14,
        )
        counts = run_short_loop(crawler_conn=self.conn, config=cfg)
        self.assertEqual(counts["due"], 2)
        self.assertEqual(counts["fresh"], 2)

        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT url, url_score, source, should_crawl "
                f"FROM url_state_current_{espn_shard:03d} WHERE url = %s",
                (espn_raw,),
            )
            url, score, source, scrawl = cur.fetchone()
        self.assertEqual(score, 1.0)
        self.assertEqual(source, SOURCE_GOLDEN_PARENT_PATROL)
        self.assertTrue(scrawl)

        # Step 7: cross-shard transition — espn fetched + new outlinks
        # written to a child shard != espn's shard. The cron's
        # count_new_urls_since UNION ALL must find them.
        with self.conn.cursor() as cur:
            cur.execute(
                f"UPDATE url_state_current_{espn_shard:03d} "
                "SET last_fetch_ok = NOW() + INTERVAL '1 second', should_crawl = FALSE "
                "WHERE url = %s",
                (espn_raw,),
            )
            for u in [
                "https://nba.com/new1",
                "https://nba.com/new2",
                "https://nba.com/new3",
            ]:
                child_dom = _extract_domain(u)
                child_shard = _domain_to_shard(child_dom, self.overrides, self.splits)
                cur.execute(
                    f"INSERT INTO url_state_current_{child_shard:03d} "
                    "(url, domain_id, source, discovered_from, first_seen) "
                    "VALUES (%s, (SELECT domain_id FROM domain_state WHERE domain = %s),"
                    " 0, %s, NOW() + INTERVAL '1 second') ON CONFLICT (url) DO NOTHING",
                    (u, child_dom, espn_raw),
                )
            cur.execute(
                "UPDATE golden_parent_patrol_state SET next_patrol_at = NOW() "
                "WHERE parent_key = %s",
                ("https://espn-test.com/scoreboard",),
            )
        self.conn.commit()

        run_short_loop(crawler_conn=self.conn, config=cfg)

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT cadence_bucket, last_seen_new_url_count, consecutive_no_new_url "
                "FROM golden_parent_patrol_state "
                "WHERE parent_key = %s",
                ("https://espn-test.com/scoreboard",),
            )
            bucket, new_count, consec_zero = cur.fetchone()
        # promote_threshold=2, espn saw 3 new URLs -> promote trial -> slow.
        self.assertEqual(bucket, "slow")
        self.assertEqual(new_count, 3)
        self.assertEqual(consec_zero, 0)

        # Step 8: long loop. Backdate first_enrolled_at and publish a
        # batch; verify both parents are credited with their cycle hits.
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE golden_parent_patrol_state "
                "SET first_enrolled_at = NOW() - INTERVAL '30 days'"
            )
            cur.execute(
                "INSERT INTO metric_batches (created_at) VALUES (%s) RETURNING id",
                (now - timedelta(days=5),),
            )
            batch_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO metric_queries (batch_id) VALUES (%s) RETURNING id",
                (batch_id,),
            )
            q_id = cur.fetchone()[0]
            for child in [
                "https://nba.com/game1",
                "https://nba.com/game2",
                "https://nba.com/news1",
                "https://nba.com/wiki-discovered",
            ]:
                cur.execute(
                    "INSERT INTO metric_url (query_id, url) VALUES (%s, %s)",
                    (q_id, child),
                )
        self.conn.commit()

        ll = run_long_loop_evaluation(
            metric_conn=self.conn,
            crawler_conn=self.conn,
            config=cfg,
            extract_domain=_extract_domain,
            domain_to_shard=_domain_to_shard,
            overrides=self.overrides,
            split_subdomains=self.splits,
        )
        self.assertEqual(ll["batches"], 1)
        self.assertEqual(ll["hits"], 2)
        self.assertEqual(ll["misses"], 0)

        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT parent_key, lifetime_golden_child_count, "
                "consecutive_miss_batches, last_eval_batch_id "
                "FROM golden_parent_patrol_state ORDER BY parent_key"
            )
            after = {r[0]: r for r in cur.fetchall()}

        # 3 backfill + 3 cycle = 6 for espn; 1 backfill + 1 cycle = 2 for wiki.
        self.assertEqual(after["https://espn-test.com/scoreboard"][1], 6)
        self.assertEqual(after["https://wiki-test.com/article"][1], 2)
        self.assertEqual(after["https://espn-test.com/scoreboard"][2], 0)
        self.assertEqual(after["https://wiki-test.com/article"][2], 0)
        self.assertEqual(after["https://espn-test.com/scoreboard"][3], batch_id)


if __name__ == "__main__":
    unittest.main()

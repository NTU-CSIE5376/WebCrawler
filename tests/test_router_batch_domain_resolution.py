"""Router used to resolve each outlink's domain with its own synchronous DB
round trip (N outlinks -> N queries), which became the pipeline's throughput
ceiling once per-folder volume grew. It now resolves every domain a record
touches (its own + all its outlinks') in one batched call, and writes all of
a record's output in one file append instead of one append per outlink.
These tests guard both: batch_ensure_and_get must not regress to one query
per domain, and _build_link_record must build the same record shape the old
per-link code did, from an already-resolved domain map (no DB/IO of its own).
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from containers.scheduler_ingest.router.domain_resolver import DomainResolver
from containers.scheduler_ingest.router.service import RouterService
from libs.db.sharding.router import ShardRouter


def _router() -> RouterService:
    """A RouterService with no DB engine, just enough to exercise the pure
    sharding/record-building path (_build_link_record never touches self.engine
    or self.Session)."""
    svc = RouterService.__new__(RouterService)
    svc.cfg = MagicMock(router_id=0)
    svc.sharder = ShardRouter(
        num_shards=256, shards_per_ingestor=16, domain_overrides={},
    )
    return svc


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def fetchall(self):
        return list(self._rows)


class FakeSession:
    """Records every execute() call so tests can assert call *count*, not
    just final values -- the whole point of batching is fewer round trips."""

    def __init__(self, domain_rows: dict):
        self.domain_rows = dict(domain_rows)  # domain -> (domain_id, score)
        self.execute_calls: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        self.execute_calls.append((sql, params))
        if "SELECT domain, domain_id" in sql:
            wanted = params["domains"]
            rows = [
                type("Row", (), {"domain": d, "domain_id": self.domain_rows[d][0],
                                  "domain_score": self.domain_rows[d][1]})()
                for d in wanted if d in self.domain_rows
            ]
            return FakeResult(rows)
        if sql.startswith("INSERT INTO domain_state"):
            # Simulate the DB assigning ids to newly inserted domains.
            next_id = 1000 + len(self.domain_rows)
            for key, value in params.items():
                if key.startswith("domain_"):
                    if value not in self.domain_rows:
                        self.domain_rows[value] = (next_id, 0.0)
                        next_id += 1
            return FakeResult([])
        raise AssertionError(f"unexpected statement: {sql}")


class BatchEnsureAndGetTest(unittest.TestCase):
    def test_one_select_regardless_of_outlink_count(self):
        """20 domains, all pre-existing: must be a single SELECT, not 20."""
        domains = {f"d{i}.example.com": (i, 0.5) for i in range(20)}
        sess = FakeSession(domains)
        resolver = DomainResolver(sess)

        needed = {k: 7 for k in domains}
        resolved = resolver.batch_ensure_and_get(needed)

        select_calls = [c for c in sess.execute_calls if "SELECT domain, domain_id" in c[0]]
        self.assertEqual(len(select_calls), 1)
        self.assertEqual(len(resolved), 20)
        for domain, (expected_id, expected_score) in domains.items():
            self.assertEqual(resolved[domain], (expected_id, expected_score))

    def test_missing_domains_get_inserted_then_resolved(self):
        sess = FakeSession({"known.example.com": (1, 0.9)})
        resolver = DomainResolver(sess)

        resolved = resolver.batch_ensure_and_get({
            "known.example.com": 7,
            "brand-new.example.com": 7,
        })

        self.assertEqual(resolved["known.example.com"], (1, 0.9))
        self.assertIn("brand-new.example.com", resolved)
        insert_calls = [c for c in sess.execute_calls if c[0].startswith("INSERT")]
        self.assertEqual(len(insert_calls), 1)

    def test_cache_hit_skips_db_entirely(self):
        sess = FakeSession({"cached.example.com": (5, 0.1)})
        resolver = DomainResolver(sess, cache={"cached.example.com": (5, 0.1)})

        resolved = resolver.batch_ensure_and_get({"cached.example.com": 7})

        self.assertEqual(resolved["cached.example.com"], (5, 0.1))
        self.assertEqual(sess.execute_calls, [])


class BuildLinkRecordTest(unittest.TestCase):
    def test_builds_record_and_summary_from_resolved_map(self):
        svc = _router()
        host = "sub.example.com"
        domain = svc.sharder.domain_key(host)
        shard_id = svc.sharder.domain_to_shard(host)
        resolved = {domain: (42, 0.77)}

        result = svc._build_link_record(
            resolved,
            {"url": "https://sub.example.com/page", "anchor": "click here"},
            host,
            "https://src.example.com/",
            "src.example.com",
            0.55,
        )

        self.assertIsNotNone(result)
        ingestor_id, out, summary = result
        self.assertEqual(ingestor_id, svc.sharder.shard_to_ingestor(shard_id))
        self.assertEqual(out["url"], "https://sub.example.com/page")
        self.assertEqual(out["domain_id"], 42)
        self.assertEqual(out["domain_score"], 0.77)
        self.assertEqual(out["parent_page_score"], 0.55)
        self.assertEqual(summary, {
            "url": "https://sub.example.com/page", "domain_id": 42, "anchor": "click here",
        })

    def test_missing_url_returns_none_without_touching_resolved_map(self):
        svc = _router()
        result = svc._build_link_record(
            {}, {"anchor": "no url here"}, "example.com", None, "src.com", None,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

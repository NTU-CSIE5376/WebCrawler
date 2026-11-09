"""Batch injection preserves URL identity, crawl state and transaction ownership."""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from golden_inject_batch import ConcurrentDeleteError, GoldenURL, RoutingMismatchError, inject_batch


class Cursor:
    def __init__(self):
        self.domains = {}
        self.rows = {}
        self.history = []
        self.fetched = []
        self.calls = []
        self.concurrent = {}
        self.delete_before_update = set()

    def execute(self, sql, params):
        self.calls.append((sql, params))
        if sql.startswith("SELECT domain,"):
            self.fetched = [(d, *self.domains[d]) for d in params[0] if d in self.domains]
        elif sql.startswith("SELECT url FROM"):
            shard = int(re.search(r"current_(\d+)", sql)[1])
            self.fetched = [(u,) for u in params[0] if (shard, u) in self.rows]
        elif sql.startswith("UPDATE"):
            shard = int(re.search(r"current_(\d+)", sql)[1])
            for key in self.delete_before_update:
                self.rows.pop(key, None)
            self.fetched = []
            for url in params[2]:
                if (shard, url) in self.rows:
                    self.rows[shard, url].update(source=params[0], score=params[1])
                    self.fetched.append((url,))
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return self.fetched

    def values(self, cur, sql, values, *, page_size, template=None, fetch=False):
        assert cur is self
        self.calls.append((sql, values))
        if sql.startswith("INSERT INTO domain_state"):
            for domain, sid in values:
                self.domains.setdefault(domain, (len(self.domains) + 1, sid, 0.8))
        elif "INSERT INTO url_state_current" in sql:
            shard = int(re.search(r"current_(\d+)", sql)[1])
            self.rows.update(self.concurrent)
            new = []
            assert "should_crawl" in sql and "TRUE" in template
            for url, domain_id, score, source, priority in values:
                if (shard, url) not in self.rows:
                    self.rows[shard, url] = dict(domain_id=domain_id, domain_score=score,
                                               source=source, score=priority, should_crawl=True)
                    new.append((url,))
            assert fetch
            return new
        elif "INSERT INTO url_state_history" in sql:
            shard = int(re.search(r"history_(\d+)", sql)[1])
            self.history.extend((shard, *row) for row in values)
        else:
            raise AssertionError(sql)


def record(url="https://example.com/p", variant=None, *, domain="example.com", shard=3, n=1):
    return GoldenURL(n, url, variant, domain, shard)


class GoldenBatchTest(unittest.TestCase):
    def run_batch(self, cur, records, cache=None):
        with patch("golden_inject_batch.execute_values", side_effect=cur.values):
            return inject_batch(cur, records, cache or {})

    def test_new_row_is_pending_and_has_one_history(self):
        cur = Cursor()
        cache = {}
        with patch("golden_inject_batch.execute_values", side_effect=cur.values):
            result = inject_batch(cur, [record()], cache)
        self.assertEqual(result.counts, {"injected": 1, "marked": 0, "marked_variant": 0})
        self.assertTrue(cur.rows[3, record().url]["should_crawl"])
        self.assertEqual(len(cur.history), 1)
        self.assertEqual(cache, {}, "The cache must not be changed before commit")
        self.assertEqual(result.domains, {"example.com": (1, 3, 0.8)})

    def test_existing_row_keeps_its_crawl_state_and_domain_score(self):
        cur = Cursor()
        cur.rows[3, record().url] = dict(should_crawl=False, source=0, score=0.2, domain_score=0.3)
        result = self.run_batch(cur, [record()])
        self.assertEqual(result.counts["marked"], 1)
        self.assertEqual(cur.rows[3, record().url],
                         dict(should_crawl=False, source=1, score=3.0, domain_score=0.3))
        self.assertEqual(cur.history, [])

    def test_only_raw_variant_is_marked_without_inserting_clean(self):
        cur = Cursor()
        raw = record().url + "?srsltid=abc"
        cur.rows[3, raw] = dict(should_crawl=False, source=0)
        result = self.run_batch(cur, [record(variant=raw)])
        self.assertEqual(result.counts["marked_variant"], 1)
        self.assertNotIn((3, record().url), cur.rows)
        self.assertEqual(cur.history, [])

    def test_clean_is_preferred_when_both_forms_exist(self):
        cur = Cursor()
        raw = record().url + "?srsltid=abc"
        cur.rows[3, raw] = dict(source=0)
        cur.rows[3, record().url] = dict(source=0)
        result = self.run_batch(cur, [record(variant=raw)])
        self.assertEqual(result.counts["marked"], 1)
        self.assertEqual(cur.rows[3, raw]["source"], 0)
        self.assertEqual(cur.rows[3, record().url]["source"], 1)

    def test_duplicate_inputs_make_one_insert_and_one_history(self):
        cur = Cursor()
        result = self.run_batch(cur, [record(), record(variant=record().url + "?srsltid=x"), record()])
        self.assertEqual(result.counts, {"injected": 1, "marked": 2, "marked_variant": 0})
        self.assertEqual(len(cur.rows), 1)
        self.assertEqual(len(cur.history), 1)

    def test_returned_insert_set_covers_more_than_execute_values_default_page(self):
        cur = Cursor()
        result = self.run_batch(cur, [record(f"https://example.com/{n}", n=n) for n in range(150)])
        self.assertEqual(result.counts["injected"], 150)
        self.assertEqual(len(cur.history), 150)

    def test_several_variants_preserve_sequential_target_selection(self):
        cur = Cursor()
        raw = record().url + "?srsltid=old"
        cur.rows[3, raw] = dict(source=0)
        result = self.run_batch(cur, [record(variant=raw),
                                     record(variant=record().url + "?srsltid=new"), record(variant=raw)])
        self.assertEqual(result.counts, {"injected": 1, "marked": 1, "marked_variant": 1})
        self.assertEqual(len(cur.history), 1)

    def test_concurrent_insert_is_marked_without_false_history(self):
        cur = Cursor()
        cur.concurrent[3, record().url] = dict(should_crawl=False, source=0)
        result = self.run_batch(cur, [record()])
        self.assertEqual(result.counts["injected"], 0)
        self.assertEqual(result.counts["marked"], 1)
        self.assertEqual(cur.history, [])
        self.assertFalse(cur.rows[3, record().url]["should_crawl"])

    def test_multiple_shards_and_reused_committed_cache(self):
        cur = Cursor()
        cache = {"example.com": (7, 3, 0.9)}
        result = self.run_batch(cur, [record(), record("https://other.com/", domain="other.com", shard=4)], cache)
        self.assertEqual(result.counts["injected"], 2)
        self.assertEqual(set(s for s, *_ in cur.history), {3, 4})
        self.assertNotIn("example.com", result.domains)
        self.assertEqual(cache, {"example.com": (7, 3, 0.9)})

    def test_gc_delete_before_mark_raises_instead_of_counting_success(self):
        cur = Cursor()
        cur.rows[3, record().url] = dict(should_crawl=False, source=0)
        cur.delete_before_update.add((3, record().url))
        cache = {}
        with patch("golden_inject_batch.execute_values", side_effect=cur.values):
            with self.assertRaisesRegex(ConcurrentDeleteError, "1 missing URL"):
                inject_batch(cur, [record()], cache)
        self.assertEqual(cache, {})
        self.assertEqual(cur.history, [])

    def test_partial_update_returning_also_retries_the_entire_batch(self):
        cur = Cursor()
        other = "https://example.com/other"
        for url in (record().url, other):
            cur.rows[3, url] = dict(should_crawl=False, source=0)
        cur.delete_before_update.add((3, other))
        with self.assertRaises(ConcurrentDeleteError):
            self.run_batch(cur, [record(), record(other)])
        self.assertEqual(cur.history, [])

    def test_existing_domain_shard_mismatch_fails_before_url_writes(self):
        cur = Cursor()
        cur.domains["example.com"] = (1, 99, 0.0)
        with self.assertRaisesRegex(RoutingMismatchError, "stored on shard 99"):
            self.run_batch(cur, [record()])
        self.assertEqual(cur.rows, {})

    def test_invalid_or_conflicting_shards_are_rejected(self):
        for records in ([record(shard=-1)], [record(shard=256)], [record(shard=True)],
                        [record(), record(shard=4)]):
            with self.subTest(records=records):
                cur = Cursor()
                with self.assertRaises(ValueError):
                    self.run_batch(cur, records)
                self.assertEqual(cur.calls, [])

    def test_empty_batch_does_no_database_work(self):
        cur = Cursor()
        result = self.run_batch(cur, [])
        self.assertEqual(sum(result.counts.values()), 0)
        self.assertEqual(cur.calls, [])


if __name__ == "__main__":
    unittest.main()

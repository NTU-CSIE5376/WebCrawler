"""Candidate discovery must not lock discarded rows or strand concurrent workers."""
from collections import deque

from containers.scheduler_control.scorer import service


def ranker(batch_size=3, **kwargs):
    return service.GoldenDiscoveryRankerService(
        service.GoldenDiscoveryRankerConfig(
            total_shards=256, num_workers=16, worker_id=0,
            batch_size=batch_size, scan_interval_sec=60,
            max_batches_per_shard=1, domain_priority_steering_enabled=True,
            **kwargs,
        ), Session=None, scorer=None,
    )


class Cursor:
    def __init__(self, responses):
        self.responses = deque(responses)
        self.executed = []

    def execute(self, query, params):
        self.executed.append((query, params))

    def fetchall(self):
        return self.responses.popleft()


def test_refills_past_locked_candidates_with_bounded_claims():
    a, b, c = ("a", 1, 0, "anchor-a"), ("b", 2, 1, "anchor-b"), ("c", 3, 0, None)
    cur = Cursor([
        [("locked",), ("a",), ("b",)], [a, b],
        [("c",)], [c],
    ])
    assert ranker()._claim_steering_batch(cur, 7) == [a, b, c]
    assert len(cur.executed) == 4
    first, claim, refill, second_claim = cur.executed
    assert "FOR UPDATE" not in first[0]
    assert "FOR UPDATE OF u SKIP LOCKED" in claim[0]
    assert claim[1][-1] == 3
    assert set(refill[1][2]) == {"locked", "a", "b"}
    assert refill[1][-1] == second_claim[1][-1] == 1


def test_refills_even_when_whole_first_page_is_locked():
    rows = [("c", 1, 0, None), ("d", 1, 0, None)]
    cur = Cursor([[("a",), ("b",)], [], [("c",), ("d",)], rows])
    assert ranker(2)._claim_steering_batch(cur, 0) == rows
    assert set(cur.executed[2][1][2]) == {"a", "b"}


def test_empty_and_exhausted_pools_return_only_claimed_rows():
    cur = Cursor([[]])
    assert ranker()._claim_steering_batch(cur, 0) == []
    cur = Cursor([[("a",)], [("a", 2, 0, None)], []])
    assert ranker()._claim_steering_batch(cur, 0) == [("a", 2, 0, None)]


def test_rechecks_all_eligibility_rules_when_claiming():
    cur = Cursor([[("changed",)], [], []])
    assert ranker(min_age_days=7, rescore_ttl_days=30)._claim_steering_batch(cur, 5) == []
    query, params = cur.executed[1]
    for rule in ("u.should_crawl = TRUE", "u.first_seen < NOW()", "u.url_score_updated_at IS NULL", "u.url_score_updated_at < NOW()", "url_score <= 1.0", "d.domain_score > 0", "d.shard_id = %s"):
        assert rule in query
    assert params == (7, 30, ["changed"], 5, 3)


def test_write_once_rule_applies_to_claim_as_well_as_discovery():
    cur = Cursor([[("a",)], [], []])
    ranker(rescore_ttl_days=0)._claim_steering_batch(cur, 0)
    query, params = cur.executed[1]
    assert "u.url_score_updated_at IS NULL" in query
    assert "OR u.url_score_updated_at <" not in query
    assert params == (3, ["a"], 0, 3)


def test_contention_scan_memory_is_bounded(monkeypatch, caplog):
    monkeypatch.setattr(service, "CLAIM_SCAN_BUFFER_BYTES", 1)
    cur = Cursor([[("locked",)], []])
    assert ranker()._claim_steering_batch(cur, 0) == []
    assert len(cur.executed) == 2
    assert "claim_scan_budget" in caplog.text

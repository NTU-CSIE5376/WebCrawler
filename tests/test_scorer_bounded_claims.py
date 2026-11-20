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


def test_partial_claim_returns_without_searching_again_while_holding_locks():
    a, b = ("a", 1, 0, "anchor-a"), ("b", 2, 1, "anchor-b")
    cur = Cursor([[("locked",), ("a",), ("b",)], [a, b]])
    assert ranker()._claim_steering_batch(cur, 7) == [a, b]
    assert len(cur.executed) == 2
    discovery, claim = cur.executed
    assert "FOR UPDATE" not in discovery[0]
    assert "FOR UPDATE OF u SKIP LOCKED" in claim[0]
    assert discovery[1] == (3, 5, 3, 7, 3)
    assert claim[1] == (3, 5, ["locked", "a", "b"], 7, 3)


def test_fully_locked_page_returns_empty_without_refilling():
    cur = Cursor([[("a",), ("b",)], []])
    assert ranker(2)._claim_steering_batch(cur, 0) == []
    assert len(cur.executed) == 2


def test_empty_candidate_pool_does_not_issue_a_claim():
    cur = Cursor([[]])
    assert ranker()._claim_steering_batch(cur, 0) == []
    assert len(cur.executed) == 1


def test_rechecks_all_eligibility_rules_when_claiming():
    cur = Cursor([[("changed",)], []])
    assert ranker(min_age_days=7, rescore_ttl_days=30)._claim_steering_batch(cur, 5) == []
    query, params = cur.executed[1]
    for rule in ("u.should_crawl = TRUE", "u.first_seen < NOW()", "u.url_score_updated_at IS NULL", "u.url_score_updated_at < NOW()", "url_score <= 1.0", "d.domain_score > 0", "d.shard_id = %s"):
        assert rule in query
    assert params == (7, 30, ["changed"], 5, 3)


def test_write_once_rule_applies_to_claim_as_well_as_discovery():
    cur = Cursor([[("a",)], []])
    ranker(rescore_ttl_days=0)._claim_steering_batch(cur, 0)
    query, params = cur.executed[1]
    assert "u.url_score_updated_at IS NULL" in query
    assert "OR u.url_score_updated_at <" not in query
    assert params == (3, ["a"], 0, 3)


def test_sql_timeout_skips_shard_instead_of_restarting_the_same_pass(monkeypatch, caplog):
    svc = ranker()
    monkeypatch.setattr(svc, "_shard_ids", lambda: [7, 8])
    visited = []

    def score(shard):
        visited.append(shard)
        if shard == 7:
            raise service.QueryCanceled("statement timeout")
        return 2

    monkeypatch.setattr(svc, "_score_batch", score)
    assert svc.run_once() == {"scored_urls": 2, "scored_batches": 1}
    assert visited == [7, 8]
    assert "statement_timeout" in caplog.text


def test_empty_claim_advances_to_next_shard(monkeypatch):
    svc = ranker()
    monkeypatch.setattr(svc, "_shard_ids", lambda: [7, 8])
    visited = []

    def score(shard):
        visited.append(shard)
        return 0 if shard == 7 else 2

    monkeypatch.setattr(svc, "_score_batch", score)
    assert svc.run_once() == {"scored_urls": 2, "scored_batches": 1}
    assert visited == [7, 8]


def test_statement_timeout_cannot_be_disabled():
    import pytest

    for value in (0, -1):
        with pytest.raises(ValueError, match="statement_timeout_sec"):
            ranker(statement_timeout_sec=value)

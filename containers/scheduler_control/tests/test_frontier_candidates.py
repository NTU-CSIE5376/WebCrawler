from containers.scheduler_control.offerer.selection.base import CandidateDomain


def test_demand_candidates_drop_empty_frontiers_with_bounded_shard_probes():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from containers.scheduler_control.offerer.selection.golden_discovery_ranker_v1_strategy import GoldenDiscoveryRankerV1Strategy
    session = MagicMock()
    session.__enter__.return_value = session
    session.execute.side_effect = [
        SimpleNamespace(fetchall=lambda: [SimpleNamespace(domain_id=1, shard_id=0, domain_score=2),
                                          SimpleNamespace(domain_id=2, shard_id=0, domain_score=1),
                                          SimpleNamespace(domain_id=3, shard_id=1, domain_score=1)]),
        SimpleNamespace(fetchall=lambda: [SimpleNamespace(domain_id=2, available_urls=3, url_score=1.5)]),
        SimpleNamespace(fetchall=lambda: []),
    ]
    selector = GoldenDiscoveryRankerV1Strategy(Session=lambda: session)
    assert selector.demand_candidates({1: 100, 2: 50, 3: 10}, 0, 1) == [CandidateDomain(2, 0, 1, 3, 1.5)]
    first, second, third = session.execute.call_args_list
    assert first.args[1] == {"domain_ids": [1, 2, 3], "start": 0, "end": 1}
    assert second.args[1]["domain_ids"] == [1, 2]
    assert third.args[1]["domain_ids"] == [3]
    assert second.args[1]["url_caps"] == [100, 50]
    assert third.args[1]["url_caps"] == [10]
    assert "LIMIT requested.url_cap" in str(second.args[0])

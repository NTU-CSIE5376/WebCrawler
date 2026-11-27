import pytest
from containers.scheduler_control.offerer.latency_allocation import allocate_jobs
from containers.scheduler_control.offerer.selection.base import CandidateDomain


def candidate(key,value,available=1000):
    return CandidateDomain(key,0,value,available_urls=available,url_score=value)


def stats(latency,parallel=1,samples=60):
    return dict(service_latency_sec=latency,observed_parallelism=parallel,samples=samples)


def solve(rows,pending=None,performance=None,**options):
    return allocate_jobs(rows,pending or {},performance or {},url_budget=options.pop('url_budget',200),
        domain_slots=options.pop('domain_slots',16),per_domain_cap=options.pop('per_domain_cap',512),
        max_active_domains=options.pop('max_active_domains',16),**options)


@pytest.mark.parametrize('method',['mip','greedy'])
def test_valuable_domain_can_exceed_its_observed_capacity(method):
    plan,info=solve([candidate(1,10),candidate(2,1)],performance={1:stats(10),2:stats(1)},method=method)
    assert plan[1]>info['targets']['1']  # Target is not an allocation constraint.
    assert sum(plan.values())<=200
    if method=='mip':assert info['method']=='mip'


def test_latency_reduces_slow_domain_work_without_making_it_a_hard_limit():
    plan,info=solve([candidate(1,1),candidate(2,1)],performance={1:stats(1),2:stats(10)},url_budget=1000)
    assert plan[1]>plan[2]
    assert plan[2]>info['targets']['2']


def test_existing_backlog_is_penalized_without_infeasibility():
    plan,info=solve([candidate(1,10),candidate(2,1)],pending={1:500},performance={1:stats(10),2:stats(1)})
    assert plan.get(1,0)==0 and plan[2]>0
    assert info['method']=='mip'


def test_domain_limit_frontier_and_batch_limits_are_hard():
    plan,_=solve([candidate(1,10,3),candidate(2,9,100)],domain_slots=1,per_domain_cap=5,url_budget=20)
    assert len(plan)==1 and sum(plan.values())<=5
    assert plan.get(1,0)<=3


def test_existing_domains_can_refill_when_no_new_domain_slots_remain():
    plan,_=solve([candidate(1,1),candidate(2,100)],pending={1:1},max_active_domains=1)
    assert set(plan)=={1}


def test_unknown_domains_get_bounded_exploratory_work():
    plan,info=solve([candidate(1,10)],url_budget=2048,per_domain_cap=512)
    assert info['targets']['1']==8
    assert 8<plan[1]<512


def test_mip_improves_domain_choice_over_marginal_greedy():
    rows=[candidate(1,10,1),candidate(2,9,100)]
    greedy,g=solve(rows,domain_slots=1,method="greedy")
    mip,m=solve(rows,domain_slots=1)
    assert m['method']=='mip' and m['objective']>=g['objective']

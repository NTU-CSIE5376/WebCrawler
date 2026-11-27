from libs.ipc.processing_feedback import ProcessingFeedback


def test_latency_and_observed_parallelism_are_reported_without_policy_fields():
    stats=ProcessingFeedback(window_sec=120)
    stats.admitted(1944,6,now=10)
    for latency in (1,2,3,4):stats.completed(1944,latency,now=11)
    report=stats.snapshot(now=12,wall_time=100)
    domain=report['domains']['1944']
    assert domain['service_latency_sec']==2.5
    assert domain['observed_parallelism']==6
    assert domain['estimated_completion_rate']==2.4
    assert domain['samples']==4
    assert 'max_qps' not in domain and 'concurrency_limit' not in domain
    assert stats.snapshot(now=200)['domains']=={}


def test_feedback_storage_is_bounded():
    stats=ProcessingFeedback(max_domains=2,max_samples=3)
    for domain in range(5):
        stats.admitted(domain,2,now=domain)
        for _ in range(10):stats.completed(domain,1,now=domain)
    assert len(stats.domains)==2
    assert all(len(entry['samples'])<=3 for entry in stats.domains.values())

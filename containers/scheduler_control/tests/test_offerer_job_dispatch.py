from unittest.mock import Mock

import pytest

from containers.scheduler_control.offerer.service import OffererConfig, OffererDerivation, OffererService
from containers.scheduler_control.offerer.selection.base import CandidateDomain
from containers.scheduler_control.offerer.job_client import CrawlJobClient


def service(tmp_path, outstanding):
    cfg = OffererConfig(offerer_id=0, scan_interval_sec=5, max_domain_files=20,
                        low_watermark_domains=10, per_domain_url_cap=100,
                        stats_dir=str(tmp_path / "stats"), job_api_url="http://api:8080",
                        max_outstanding_job_urls=10)
    selector = Mock()
    selector.peek_global_candidates.return_value = [CandidateDomain(1944, 0, 1)]
    selector.claim_domain_urls.side_effect = lambda **kw: [f"https://example.com/{i}" for i in range(kw["per_domain_cap"])]
    svc = OffererService(cfg, OffererDerivation(str(tmp_path / "queue_{id:02d}"), 16, 16), selector)
    svc.job_client = Mock()
    svc.job_client.active_inventory.return_value = ({7}, outstanding)
    svc.job_client.inventory_by_domain.return_value = ({7:outstanding}, outstanding)
    svc.job_client.performance.return_value = {}
    svc.job_client.new_reference.side_effect = lambda domain: f"offerer/0/domain/{domain}/test-reference"
    selector.demand_candidates.return_value = [CandidateDomain(1944,0,1,available_urls=100)]
    return svc, selector


def test_offerer_explicitly_disables_api_url_validation():
    client = CrawlJobClient("http://api:8080", 0)
    client._request = Mock(return_value={"job_id": "accepted"})
    urls = ["https://example.com/a#one"]
    assert client.submit(1944, urls, reference="test") == "accepted"
    assert client._request.call_args.args[1] == dict(
        urls=urls, processing_mode="ingest", validate_urls=False, client_reference="test")


def test_offerer_submits_ingest_jobs_with_bounded_outstanding_work(tmp_path):
    svc, selector = service(tmp_path, 7)
    result = svc._refill_once_if_needed()
    assert result["picked_urls"] == 3
    assert selector.claim_domain_urls.call_args.kwargs["per_domain_cap"] == 3
    assert selector.peek_global_candidates.call_args.kwargs["exclude_domain_ids"] == set()
    svc.job_client.submit.assert_called_once_with(1944, [f"https://example.com/{i}" for i in range(3)],
        reference="offerer/0/domain/1944/test-reference")
    assert not list(tmp_path.rglob("domain_*.json"))


def test_full_job_inventory_prevents_database_claims(tmp_path):
    svc, selector = service(tmp_path, 10)
    assert svc._refill_once_if_needed()["action"] == "noop"
    selector.claim_domain_urls.assert_not_called()
    selector.peek_global_candidates.assert_not_called()


def test_unavailable_api_prevents_claiming_or_legacy_fallback(tmp_path):
    svc, selector = service(tmp_path, 0)
    svc.job_client.inventory_by_domain.side_effect = TimeoutError("API unavailable")
    with pytest.raises(TimeoutError):
        svc._refill_once_if_needed()
    selector.claim_domain_urls.assert_not_called()
    assert not list(tmp_path.rglob("domain_*.json"))


def test_failed_submission_preserves_claimed_urls_for_retry(tmp_path):
    svc, selector = service(tmp_path, 10)
    svc.job_client.submit.side_effect = TimeoutError("lost reply")
    urls = ["https://example.com/claimed"]
    with pytest.raises(TimeoutError):
        svc._publish_domain(str(tmp_path / "queue_00"), 1944, urls)
    assert (tmp_path / "queue_00/dispatch_outbox/1944.json").exists()
    svc.job_client.submit.side_effect = None
    svc._refill_once_if_needed()
    svc.job_client.submit.assert_called_with(1944, urls, reconcile=True,
        reference="offerer/0/domain/1944/test-reference")
    assert not (tmp_path / "queue_00/dispatch_outbox/1944.json").exists()
    selector.claim_domain_urls.assert_not_called()

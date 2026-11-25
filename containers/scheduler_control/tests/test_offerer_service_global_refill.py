from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from typing import Optional

from containers.scheduler_control.offerer.selection.base import CandidateDomain
from containers.scheduler_control.offerer.service import (
    OffererConfig,
    OffererDerivation,
    OffererService,
)


class _StubLegacyStrategy:
    """Strategy without peek_global_candidates — exercises the per-shard fallback."""

    def __init__(self):
        self.calls = []

    def select_by_domain(self, shard_id, exclude_domain_ids, per_domain_cap, max_domains):
        self.calls.append(
            {
                "shard_id": shard_id,
                "exclude": set(exclude_domain_ids),
                "per_domain_cap": per_domain_cap,
                "max_domains": max_domains,
            }
        )
        return {}


class _StubGlobalStrategy:
    """Strategy with the new peek/claim interface; behavior driven by constructor args."""

    def __init__(
        self,
        candidates: Optional[list[CandidateDomain]] = None,
        per_domain_urls: Optional[dict[int, list[str]]] = None,
        claim_exceptions: Optional[dict[int, Exception]] = None,
        peek_exception: Optional[Exception] = None,
    ):
        self.candidates = candidates if candidates is not None else []
        self.per_domain_urls = per_domain_urls if per_domain_urls is not None else {}
        self.claim_exceptions = claim_exceptions or {}
        self.peek_exception = peek_exception
        self.peek_calls = []
        self.claim_calls = []

    def peek_global_candidates(self, limit, exclude_domain_ids, shard_start, shard_end):
        self.peek_calls.append({
            "limit": limit,
            "exclude": set(exclude_domain_ids),
            "shard_start": shard_start,
            "shard_end": shard_end,
        })
        if self.peek_exception is not None:
            raise self.peek_exception
        return list(self.candidates)

    def claim_domain_urls(self, shard_id, domain_id, per_domain_cap):
        self.claim_calls.append(
            {"shard_id": shard_id, "domain_id": domain_id, "per_domain_cap": per_domain_cap}
        )
        if domain_id in self.claim_exceptions:
            raise self.claim_exceptions[domain_id]
        return list(self.per_domain_urls.get(domain_id, []))


def _make_service(tmpdir: str, selector, **cfg_overrides) -> OffererService:
    queue_dir_template = os.path.join(tmpdir, "queue_{id:02d}")
    stats_dir = os.path.join(tmpdir, "stats")
    Path(stats_dir).mkdir(parents=True, exist_ok=True)

    deriv = OffererDerivation(
        queue_dir_template=queue_dir_template,
        total_shards=16,
        shards_per_offerer=16,
    )

    cfg_args = dict(
        offerer_id=0,
        scan_interval_sec=60,
        max_domain_files=4,
        low_watermark_domains=1,
        per_domain_url_cap=10,
        stats_dir=stats_dir,
        peek_multiplier=3,
        peek_hard_cap=1000,
    )
    cfg_args.update(cfg_overrides)
    cfg = OffererConfig(**cfg_args)
    return OffererService(cfg, deriv, selector)


class OffererRefillDispatchTest(unittest.TestCase):
    def test_dispatch_uses_global_when_strategy_supports_peek(self):
        selector = _StubGlobalStrategy(
            candidates=[CandidateDomain(domain_id=1, shard_id=0, domain_score=0.9)],
            per_domain_urls={1: ["https://example.com/x"]},
        )
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector)
            res = svc._refill_once_if_needed()

        self.assertEqual(res["action"], "refill")
        self.assertEqual(selector.peek_calls[0]["exclude"], set())
        self.assertEqual(len(selector.claim_calls), 1)

    def test_dispatch_falls_back_to_per_shard_for_legacy_strategy(self):
        selector = _StubLegacyStrategy()
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector)
            res = svc._refill_once_if_needed()

        self.assertIn(res["action"], {"refill_empty", "refill", "noop"})
        # Per-shard path is what's exercised — it calls select_by_domain across
        # the rotated shard range until quota is met (or shards are exhausted).
        self.assertGreater(len(selector.calls), 0)
        self.assertEqual(selector.calls[0]["shard_id"], 0)


class OffererRefillGlobalTest(unittest.TestCase):
    def test_peek_called_with_overfetch_limit(self):
        selector = _StubGlobalStrategy(candidates=[])
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector, max_domain_files=4, peek_multiplier=3)
            svc._refill_once_if_needed()

        self.assertEqual(selector.peek_calls[0]["limit"], 4 * 3)

    def test_peek_receives_offerer_shard_range(self):
        selector = _StubGlobalStrategy(candidates=[])
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector)
            svc._refill_once_if_needed()

        # _make_service uses total_shards=16, shards_per_offerer=16, offerer_id=0.
        self.assertEqual(selector.peek_calls[0]["shard_start"], 0)
        self.assertEqual(selector.peek_calls[0]["shard_end"], 15)

    def test_peek_hard_cap_applied(self):
        selector = _StubGlobalStrategy(candidates=[])
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(
                tmp,
                selector,
                max_domain_files=100,
                peek_multiplier=50,
                peek_hard_cap=200,
            )
            svc._refill_once_if_needed()

        self.assertEqual(selector.peek_calls[0]["limit"], 200)

    def test_empty_claim_skipped_and_loop_continues(self):
        candidates = [
            CandidateDomain(domain_id=1, shard_id=0, domain_score=0.9),
            CandidateDomain(domain_id=2, shard_id=1, domain_score=0.8),
            CandidateDomain(domain_id=3, shard_id=2, domain_score=0.7),
            CandidateDomain(domain_id=4, shard_id=3, domain_score=0.6),
            CandidateDomain(domain_id=5, shard_id=4, domain_score=0.5),
        ]
        per_domain_urls = {
            # First two are empty (race / nothing eligible); next three fill the quota.
            1: [],
            2: [],
            3: ["https://3.example/a"],
            4: ["https://4.example/a"],
            5: ["https://5.example/a"],
        }
        selector = _StubGlobalStrategy(
            candidates=candidates, per_domain_urls=per_domain_urls
        )
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector, max_domain_files=3, low_watermark_domains=1)
            res = svc._refill_once_if_needed()

        self.assertEqual(res["action"], "refill")
        self.assertEqual(res["new_domains"], 3)
        self.assertEqual(len(selector.claim_calls), 5)
        # Visited shards reflect only those that actually contributed.
        self.assertEqual(sorted(res["shards_visited"]), [2, 3, 4])

    def test_claim_exception_logs_and_continues(self):
        candidates = [
            CandidateDomain(domain_id=1, shard_id=0, domain_score=0.9),
            CandidateDomain(domain_id=2, shard_id=1, domain_score=0.8),
        ]
        selector = _StubGlobalStrategy(
            candidates=candidates,
            per_domain_urls={2: ["https://2.example/a"]},
            claim_exceptions={1: RuntimeError("boom")},
        )
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector, max_domain_files=2, low_watermark_domains=1)
            res = svc._refill_once_if_needed()

        # Refill keeps going past the failing claim and fills the surviving slot.
        self.assertEqual(res["action"], "refill")
        self.assertEqual(res["new_domains"], 1)
        self.assertEqual(res["shards_visited"], [1])

    def test_peek_exception_returns_refill_empty(self):
        selector = _StubGlobalStrategy(peek_exception=RuntimeError("db down"))
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector)
            res = svc._refill_once_if_needed()

        self.assertEqual(res["action"], "refill_empty")
        self.assertEqual(selector.claim_calls, [])

    def test_exclude_set_contains_existing_queued_domains(self):
        selector = _StubGlobalStrategy(candidates=[])
        with tempfile.TemporaryDirectory() as tmp:
            queue_dir = os.path.join(tmp, "queue_00")
            Path(queue_dir).mkdir(parents=True)
            # Pre-seed the queue with two domain files.
            for domain_id in (42, 99):
                with open(os.path.join(queue_dir, f"domain_{domain_id:06d}.json"), "w") as f:
                    json.dump({"domain_id": domain_id, "urls": []}, f)

            svc = _make_service(tmp, selector, low_watermark_domains=10)
            svc._refill_once_if_needed()

        self.assertEqual(selector.peek_calls[0]["exclude"], {42, 99})

    def test_low_watermark_short_circuits_before_peek(self):
        selector = _StubGlobalStrategy(candidates=[])
        with tempfile.TemporaryDirectory() as tmp:
            queue_dir = os.path.join(tmp, "queue_00")
            Path(queue_dir).mkdir(parents=True)
            # Fill the queue past low_watermark — refill should bail.
            for domain_id in range(5):
                with open(os.path.join(queue_dir, f"domain_{domain_id:06d}.json"), "w") as f:
                    json.dump({"domain_id": domain_id, "urls": []}, f)

            svc = _make_service(tmp, selector, low_watermark_domains=3, max_domain_files=4)
            res = svc._refill_once_if_needed()

        self.assertEqual(res["action"], "noop")
        self.assertEqual(selector.peek_calls, [])

    def test_global_flow_writes_domain_files(self):
        candidates = [
            CandidateDomain(domain_id=11, shard_id=2, domain_score=0.9),
            CandidateDomain(domain_id=22, shard_id=5, domain_score=0.8),
        ]
        selector = _StubGlobalStrategy(
            candidates=candidates,
            per_domain_urls={
                11: ["https://a.example/1", "https://a.example/2"],
                22: ["https://b.example/1"],
            },
        )
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector, max_domain_files=4, low_watermark_domains=1)
            res = svc._refill_once_if_needed()

            queue_dir = os.path.join(tmp, "queue_00")
            written = sorted(os.listdir(queue_dir))
            self.assertEqual(written, ["domain_000011.json", "domain_000022.json"])

            with open(os.path.join(queue_dir, "domain_000011.json")) as f:
                payload = json.load(f)
            self.assertEqual(payload["domain_id"], 11)
            self.assertEqual(payload["urls"], ["https://a.example/1", "https://a.example/2"])

        self.assertEqual(res["picked_urls"], 3)
        self.assertEqual(res["new_domains"], 2)
        self.assertEqual(sorted(res["shards_visited"]), [2, 5])
        self.assertEqual(res["shard_picked_urls"], {2: 2, 5: 1})

    def test_does_not_revisit_domain_within_one_refill(self):
        # Defensive: even if peek returns a duplicate domain_id, claim runs at most once.
        candidates = [
            CandidateDomain(domain_id=7, shard_id=0, domain_score=0.9),
            CandidateDomain(domain_id=7, shard_id=0, domain_score=0.9),
        ]
        selector = _StubGlobalStrategy(
            candidates=candidates,
            per_domain_urls={7: ["https://7.example/a"]},
        )
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector, max_domain_files=4, low_watermark_domains=1)
            svc._refill_once_if_needed()

        self.assertEqual(len(selector.claim_calls), 1)

    def test_publishes_each_claim_before_starting_the_next(self):
        selector = _StubGlobalStrategy(
            candidates=[
                CandidateDomain(domain_id=11, shard_id=0, domain_score=0.9),
                CandidateDomain(domain_id=22, shard_id=1, domain_score=0.8),
                # Consuming a file during refill must not allow a second claim.
                CandidateDomain(domain_id=11, shard_id=0, domain_score=0.9),
            ],
            per_domain_urls={11: ["https://a.example/1"], 22: ["https://b.example/1"]},
        )
        original_claim = selector.claim_domain_urls
        with tempfile.TemporaryDirectory() as tmp:
            svc = _make_service(tmp, selector)
            first_file = Path(tmp) / "queue_00/domain_000011.json"

            def claim(**kwargs):
                if kwargs["domain_id"] == 22:
                    # Simulate the crawler reading/deleting a batch while the
                    # offerer is still working on later domains.
                    self.assertEqual(json.loads(first_file.read_text())["urls"],
                                     ["https://a.example/1"])
                    first_file.unlink()
                return original_claim(**kwargs)

            with patch.object(selector, "claim_domain_urls", side_effect=claim):
                result = svc._refill_once_if_needed()

            self.assertFalse(first_file.exists())
            self.assertTrue((first_file.parent / "domain_000022.json").exists())
            self.assertEqual(result["new_domains"], 2)
            self.assertEqual(result["picked_urls"], 2)
            self.assertEqual(len(selector.claim_calls), 2)


if __name__ == "__main__":
    unittest.main()

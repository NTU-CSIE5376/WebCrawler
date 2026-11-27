"""Unit tests for GoldenDiscoveryRuntimeScorerV2 routing.

The v2 ranker's whole correctness story is the 3-bucket router: a URL goes to
its per-domain specialized head if the domain has one, else the general head if
the domain is a known categorical level, else the v1 fallback (or the general
head when no v1 scorer is wired). These tests pin that dispatch without loading
a real model — fake heads return a bucket-identifying constant, and the matrix
builders are stubbed so the test does not depend on the (separately tested)
feature code.

What we are protecting:
  * Each URL is scored by exactly the bucket its registrable domain selects.
  * The general head runs as ONE batch (domain carried as a categorical),
    not once per URL.
  * With no v1 scorer, unknown domains fall through to the general head
    rather than crashing or being dropped.
  * score_many (URL-only) reuses the same routing as score_many_rows.
  * Output is clipped into [0, 1].
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

import numpy as np

from libs.scoring.golden_discovery_runtime_v2 import GoldenDiscoveryRuntimeScorerV2
from libs.scoring.golden_discovery_v2_features import registrable_domain


def _constant_proba_model(score: float) -> MagicMock:
    """Fake LightGBM sklearn model: predict_proba returns `score` for every
    row in (n, 2) shape so the runtime's [:, 1] slice picks `score`."""
    model = MagicMock()

    def predict_proba(x):
        n = x.shape[0]
        return np.column_stack([np.full(n, 1.0 - score), np.full(n, score)])

    model.predict_proba = MagicMock(side_effect=predict_proba)
    return model


def _build_scorer(*, spec_domain: str, gen_domain: str, v1_scorer=None):
    """Artifact with one specialized domain (score 0.9) and one general-level
    domain (score 0.5). Matrix builders are stubbed on the instance so routing
    is exercised in isolation from the feature code."""
    spec_model = _constant_proba_model(0.9)
    gen_model = _constant_proba_model(0.5)
    artifact = {
        "specialized": {
            spec_domain: {
                "model": spec_model,
                "tfidf": None,
                "featcols": [],
                "cat_levels": {},
            }
        },
        "general": {
            "model": gen_model,
            "tfidf": MagicMock(),
            "gfcols": [],
            "domain_levels": [gen_domain],
            "domain_cat_idx": 0,
        },
        "metadata": {"model_name": "golden_discovery_ranker_v2", "score_version": "v2"},
    }
    scorer = GoldenDiscoveryRuntimeScorerV2(artifact, v1_scorer=v1_scorer)
    # Stub matrix assembly: return an (n, 1) array so predict_proba sees the
    # right row count. Routing — not feature math — is under test here.
    scorer._spec_matrix = MagicMock(side_effect=lambda dom, rows: np.zeros((len(rows), 1)))
    scorer._gen_matrix = MagicMock(side_effect=lambda rows: np.zeros((len(rows), 1)))
    return scorer, spec_model, gen_model


class V2RoutingTest(unittest.TestCase):
    def setUp(self):
        self.spec_url = "https://www.espn.com/nba/story"
        self.gen_url = "https://medium.com/@a/post"
        self.unknown_url = "https://some-random-unseen-domain-xyz.net/p"
        self.spec_domain = registrable_domain(self.spec_url)
        self.gen_domain = registrable_domain(self.gen_url)
        # Guard the fixture: the three URLs must resolve to three distinct
        # registrable domains, otherwise the routing assertions are vacuous.
        self.assertNotEqual(self.spec_domain, self.gen_domain)
        self.assertNotEqual(registrable_domain(self.unknown_url), self.spec_domain)
        self.assertNotEqual(registrable_domain(self.unknown_url), self.gen_domain)

    def test_specialized_domain_uses_specialized_head(self):
        scorer, spec_model, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain
        )
        scores = scorer.score_many_rows([{"url": self.spec_url}])
        self.assertEqual(scores, [0.9])
        spec_model.predict_proba.assert_called_once()
        gen_model.predict_proba.assert_not_called()

    def test_general_level_domain_uses_general_head(self):
        scorer, spec_model, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain
        )
        scores = scorer.score_many_rows([{"url": self.gen_url}])
        self.assertEqual(scores, [0.5])
        gen_model.predict_proba.assert_called_once()
        spec_model.predict_proba.assert_not_called()

    def test_unknown_domain_uses_v1_fallback_when_present(self):
        v1 = MagicMock(side_effect=lambda urls: [0.1 for _ in urls])
        scorer, spec_model, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain, v1_scorer=v1
        )
        scores = scorer.score_many_rows([{"url": self.unknown_url}])
        self.assertEqual(scores, [0.1])
        v1.assert_called_once_with([self.unknown_url])
        spec_model.predict_proba.assert_not_called()
        gen_model.predict_proba.assert_not_called()

    def test_unknown_domain_falls_through_to_general_without_v1(self):
        scorer, spec_model, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain, v1_scorer=None
        )
        scores = scorer.score_many_rows([{"url": self.unknown_url}])
        self.assertEqual(scores, [0.5])
        gen_model.predict_proba.assert_called_once()

    def test_mixed_batch_routes_each_url_to_its_bucket(self):
        v1 = MagicMock(side_effect=lambda urls: [0.1 for _ in urls])
        scorer, spec_model, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain, v1_scorer=v1
        )
        rows = [
            {"url": self.spec_url},
            {"url": self.gen_url},
            {"url": self.unknown_url},
        ]
        scores = scorer.score_many_rows(rows)
        self.assertEqual(scores, [0.9, 0.5, 0.1])

    def test_general_head_runs_once_for_multiple_general_urls(self):
        scorer, _, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain
        )
        scorer.score_many_rows(
            [{"url": self.gen_url}, {"url": self.gen_url + "/2"}]
        )
        # Domain is carried as a categorical so the general head is a single
        # batched predict_proba, not one call per URL.
        gen_model.predict_proba.assert_called_once()

    def test_empty_input_short_circuits(self):
        scorer, spec_model, gen_model = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain
        )
        self.assertEqual(scorer.score_many_rows([]), [])
        self.assertEqual(scorer.score_many([]), [])
        spec_model.predict_proba.assert_not_called()
        gen_model.predict_proba.assert_not_called()

    def test_output_clipped_into_unit_interval(self):
        scorer, _, _ = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain
        )
        # Force an out-of-range probability through the specialized head.
        scorer.spec[self.spec_domain]["model"].predict_proba = MagicMock(
            return_value=np.array([[0.0, 1.7]])
        )
        self.assertEqual(scorer.score_many_rows([{"url": self.spec_url}]), [1.0])

    def test_score_many_matches_score_many_rows_routing(self):
        scorer, _, _ = _build_scorer(
            spec_domain=self.spec_domain, gen_domain=self.gen_domain
        )
        # URL-only entrypoint must route identically to the row entrypoint.
        self.assertEqual(scorer.score_many([self.spec_url]), [0.9])


if __name__ == "__main__":
    unittest.main()

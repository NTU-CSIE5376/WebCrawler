"""Unit tests for GoldenDiscoveryRuntimeScorer.score_many.

Focus is on the head-independent feature caching and threaded per-head
inference. Tests build fake artifact components rather than loading a
real model, so they run in milliseconds and have no joblib / model-file
dependency.

What we are protecting:

  * Output equals the element-wise max across the per-head predictions
    (preserving the production semantic) and is clipped to [0, 1].
  * The vectorizer and numeric_feature_matrix are each invoked ONCE per
    call regardless of how many heads exist — i.e. the caching is real,
    a future refactor that re-introduces the per-head recompute would
    break these tests.
  * Each head's scaler.transform and head.predict_proba are invoked once
    (not skipped, not duplicated) — the parallel dispatch fans out
    correctly.
  * Empty input is a short-circuit and never touches the heads or
    feature builders.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import numpy as np
from scipy import sparse

from libs.scoring.golden_discovery_runtime import (
    GOLDEN_DISCOVERY_HEADS,
    GoldenDiscoveryRuntimeScorer,
)


def _make_constant_head(score: float) -> MagicMock:
    """Construct a fake sklearn-like head whose predict_proba returns the
    same probability `score` for every row, in (n, 2) shape."""
    head = MagicMock()

    def predict_proba(x):
        n_rows = x.shape[0]
        return np.column_stack(
            [np.full(n_rows, 1.0 - score), np.full(n_rows, score)]
        )

    head.predict_proba = MagicMock(side_effect=predict_proba)
    return head


def _make_identity_scaler() -> MagicMock:
    """A scaler whose .transform returns the input untouched."""
    scaler = MagicMock()
    scaler.transform = MagicMock(side_effect=lambda x: x)
    return scaler


def _build_artifact_with_head_scores(
    head_scores: dict[str, float],
    char_feature_count: int = 2,
):
    """Build a fake artifact in the exact shape GoldenDiscoveryRuntimeScorer
    expects. Each head returns its constant score; scalers are identity;
    vectorizer.transform returns a fixed zero sparse matrix."""
    artifact = {
        "vectorizer": MagicMock(),
        "priors": MagicMock(),
    }
    # The vectorizer is patched per-call below to control char_x shape.
    for head_name, score in head_scores.items():
        artifact[f"{head_name}_scaler"] = _make_identity_scaler()
        artifact[f"{head_name}_head"] = _make_constant_head(score)
    return artifact


def _patch_vectorizer_to_return(artifact: dict, n_urls: int, n_features: int = 2):
    fake_char_x = sparse.csr_matrix(np.zeros((n_urls, n_features)))
    artifact["vectorizer"].transform = MagicMock(return_value=fake_char_x)
    return fake_char_x


class ScoreManyTest(unittest.TestCase):
    def setUp(self):
        self.head_scores = {
            "url": 0.2,
            "parent": 0.4,
            "pathfinder": 0.6,
            "refresh": 0.8,
        }
        self.artifact = _build_artifact_with_head_scores(self.head_scores)
        self.scorer = GoldenDiscoveryRuntimeScorer(self.artifact)

    def tearDown(self):
        # Persistent executor: shut its threads down so the test process
        # doesn't accumulate idle workers between cases.
        self.scorer.close()

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_returns_max_across_heads(self, mock_nfm):
        _patch_vectorizer_to_return(self.artifact, n_urls=3)
        mock_nfm.return_value = np.zeros((3, 4))

        scores = self.scorer.score_many(["a", "b", "c"])

        # Heads return 0.2, 0.4, 0.6, 0.8 → element-wise max = 0.8 for
        # all three URLs.
        self.assertEqual(scores, [0.8, 0.8, 0.8])

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_empty_input_short_circuits(self, mock_nfm):
        scores = self.scorer.score_many([])
        self.assertEqual(scores, [])
        # Empty input must not touch the feature builders or heads.
        mock_nfm.assert_not_called()
        self.artifact["vectorizer"].transform.assert_not_called()
        for head_name in self.head_scores:
            self.artifact[f"{head_name}_head"].predict_proba.assert_not_called()

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_vectorizer_called_once_per_call_not_per_head(self, mock_nfm):
        _patch_vectorizer_to_return(self.artifact, n_urls=2)
        mock_nfm.return_value = np.zeros((2, 4))

        self.scorer.score_many(["a", "b"])

        # The caching is the headline optimisation: vectorizer.transform
        # used to be called once per head (4x). The fix collapses it to 1.
        self.assertEqual(self.artifact["vectorizer"].transform.call_count, 1)

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_numeric_feature_matrix_called_once_per_call(self, mock_nfm):
        _patch_vectorizer_to_return(self.artifact, n_urls=2)
        mock_nfm.return_value = np.zeros((2, 4))

        self.scorer.score_many(["a", "b"])

        self.assertEqual(mock_nfm.call_count, 1)

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_each_head_runs_exactly_once(self, mock_nfm):
        _patch_vectorizer_to_return(self.artifact, n_urls=2)
        mock_nfm.return_value = np.zeros((2, 4))

        self.scorer.score_many(["a", "b"])

        for head_name in self.head_scores:
            self.assertEqual(
                self.artifact[f"{head_name}_scaler"].transform.call_count,
                1,
                f"{head_name}_scaler should be called once",
            )
            self.assertEqual(
                self.artifact[f"{head_name}_head"].predict_proba.call_count,
                1,
                f"{head_name}_head should be called once",
            )

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_output_clipped_into_unit_interval(self, mock_nfm):
        _patch_vectorizer_to_return(self.artifact, n_urls=1)
        mock_nfm.return_value = np.zeros((1, 4))

        # Inject a "broken" head that returns a probability > 1.0 to
        # confirm the clip step still runs in the threaded path. Real
        # predict_proba outputs are always in [0, 1] but defensive
        # clipping has been part of this API since day one.
        self.artifact["url_head"].predict_proba = MagicMock(
            return_value=np.array([[0.0, 1.5]])
        )

        scores = self.scorer.score_many(["x"])
        self.assertEqual(scores, [1.0])

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_single_head_artifact_still_works(self, mock_nfm):
        # Some training runs only export one head — the scorer must still
        # produce that head's score directly without dying on
        # np.maximum.reduce of a single-element list.
        artifact = _build_artifact_with_head_scores({"url": 0.55})
        _patch_vectorizer_to_return(artifact, n_urls=2)
        mock_nfm.return_value = np.zeros((2, 4))

        scorer = GoldenDiscoveryRuntimeScorer(artifact)
        try:
            self.assertEqual(scorer.heads, ["url"])
            self.assertEqual(scorer.score_many(["a", "b"]), [0.55, 0.55])
        finally:
            scorer.close()

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_partial_heads_artifact_uses_only_present_heads(self, mock_nfm):
        # Drop two of the four heads — scorer should iterate exactly the
        # heads it actually has.
        artifact = _build_artifact_with_head_scores(
            {"url": 0.3, "refresh": 0.9}
        )
        _patch_vectorizer_to_return(artifact, n_urls=1)
        mock_nfm.return_value = np.zeros((1, 4))

        scorer = GoldenDiscoveryRuntimeScorer(artifact)
        try:
            self.assertEqual(sorted(scorer.heads), ["refresh", "url"])
            self.assertEqual(scorer.score_many(["a"]), [0.9])
        finally:
            scorer.close()


class ScoreHeadInputContractTest(unittest.TestCase):
    """The refactor changed _score_head's signature from (name, urls) to
    (name, char_x, num_x_raw). Since the method is module-private (single
    underscore) this is allowed, but pin the new contract so a future
    refactor cannot accidentally regress."""

    def test_score_head_now_takes_prebuilt_features(self):
        artifact = _build_artifact_with_head_scores({"url": 0.5})
        scorer = GoldenDiscoveryRuntimeScorer(artifact)
        try:
            char_x = sparse.csr_matrix(np.zeros((2, 3)))
            num_x_raw = np.zeros((2, 4))

            result = scorer._score_head("url", char_x, num_x_raw)
            np.testing.assert_allclose(result, [0.5, 0.5])
        finally:
            scorer.close()


class PersistentExecutorTest(unittest.TestCase):
    """Verifies the long-lived thread pool wiring.

    The headline performance win of PR A is collapsing 4 head inferences
    into a single fan-out; the secondary win is not paying thread
    spawn+join overhead on every score_many call. These tests pin the
    second contract.
    """

    @patch("libs.scoring.golden_discovery_runtime.numeric_feature_matrix")
    def test_executor_is_reused_across_calls(self, mock_nfm):
        # We intentionally inspect a private attribute here: the whole
        # point of this contract is that the same executor object survives
        # across calls. Externally observable behaviour alone cannot tell
        # apart "one persistent pool" from "a fresh pool each call".
        artifact = _build_artifact_with_head_scores({"url": 0.1, "refresh": 0.2})
        scorer = GoldenDiscoveryRuntimeScorer(artifact)
        try:
            _patch_vectorizer_to_return(artifact, n_urls=1)
            mock_nfm.return_value = np.zeros((1, 3))

            initial_executor = scorer._executor
            scorer.score_many(["a"])
            scorer.score_many(["b"])
            scorer.score_many(["c"])

            self.assertIs(scorer._executor, initial_executor)
        finally:
            scorer.close()

    def test_close_shuts_down_executor(self):
        artifact = _build_artifact_with_head_scores({"url": 0.1})
        scorer = GoldenDiscoveryRuntimeScorer(artifact)

        scorer.close()

        # After shutdown the executor refuses new submissions. We poke
        # the executor directly rather than score_many because we want a
        # deterministic, type-stable assertion of the shutdown contract.
        with self.assertRaises(RuntimeError):
            scorer._executor.submit(lambda: None)

    def test_context_manager_closes_on_exit(self):
        artifact = _build_artifact_with_head_scores({"url": 0.1})

        with GoldenDiscoveryRuntimeScorer(artifact) as scorer:
            self.assertIsNotNone(scorer._executor)
            inner_executor = scorer._executor

        with self.assertRaises(RuntimeError):
            inner_executor.submit(lambda: None)

    def test_executor_pool_size_matches_head_count(self):
        # Pool width is intrinsic to the artifact (one thread per head),
        # not configurable. Adding a config knob would mean wasting
        # threads on heads that don't exist; the right way to scale is
        # supervisord numprocs, see PR A commit message.
        artifact = _build_artifact_with_head_scores(
            {"url": 0.1, "parent": 0.2, "pathfinder": 0.3, "refresh": 0.4}
        )
        scorer = GoldenDiscoveryRuntimeScorer(artifact)
        try:
            self.assertEqual(scorer._executor._max_workers, 4)
        finally:
            scorer.close()

        artifact_one_head = _build_artifact_with_head_scores({"url": 0.5})
        scorer_one = GoldenDiscoveryRuntimeScorer(artifact_one_head)
        try:
            self.assertEqual(scorer_one._executor._max_workers, 1)
        finally:
            scorer_one.close()


class KnownHeadsConstantTest(unittest.TestCase):
    def test_known_heads_unchanged(self):
        # The threading width is implicitly len(self.heads), and the
        # cadence policy assumes the same four-head fan-out.
        self.assertEqual(
            GOLDEN_DISCOVERY_HEADS,
            ("url", "parent", "pathfinder", "refresh"),
        )


if __name__ == "__main__":
    unittest.main()

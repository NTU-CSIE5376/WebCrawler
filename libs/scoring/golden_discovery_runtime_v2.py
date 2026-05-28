"""Golden Discovery Ranker v2 — prefetch-only hybrid runtime scorer.

Loads a v2 artifact (per-domain specialized LightGBM + per-domain regex +
anchor TF-IDF for data-rich domains; one general head + domain categorical for
the rest) and routes each URL: specialized -> general head -> v1 fallback.

Additive: does NOT modify the v1 GoldenDiscoveryRuntimeScorer. Same interface
shape (load / heads / metadata / close). Two scoring entry points:
  * score_many(urls)        -- drop-in, URL-only (inlink/anchor absent -> ~v1)
  * score_many_rows(rows)   -- full, rows carry inlink_count_* + anchor_text

THREAD SAFETY: every LightGBM model is pinned to num_threads=1 at load. The
inline-ingestor runs 16 processes x this scorer; un-pinned LightGBM OpenMP would
re-parallelise under each and thrash (the failure mode the first inline-scoring
deploy hit -- see scheduler_ingest/supervisord.conf).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

import joblib
import numpy as np
from scipy import sparse

from .golden_discovery_v2_features import (
    RULES_MAP, GENERAL_FEATCOLS, specialized_feature_row, general_feature_row,
    anchor_features, _anchor_str, registrable_domain,
)


def _pin_threads(model) -> None:
    """Force single-threaded inference on a LightGBM sklearn model."""
    try:
        model.set_params(n_jobs=1, num_threads=1)
    except Exception:
        pass
    booster = getattr(model, "_Booster", None) or getattr(model, "booster_", None)
    if booster is not None:
        try:
            booster.params = {**(booster.params or {}), "num_threads": 1, "n_jobs": 1}
        except Exception:
            pass


class GoldenDiscoveryRuntimeScorerV2:
    def __init__(self, artifact: dict[str, Any], v1_scorer: Optional[Callable[[list[str]], Any]] = None):
        self.artifact = artifact
        self.spec = artifact["specialized"]            # {domain: {model,tfidf,featcols,cat_levels,...}}
        self.gen = artifact["general"]                 # {model,tfidf,gfcols,domain_levels,domain_cat_idx}
        self.v1 = v1_scorer
        self._spec_doms = set(self.spec)
        self._gen_doms = set(self.gen["domain_levels"])
        for s in self.spec.values():
            _pin_threads(s["model"])
        _pin_threads(self.gen["model"])

    # ---- interface parity with v1 ----
    @classmethod
    def load(cls, path: str | Path, v1_scorer=None) -> "GoldenDiscoveryRuntimeScorerV2":
        art = joblib.load(Path(path))
        if not isinstance(art, dict) or "specialized" not in art or "general" not in art:
            raise ValueError("v2 artifact must be a dict with 'specialized' and 'general'")
        return cls(art, v1_scorer=v1_scorer)

    @property
    def metadata(self) -> dict[str, Any]:
        md = self.artifact.get("metadata") or {}
        return md if isinstance(md, dict) else {}

    @property
    def heads(self) -> list[str]:
        return ["specialized:" + ",".join(sorted(self.spec))] + (["general"]) + (["v1_fallback"] if self.v1 else [])

    def close(self) -> None:  # symmetry with v1; no resources to release
        return None

    def __enter__(self): return self
    def __exit__(self, *a): self.close()

    # ---- feature assembly ----
    def _spec_matrix(self, dom: str, rows: list[dict]):
        S = self.spec[dom]; rules_fn = RULES_MAP[dom]
        featcols = S["featcols"]; cat_levels = S["cat_levels"]
        idx = {c: j for j, c in enumerate(featcols)}
        M = np.zeros((len(rows), len(featcols)), dtype=np.float32)
        anchors = []
        for i, r in enumerate(rows):
            f = specialized_feature_row(r, rules_fn)
            anchors.append(_anchor_str(r.get("anchor_text")))
            for c, v in f.items():
                j = idx.get(c)
                if j is None:
                    continue
                if c in cat_levels:
                    levels = cat_levels[c]
                    M[i, j] = levels.index(v) if v in levels else -1
                else:
                    M[i, j] = v
        T = S["tfidf"].transform(anchors) if S.get("tfidf") is not None else sparse.csr_matrix((len(rows), 0))
        return sparse.hstack([sparse.csr_matrix(M), T], format="csr")

    def _gen_matrix(self, rows: list[dict]):
        G = self.gen; gfcols = G["gfcols"]; dlevels = G["domain_levels"]; dci = G["domain_cat_idx"]
        M = np.zeros((len(rows), len(gfcols) + 1), dtype=np.float32)
        anchors = []
        for i, r in enumerate(rows):
            gf = general_feature_row(r)
            anchors.append(_anchor_str(r.get("anchor_text")))
            for j, c in enumerate(gfcols):
                M[i, j] = gf[c]
            d = registrable_domain(r["url"])
            M[i, dci] = dlevels.index(d) if d in dlevels else -1
        T = G["tfidf"].transform(anchors)
        return sparse.hstack([sparse.csr_matrix(M), T], format="csr")

    # ---- scoring ----
    def score_many_rows(self, rows: list[dict]) -> list[float]:
        """rows: dicts with url (+ optional inlink_count_approx/external, anchor_text)."""
        n = len(rows)
        if n == 0:
            return []
        out = np.full(n, np.nan, dtype=float)
        rd = [registrable_domain(r["url"]) for r in rows]
        by_dom: dict[str, list[int]] = {}
        gen_idx: list[int] = []
        fb_idx: list[int] = []
        for i, d in enumerate(rd):
            if d in self._spec_doms:
                by_dom.setdefault(d, []).append(i)
            elif d in self._gen_doms:
                gen_idx.append(i)
            else:
                fb_idx.append(i)
        # 1) specialized, per top-domain (few groups)
        for dom, idxs in by_dom.items():
            sub = [rows[i] for i in idxs]
            s = self.spec[dom]["model"].predict_proba(self._spec_matrix(dom, sub))[:, 1]
            for k, i in enumerate(idxs):
                out[i] = s[k]
        # 2) general head: ONE batch (domain carried as categorical)
        if gen_idx:
            sub = [rows[i] for i in gen_idx]
            s = self.gen["model"].predict_proba(self._gen_matrix(sub))[:, 1]
            for k, i in enumerate(gen_idx):
                out[i] = s[k]
        # 3) v1 fallback (or general when no v1): ONE batch
        if fb_idx:
            sub_urls = [rows[i]["url"] for i in fb_idx]
            if self.v1 is not None:
                s = list(self.v1(sub_urls))
            else:
                s = self.gen["model"].predict_proba(self._gen_matrix([rows[i] for i in fb_idx]))[:, 1]
            for k, i in enumerate(fb_idx):
                out[i] = float(s[k])
        return [float(np.clip(v, 0.0, 1.0)) for v in out]

    def score_many(self, urls: list[str]) -> list[float]:
        """Drop-in v1-compatible path: URL-only (no inlink/anchor -> degrades to ~v1)."""
        return self.score_many_rows([{"url": u} for u in urls])

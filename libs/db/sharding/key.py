"""Single source of truth for domain -> shard_id routing.

Rules (in order):
  1. key = hostname if its eTLD+1 is in `split_etld1`, else the eTLD+1.
  2. `overrides[key]` if present.
  3. `md5(key) % num_shards`.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import tldextract

from libs.config.loader import load_yaml


def shard_key(name: str, split_etld1: Iterable[str] | None = None) -> str:
    if not name:
        return "unknown"
    ext = tldextract.extract(name)
    etld1 = ext.registered_domain or name
    split_set = set(split_etld1) if split_etld1 else set()
    if etld1 in split_set and name != etld1:
        return name
    return etld1


def compute_shard(
    name: str,
    num_shards: int,
    overrides: dict[str, int] | None = None,
    split_etld1: Iterable[str] | None = None,
) -> int:
    key = shard_key(name, split_etld1)
    if overrides and key in overrides:
        return int(overrides[key])
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h, 16) % num_shards


def load_split_etld1(path: str | Path) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    cfg = load_yaml(str(p))
    return set(cfg.get("split_etld1") or [])


def load_sharding_config(
    ingest_path: str | Path,
    split_path: str | Path,
) -> tuple[dict[str, int], set[str]]:
    """Returns (overrides, split_etld1). Any eTLD+1 in both is dropped from
    overrides, otherwise the old hard-pin traps the apex on the old shard."""
    ingest = load_yaml(str(ingest_path)) if Path(ingest_path).exists() else {}
    overrides = dict((ingest.get("router") or {}).get("domain_overrides") or {})
    split_etld1 = load_split_etld1(split_path)
    for k in split_etld1 & overrides.keys():
        overrides.pop(k, None)
    return overrides, split_etld1

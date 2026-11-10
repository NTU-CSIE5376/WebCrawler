"""Bounded golden writes; the caller owns commit, rollback and checkpointing."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Mapping, Sequence

from psycopg2.extras import execute_values

from constants import NUM_SHARDS, SOURCE_GOLDEN


GOLDEN_URL_SCORE = 3.0
DomainInfo = tuple[int, int, float]


@dataclass(frozen=True)
class GoldenURL:
    metric_id: int
    url: str
    variant: str | None
    domain: str
    shard_id: int


@dataclass(frozen=True)
class BatchResult:
    counts: dict[str, int]
    # Merge these entries into the caller's cache only after commit succeeds.
    domains: dict[str, DomainInfo]


class RoutingMismatchError(ValueError):
    """Existing domain metadata disagrees with the current shared router."""


class ConcurrentDeleteError(RuntimeError):
    """A target disappeared before it was marked; retry the whole transaction."""


def _domains(cur, records: Sequence[GoldenURL],
             cache: Mapping[str, DomainInfo]) -> dict[str, DomainInfo]:
    wanted: dict[str, int] = {}
    for rec in records:
        if type(rec.shard_id) is not int or not 0 <= rec.shard_id < NUM_SHARDS:
            raise ValueError(f"Invalid shard ID: {rec.shard_id!r}")
        if not rec.domain:
            raise ValueError("A golden URL must have a domain key")
        previous = wanted.setdefault(rec.domain, rec.shard_id)
        if previous != rec.shard_id:
            raise RoutingMismatchError(f"Conflicting shard IDs for {rec.domain!r}")
        if rec.domain in cache and cache[rec.domain][1] != rec.shard_id:
            raise RoutingMismatchError(f"Cached shard disagrees with router for {rec.domain!r}")
    missing = {domain: sid for domain, sid in wanted.items() if domain not in cache}
    if not missing:
        return {}
    values = sorted(missing.items())
    execute_values(cur, "INSERT INTO domain_state (domain, shard_id) VALUES %s "
                   "ON CONFLICT (domain) DO NOTHING", values, page_size=len(values))
    cur.execute("SELECT domain, domain_id, shard_id, COALESCE(domain_score, 0.0) "
                "FROM domain_state WHERE domain = ANY(%s)", (list(missing),))
    result = {}
    for domain, domain_id, sid, score in cur.fetchall():
        if sid is None or int(sid) != missing[domain]:
            raise RoutingMismatchError(
                f"Domain {domain!r} is stored on shard {sid}, but shared router selects "
                f"{missing[domain]}; reconcile domain routing before injection")
        result[domain] = (int(domain_id), int(sid), float(score))
    if result.keys() != missing.keys():
        raise RuntimeError("A domain disappeared while preparing golden injection")
    return result


def inject_batch(cur, records: Sequence[GoldenURL],
                 domain_cache: Mapping[str, DomainInfo]) -> BatchResult:
    """Mark a batch without resetting existing crawl state or committing it.

    Each shard uses a presence probe, a bulk insert returning the *actual* new
    URLs, an update of existing URLs and a history insert for the actual new
    rows. RETURNING avoids YSQL's unsupported xmax system column and prevents
    a concurrent normal insert from being mistaken for our new history row.
    Missing UPDATE targets fail the batch rather than advancing a checkpoint
    past a URL that concurrent GC deleted before it could be marked.
    """
    counts = {"injected": 0, "marked": 0, "marked_variant": 0}
    if not records:
        return BatchResult(counts, {})
    additions = _domains(cur, records, domain_cache)
    domains = {**domain_cache, **additions}
    groups: dict[int, list[GoldenURL]] = defaultdict(list)
    for rec in records:
        groups[rec.shard_id].append(rec)

    for sid, group in sorted(groups.items()):
        table = f"url_state_current_{sid:03d}"
        candidates = sorted({u for rec in group for u in (rec.url, rec.variant) if u})
        cur.execute(f"SELECT url FROM {table} WHERE url = ANY(%s)", (candidates,))
        present = {row[0] for row in cur.fetchall()}
        # Account for earlier inputs in this batch exactly as sequential
        # injection does, including several srsltid variants of one page.
        choices: list[tuple[str, bool]] = []
        targets: dict[str, GoldenURL] = {}
        for rec in group:
            target = rec.url
            variant = bool(target not in present and rec.variant and rec.variant in present)
            if variant:
                assert rec.variant is not None
                target = rec.variant
            choices.append((target, variant))
            targets.setdefault(target, rec)
            present.add(target)
        values = [(url, domains[rec.domain][0], domains[rec.domain][2],
                   SOURCE_GOLDEN, GOLDEN_URL_SCORE) for url, rec in sorted(targets.items())]
        inserted = execute_values(
            cur, f"INSERT INTO {table} (url, domain_id, domain_score, source, url_score, "
                 "url_score_updated_at, should_crawl) VALUES %s "
                 "ON CONFLICT (url) DO NOTHING RETURNING url",
            values, template="(%s, %s, %s, %s, %s, NOW(), TRUE)",
            page_size=len(values), fetch=True,
        )
        new = {row[0] for row in inserted}
        existing = sorted(targets.keys() - new)
        if existing:
            cur.execute(f"UPDATE {table} SET source = %s, url_score = %s, "
                        "url_score_updated_at = NOW() WHERE url = ANY(%s) RETURNING url",
                        (SOURCE_GOLDEN, GOLDEN_URL_SCORE, existing))
            updated = {row[0] for row in cur.fetchall()}
            if updated != set(existing):
                raise ConcurrentDeleteError(
                    f"Golden targets changed during shard {sid} update "
                    f"({len(set(existing) - updated)} missing URL(s)); retry the transaction")
        if new:
            history = [(url, domains[targets[url].domain][0],
                        domains[targets[url].domain][2], SOURCE_GOLDEN) for url in sorted(new)]
            execute_values(cur, f"INSERT INTO url_state_history_{sid:03d} "
                           "(url, domain_id, domain_score, source) VALUES %s",
                           history, page_size=len(history))
        counted: set[str] = set()
        for target, variant in choices:
            if target in new and target not in counted:
                counts["injected"] += 1
                counted.add(target)
            else:
                counts["marked_variant" if variant else "marked"] += 1
    assert sum(counts.values()) == len(records)
    return BatchResult(counts, additions)

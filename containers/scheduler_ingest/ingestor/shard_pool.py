"""Write one folder's shards in parallel worker processes.

Each shard has its own url_state_current/history/event_counter tables, and
every domain_state row belongs to exactly one shard, so two shards never
touch the same row. Inside a shard the order stays links first, then
results, which is all the URL-level ordering the ingestor relies on.

Workers are processes, not threads: building execute_values payloads and
inline scoring are CPU-bound Python and would serialise on the GIL.
"""
from __future__ import annotations

import multiprocessing
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable

from libs.config.loader import load_yaml
from libs.obslog import configure as configure_logging

from .service import merge_counts, run_batches


_db = None


def db_from_config(config_path: str):
    # Imported here: main imports this module.
    from .main import build_ingest_db

    return build_ingest_db(load_yaml(config_path))


def _init_worker(db_factory: Callable, ingestor_id: int) -> None:
    global _db
    configure_logging(service="ingestor", worker_id=ingestor_id)
    _db = db_factory()


def _ingest_shard(link_recs: list[dict], result_recs: list[dict]):
    counters, domains, shard_stats = defaultdict(int), {}, {}
    run_batches(_db, link_recs, "links", counters, domains, shard_stats)
    run_batches(_db, result_recs, "results", counters, domains, shard_stats)
    return counters, domains, shard_stats


class ShardPool:
    def __init__(self, workers: int, db_factory: Callable, ingestor_id: int):
        """db_factory must be picklable (a module-level function or a
        functools.partial of one); each worker calls it once at start."""
        # spawn, not fork: the parent may already hold logging handlers and
        # threads, and a forked child must not inherit DB sockets.
        self._executor = ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_init_worker,
            initargs=(db_factory, ingestor_id),
        )

    def run(self, link_recs, result_recs, counters, domains, shard_stats) -> None:
        by_shard = defaultdict(lambda: ([], []))
        for rec in link_recs:
            by_shard[int(rec["shard_id"])][0].append(rec)
        for rec in result_recs:
            by_shard[int(rec["shard_id"])][1].append(rec)

        # Largest shards first so a big one does not start last and set the
        # folder's wall time on its own.
        order = sorted(by_shard, key=lambda sid: -sum(map(len, by_shard[sid])))
        futures = [self._executor.submit(_ingest_shard, *by_shard[sid]) for sid in order]
        # A worker crash (BrokenProcessPool) propagates: the folder is not
        # advanced and supervisor restarts the ingestor.
        for fut in as_completed(futures):
            part_counters, part_domains, part_stats = fut.result()
            merge_counts(counters, part_counters)
            merge_counts(domains, part_domains)
            merge_counts(shard_stats, part_stats)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)

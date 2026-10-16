"""Deliver accepted batches to ingestors; retain the batch on delivery failure."""
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
import time

import click
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

from libs.db.sharding.table_factory import url_state_current_table
from libs.config.loader import load_yaml
from libs.ipc.folder_reader import current_interval
from libs.obslog import configure
from .queue_lock import batch_claim, queue_lock


logger = logging.getLogger("crawl_request_dispatcher")


class SubmissionDispatcher:
    def __init__(self, config: dict, *, session_factory=None):
        settings = config["api"]["crawl_request"]
        self.pending_dir = Path(settings["pending_dir"])
        self.workers = settings.get("dispatcher_workers", 8)
        self.max_urls = settings.get("dispatcher_max_urls", 100000)
        self.max_batches = settings.get("dispatcher_max_batches", 32)
        for name, value in (("dispatcher_workers", self.workers),
                            ("dispatcher_max_urls", self.max_urls),
                            ("dispatcher_max_batches", self.max_batches)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"api.crawl_request.{name} must be a positive integer")
        self.shard_locks = self.pending_dir / ".shard_locks"
        self.router = config["router"]
        self.Session = session_factory or sessionmaker(
            bind=create_engine(config["postgres"]["dsn"], pool_pre_ping=True,
                               pool_size=self.workers, max_overflow=0),
        )

    def process_file(self, batch: Path) -> None:
        with batch_claim(batch) as owned:
            if owned:
                self._process_locked(batch)

    def _process_locked(self, batch: Path) -> None:
        self._process_batches([(batch, self._read_records(batch))])

    def _read_records(self, batch: Path) -> list[dict]:
        records = json.loads(batch.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise ValueError("Expected a batch of crawl request records")
        for record in records:
            shard = int(record["shard_id"])
            if not 0 <= shard < int(self.router.get("num_shards", 256)):
                raise ValueError("Invalid crawl request shard")
            if not isinstance(record["url"], str) or not record["url"]:
                raise ValueError("Invalid crawl request URL")
        return records

    def _update_shard(self, shard_id, table, urls):
        # No worker holds more than one shard lock. Commit before releasing
        # it, so separate dispatcher processes serialize the same shard too.
        with queue_lock(self.shard_locks / f"{shard_id:03d}"):
            with self.Session.begin() as session:
                session.execute(
                    update(table).where(table.c.url.in_(urls)).values(should_crawl=True)
                )

    def _process_batches(self, batches):
        # Combine independent accepted requests into one UPDATE per shard.
        by_shard = defaultdict(list)
        for _, records in batches:
            for record in records:
                by_shard[int(record["shard_id"])].append(record["url"])
        # Construct/cache ORM tables before starting SQL threads.
        tables = {shard: url_state_current_table(shard).__table__ for shard in sorted(by_shard)}
        failures = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            jobs = {shard: pool.submit(self._update_shard, shard, tables[shard], list(dict.fromkeys(urls)))
                    for shard, urls in by_shard.items()}
            for shard, job in jobs.items():
                try:
                    job.result()
                except Exception as error:
                    failures[shard] = error
        # All SQL workers finish before batch claims are released, including
        # on failure. Successful shards may commit while another shard fails;
        # affected batches remain pending and retrying may request a recrawl.
        first_error = None
        for batch, records in batches:
            failed_shards = sorted({int(record["shard_id"]) for record in records} & failures.keys())
            if failed_shards:
                first_error = first_error or failures[failed_shards[0]]
                continue
            try:
                self._publish_batch(batch, records)
                batch.unlink()
            except Exception as error:
                first_error = first_error or error
        if first_error is not None:
            raise first_error

    def _publish_batch(self, batch, records):
        # Preserve one file per ingestor per accepted request. Shard-level
        # SQL concurrency must not multiply the number of ingestion files.
        groups = defaultdict(list)
        for record in records:
            worker = record["shard_id"] // int(self.router["shards_per_ingestor"])
            groups[worker].append(record)
        for worker, group in groups.items():
            # Stage outside input buckets; slow writes must not expose a
            # bucket that readers could checkpoint before publication.
            temporary = batch.with_suffix(f".{worker}.jsonl.tmp")
            try:
                with temporary.open("w", encoding="utf-8") as handle:
                    for record in group:
                        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                # Select a fresh bucket only after all data has been staged.
                date, bucket = current_interval(int(self.router["interval_minutes"]))
                directory = Path(self.router["ingestor_dir_template"].format(id=worker)) / date / bucket
                directory.mkdir(parents=True, exist_ok=True)
                target = directory / f"api_{batch.stem}.jsonl"
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)

    def run_once(self) -> None:
        candidates = deque(sorted(self.pending_dir.glob("*.json")))
        while candidates:
            with ExitStack() as claims:
                batches = []
                count = 0
                while candidates and len(batches) < self.max_batches and count < self.max_urls:
                    batch = candidates.popleft()
                    with ExitStack() as claim:
                        if not claim.enter_context(batch_claim(batch)):
                            continue
                        try:
                            records = self._read_records(batch)
                        except Exception:
                            logger.exception("submission.invalid_batch", extra={"batch": str(batch)})
                            continue
                        if batches and count + len(records) > self.max_urls:
                            candidates.appendleft(batch)
                            break
                        claims.enter_context(claim.pop_all())
                        batches.append((batch, records))
                        count += len(records)
                if batches:
                    try:
                        self._process_batches(batches)
                    except Exception:
                        logger.exception("submission.delivery_failed",
                                         extra={"batches": [str(batch) for batch, _ in batches]})


@click.command()
@click.option("--config", "config_path", required=True,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Path to scheduler_ingest's YAML configuration.")
def main(config_path: Path):
    """Dispatch accepted crawl requests to ingestor queues."""
    configure(service="crawl_request_dispatcher")
    config = load_yaml(str(config_path))
    dispatcher = SubmissionDispatcher(config)
    while True:
        dispatcher.run_once()
        time.sleep(5)


if __name__ == "__main__":
    main()

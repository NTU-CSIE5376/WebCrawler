"""Deliver accepted batches to ingestors; retain the batch on delivery failure."""
from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
import time

from libs.config.loader import load_yaml
from libs.ipc.folder_reader import current_interval
from libs.obslog import configure
from .queue_lock import queue_lock


logger = logging.getLogger("url_submission_dispatcher")


class SubmissionDispatcher:
    def __init__(self, config: dict):
        self.pending_dir = Path(config["api"]["pending_dir"])
        self.router = config["router"]

    def process_file(self, batch: Path) -> None:
        with queue_lock(self.pending_dir):
            # Another dispatcher may have delivered this file while we waited.
            if batch.exists():
                self._process_locked(batch)

    def _process_locked(self, batch: Path) -> None:
        records = json.loads(batch.read_text(encoding="utf-8"))
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
        # Keep accepted batches until every destination has been published.
        batch.unlink()

    def run_once(self) -> None:
        for batch in sorted(self.pending_dir.glob("*.json")):
            try:
                self.process_file(batch)
            except Exception:
                logger.exception("submission.delivery_failed", extra={"batch": str(batch)})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    configure(service="url_submission_dispatcher")
    config = load_yaml(args.config)
    dispatcher = SubmissionDispatcher(config)
    while True:
        dispatcher.run_once()
        time.sleep(5)


if __name__ == "__main__":
    main()

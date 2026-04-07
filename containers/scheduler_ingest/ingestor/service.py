from __future__ import annotations
from pathlib import Path
from collections import defaultdict

from libs.ipc.jsonio import read_json, read_jsonl
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import IngestDB, IngestResult, BATCH_SIZE


class IngestService:
    def __init__(self, ingestor_id: int, db: IngestDB, stats: StatsDeltaWriter):
        self.ingestor_id = ingestor_id
        self.db = db
        self.stats = stats

    def _accumulate_result(self, result, counters, domains):
        if isinstance(result, bool):
            if result:
                counters["new_links"] += 1
            return

        if not result:
            return

        domains.setdefault(result.domain_id, defaultdict(int))

        if result.new_link:
            counters["new_links"] += 1
        if result.is_ok:
            counters["num_fetch_ok"] += 1
            domains[result.domain_id]["num_fetch_ok"] += 1
        else:
            counters["num_fetch_fail"] += 1
            domains[result.domain_id]["num_fetch_fail"] += 1
        if result.is_upd:
            counters["num_content_update"] += 1
            domains[result.domain_id]["num_content_update"] += 1
        if result.fail_reason:
            counters.setdefault("fail_reasons", defaultdict(int))[result.fail_reason] += 1
            domains[result.domain_id].setdefault("fail_reasons", defaultdict(int))[result.fail_reason] += 1

    def process_folder(self, folder: Path):
        print(f"[ingestor {self.ingestor_id:02d}] start processing '{folder}'", flush=True)
        file_cnt = 0
        counters = defaultdict(int)
        domains = {}

        # Collect all records first, then process in batches.
        all_recs = []
        for f in folder.iterdir():
            if not f.is_file():
                continue
            if f.suffix == ".json":
                all_recs.append(read_json(f))
                file_cnt += 1
            elif f.suffix == ".jsonl":
                all_recs.extend(read_jsonl(f))
                file_cnt += 1

        for i in range(0, len(all_recs), BATCH_SIZE):
            batch = all_recs[i : i + BATCH_SIZE]
            try:
                results = self.db.process_batch(batch)
                for result in results:
                    self._accumulate_result(result, counters, domains)
            except Exception as e:
                print(f"[ingestor {self.ingestor_id:02d}] ERROR in batch: {e}", flush=True)
                counters["error_count"] += len(batch)
                counters["ingest_error"] += len(batch)

        domains.pop(None, None)
        self.stats.write(
            source="ingestor",
            counters=counters,
            domains=domains,
        )
        print(f"[ingestor {self.ingestor_id:02d}] finish processing '{folder}', {file_cnt} files", flush=True)


from __future__ import annotations
from pathlib import Path
from collections import defaultdict

from libs.ipc.jsonio import read_json
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import IngestDB, IngestResult


class IngestService:
    def __init__(self, ingestor_id: int, db: IngestDB, stats: StatsDeltaWriter):
        self.ingestor_id = ingestor_id
        self.db = db
        self.stats = stats

    def process_folder(self, folder: Path):
        print(f"[ingestor {self.ingestor_id:02d}] start processing '{folder}'", flush=True)
        counters = defaultdict(int)
        domains = {}
        for p in folder.glob("*.json"):
            try:
                rec = read_json(p)
                if rec.get("status") == "new":
                    if self.db.process_link(rec):
                        # discover new link
                        counters["new_links"] += 1
                    continue

                result = self.db.process_result(rec)
                if not result:
                    continue

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
            except Exception as e:
                print(f"[ingestor {self.ingestor_id:02d}] ERROR: {e}", flush=True)
                counters["error_count"] += 1
                counters["ingest_error"] += 1
        domains.pop(None, None)
        self.stats.write(
            source="ingestor",
            counters=counters,
            domains=domains
        )
        print(f"[ingestor {self.ingestor_id:02d}] finish processing '{folder}'", flush=True)


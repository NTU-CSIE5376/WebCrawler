"""Compare baseline and streaming ingestion peak RSS in fresh processes.

Run with the scheduler_ingest virtualenv, e.g.:
  python scripts/benchmark_ingestor_memory.py --baseline-ref 9f20bfd --records 20000 200000

Database writes are replaced with a sink that counts batches without retaining
records. This measures parsing/buffering/aggregation, not database throughput,
ranker memory, or production container usage. No production files are touched.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time
import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def worker(args):
    from containers.scheduler_ingest.ingestor.db_ops import IngestDB
    from containers.scheduler_ingest.ingestor.service import IngestService

    if args.worker == "baseline":
        source = subprocess.check_output(
            ["git", "show", f"{args.baseline_ref}:containers/scheduler_ingest/ingestor/service.py"],
            cwd=ROOT, text=True,
        )
        module = types.ModuleType("containers.scheduler_ingest.ingestor._benchmark_baseline")
        exec(compile(source, "<baseline service>", "exec"), module.__dict__)
        IngestService = module.IngestService

    class SinkDB:
        aggregate_links = staticmethod(IngestDB.aggregate_links)

        def __init__(self):
            self.records = self.batches = 0

        def process_batch(self, records):
            self.records += len(records)
            self.batches += 1
            return [None] * len(records)

    class SinkStats:
        def write(self, **kwargs):
            pass

    db = SinkDB()
    started = time.perf_counter()
    IngestService(0, db, SinkStats()).process_folder(Path(args.folder))
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({
        "implementation": args.worker,
        "peak_rss_mib": round(peak / (1024**2 if sys.platform == "darwin" else 1024), 2),
        "seconds": round(time.perf_counter() - started, 3),
        "records_written": db.records,
        "database_batches": db.batches,
    }))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--records", type=int, nargs="+", default=[20000, 200000])
    parser.add_argument("--worker", choices=["baseline", "streaming"], help=argparse.SUPPRESS)
    parser.add_argument("--folder", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args)
        return

    # Both implementations read the identical generated JSONL in separate
    # processes. Unique links keep record and database batch counts comparable.
    with tempfile.TemporaryDirectory(prefix="ingestor-memory-") as tmp:
        folder = Path(tmp)
        for count in args.records:
            path = folder / "records.jsonl"
            with path.open("w", encoding="utf-8") as output:
                for i in range(count):
                    rec = {
                        "url": f"https://example.test/{i}",
                        "status": "new" if (i // 16) % 2 else "ok",
                        "shard_id": i % 16,
                        "domain_id": i % 16,
                        "content": "x" * 1024,
                    }
                    output.write(json.dumps(rec) + "\n")
            for implementation in ("baseline", "streaming"):
                raw = subprocess.check_output([
                    sys.executable, str(Path(__file__).resolve()),
                    "--baseline-ref", args.baseline_ref,
                    "--worker", implementation, "--folder", str(folder),
                ], cwd=ROOT, text=True)
                result = json.loads(raw)
                result.update(input_records=count, input_mib=round(path.stat().st_size / 1024**2, 2))
                print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

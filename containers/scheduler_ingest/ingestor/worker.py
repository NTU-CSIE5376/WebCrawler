"""Runtime shared by the independently checkpointed ingestion components."""
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
import fcntl
import logging
from pathlib import Path
import signal
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from libs.config.loader import load_yaml, require
from libs.ipc.folder_reader import FolderReader, Progress
from libs.ipc.jsonio import atomic_write_json
from libs.obslog import configure
from libs.stats.delta_writer import StatsDeltaWriter
from .db_ops import IngestDB

LOGGER = logging.getLogger("ingestor")


@contextmanager
def exclusive(path: Path, *, nonblocking=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class ShardWriter:
    """One-shard transactions; both components must share the lock directory.

    This coordinates ingestion processes on a shared POSIX filesystem. Other
    database writers still use database concurrency control as before.
    """
    def __init__(self, db, worker_id, shards_per_worker, lock_dir, discovery):
        self.db = db
        self.worker_id = worker_id
        self.shards_per_worker = shards_per_worker
        self.lock_dir = Path(lock_dir)
        self.discovery = discovery

    def validate(self, record):
        sid = int(record["shard_id"])
        if sid // self.shards_per_worker != self.worker_id:
            raise ValueError(f"shard {sid} is not owned by worker {self.worker_id}")
        if (record.get("status") == "new") != self.discovery:
            raise ValueError("record sent to the wrong ingestion component")
        return sid

    def process_batch(self, records):
        if not records:
            return []
        shards = {self.validate(r) for r in records}
        if len(shards) != 1:
            raise ValueError("ingestion transactions must contain exactly one shard")
        sid = shards.pop()
        started = time.monotonic()
        with exclusive(self.lock_dir / f"shard_{sid:03d}.lock"):
            waited = time.monotonic() - started
            # Preserve repeated-result order; discoveries have unique URLs.
            if self.discovery:
                records = sorted(records, key=lambda r: r["url"])
            stages = defaultdict(float)
            self.db.timing_observer = lambda name, seconds: stages.__setitem__(name, stages[name]+seconds)
            try:
                result = self.db.process_batch(records)
            except Exception as exc:
                LOGGER.error("ingest.batch_error", extra={"event": "ingest.batch_error",
                             "batch_kind": "discovery" if self.discovery else "result",
                             "error": str(exc), "batch_size": len(records), "shard_id": sid})
                raise
            finally:
                self.db.timing_observer = None
        LOGGER.info("ingest.shard_write", extra={"event": "ingest.shard_write",
                    "shard_id": sid, "records": len(records),
                    "batch_kind": "discovery" if self.discovery else "result",
                    "lock_wait_s": round(waited, 3),
                    "stage_seconds": {k: round(v, 4) for k, v in stages.items()},
                    "duration_s": round(time.monotonic() - started, 3)})
        return result


class Checkpoint:
    def __init__(self, path, legacy_path, peer_path):
        self.progress = Progress(str(path))
        self.legacy_path = Path(legacy_path)
        self.peer_path = Path(peer_path)
        if self.progress.state["date"] is None:
            # Seed only on first migration. Never reset an existing checkpoint.
            self.progress.state = dict(Progress(str(legacy_path)).state)
            atomic_write_json(str(self.progress.path), self.progress.state)

    def advance(self, date, bucket):
        if self.progress.seen(date, bucket):
            return
        self.progress.state = {"date": date, "time": bucket}
        atomic_write_json(str(self.progress.path), self.progress.state)
        # Keep the old cleanup script safe during a worker-by-worker rollout.
        with exclusive(self.legacy_path.with_suffix(".lock")):
            peer = Progress(str(self.peer_path)).state
            if peer["date"] is None:
                return
            mark = min((date, bucket), (peer["date"], peer["time"]))
            old = Progress(str(self.legacy_path))
            if not old.seen(*mark):
                atomic_write_json(str(self.legacy_path), {"date": mark[0], "time": mark[1]})


def run(kind):
    import argparse
    from .main import _load_inline_ranker, _load_trap_filter, _inline_score_timeout_sec, _inline_score_batch_size
    from ..discovery_ingestor.service import DiscoveryIngestor
    from ..content_ingestor.service import ContentIngestor

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--worker-id", type=int, required=True)
    args = parser.parse_args()
    raw = load_yaml(args.config)
    settings = require(raw, kind)
    legacy = require(raw, "ingestor")
    shards_per_worker = int(require(raw["router"], "shards_per_ingestor"))
    num_shards = int(require(raw["router"], "num_shards"))
    if not 0 <= args.worker_id < (num_shards + shards_per_worker - 1) // shards_per_worker:
        raise ValueError("worker id outside configured shard range")
    configure(service="ingestor", worker_id=args.worker_id)
    LOGGER.info("ingest.component_start", extra={"event": "ingest.component_start", "component": kind})
    peer_kind = "content_ingestor" if kind == "discovery_ingestor" else "discovery_ingestor"
    fmt = lambda value: str(value).format(id=args.worker_id)
    lock_dir = Path(settings.get("lock_dir", "/data/ipc/locks/ingestion"))
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with exclusive(lock_dir / f"{kind}_{args.worker_id:02d}.lock", nonblocking=True):
        # An old combined worker must be stopped before starting this pair.
        checkpoint = Checkpoint(fmt(require(settings, "progress_template")),
                                fmt(require(legacy, "progress_template")),
                                fmt(require(raw[peer_kind], "progress_template")))
        reader = FolderReader(fmt(require(settings, "result_dir_template")), checkpoint.progress,
                              int(settings.get("interval_minutes", 10)))
        engine = create_engine(str(require(raw["postgres"], "dsn")), pool_pre_ping=True,
                               pool_recycle=1800, pool_size=1, max_overflow=0,
                               connect_args={"keepalives": 1, "keepalives_idle": 30,
                                             "keepalives_interval": 5, "keepalives_count": 5})
        is_discovery = kind == "discovery_ingestor"
        db = IngestDB(sessionmaker(bind=engine, autoflush=False, future=True),
                      inline_ranker=_load_inline_ranker(raw) if is_discovery else None,
                      inline_score_timeout_sec=_inline_score_timeout_sec(raw),
                      inline_score_batch_size=_inline_score_batch_size(raw))
        writer = ShardWriter(db, args.worker_id, shards_per_worker, lock_dir, is_discovery)
        kwargs = dict(db=writer, stats=StatsDeltaWriter(require(settings, "stats_dir")),
                      checkpoint=checkpoint, max_buffer_bytes=int(settings.get("max_buffer_mib", 2048 if is_discovery else 64)) * 1024**2)
        if is_discovery:
            service = DiscoveryIngestor(**kwargs, trap_filter=_load_trap_filter(raw, args.worker_id),
                       distinct_url_limit=int(settings.get("distinct_urls_per_shard", 64000)),
                       idle_seconds=float(settings.get("idle_flush_seconds", 600)),
                       max_age_seconds=float(settings.get("max_buffer_age_seconds", 600)))
        else:
            service = ContentIngestor(**kwargs)
        try:
            while not stopping:
                progressed = False
                for date, bucket, folder in reader.iter_ready_folders():
                    if stopping:
                        break
                    if service.seen(date, bucket):
                        continue
                    service.process_folder(folder, date, bucket)
                    progressed = True
                service.flush_if_due()
                if not progressed and not stopping:
                    time.sleep(float(settings.get("poll_seconds", 1)))
            service.close()
        finally:
            try:
                if is_discovery and service.trap_filter is not None:
                    service.trap_filter.close()
            finally:
                engine.dispose()

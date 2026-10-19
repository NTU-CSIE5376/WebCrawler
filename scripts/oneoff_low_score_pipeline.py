#!/usr/bin/env python3
"""
One-off, shard by shard: DELETE low-score pending URLs -> VACUUM -> REINDEX CONCURRENTLY,
watching free space on the database disk. Run it ON THE DB HOST (it reads free space of
--disk-path with statvfs).

DELETE rules and batching are those of scripts/oneoff_delete_low_score.py (imported, not copied):
should_crawl AND source = 0 AND last_scheduled IS NULL AND last_fetch_ok IS NULL AND
url_score <= T AND not a metricdb golden URL; only url_state_current_NNN is touched.

Why REINDEX: DELETE and VACUUM leave every file the same size (the space is only reusable
inside the table). Rebuilding each index packs it again and returns the difference to the
OS; on a local copy of prod data, removing 30% of rows let the indexes shrink by ~54%.
Each REINDEX INDEX CONCURRENTLY needs at most the index's current size in free space.

Disk guard (every DELETE batch, before VACUUM, before each REINDEX):
  free < --min-free           -> pause, poll, resume once free >= --resume-free
  free - index size < min-free -> wait before rebuilding that index
  free < --abort-free while VACUUM/REINDEX runs -> cancel the statement (leftover *_ccnew
                                  indexes are dropped), then pause as above
Progress per shard is kept in <state-dir>/shard_NNN.json (stage: delete -> vacuum -> reindex
-> done); Ctrl-C stops after the current step (twice: cancel the running statement) and the
same command resumes. Timings and sizes of every step are appended to <state-dir>/report.csv.

Pause the golden rescorer first: its non-HOT updates otherwise use up the space VACUUM frees.

    scripts/oneoff_low_score_pipeline.py --shards 5                      # dry-run: sizes, free space, owner check
    scripts/oneoff_low_score_pipeline.py --execute --threshold 0.025232553482055664 --shards 5
    scripts/oneoff_low_score_pipeline.py --execute --threshold 0.025232553482055664 --shards 0-255
"""
from __future__ import annotations

import argparse
import csv
import pathlib
import shutil
import signal
import sys
import threading
import time

import psycopg2
from psycopg2 import errors, sql

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from scripts import oneoff_delete_low_score as L
    from scripts.constants import NUM_SHARDS
except ModuleNotFoundError:
    import oneoff_delete_low_score as L
    from constants import NUM_SHARDS

GIB = 2**30
REPORT_FIELDS = ["time", "shard", "step", "seconds", "free_before", "free_after", "heap_before", "heap_after",
                 "index_before", "index_after", "rows_deleted", "note"]


def parse_size(s: str) -> int:
    units = {"K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40}
    s = s.strip().upper().rstrip("B")
    return int(float(s[:-1]) * units[s[-1]]) if s and s[-1] in units else int(s)


def gib(b: float) -> str:
    return f"{b / GIB:,.1f}G"


class DiskGuard:
    """Free space checks on the database disk. `usage` and `sleep` are injectable for tests."""

    def __init__(self, path: str, min_free: int, resume_free: int, abort_free: int,
                 poll: float = 60.0, max_wait: float = 6 * 3600, usage=shutil.disk_usage, sleep=time.sleep):
        if not abort_free < min_free <= resume_free:
            raise SystemExit("need --abort-free < --min-free <= --resume-free")
        self.path, self.min_free, self.resume_free, self.abort_free = path, min_free, resume_free, abort_free
        self.poll, self.max_wait, self.usage, self.sleep = poll, max_wait, usage, sleep

    def free(self) -> int:
        return self.usage(self.path).free

    def wait(self, need: int = 0, what: str = "") -> None:
        """Return once free - need >= min_free (after a pause: >= resume_free)."""
        free = self.free()
        if free - need >= self.min_free:
            return
        target = self.resume_free
        print(f"  disk: {gib(free)} free on {self.path}, {what or 'next step'} needs {gib(need)} + "
              f"{gib(self.min_free)} margin; pausing until {gib(target + need)} free", flush=True)
        waited = 0.0
        while not L._stop:
            self.sleep(self.poll)
            waited += self.poll
            free = self.free()
            if free - need >= target:
                print(f"  disk: {gib(free)} free, resuming", flush=True)
                return
            if waited >= self.max_wait:
                raise SystemExit(f"disk: still only {gib(free)} free on {self.path} after {waited / 3600:.1f}h "
                                 f"({what or 'next step'} needs {gib(need)} + margin); stopping, rerun to resume")
            if waited % (10 * self.poll) < self.poll:
                print(f"  disk: {gib(free)} free, still waiting", flush=True)


class Watchdog:
    """While a long statement runs, cancel it if free space drops under abort_free."""

    def __init__(self, conn, guard: DiskGuard, interval: float = 10.0):
        self.conn, self.guard, self.interval = conn, guard, interval
        self.fired = False
        self._done = threading.Event()

    def _run(self):
        while not self._done.wait(self.interval):
            free = self.guard.free()
            if free < self.guard.abort_free:
                print(f"  disk: {gib(free)} free < abort {gib(self.guard.abort_free)}, cancelling statement",
                      flush=True)
                self.fired = True
                self.conn.cancel()
                return

    def __enter__(self):
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._done.set()
        self._t.join()


class Pipeline:
    def __init__(self, connect, guard: DiskGuard, state_dir: pathlib.Path, threshold: float,
                 golden: dict[int, list[str]], batch_pages: int = L.DEFAULT_BATCH_PAGES, sleep: float = 0.0,
                 max_rows: float = 0, skip_reindex: bool = False, reindex_min_bytes: int = 100 * 2**20,
                 lock_retries: int = 3, retry_sleep: float = 30.0, watch_interval: float = 10.0):
        self.connect, self.guard, self.state_dir, self.t = connect, guard, state_dir, threshold
        self.golden, self.batch_pages, self.sleep = golden, batch_pages, sleep
        self.budget = [max_rows if max_rows else float("inf")]
        self.skip_reindex, self.reindex_min_bytes = skip_reindex, reindex_min_bytes
        self.lock_retries, self.retry_sleep, self.watch_interval = lock_retries, retry_sleep, watch_interval
        self.active_conn = None
        state_dir.mkdir(parents=True, exist_ok=True)
        self.report = state_dir / "report.csv"

    # --- helpers -----------------------------------------------------------
    def _log(self, **row) -> None:
        new = not self.report.exists()
        with open(self.report, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
            if new:
                w.writeheader()
            w.writerow({"time": time.strftime("%Y-%m-%dT%H:%M:%S%z"), **row})

    @staticmethod
    def sizes(cur, tbl: str) -> tuple[int, int]:
        cur.execute("SELECT pg_relation_size(%s::regclass), pg_indexes_size(%s::regclass)", (tbl, tbl))
        return cur.fetchone()

    @staticmethod
    def indexes(cur, tbl: str) -> list[tuple[str, int, bool]]:
        cur.execute("""SELECT i.relname, pg_relation_size(i.oid), x.indisvalid
                       FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
                       WHERE x.indrelid = %s::regclass ORDER BY 2, 1""", (tbl,))
        return cur.fetchall()

    def _autocommit_conn(self):
        conn = self.connect()
        conn.rollback()
        conn.autocommit = True
        conn.notices = []
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
        return conn

    def _long(self, conn, statement) -> tuple[float, bool]:
        """Run one long statement under the watchdog. Returns (seconds, cancelled_for_disk)."""
        self.active_conn = conn
        t0 = time.time()
        try:
            with Watchdog(conn, self.guard, self.watch_interval) as wd, conn.cursor() as cur:
                try:
                    cur.execute(statement)
                except errors.QueryCanceled:
                    if wd.fired or L._stop:
                        return time.time() - t0, True
                    raise
        finally:
            self.active_conn = None
        return time.time() - t0, False

    def drop_leftovers(self, conn, tbl: str) -> None:
        """Invalid *_ccnew / *_ccold indexes are what a cancelled REINDEX CONCURRENTLY leaves behind."""
        with conn.cursor() as cur:
            for name, size, valid in self.indexes(cur, tbl):
                if not valid and ("_ccnew" in name or "_ccold" in name):
                    print(f"  dropping leftover invalid index {name} ({gib(size)})", flush=True)
                    cur.execute(sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(sql.Identifier(name)))

    # --- stages ------------------------------------------------------------
    def run_shard(self, shard: int) -> bool:
        """Advance one shard as far as possible. Returns False when the run should stop."""
        tbl = L.table(shard)
        st = L.ShardState(self.state_dir, shard, self.t)
        st.d.setdefault("stage", "delete")
        st.d.setdefault("reindexed", [])
        st.save()
        if st.d["stage"] == "done":
            return True
        print(f"shard {shard:03d}: stage {st.d['stage']}", flush=True)
        if st.d["stage"] == "delete" and not self.stage_delete(shard, tbl, st):
            return False
        if st.d["stage"] == "vacuum" and not self.stage_vacuum(shard, tbl, st):
            return False
        if st.d["stage"] == "reindex" and not self.stage_reindex(shard, tbl, st):
            return False
        return not L._stop

    def stage_delete(self, shard: int, tbl: str, st) -> bool:
        conn = self.connect()
        cur = conn.cursor()
        heap0, idx0 = self.sizes(cur, tbl)
        conn.commit()
        free0, n0, t0 = self.guard.free(), st.d["deleted"], time.time()
        for attempt in range(4):
            try:
                L.delete_shard(conn, tbl, self.t, self.golden.get(shard, []), st, self.batch_pages, self.budget,
                               self.sleep, before_batch=lambda: self.guard.wait(what=f"{tbl} DELETE batch"))
                break
            except psycopg2.OperationalError as e:
                print(f"  {tbl}: connection error ({str(e).strip()}), reconnecting", flush=True)
                time.sleep(30 * (attempt + 1))
                conn = self.connect()
        else:
            raise SystemExit(f"{tbl}: giving up after connection errors; rerun to resume")
        cur = conn.cursor()
        heap1, idx1 = self.sizes(cur, tbl)
        conn.commit()
        conn.close()
        self._log(shard=shard, step="delete", seconds=round(time.time() - t0, 1), free_before=free0,
                  free_after=self.guard.free(), heap_before=heap0, heap_after=heap1, index_before=idx0,
                  index_after=idx1, rows_deleted=st.d["deleted"] - n0, note="" if st.d["done"] else "paused")
        print(f"  {tbl}: deleted {st.d['deleted']:,} so far ({'done' if st.d['done'] else 'paused'})", flush=True)
        if not st.d["done"]:
            return False
        st.d["stage"] = "vacuum"
        st.save()
        return True

    def stage_vacuum(self, shard: int, tbl: str, st) -> bool:
        while not L._stop:
            self.guard.wait(what=f"VACUUM {tbl}")
            if L._stop:
                break
            conn = self._autocommit_conn()
            with conn.cursor() as cur:
                heap0, idx0 = self.sizes(cur, tbl)
            free0 = self.guard.free()
            print(f"  VACUUM (VERBOSE) {tbl}", flush=True)
            try:
                secs, cancelled = self._long(conn, sql.SQL("VACUUM (VERBOSE) {}").format(sql.Identifier(tbl)))
            except errors.LockNotAvailable:
                conn.close()
                print(f"  {tbl}: lock timeout, retrying in {self.retry_sleep:.0f}s", flush=True)
                time.sleep(self.retry_sleep)
                continue
            (self.state_dir / f"shard_{shard:03d}_vacuum.log").write_text("".join(conn.notices))
            with conn.cursor() as cur:
                heap1, idx1 = self.sizes(cur, tbl)
            conn.close()
            self._log(shard=shard, step="vacuum", seconds=round(secs, 1), free_before=free0,
                      free_after=self.guard.free(), heap_before=heap0, heap_after=heap1, index_before=idx0,
                      index_after=idx1, note="cancelled" if cancelled else "")
            if cancelled:
                print(f"  VACUUM {tbl} cancelled after {secs:,.0f}s", flush=True)
                continue        # disk: guard.wait() pauses at the top; Ctrl-C: loop exits
            print(f"  VACUUM {tbl}: {secs:,.0f}s", flush=True)
            st.d["stage"] = "reindex" if not self.skip_reindex else "done"
            st.save()
            return True
        return False

    def stage_reindex(self, shard: int, tbl: str, st) -> bool:
        conn = self._autocommit_conn()
        try:
            self.drop_leftovers(conn, tbl)
            with conn.cursor() as cur:
                todo = [(n, s) for n, s, valid in self.indexes(cur, tbl)
                        if valid and n not in st.d["reindexed"] and s >= self.reindex_min_bytes]
            for name, size in todo:
                retries = 0
                while True:
                    if L._stop:
                        return False
                    self.guard.wait(need=size, what=f"REINDEX {name}")
                    if L._stop:
                        return False
                    free0 = self.guard.free()
                    print(f"  REINDEX INDEX CONCURRENTLY {name} ({gib(size)}), {gib(free0)} free", flush=True)
                    try:
                        secs, cancelled = self._long(conn, sql.SQL("REINDEX INDEX CONCURRENTLY {}")
                                                     .format(sql.Identifier(name)))
                    except errors.LockNotAvailable:
                        self.drop_leftovers(conn, tbl)
                        retries += 1
                        if retries > self.lock_retries:
                            raise SystemExit(f"{name}: lock timeout {retries} times; rerun to resume")
                        print(f"  {name}: lock timeout, retrying in {self.retry_sleep:.0f}s", flush=True)
                        time.sleep(self.retry_sleep)
                        continue
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_relation_size(%s::regclass)", (name,))
                        after = cur.fetchone()[0]
                    self._log(shard=shard, step=f"reindex:{name}", seconds=round(secs, 1), free_before=free0,
                              free_after=self.guard.free(), index_before=size,
                              index_after=None if cancelled else after, note="cancelled" if cancelled else "")
                    if cancelled:
                        print(f"  REINDEX {name} cancelled after {secs:,.0f}s", flush=True)
                        self.drop_leftovers(conn, tbl)
                        continue
                    print(f"  {name}: {gib(size)} -> {gib(after)} in {secs:,.0f}s", flush=True)
                    st.d["reindexed"].append(name)
                    st.save()
                    break
            st.d["stage"] = "done"
            st.save()
            return True
        finally:
            conn.close()

    def run(self, shards: list[int]) -> None:
        for s in shards:
            if L._stop or not self.run_shard(s):
                break
        done = [s for s in shards if L.ShardState(self.state_dir, s, self.t).d.get("stage") == "done"]
        print(f"\n{len(done)}/{len(shards)} selected shards done; {gib(self.guard.free())} free on "
              f"{self.guard.path}; steps logged in {self.report}", flush=True)


# --- dry-run -----------------------------------------------------------------

def dry_run(conn, shards: list[int], guard: DiskGuard) -> None:
    cur = conn.cursor()
    free = guard.free()
    print(f"{guard.path}: {gib(free)} free (min {gib(guard.min_free)}, resume {gib(guard.resume_free)}, "
          f"abort {gib(guard.abort_free)})\n")
    print(f"{'table':<24} {'heap':>9} {'indexes':>9} {'largest index':>14} {'owner ok':>9}")
    worst, bad_owner = 0, []
    for s in shards:
        tbl = L.table(s)
        heap, idx = Pipeline.sizes(cur, tbl)
        largest = max((size for _, size, _ in Pipeline.indexes(cur, tbl)), default=0)
        cur.execute("SELECT pg_get_userbyid(relowner) = current_user FROM pg_class WHERE oid = %s::regclass", (tbl,))
        owner = cur.fetchone()[0]
        worst = max(worst, largest)
        if not owner:
            bad_owner.append(tbl)
        print(f"{tbl:<24} {gib(heap):>9} {gib(idx):>9} {gib(largest):>14} {'yes' if owner else 'NO':>9}")
    conn.rollback()
    print(f"\nlargest single index: {gib(worst)}; REINDEX needs free >= that + min-free "
          f"({gib(worst + guard.min_free)}): {'ok' if free >= worst + guard.min_free else 'NOT NOW'}")
    if bad_owner:
        print(f"REINDEX needs table ownership; current user does not own {len(bad_owner)} table(s), "
              f"e.g. {bad_owner[0]} (use --skip-reindex or connect as the owner)")
    print("\ndry-run: nothing changed. Pause the rescorer, then run with --execute --threshold T")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true", help="Run the pipeline (default: read-only dry-run).")
    p.add_argument("--threshold", type=float, help="Delete url_score <= this (required with --execute).")
    p.add_argument("--shards", default=f"0-{NUM_SHARDS - 1}")
    p.add_argument("--disk-path", default="/data", help="Mount point holding the PostgreSQL data directory.")
    p.add_argument("--min-free", default="60G", help="Pause when free space drops below this.")
    p.add_argument("--resume-free", default="80G", help="Resume a pause once free space is back above this.")
    p.add_argument("--abort-free", default="30G", help="Cancel a running VACUUM/REINDEX below this.")
    p.add_argument("--batch-pages", type=int, default=L.DEFAULT_BATCH_PAGES)
    p.add_argument("--sleep", type=float, default=0.0, help="Seconds to pause between DELETE batches.")
    p.add_argument("--max-rows", type=float, default=0, help="Stop deleting after about this many rows; 0 = no limit.")
    p.add_argument("--skip-reindex", action="store_true", help="Only DELETE + VACUUM.")
    p.add_argument("--reindex-min-bytes", default="100M", help="Do not rebuild indexes smaller than this.")
    p.add_argument("--state-dir", default="low_score_pipeline_state")
    args = p.parse_args()
    if args.execute and args.threshold is None:
        raise SystemExit("--execute needs --threshold (get it from scripts/oneoff_delete_low_score.py dry-run)")

    guard = DiskGuard(args.disk_path, parse_size(args.min_free), parse_size(args.resume_free),
                      parse_size(args.abort_free))
    shards = L.parse_shards(args.shards)
    if not args.execute:
        conn = L.connect()
        conn.set_session(readonly=True)
        dry_run(conn, shards, guard)
        conn.close()
        return

    rconn = L.connect()
    rconn.set_session(readonly=True)
    golden = L.load_golden(rconn)
    rconn.close()
    pipe = Pipeline(L.connect, guard, pathlib.Path(args.state_dir), args.threshold, golden,
                    batch_pages=args.batch_pages, sleep=args.sleep, max_rows=args.max_rows,
                    skip_reindex=args.skip_reindex, reindex_min_bytes=parse_size(args.reindex_min_bytes))

    def on_sigint(*_):
        if not L._stop:
            L._stop = True
            print("\nstopping after the current step (Ctrl-C again cancels a running VACUUM/REINDEX)...",
                  flush=True)
        elif pipe.active_conn is not None:
            print("\ncancelling the running statement...", flush=True)
            pipe.active_conn.cancel()
    signal.signal(signal.SIGINT, on_sigint)
    print(f"EXECUTE  url_score <= {args.threshold!r}  shards={args.shards}  disk={args.disk_path} "
          f"({gib(guard.free())} free)  max_rows={args.max_rows or 'none'}  "
          f"reindex={'no' if args.skip_reindex else 'yes'}\n"
          "Make sure the golden rescorer is paused.\n", flush=True)
    pipe.run(shards)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
One-off: delete the lowest-scored pending URLs (about --target rows, default 2e9).

Deletes rows with should_crawl = TRUE AND source = 0 AND url_score <= T that were
never scheduled or fetched (last_scheduled IS NULL AND last_fetch_ok IS NULL). Golden
URLs are kept: every injected row has source <> 0, and organically discovered
golden URLs (source = 0) are excluded by exact match against metricdb
`metric_url`, routed to their shard with the crawler's ShardRouter.

T is one global threshold, so "lowest score first up to N rows" is the same as
deleting everything at or below T. The dry-run estimates T for --target from a
TABLESAMPLE SYSTEM sample; --execute needs T passed explicitly with --threshold.

No backup of deleted rows. Each shard is walked once in heap-block ranges (TID
range scan), one transaction per batch. Progress, T and the deleted count are
saved to <state-dir>/shard_NNN.json, so the run can be stopped (Ctrl-C) and
resumed with the same command; resuming with a different T is refused.

    scripts/oneoff_delete_low_score.py                                   # dry-run: estimate T for 2e9
    scripts/oneoff_delete_low_score.py --target 1.5e9                    # dry-run for another target
    scripts/oneoff_delete_low_score.py --execute --threshold 0.0253 --shards 0
    scripts/oneoff_delete_low_score.py --execute --threshold 0.0253 --max-rows 300000000 --vacuum
"""
from __future__ import annotations

import argparse
import bisect
import json
import pathlib
import signal
import sys
import time
from collections import defaultdict

import psycopg2

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from libs.db.sharding.key import load_sharding_config  # noqa: E402
from libs.db.sharding.router import ShardRouter  # noqa: E402

try:
    from scripts.constants import CRAWLERDB, METRICDB, NUM_SHARDS, SOURCE_NATURAL
except ModuleNotFoundError:
    from constants import CRAWLERDB, METRICDB, NUM_SHARDS, SOURCE_NATURAL

INGEST_YAML = ROOT / "containers" / "scheduler_ingest" / "config" / "ingest.yaml"
SHARDS_PER_INGESTOR = 16
DEFAULT_BATCH_PAGES = 2048        # 16 MB of heap per statement
DEFAULT_SAMPLE_PCT = 0.05
DEFAULT_TARGET = 2_000_000_000
_stop = False

# Never-scheduled, never-fetched natural pending rows that are not golden URLs. Such rows have
# no url_event_counter / content_feature_current rows, so only url_state_current is touched.
ELIGIBLE = (f"{{a}}.should_crawl AND {{a}}.source = {SOURCE_NATURAL} "
            f"AND {{a}}.last_scheduled IS NULL AND {{a}}.last_fetch_ok IS NULL "
            f"AND NOT ({{a}}.url = ANY(%(golden)s))")

DELETE_SQL = f"""
DELETE FROM {{table}} t
WHERE t.ctid >= %(lo)s::tid AND t.ctid < %(hi)s::tid
  AND {ELIGIBLE.format(a="t")}
  AND t.url_score <= %(t)s
"""

SAMPLE_SQL = f"""
SELECT c.url_score
FROM {{table}} c TABLESAMPLE SYSTEM (%(pct)s) REPEATABLE (42)
WHERE {ELIGIBLE.format(a="c")} AND c.url_score IS NOT NULL
"""


def parse_shards(spec: str) -> list[int]:
    out: list[int] = []
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out.extend(range(int(a), int(b or a) + 1))
    bad = [s for s in out if not 0 <= s < NUM_SHARDS]
    if bad:
        raise SystemExit(f"shard out of range: {bad}")
    return out


def table(shard: int) -> str:
    return f"url_state_current_{shard:03d}"


def connect():
    conn = psycopg2.connect(**CRAWLERDB, application_name="low-score-delete")
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '10min'")
        cur.execute("SET lock_timeout = '5s'")
        # The DB host is disk-constrained; avoid parallel workers' shm segments.
        cur.execute("SET max_parallel_workers_per_gather = 0")
        cur.execute("SET jit = off")
    conn.commit()
    return conn


def load_golden(crawler_conn) -> dict[int, list[str]]:
    """Every metricdb golden URL, grouped by the shard the crawler routes it to."""
    mconn = psycopg2.connect(**METRICDB, application_name="low-score-delete")
    mconn.set_session(readonly=True, autocommit=True)
    with mconn.cursor() as cur:
        cur.execute("SELECT DISTINCT url FROM metric_url WHERE url ~ '^https?://'")
        urls = [r[0] for r in cur.fetchall()]
    mconn.close()
    overrides, split = load_sharding_config(INGEST_YAML, crawler_conn)
    crawler_conn.rollback()
    router = ShardRouter(num_shards=NUM_SHARDS, shards_per_ingestor=SHARDS_PER_INGESTOR,
                         domain_overrides=overrides, split_subdomains=split)
    by_shard: dict[int, list[str]] = defaultdict(list)
    for u in urls:
        by_shard[router.route(u)[1]].append(u)
    print(f"golden URLs from metricdb: {len(urls):,} (kept wherever found)", flush=True)
    return by_shard


# --- dry-run ---------------------------------------------------------------

def dry_run(shards: list[int], golden: dict[int, list[str]], target: float, pct: float) -> None:
    conn = connect()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    scale = 100.0 / pct
    per_shard: dict[int, list[float]] = {}
    for s in shards:
        cur.execute(SAMPLE_SQL.format(table=table(s)), {"pct": pct, "golden": golden.get(s, [])})
        per_shard[s] = sorted(r[0] for r in cur.fetchall())
    scores = sorted(x for v in per_shard.values() for x in v)
    eligible = len(scores) * scale
    k = min(len(scores), max(1, round(target / scale)))
    t = scores[k - 1] if scores else 0.0
    print(f"{'shard':>5} {'eligible_est':>14} {'delete_est':>13}")
    for s in shards:
        n_del = bisect.bisect_right(per_shard[s], t)
        print(f"{s:>5} {len(per_shard[s]) * scale:>14,.0f} {n_del * scale:>13,.0f}")
    print(f"\nestimate from TABLESAMPLE SYSTEM ({pct}%), {len(scores):,} sampled rows")
    print(f"  eligible pending (source=0, never scheduled or fetched, not golden): {eligible:,.0f}")
    print(f"  target: {target:,.0f}")
    print(f"  threshold T (url_score <= T): {t!r}")
    print(f"  would delete: {bisect.bisect_right(scores, t) * scale:,.0f}")
    kept = 0
    for s in shards:
        if golden.get(s):
            cur.execute(f"SELECT count(*) FROM {table(s)} WHERE url = ANY(%s) AND should_crawl "
                        f"AND source = {SOURCE_NATURAL} AND url_score <= %s", (golden[s], t))
            kept += cur.fetchone()[0]
    print(f"  golden source=0 pending rows at or below T (kept): {kept:,}")
    print(f"\ndry-run: nothing changed. Delete with:\n  {sys.argv[0]} --execute --threshold {t!r}")


# --- execute ---------------------------------------------------------------

class ShardState:
    def __init__(self, state_dir: pathlib.Path, shard: int, threshold: float):
        self.path = state_dir / f"shard_{shard:03d}.json"
        self.d = {"next_block": 0, "deleted": 0, "done": False, "threshold": threshold}
        if self.path.exists():
            self.d = json.loads(self.path.read_text())
            if self.d["threshold"] != threshold:
                raise SystemExit(f"{self.path}: started with threshold {self.d['threshold']!r}, "
                                 f"now {threshold!r}; use another --state-dir or delete the state file")

    def save(self) -> None:
        self.d["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.d))
        tmp.replace(self.path)


def delete_shard(conn, tbl: str, threshold: float, golden: list[str], st: ShardState,
                 batch_pages: int, budget: list[float], sleep: float = 0.0) -> None:
    cur = conn.cursor()
    cur.execute("SELECT pg_relation_size(%s::regclass) / current_setting('block_size')::int", (tbl,))
    nblocks = cur.fetchone()[0]
    conn.commit()
    sql = DELETE_SQL.format(table=tbl)
    t0, start_deleted = time.time(), st.d["deleted"]
    while st.d["next_block"] < nblocks and not _stop and budget[0] > 0:
        lo = st.d["next_block"]
        hi = lo + batch_pages
        try:
            cur.execute(sql, {"lo": f"({lo},0)", "hi": f"({hi},0)", "t": threshold, "golden": golden})
            n = cur.rowcount
            conn.commit()
        except psycopg2.errors.LockNotAvailable:
            conn.rollback()
            print(f"  {tbl} blocks {lo}-{hi}: lock timeout, retrying", flush=True)
            time.sleep(5)
            continue
        st.d["next_block"] = hi
        st.d["deleted"] += n
        st.save()
        budget[0] -= n
        if sleep:
            time.sleep(sleep)
        if (hi // batch_pages) % 50 == 0:
            rate = (st.d["deleted"] - start_deleted) / max(time.time() - t0, 1e-9)
            print(f"  {tbl} {hi / nblocks * 100:5.1f}%  deleted {st.d['deleted']:,}  "
                  f"({rate:,.0f} rows/s)", flush=True)
    if st.d["next_block"] >= nblocks:
        st.d["done"] = True
        st.save()


def execute(shards: list[int], golden: dict[int, list[str]], args) -> None:
    state_dir = pathlib.Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    conn = connect()
    budget = [args.max_rows if args.max_rows else float("inf")]
    touched = []
    for s in shards:
        st = ShardState(state_dir, s, args.threshold)
        if st.d["done"]:
            continue
        if _stop or budget[0] <= 0:
            break
        print(f"shard {s:03d}: from block {st.d['next_block']}", flush=True)
        for attempt in range(4):
            try:
                delete_shard(conn, table(s), args.threshold, golden.get(s, []), st,
                             args.batch_pages, budget, args.sleep)
                break
            except psycopg2.OperationalError as e:
                print(f"  shard {s:03d}: connection error ({str(e).strip()}), reconnecting", flush=True)
                time.sleep(30 * (attempt + 1))
                conn = connect()
        else:
            raise SystemExit(f"shard {s:03d}: giving up; rerun to resume")
        touched.append(s)
        print(f"shard {s:03d}: {'done' if st.d['done'] else 'paused'}, deleted {st.d['deleted']:,}",
              flush=True)
        if st.d["done"] and args.vacuum:
            conn.autocommit = True
            print(f"  VACUUM {table(s)}", flush=True)
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")
                cur.execute(f"VACUUM {table(s)}")
                cur.execute("SET statement_timeout = '10min'")
            conn.autocommit = False
    conn.close()
    total = sum(ShardState(state_dir, s, args.threshold).d["deleted"] for s in shards)
    print(f"\ndeleted so far on selected shards: {total:,}  (progress in {state_dir}/)")
    if touched and not args.vacuum:
        print("Deleted rows become reusable space only after VACUUM; autovacuum does not keep up on "
              "the large shards, so run e.g.:")
        for s in touched:
            print(f"  VACUUM {table(s)};")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true", help="Delete (default is a sampled dry-run).")
    p.add_argument("--threshold", type=float, help="Delete url_score <= this (required with --execute).")
    p.add_argument("--target", type=float, default=DEFAULT_TARGET, help="Dry-run: rows to delete.")
    p.add_argument("--shards", default=f"0-{NUM_SHARDS - 1}")
    p.add_argument("--batch-pages", type=int, default=DEFAULT_BATCH_PAGES)
    p.add_argument("--sleep", type=float, default=0.0, help="Seconds to pause between batches.")
    p.add_argument("--max-rows", type=float, default=0,
                   help="Stop after about this many deletes (e.g. a daily quota); 0 = no limit.")
    p.add_argument("--state-dir", default="low_score_delete_state")
    p.add_argument("--vacuum", action="store_true", help="VACUUM each shard once it is finished.")
    p.add_argument("--sample-pct", type=float, default=DEFAULT_SAMPLE_PCT, help="Dry-run sample size.")
    args = p.parse_args()
    if args.execute and args.threshold is None:
        raise SystemExit("--execute needs --threshold (get it from a dry-run)")

    shards = parse_shards(args.shards)
    rconn = connect()
    rconn.set_session(readonly=True)
    golden = load_golden(rconn)
    rconn.close()
    if not args.execute:
        dry_run(shards, golden, args.target, args.sample_pct)
        return

    def on_sigint(*_):
        global _stop
        _stop = True
        print("\nstopping after the current batch...", flush=True)
    signal.signal(signal.SIGINT, on_sigint)
    print(f"EXECUTE  url_score <= {args.threshold!r}  shards={args.shards}  batch_pages={args.batch_pages}  "
          f"max_rows={args.max_rows or 'none'}\n", flush=True)
    execute(shards, golden, args)


if __name__ == "__main__":
    main()

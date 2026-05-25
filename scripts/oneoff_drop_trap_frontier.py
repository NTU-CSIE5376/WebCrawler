#!/usr/bin/env python3
"""
One-off: drain the existing trap-domain frontier backlog.

The crawl frontier (url_state_current rows with should_crawl=TRUE) is never
evicted, so a few low-yield eTLD+1s have each accumulated hundreds of millions
of never-fetched rows that dominate disk and skew the shards. This finds those
domains (pending > --cap AND fetch yield < --yield-floor) and:
  1. sets domain_state.discovery_frozen = TRUE and pushes crawl_paused_until
     far out, so the offerer, ingestor, and GC all leave them alone;
  2. deletes their should_crawl=TRUE rows in batches by domain_id, committing
     per batch so it is resumable and never holds a long lock.

Fetched rows are kept. url_state_history only with --include-history. Deletes
release space to the table free list; run pg_repack / VACUUM FULL afterwards to
return it to the OS (the script prints the per-shard commands).

    scripts/oneoff_drop_trap_frontier.py                      # dry-run, list targets
    scripts/oneoff_drop_trap_frontier.py --execute            # freeze + drain frontier
    scripts/oneoff_drop_trap_frontier.py --execute --include-history
"""
from __future__ import annotations

import argparse

import psycopg2

try:
    from scripts.constants import CRAWLERDB, NUM_SHARDS
except ModuleNotFoundError:
    from constants import CRAWLERDB, NUM_SHARDS

DEFAULT_CAP = 1_000_000
DEFAULT_YIELD_FLOOR = 0.01
DEFAULT_SAMPLE_PCT = 0.5
DEFAULT_BATCH = 20_000
PAUSE_INTERVAL = "100 years"


def find_targets(cur, cap: int, floor: float, sample_pct: float) -> list[dict]:
    """Estimate per-domain pending/yield from a per-shard sample, scaled by the
    table row estimate. Sampling keeps this off the full multi-hundred-million
    row scans that exhaust temp space on the (nearly full) DB host."""
    targets: list[dict] = []
    for shard in range(NUM_SHARDS):
        tcur = f"url_state_current_{shard:03d}"
        cur.execute(f"SELECT reltuples::bigint FROM pg_class WHERE relname = '{tcur}'")
        reltuples = cur.fetchone()[0]
        cur.execute(
            f"""
            SELECT domain_id,
                   count(*) AS total,
                   count(*) FILTER (WHERE should_crawl) AS pending,
                   count(*) FILTER (WHERE last_fetch_ok IS NOT NULL) AS ok
            FROM {tcur} TABLESAMPLE SYSTEM (%s)
            GROUP BY domain_id
            """,
            (sample_pct,),
        )
        rows = cur.fetchall()
        sampled = sum(r[1] for r in rows)
        if not sampled:
            continue
        scale = reltuples / sampled
        for domain_id, s_total, s_pending, s_ok in rows:
            pending = int(s_pending * scale)
            yield_rate = s_ok / s_total if s_total else 0.0
            if pending > cap and yield_rate < floor:
                cur.execute(
                    "SELECT domain FROM domain_state WHERE domain_id = %s", (domain_id,)
                )
                row = cur.fetchone()
                targets.append({
                    "shard": shard, "domain_id": domain_id,
                    "domain": row[0] if row else "?",
                    "pending": pending, "total": int(s_total * scale),
                    "ok": int(s_ok * scale), "yield": yield_rate,
                })
    return targets


def freeze_and_pause(cur, domain_id: int) -> None:
    cur.execute(
        f"""
        UPDATE domain_state
        SET discovery_frozen = TRUE,
            crawl_paused_until = now() + INTERVAL '{PAUSE_INTERVAL}'
        WHERE domain_id = %s
        """,
        (domain_id,),
    )


def drain_frontier(conn, cur, t: dict, batch: int, include_history: bool) -> int:
    tcur = f"url_state_current_{t['shard']:03d}"
    deleted = 0
    while True:
        cur.execute(
            f"""
            WITH picked AS (
                SELECT ctid FROM {tcur}
                WHERE domain_id = %s AND should_crawl = TRUE
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {tcur} WHERE ctid IN (SELECT ctid FROM picked)
            """,
            (t["domain_id"], batch),
        )
        n = cur.rowcount
        conn.commit()
        deleted += n
        if n == 0:
            break

    if include_history:
        thist = f"url_state_history_{t['shard']:03d}"
        while True:
            cur.execute(
                f"""
                WITH picked AS (
                    SELECT ctid FROM {thist}
                    WHERE domain_id = %s
                    LIMIT %s
                )
                DELETE FROM {thist} WHERE ctid IN (SELECT ctid FROM picked)
                """,
                (t["domain_id"], batch),
            )
            n = cur.rowcount
            conn.commit()
            if n == 0:
                break
    return deleted


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Freeze targets and delete their frontier.")
    p.add_argument("--include-history", action="store_true",
                   help="Also delete url_state_history rows for the targets.")
    p.add_argument("--cap", type=int, default=DEFAULT_CAP)
    p.add_argument("--yield-floor", type=float, default=DEFAULT_YIELD_FLOOR)
    p.add_argument("--sample-pct", type=float, default=DEFAULT_SAMPLE_PCT)
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    args = p.parse_args()

    print(f"mode: {'DRY-RUN' if args.dry_run else 'EXECUTE'}  "
          f"cap={args.cap:,}  yield_floor={args.yield_floor}  "
          f"include_history={args.include_history}\n")

    conn = psycopg2.connect(**CRAWLERDB)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")
    # The DB host is disk-constrained; parallel workers' shared-memory segments
    # can fail to allocate. Single-threaded scans avoid that.
    cur.execute("SET max_parallel_workers_per_gather = 0")

    targets = find_targets(cur, args.cap, args.yield_floor, args.sample_pct)
    targets.sort(key=lambda t: -t["pending"])

    print(f"{'shard':>5} {'domain':30} {'pending':>14} {'ever_ok':>10} {'yield':>7}")
    total_pending = 0
    for t in targets:
        total_pending += t["pending"]
        print(f"{t['shard']:>5} {t['domain']:30} {t['pending']:>14,} "
              f"{t['ok']:>10,} {t['yield']*100:>6.2f}%")
    print(f"\n{len(targets)} target domains, {total_pending:,} frontier rows to drop")

    if args.dry_run:
        print("\ndry-run: nothing changed. Re-run with --execute to apply.")
        return

    grand = 0
    for t in targets:
        freeze_and_pause(cur, t["domain_id"])
        n = drain_frontier(conn, cur, t, args.batch, args.include_history)
        grand += n
        print(f"  drained {t['domain']:30} shard {t['shard']:03d}: {n:,} rows")
    print(f"\ndeleted {grand:,} frontier rows across {len(targets)} domains")

    shards = sorted({t["shard"] for t in targets})
    print("\nNext: reclaim the freed space to the OS on the affected shards "
          "(deletes only released it to the table free list):")
    for s in shards:
        print(f"  pg_repack -t url_state_current_{s:03d}   # or VACUUM FULL in a window")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

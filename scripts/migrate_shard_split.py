#!/usr/bin/env python3
"""
Move url_state rows for eTLD+1s listed in shard_split.yaml to per-hostname
shards. Migrates url_state_current / history / url_event_counter;
content_feature_* and domain_stats_daily start fresh.

--execute requires scheduler_ingest to be paused.

    scripts/migrate_shard_split.py             # dry-run
    scripts/migrate_shard_split.py --execute   # actually move rows
"""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

import psycopg2

from constants import CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import compute_shard, load_sharding_config

REPO = Path(__file__).resolve().parents[1]
SPLIT_CFG = REPO / "containers/scheduler_ingest/config/shard_split.yaml"
INGEST_CFG = REPO / "containers/scheduler_ingest/config/ingest.yaml"

HOST_EXTRACT = "LOWER(SUBSTRING(url FROM '^https?://([^/:]+)'))"


def load_num_shards() -> int:
    cfg = load_yaml(str(INGEST_CFG))
    return int(cfg.get("router", {}).get("num_shards", 256))


def fetch_hostname_counts(cur, shard: int, domain_id: int) -> list[tuple[str, int]]:
    cur.execute(
        f"""
        SELECT {HOST_EXTRACT} AS host, COUNT(*)
        FROM url_state_current_{shard:03d}
        WHERE domain_id = %s
        GROUP BY 1 ORDER BY 2 DESC
        """,
        (domain_id,),
    )
    return [(h or "(null)", c) for h, c in cur.fetchall()]


def dry_run_one(cur, etld1: str, num_shards: int,
                overrides: dict[str, int], split_etld1: set[str]) -> None:
    print(f"\n======== {etld1} ========")
    cur.execute("SELECT domain_id, shard_id FROM domain_state WHERE domain = %s", (etld1,))
    row = cur.fetchone()
    if not row:
        print("  not found in domain_state, skip")
        return
    domain_id, cur_shard = row
    print(f"  current: shard={cur_shard:03d}  domain_id={domain_id}")

    cur.execute(f"SELECT COUNT(*) FROM url_state_current_{cur_shard:03d} WHERE domain_id = %s", (domain_id,))
    total = cur.fetchone()[0]
    print(f"  rows in url_state_current_{cur_shard:03d}: {total:,}")
    if total == 0:
        return

    print("  scanning per-hostname breakdown...")
    host_counts = fetch_hostname_counts(cur, cur_shard, domain_id)

    per_host = []
    new_shard_dist: Counter[int] = Counter()
    for host, cnt in host_counts:
        ns = compute_shard(host, num_shards, overrides, split_etld1)
        per_host.append((host, cnt, ns))
        new_shard_dist[ns] += cnt

    rows_move = sum(c for _, c, ns in per_host if ns != cur_shard)
    print(f"  distinct hostnames: {len(per_host):,}")
    print(f"  rows moving: {rows_move:,}  staying: {total - rows_move:,}")

    print("\n  top 20 hostnames:")
    print(f"    {'host':55s} {'rows':>12s} {'new_shard':>10s}")
    for host, cnt, ns in per_host[:20]:
        tag = "  [same]" if ns == cur_shard else ""
        print(f"    {host[:55]:55s} {cnt:>12,} {ns:>10d}{tag}")

    print("\n  projected new shard distribution (top 15):")
    for sid, cnt in new_shard_dist.most_common(15):
        bar = "#" * min(40, int(cnt / max(1, total) * 40))
        print(f"    shard {sid:03d}: {cnt:>12,}  {bar}")
    print(f"  (spread across {len(new_shard_dist)} shards)")

    print("\n  row counts in other shard tables (same domain_id):")
    hist_tbl = f"url_state_history_{cur_shard:03d}"
    evt_tbl = f"url_event_counter_{cur_shard:03d}"
    cur.execute(f"SELECT COUNT(*) FROM {hist_tbl} WHERE domain_id = %s", (domain_id,))
    print(f"    {hist_tbl}: {cur.fetchone()[0]:,}")
    cur.execute(
        f"""SELECT COUNT(*) FROM {evt_tbl} e
             WHERE EXISTS (SELECT 1 FROM url_state_current_{cur_shard:03d} u
                           WHERE u.url = e.url AND u.domain_id = %s)""",
        (domain_id,),
    )
    print(f"    {evt_tbl}: {cur.fetchone()[0]:,}")


def ensure_domain_state(cur, host: str, shard: int) -> int:
    cur.execute(
        """
        INSERT INTO domain_state (domain, shard_id)
        VALUES (%s, %s)
        ON CONFLICT (domain) DO UPDATE SET shard_id = EXCLUDED.shard_id
        RETURNING domain_id
        """,
        (host, shard),
    )
    return int(cur.fetchone()[0])


def move_host(cur, host: str, old_shard: int, old_did: int,
              new_shard: int, new_did: int) -> dict[str, int]:
    """One DELETE ... RETURNING -> INSERT per table. Idempotent: on PK
    conflict the destination row wins and the source is still removed."""
    old_cur = f"url_state_current_{old_shard:03d}"
    new_cur = f"url_state_current_{new_shard:03d}"
    old_hist = f"url_state_history_{old_shard:03d}"
    new_hist = f"url_state_history_{new_shard:03d}"
    old_evt = f"url_event_counter_{old_shard:03d}"
    new_evt = f"url_event_counter_{new_shard:03d}"

    totals = {"current": 0, "history": 0, "event": 0}

    cur.execute(
        f"""
        WITH moved AS (
            DELETE FROM {old_cur}
            WHERE domain_id = %s AND {HOST_EXTRACT} = %s
            RETURNING url, first_seen, last_scheduled, last_fetch_ok,
                      last_content_update, num_scheduled_90d, num_fetch_ok_90d,
                      num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                      last_fail_reason, content_hash, should_crawl, url_score,
                      domain_score, source, discovered_from
        )
        INSERT INTO {new_cur}
        SELECT url, %s AS domain_id, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, source, discovered_from
        FROM moved
        ON CONFLICT (url) DO NOTHING
        """,
        (old_did, host, new_did),
    )
    totals["current"] = cur.rowcount
    cur.connection.commit()

    cur.execute(
        f"""
        WITH moved AS (
            DELETE FROM {old_hist}
            WHERE domain_id = %s AND {HOST_EXTRACT} = %s
            RETURNING url, first_seen, last_scheduled, last_fetch_ok,
                      last_content_update, num_scheduled_90d, num_fetch_ok_90d,
                      num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                      last_fail_reason, content_hash, should_crawl, url_score,
                      domain_score, snapshot_id, snapshot_at, source, discovered_from
        )
        INSERT INTO {new_hist}
        SELECT url, %s AS domain_id, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, snapshot_id, snapshot_at, source, discovered_from
        FROM moved
        ON CONFLICT (snapshot_id) DO NOTHING
        """,
        (old_did, host, new_did),
    )
    totals["history"] = cur.rowcount
    cur.connection.commit()

    cur.execute(
        f"""
        WITH moved AS (
            DELETE FROM {old_evt}
            WHERE {HOST_EXTRACT} = %s
            RETURNING url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
                      num_content_update, accounted
        )
        INSERT INTO {new_evt}
        SELECT url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
               num_content_update, accounted
        FROM moved
        ON CONFLICT (url, event_date) DO NOTHING
        """,
        (host,),
    )
    totals["event"] = cur.rowcount
    cur.connection.commit()

    return totals


def execute_one(cur, etld1: str, num_shards: int,
                overrides: dict[str, int], split_etld1: set[str]) -> None:
    print(f"\n======== EXECUTE {etld1} ========")
    cur.execute("SELECT domain_id, shard_id FROM domain_state WHERE domain = %s", (etld1,))
    row = cur.fetchone()
    if not row:
        print("  not found in domain_state, skip")
        return
    old_did, old_shard = row
    print(f"  source: shard={old_shard:03d} domain_id={old_did}")

    host_counts = fetch_hostname_counts(cur, old_shard, old_did)
    print(f"  {len(host_counts)} hostnames to process")

    grand = {"current": 0, "history": 0, "event": 0}
    for host, cnt in host_counts:
        new_shard = compute_shard(host, num_shards, overrides, split_etld1)
        if new_shard == old_shard and host == etld1:
            print(f"    [skip] {host:50s} stays on shard {new_shard:03d}")
            continue
        new_did = ensure_domain_state(cur, host, new_shard)
        cur.connection.commit()
        stats = move_host(cur, host, old_shard, old_did, new_shard, new_did)
        print(f"    {host:50s} -> shard {new_shard:03d} did={new_did}  "
              f"cur={stats['current']:>9,} hist={stats['history']:>9,} evt={stats['event']:>9,}  "
              f"(~{cnt:,} expected)")
        for k, v in stats.items():
            grand[k] += v

    print(f"\n  totals: url_state_current={grand['current']:,}  "
          f"history={grand['history']:,}  event_counter={grand['event']:,}")


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually move rows. Requires pipeline pause beforehand.")
    args = p.parse_args()

    overrides, split_etld1 = load_sharding_config(INGEST_CFG, SPLIT_CFG)
    targets = sorted(split_etld1)
    if not targets:
        print(f"no targets. edit {SPLIT_CFG}.")
        return

    num_shards = load_num_shards()
    mode = "DRY-RUN" if args.dry_run else "EXECUTE"
    print(f"mode: {mode}")
    print(f"split_etld1: {targets}")
    print(f"overrides (after strip): {overrides}")
    print(f"num_shards: {num_shards}")

    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            for etld1 in targets:
                if args.dry_run:
                    dry_run_one(cur, etld1, num_shards, overrides, split_etld1)
                else:
                    execute_one(cur, etld1, num_shards, overrides, split_etld1)


if __name__ == "__main__":
    main()

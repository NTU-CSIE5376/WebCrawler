#!/usr/bin/env python3
"""
For each entry in ingest.yaml::router.domain_overrides, ensure the parent's
domain_state row sits on the override's shard. If not, move domain_state +
all url_state_current / history / url_event_counter rows for that domain_id
from the old shard tables to the override shard's tables.

Run migrate_shard_split.py first so whitelisted subdomain URLs are already
detached from the parent's domain_id (otherwise they'd be dragged along).

--execute requires scheduler_ingest to be paused.

    scripts/migrate_parent_reshard.py             # dry-run
    scripts/migrate_parent_reshard.py --execute   # actually move rows
"""
from __future__ import annotations

import argparse
from pathlib import Path

import psycopg2

from constants import CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import compute_shard, load_sharding_config

REPO = Path(__file__).resolve().parents[1]
SPLIT_CFG = REPO / "containers/scheduler_ingest/config/shard_split.yaml"
INGEST_CFG = REPO / "containers/scheduler_ingest/config/ingest.yaml"


def load_num_shards() -> int:
    cfg = load_yaml(str(INGEST_CFG))
    return int(cfg.get("router", {}).get("num_shards", 256))


def move_parent(cur, did: int, old_shard: int, new_shard: int) -> dict[str, int]:
    """Move all rows for domain_id=did from old_shard to new_shard. Assumes
    whitelisted subdomain URLs already moved out (see migrate_shard_split.py).
    snapshot_id / snapshot_at on history are dropped so the destination
    sequence assigns fresh values."""
    src_cur = f"url_state_current_{old_shard:03d}"
    src_hist = f"url_state_history_{old_shard:03d}"
    src_evt = f"url_event_counter_{old_shard:03d}"
    dst_cur = f"url_state_current_{new_shard:03d}"
    dst_hist = f"url_state_history_{new_shard:03d}"
    dst_evt = f"url_event_counter_{new_shard:03d}"

    counts = {"current": 0, "history": 0, "event": 0}

    cur.execute(
        f"""
        WITH moved AS (
            DELETE FROM {src_cur}
            WHERE domain_id = %s
            RETURNING url, first_seen, last_scheduled, last_fetch_ok,
                      last_content_update, last_modified,
                      num_scheduled_90d, num_fetch_ok_90d,
                      num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                      last_fail_reason, content_hash, should_crawl, url_score,
                      domain_score, source, discovered_from, title,
                      hreflang_count, etag,
                      cache_control, is_redirect, redirect_hop_count,
                      discovery_source_type, parent_page_score,
                      inlink_count_approx, inlink_count_external, anchor_text,
                      robots_bits
        )
        INSERT INTO {dst_cur}
              (url, domain_id, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, last_modified,
               num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, source, discovered_from, title,
               hreflang_count, etag,
               cache_control, is_redirect, redirect_hop_count,
               discovery_source_type, parent_page_score,
               inlink_count_approx, inlink_count_external, anchor_text,
               robots_bits)
        SELECT url, %s AS domain_id, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, last_modified,
               num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, source, discovered_from, title,
               hreflang_count, etag,
               cache_control, is_redirect, redirect_hop_count,
               discovery_source_type, parent_page_score,
               inlink_count_approx, inlink_count_external, anchor_text,
               robots_bits
        FROM moved
        ON CONFLICT (url) DO NOTHING
        """,
        (did, did),
    )
    counts["current"] = cur.rowcount

    cur.execute(
        f"""
        WITH moved AS (
            DELETE FROM {src_hist}
            WHERE domain_id = %s
            RETURNING url, first_seen, last_scheduled, last_fetch_ok,
                      last_content_update, last_modified,
                      num_scheduled_90d, num_fetch_ok_90d,
                      num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                      last_fail_reason, content_hash, should_crawl, url_score,
                      domain_score, source, discovered_from, title,
                      hreflang_count, etag,
                      cache_control, is_redirect, redirect_hop_count,
                      discovery_source_type, parent_page_score,
                      inlink_count_approx, inlink_count_external, anchor_text,
                      robots_bits
        )
        INSERT INTO {dst_hist}
              (url, domain_id, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, last_modified,
               num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, source, discovered_from, title,
               hreflang_count, etag,
               cache_control, is_redirect, redirect_hop_count,
               discovery_source_type, parent_page_score,
               inlink_count_approx, inlink_count_external, anchor_text,
               robots_bits)
        SELECT url, %s, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, last_modified,
               num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, source, discovered_from, title,
               hreflang_count, etag,
               cache_control, is_redirect, redirect_hop_count,
               discovery_source_type, parent_page_score,
               inlink_count_approx, inlink_count_external, anchor_text,
               robots_bits
        FROM moved
        """,
        (did, did),
    )
    counts["history"] = cur.rowcount

    cur.execute(
        f"""
        WITH moved AS (
            DELETE FROM {src_evt} e
            USING {dst_cur} c
            WHERE e.url = c.url AND c.domain_id = %s
            RETURNING e.url, e.event_date, e.num_scheduled, e.num_fetch_ok,
                      e.num_fetch_fail, e.num_content_update, e.accounted
        )
        INSERT INTO {dst_evt} AS g
              (url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
               num_content_update, accounted)
        SELECT url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
               num_content_update, accounted
        FROM moved
        ON CONFLICT (url, event_date) DO UPDATE SET
            num_scheduled      = g.num_scheduled      + EXCLUDED.num_scheduled,
            num_fetch_ok       = g.num_fetch_ok       + EXCLUDED.num_fetch_ok,
            num_fetch_fail     = g.num_fetch_fail     + EXCLUDED.num_fetch_fail,
            num_content_update = g.num_content_update + EXCLUDED.num_content_update,
            accounted          = g.accounted OR EXCLUDED.accounted
        """,
        (did,),
    )
    counts["event"] = cur.rowcount

    cur.execute("UPDATE domain_state SET shard_id = %s WHERE domain_id = %s",
                (new_shard, did))
    return counts


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually move rows. Requires pipeline pause beforehand.")
    args = p.parse_args()

    overrides, split_subdomains = load_sharding_config(INGEST_CFG, SPLIT_CFG)
    num_shards = load_num_shards()
    mode = "DRY-RUN" if args.dry_run else "EXECUTE"
    print(f"mode: {mode}")
    print(f"overrides: {overrides}")

    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            for domain in sorted(overrides):
                target = compute_shard(domain, num_shards, overrides, split_subdomains)
                cur.execute("SELECT domain_id, shard_id FROM domain_state WHERE domain = %s", (domain,))
                row = cur.fetchone()
                if not row:
                    print(f"\n{domain}: no domain_state row, skip")
                    continue
                did, cur_shard = int(row[0]), int(row[1])
                if cur_shard == target:
                    print(f"\n{domain}: already on shard {target:03d}, skip")
                    continue

                print(f"\n======== {domain}: shard {cur_shard:03d} -> {target:03d} (did={did}) ========")
                src_cur = f"url_state_current_{cur_shard:03d}"
                src_hist = f"url_state_history_{cur_shard:03d}"
                src_evt = f"url_event_counter_{cur_shard:03d}"
                cur.execute(f"SELECT COUNT(*) FROM {src_cur} WHERE domain_id=%s", (did,))
                n_cur = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM {src_hist} WHERE domain_id=%s", (did,))
                n_hist = cur.fetchone()[0]
                cur.execute(
                    f"""SELECT COUNT(*) FROM {src_evt} e
                         WHERE EXISTS (SELECT 1 FROM {src_cur} c
                                        WHERE c.url = e.url AND c.domain_id = %s)""",
                    (did,),
                )
                n_evt = cur.fetchone()[0]
                print(f"  to move: cur={n_cur:,}  hist={n_hist:,}  evt={n_evt:,}")

                if args.dry_run:
                    continue

                stats = move_parent(cur, did, cur_shard, target)
                conn.commit()
                print(f"  moved:   cur={stats['current']:,}  hist={stats['history']:,}  evt={stats['event']:,}")


if __name__ == "__main__":
    main()

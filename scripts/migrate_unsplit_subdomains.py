#!/usr/bin/env python3
"""
Move rows for non-whitelisted subdomains under the eTLD+1s implied by
shard_split.yaml back to the parent eTLD+1's (shard, domain_id). Symmetric
inverse of migrate_shard_split.py: handles url_state_current / history /
url_event_counter.

--execute requires scheduler_ingest to be paused.

    scripts/migrate_unsplit_subdomains.py             # dry-run
    scripts/migrate_unsplit_subdomains.py --execute   # actually move rows
"""
from __future__ import annotations

import argparse
from pathlib import Path

import psycopg2
import tldextract

from constants import CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import compute_shard, load_sharding_config

REPO = Path(__file__).resolve().parents[1]
SPLIT_CFG = REPO / "containers/scheduler_ingest/config/shard_split.yaml"
INGEST_CFG = REPO / "containers/scheduler_ingest/config/ingest.yaml"


def load_num_shards() -> int:
    cfg = load_yaml(str(INGEST_CFG))
    return int(cfg.get("router", {}).get("num_shards", 256))


def parents_of(hosts: set[str]) -> set[str]:
    parents: set[str] = set()
    for h in hosts:
        e = tldextract.extract(h)
        if e.registered_domain:
            parents.add(e.registered_domain)
    return parents


def ensure_parent(cur, parent: str, shard: int) -> int:
    cur.execute(
        """
        INSERT INTO domain_state (domain, shard_id)
        VALUES (%s, %s)
        ON CONFLICT (domain) DO UPDATE SET shard_id = EXCLUDED.shard_id
        RETURNING domain_id
        """,
        (parent, shard),
    )
    return int(cur.fetchone()[0])


def find_victims(cur, parent: str, whitelist: set[str]) -> list[tuple[str, int, int]]:
    cur.execute(
        """
        SELECT domain, shard_id, domain_id FROM domain_state
        WHERE domain LIKE %s AND domain <> %s
        ORDER BY domain
        """,
        (f"%.{parent}", parent),
    )
    return [(d, s, did) for d, s, did in cur.fetchall() if d not in whitelist]


def move_one(cur, src: tuple[str, int, int], dst_shard: int, dst_did: int) -> dict[str, int]:
    """Move all rows for src=(domain, shard, domain_id) under dst_did on
    dst_shard. snapshot_id / snapshot_at are dropped so the destination
    sequence assigns fresh values (avoids the drift fixed in
    migrate_resync_snapshot_id_seq.py)."""
    _, src_shard, src_did = src
    src_cur = f"url_state_current_{src_shard:03d}"
    src_hist = f"url_state_history_{src_shard:03d}"
    src_evt = f"url_event_counter_{src_shard:03d}"
    dst_cur = f"url_state_current_{dst_shard:03d}"
    dst_hist = f"url_state_history_{dst_shard:03d}"
    dst_evt = f"url_event_counter_{dst_shard:03d}"

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
        (src_did, dst_did),
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
        (src_did, dst_did),
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
        (dst_did,),
    )
    counts["event"] = cur.rowcount

    cur.execute("DELETE FROM domain_state WHERE domain_id = %s", (src_did,))
    return counts


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually move rows. Requires pipeline pause beforehand.")
    args = p.parse_args()

    overrides, split_subdomains = load_sharding_config(INGEST_CFG, SPLIT_CFG)
    parents = sorted(parents_of(split_subdomains))
    if not parents:
        print(f"no parents in {SPLIT_CFG}")
        return

    num_shards = load_num_shards()
    mode = "DRY-RUN" if args.dry_run else "EXECUTE"
    print(f"mode: {mode}")
    print(f"split_subdomains: {sorted(split_subdomains)}")
    print(f"parents: {parents}")

    with psycopg2.connect(**CRAWLERDB) as conn:
        with conn.cursor() as cur:
            for parent in parents:
                dst_shard = compute_shard(parent, num_shards, overrides, split_subdomains)
                print(f"\n======== {parent} -> shard {dst_shard:03d} ========")

                if args.dry_run:
                    cur.execute("SELECT domain_id FROM domain_state WHERE domain = %s", (parent,))
                    r = cur.fetchone()
                    dst_did = int(r[0]) if r else -1
                else:
                    dst_did = ensure_parent(cur, parent, dst_shard)
                    conn.commit()

                victims = find_victims(cur, parent, split_subdomains)
                print(f"  {len(victims)} non-whitelisted subdomains found")
                if not victims:
                    continue

                grand = {"current": 0, "history": 0, "event": 0}
                for v_dom, v_shard, v_did in victims:
                    if args.dry_run:
                        cur.execute(
                            f"SELECT COUNT(*) FROM url_state_current_{v_shard:03d} WHERE domain_id=%s",
                            (v_did,),
                        )
                        n_cur = cur.fetchone()[0]
                        cur.execute(
                            f"SELECT COUNT(*) FROM url_state_history_{v_shard:03d} WHERE domain_id=%s",
                            (v_did,),
                        )
                        n_hist = cur.fetchone()[0]
                        print(f"    [dry] {v_dom:50s} shard={v_shard:03d} did={v_did}  "
                              f"cur={n_cur:>9,} hist={n_hist:>9,}")
                    else:
                        c = move_one(cur, (v_dom, v_shard, v_did), dst_shard, dst_did)
                        conn.commit()
                        for k, v in c.items():
                            grand[k] += v
                        print(f"    {v_dom:50s} shard={v_shard:03d} did={v_did} -> "
                              f"shard {dst_shard:03d} did={dst_did}  "
                              f"cur={c['current']:>9,} hist={c['history']:>9,} evt={c['event']:>9,}")

                if not args.dry_run:
                    print(f"  totals: cur={grand['current']:,} hist={grand['history']:,} evt={grand['event']:,}")


if __name__ == "__main__":
    main()

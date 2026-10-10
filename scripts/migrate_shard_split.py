#!/usr/bin/env python3
"""
Move url_state rows for hostnames listed in the shard_split_subdomain
table out to their own per-host shards. Migrates url_state_current / history /
url_event_counter; content_feature_* and domain_stats_daily start fresh.

--execute requires scheduler_ingest to be paused.

Scope (--execute): only hosts with shard_split_subdomain.migrated_at IS NULL
(newly added by refresh_split_whitelist.py). Each host is processed in
batches and its migrated_at is stamped after the host finishes, so the run
is resumable: a deadline hit or crash leaves partially-migrated hosts ready
to resume on the next run.

    scripts/migrate_shard_split.py             # dry-run
    scripts/migrate_shard_split.py --execute   # actually move rows
"""
from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from pathlib import Path

import psycopg2
import tldextract

from constants import CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import (
    compute_shard,
    load_pending_split_subdomains,
    load_sharding_config,
    mark_split_subdomain_migrated,
)
from libs.obslog import configure as configure_logging


log = logging.getLogger(__name__)


def parents_of(hosts: set[str]) -> set[str]:
    parents: set[str] = set()
    for h in hosts:
        e = tldextract.extract(h)
        if e.registered_domain:
            parents.add(e.registered_domain)
    return parents

REPO = Path(__file__).resolve().parents[1]
INGEST_CFG = REPO / "containers/scheduler_ingest/config/ingest.yaml"

HOST_EXTRACT = "LOWER(SUBSTRING(url FROM '^https?://([^/:]+)'))"
MOVED_TBL = "_split_moved_urls"
DEFAULT_BATCH = 5000


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
                overrides: dict[str, int], split_subdomains: set[str],
                pending: set[str]) -> None:
    log.info(f'======== {etld1} ========')
    cur.execute("SELECT domain_id, shard_id FROM domain_state WHERE domain = %s", (etld1,))
    row = cur.fetchone()
    if not row:
        log.warning('  not found in domain_state, skip')
        return
    domain_id, cur_shard = row
    log.info(f'  current: shard={cur_shard:03d}  domain_id={domain_id}')

    cur.execute(f"SELECT COUNT(*) FROM url_state_current_{cur_shard:03d} WHERE domain_id = %s", (domain_id,))
    total = cur.fetchone()[0]
    log.info(f'  rows in url_state_current_{cur_shard:03d}: {total:,}')
    if total == 0:
        return

    log.info('  scanning per-hostname breakdown...')
    host_counts = fetch_hostname_counts(cur, cur_shard, domain_id)

    per_host = []
    new_shard_dist: Counter[int] = Counter()
    for host, cnt in host_counts:
        ns = compute_shard(host, num_shards, overrides, split_subdomains)
        moves = host in pending
        per_host.append((host, cnt, ns, moves))
        new_shard_dist[ns] += cnt

    rows_move = sum(c for _, c, _, moves in per_host if moves)
    log.info(f'  distinct hostnames: {len(per_host):,}')
    log.info(f'  rows moving (pending only): {rows_move:,}  staying: {total - rows_move:,}')

    log.debug('  top 20 hostnames:')
    log.debug(f'    host                                                            rows  new_shard')
    for host, cnt, ns, moves in per_host[:20]:
        if moves:
            tag = "  [pending]"
        elif host in split_subdomains:
            tag = "  [already migrated]"
        else:
            tag = "  [stay]"
        log.debug(f'    {host[:55]:55s} {cnt:>12,} {ns:>10d}{tag}')

    log.debug('  projected new shard distribution (top 15):')
    for sid, cnt in new_shard_dist.most_common(15):
        bar = "#" * min(40, int(cnt / max(1, total) * 40))
        log.debug(f'    shard {sid:03d}: {cnt:>12,}  {bar}')
    log.info(f'  (spread across {len(new_shard_dist)} shards)')

    log.debug('  row counts in other shard tables (same domain_id):')
    hist_tbl = f"url_state_history_{cur_shard:03d}"
    evt_tbl = f"url_event_counter_{cur_shard:03d}"
    cur.execute(f"SELECT COUNT(*) FROM {hist_tbl} WHERE domain_id = %s", (domain_id,))
    log.debug(f'    {hist_tbl}: {cur.fetchone()[0]:,}')
    cur.execute(
        f"""SELECT COUNT(*) FROM {evt_tbl} e
             WHERE EXISTS (SELECT 1 FROM url_state_current_{cur_shard:03d} u
                           WHERE u.url = e.url AND u.domain_id = %s)""",
        (domain_id,),
    )
    log.debug(f'    {evt_tbl}: {cur.fetchone()[0]:,}')


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


def ensure_moved_table(cur) -> None:
    cur.execute(
        f"CREATE TEMP TABLE IF NOT EXISTS {MOVED_TBL} (url TEXT PRIMARY KEY) "
        f"ON COMMIT PRESERVE ROWS"
    )


def _deadline_hit(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() > deadline


def move_host(cur, host: str, old_shard: int, old_did: int,
              new_shard: int, new_did: int,
              batch_size: int = DEFAULT_BATCH,
              deadline: float | None = None) -> tuple[dict[str, int], bool]:
    """Batched, checkpointed move for one host. Each batch commits, so a
    deadline interruption preserves partial progress. Idempotent: ON CONFLICT
    DO NOTHING means a re-run picks up only rows still under the source.
    Returns (totals, finished). finished=False when the deadline cut us short."""
    old_cur_t = f"url_state_current_{old_shard:03d}"
    new_cur_t = f"url_state_current_{new_shard:03d}"
    old_hist_t = f"url_state_history_{old_shard:03d}"
    new_hist_t = f"url_state_history_{new_shard:03d}"
    old_evt_t = f"url_event_counter_{old_shard:03d}"
    new_evt_t = f"url_event_counter_{new_shard:03d}"

    totals = {"current": 0, "history": 0, "event": 0}
    ensure_moved_table(cur)

    while True:
        if _deadline_hit(deadline):
            return totals, False

        cur.execute(f"TRUNCATE {MOVED_TBL}")
        cur.execute(
            f"""
            INSERT INTO {MOVED_TBL} (url)
            SELECT url FROM {old_cur_t}
            WHERE domain_id = %s AND {HOST_EXTRACT} = %s
            LIMIT %s
            """,
            (old_did, host, batch_size),
        )
        if cur.rowcount == 0:
            cur.connection.commit()
            break

        cur.execute(
            f"""
            WITH moved AS (
                DELETE FROM {old_cur_t} u
                USING {MOVED_TBL} m
                WHERE u.url = m.url
                RETURNING u.url, u.first_seen, u.last_scheduled, u.last_fetch_ok,
                          u.last_content_update, u.last_modified,
                          u.num_scheduled_90d, u.num_fetch_ok_90d,
                          u.num_fetch_fail_90d, u.num_content_update_90d, u.num_consecutive_fail,
                          u.last_fail_reason, u.content_hash, u.should_crawl, u.url_score,
                          u.url_score_updated_at,
                          u.domain_score, u.source, u.discovered_from, u.title,
                          u.hreflang_count, u.has_json_ld, u.etag,
                          u.cache_control, u.is_redirect, u.redirect_hop_count,
                          u.discovery_source_type, u.parent_page_score,
                          u.inlink_count_approx, u.inlink_count_external, u.anchor_text,
                          u.robots_bits
            )
            INSERT INTO {new_cur_t}
                  (url, domain_id, first_seen, last_scheduled, last_fetch_ok,
                   last_content_update, last_modified,
                   num_scheduled_90d, num_fetch_ok_90d,
                   num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                   last_fail_reason, content_hash, should_crawl, url_score,
                   url_score_updated_at,
                   domain_score, source, discovered_from, title,
                   hreflang_count, has_json_ld, etag,
                   cache_control, is_redirect, redirect_hop_count,
                   discovery_source_type, parent_page_score,
                   inlink_count_approx, inlink_count_external, anchor_text,
                   robots_bits)
            SELECT url, %s, first_seen, last_scheduled, last_fetch_ok,
                   last_content_update, last_modified,
                   num_scheduled_90d, num_fetch_ok_90d,
                   num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                   last_fail_reason, content_hash, should_crawl, url_score,
                   url_score_updated_at,
                   domain_score, source, discovered_from, title,
                   hreflang_count, has_json_ld, etag,
                   cache_control, is_redirect, redirect_hop_count,
                   discovery_source_type, parent_page_score,
                   inlink_count_approx, inlink_count_external, anchor_text,
                   robots_bits
            FROM moved
            ON CONFLICT (url) DO NOTHING
            """,
            (new_did,),
        )
        totals["current"] += cur.rowcount

        cur.execute(
            f"""
            WITH moved AS (
                DELETE FROM {old_evt_t} e
                USING {MOVED_TBL} m
                WHERE e.url = m.url
                RETURNING e.url, e.event_date, e.num_scheduled, e.num_fetch_ok,
                          e.num_fetch_fail, e.num_content_update, e.accounted
            )
            INSERT INTO {new_evt_t}
                  (url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
                   num_content_update, accounted)
            SELECT url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
                   num_content_update, accounted
            FROM moved
            ON CONFLICT (url, event_date) DO NOTHING
            """
        )
        totals["event"] += cur.rowcount
        cur.connection.commit()

    # History: separate batched loop. We can't use moved_urls because history
    # may retain URLs no longer in current. Index seek on (domain_id) + per-row
    # HOST_EXTRACT is the same per-host scan as current.
    while True:
        if _deadline_hit(deadline):
            return totals, False

        cur.execute(
            f"""
            WITH batch AS (
                SELECT snapshot_id FROM {old_hist_t}
                WHERE domain_id = %s AND {HOST_EXTRACT} = %s
                LIMIT %s
            ),
            moved AS (
                DELETE FROM {old_hist_t} h
                USING batch b
                WHERE h.snapshot_id = b.snapshot_id
                RETURNING h.url, h.first_seen, h.last_scheduled, h.last_fetch_ok,
                          h.last_content_update, h.last_modified,
                          h.num_scheduled_90d, h.num_fetch_ok_90d,
                          h.num_fetch_fail_90d, h.num_content_update_90d, h.num_consecutive_fail,
                          h.last_fail_reason, h.content_hash, h.should_crawl, h.url_score,
                          h.url_score_updated_at,
                          h.domain_score, h.snapshot_id, h.snapshot_at, h.source,
                          h.discovered_from, h.title, h.hreflang_count, h.has_json_ld, h.etag,
                          h.cache_control, h.is_redirect,
                          h.redirect_hop_count, h.discovery_source_type, h.parent_page_score,
                          h.inlink_count_approx, h.inlink_count_external, h.anchor_text,
                          h.robots_bits
            )
            INSERT INTO {new_hist_t}
                  (url, domain_id, first_seen, last_scheduled, last_fetch_ok,
                   last_content_update, last_modified,
                   num_scheduled_90d, num_fetch_ok_90d,
                   num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                   last_fail_reason, content_hash, should_crawl, url_score,
                   url_score_updated_at,
                   domain_score, snapshot_id, snapshot_at, source, discovered_from,
                   title, hreflang_count, has_json_ld, etag, cache_control, is_redirect,
                   redirect_hop_count,
                   discovery_source_type, parent_page_score,
                   inlink_count_approx, inlink_count_external, anchor_text,
                   robots_bits)
            SELECT url, %s, first_seen, last_scheduled, last_fetch_ok,
                   last_content_update, last_modified,
                   num_scheduled_90d, num_fetch_ok_90d,
                   num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
                   last_fail_reason, content_hash, should_crawl, url_score,
                   url_score_updated_at,
                   domain_score, snapshot_id, snapshot_at, source, discovered_from,
                   title, hreflang_count, has_json_ld, etag, cache_control, is_redirect,
                   redirect_hop_count,
                   discovery_source_type, parent_page_score,
                   inlink_count_approx, inlink_count_external, anchor_text,
                   robots_bits
            FROM moved
            ON CONFLICT (snapshot_id) DO NOTHING
            """,
            (old_did, host, batch_size, new_did),
        )
        moved = cur.rowcount
        cur.connection.commit()
        if moved == 0:
            break
        totals["history"] += moved

    return totals, True


def execute_one(cur, etld1: str, pending_under_parent: list[str], num_shards: int,
                overrides: dict[str, int], split_subdomains: set[str],
                batch_size: int, deadline: float | None) -> bool:
    """Returns False if the deadline was hit and we should stop the outer loop."""
    log.info(f'======== EXECUTE {etld1} ({len(pending_under_parent)} pending host(s)) ========')
    cur.execute("SELECT domain_id, shard_id FROM domain_state WHERE domain = %s", (etld1,))
    row = cur.fetchone()
    if not row:
        log.warning('  parent not found in domain_state, skip')
        return True
    old_did, old_shard = row
    log.info(f'  source: shard={old_shard:03d} domain_id={old_did}')

    grand = {"current": 0, "history": 0, "event": 0}
    for host in pending_under_parent:
        new_shard = compute_shard(host, num_shards, overrides, split_subdomains)
        new_did = ensure_domain_state(cur, host, new_shard)
        cur.connection.commit()
        stats, finished = move_host(cur, host, old_shard, old_did, new_shard, new_did,
                                    batch_size=batch_size, deadline=deadline)
        for k in grand:
            grand[k] += stats[k]
        if finished:
            mark_split_subdomain_migrated(cur.connection, host)
            cur.connection.commit()
        tag = "" if finished else "  [DEADLINE; resume next run]"
        log.info(
            f"    {host:50s} -> shard {new_shard:03d} did={new_did}  cur={stats['current']:>9,}"
            f" hist={stats['history']:>9,} evt={stats['event']:>9,}{tag}"
        )
        if not finished:
            log.warning(f'Deadline reached while migrating {host}; resume next run')
            return False

    log.info(
        f"  totals: url_state_current={grand['current']:,}  history={grand['history']:,}"
        f"  event_counter={grand['event']:,}"
    )
    return True


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually move rows. Requires pipeline pause beforehand.")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH,
                   help=f"rows per batch in move_host (default {DEFAULT_BATCH})")
    p.add_argument("--deadline-sec", type=float, default=None,
                   help="hard deadline in seconds; mid-batch stop, partial hosts "
                        "stay migrated_at NULL and resume next run")
    args = p.parse_args()
    configure_logging(service="migrate_shard_split")

    deadline = (time.monotonic() + args.deadline_sec) if args.deadline_sec else None

    with psycopg2.connect(**CRAWLERDB) as conn:
        overrides, split_subdomains = load_sharding_config(INGEST_CFG, conn)
        if not split_subdomains:
            log.info('no whitelist entries in shard_split_subdomain.')
            return

        if args.dry_run:
            scope = split_subdomains
        else:
            scope = load_pending_split_subdomains(conn)
            if not scope:
                log.info('no pending hosts (all whitelist entries already migrated).')
                return

        targets = sorted(parents_of(scope))
        num_shards = load_num_shards()
        mode = "DRY-RUN (full whitelist)" if args.dry_run else "EXECUTE (pending only)"
        log.info(f'mode: {mode}')
        log.debug(f'scope hosts: {sorted(scope)}')
        log.debug(f'parents: {targets}')
        log.debug(f'overrides: {overrides}')
        log.debug(f'num_shards: {num_shards}')
        if deadline:
            log.info(f'deadline: {args.deadline_sec:.0f}s')

        with conn.cursor() as cur:
            for etld1 in targets:
                if args.dry_run:
                    dry_run_one(cur, etld1, num_shards, overrides,
                                split_subdomains, scope)
                else:
                    pending_here = sorted(
                        h for h in scope
                        if (tldextract.extract(h).registered_domain or "") == etld1
                    )
                    cont = execute_one(cur, etld1, pending_here, num_shards,
                                       overrides, split_subdomains,
                                       args.batch_size, deadline)
                    if not cont:
                        return


if __name__ == "__main__":
    main()

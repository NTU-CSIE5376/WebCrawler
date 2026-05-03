"""
Migration: merge subdomain-form domain_state rows into their eTLD+1
canonical domain, fixing legacy rows left by golden_inject's old
urlparse().hostname path (e.g. `en.wikipedia.org` -> `wikipedia.org`).

For each dirty row, moves per-shard url_state_current, url_event_counter,
content_feature_current, and domain_stats_daily to the canonical
(shard, domain_id); on URL conflicts keeps the canonical row and bumps
`source` to GREATEST. History tables are left untouched (append-only).

Usage:
    uv run scripts/migrate_merge_subdomain_rows.py --dry-run
    uv run scripts/migrate_merge_subdomain_rows.py --execute
"""

import argparse
import logging
import re
from pathlib import Path

import psycopg2
import tldextract

from constants import NUM_SHARDS, CRAWLERDB
from libs.config.loader import load_yaml
from libs.db.sharding.key import compute_shard, load_sharding_config

# Skip rows whose `domain` value is anchor-text leakage (contains spaces,
# punctuation). Matches strict DNS label form.
DNS_HOST_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?)*$")

INGEST_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_ingest/config/ingest.yaml"
)
SPLIT_CONFIG = INGEST_CONFIG.parent / "shard_split.yaml"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def domain_to_shard(domain: str, overrides: dict[str, int], split_subdomains: set[str] | None = None) -> int:
    return compute_shard(domain, NUM_SHARDS, overrides, split_subdomains)


def canonical_domain(domain: str) -> str | None:
    try:
        e = tldextract.extract(domain)
    except Exception:
        return None
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


def ensure_canonical(cur, domain: str, shard_id: int) -> int:
    cur.execute(
        """
        INSERT INTO domain_state (domain, shard_id)
        VALUES (%s, %s)
        ON CONFLICT (domain) DO NOTHING
        """,
        (domain, shard_id),
    )
    cur.execute("SELECT domain_id FROM domain_state WHERE domain = %s", (domain,))
    return int(cur.fetchone()[0])


def merge_one(cur, bad, good, dry_run: bool) -> dict:
    """Migrate all per-shard rows from bad -> good. Returns counts."""
    bad_domain, bad_shard, bad_did = bad
    good_domain, good_shard, good_did = good

    cur.execute(
        f"SELECT COUNT(*) FROM url_state_current_{bad_shard:03d} WHERE domain_id=%s",
        (bad_did,),
    )
    n_cur = cur.fetchone()[0]
    cur.execute(
        f"SELECT COUNT(*) FROM content_feature_current_{bad_shard:03d} WHERE domain_id=%s",
        (bad_did,),
    )
    n_feat = cur.fetchone()[0]
    cur.execute(
        """
        SELECT COUNT(*) FROM domain_stats_daily WHERE domain_id = %s
        """,
        (bad_did,),
    )
    n_stats = cur.fetchone()[0]

    log.info(
        "  %s (shard=%d did=%d) -> %s (shard=%d did=%d): url=%d feat=%d stats=%d",
        bad_domain, bad_shard, bad_did,
        good_domain, good_shard, good_did,
        n_cur, n_feat, n_stats,
    )

    if dry_run:
        return {"url_current": n_cur, "feat_current": n_feat, "stats_daily": n_stats}

    cur.execute(
        f"""
        INSERT INTO url_state_current_{good_shard:03d} AS g
          (url, domain_id, first_seen, last_scheduled, last_fetch_ok,
           last_content_update, last_modified,
           num_scheduled_90d, num_fetch_ok_90d,
           num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
           last_fail_reason, content_hash, should_crawl, url_score,
           domain_score, source, discovered_from, title, hreflang_count,
           etag, cache_control,
           is_redirect, redirect_hop_count, discovery_source_type,
           parent_page_score, inlink_count_approx, inlink_count_external,
           anchor_text, robots_bits)
        SELECT url, %s, first_seen, last_scheduled, last_fetch_ok,
               last_content_update, last_modified,
               num_scheduled_90d, num_fetch_ok_90d,
               num_fetch_fail_90d, num_content_update_90d, num_consecutive_fail,
               last_fail_reason, content_hash, should_crawl, url_score,
               domain_score, source, discovered_from, title, hreflang_count,
               etag,
               cache_control, is_redirect, redirect_hop_count,
               discovery_source_type, parent_page_score,
               inlink_count_approx, inlink_count_external, anchor_text,
               robots_bits
        FROM url_state_current_{bad_shard:03d}
        WHERE domain_id = %s
        ON CONFLICT (url) DO UPDATE
          SET source = GREATEST(g.source, EXCLUDED.source),
              discovered_from = COALESCE(g.discovered_from, EXCLUDED.discovered_from),
              title = COALESCE(g.title, EXCLUDED.title),
              hreflang_count = COALESCE(g.hreflang_count, EXCLUDED.hreflang_count),
              last_modified = COALESCE(g.last_modified, EXCLUDED.last_modified),
              etag = COALESCE(g.etag, EXCLUDED.etag),
              cache_control = COALESCE(g.cache_control, EXCLUDED.cache_control),
              is_redirect = COALESCE(g.is_redirect, EXCLUDED.is_redirect),
              redirect_hop_count = COALESCE(g.redirect_hop_count, EXCLUDED.redirect_hop_count),
              discovery_source_type = GREATEST(g.discovery_source_type, EXCLUDED.discovery_source_type),
              parent_page_score = COALESCE(g.parent_page_score, EXCLUDED.parent_page_score),
              inlink_count_approx = g.inlink_count_approx + EXCLUDED.inlink_count_approx,
              inlink_count_external = g.inlink_count_external + EXCLUDED.inlink_count_external,
              anchor_text = COALESCE(g.anchor_text, EXCLUDED.anchor_text),
              robots_bits = CASE
                WHEN EXCLUDED.robots_bits = 0 THEN g.robots_bits
                ELSE EXCLUDED.robots_bits
              END
        """,
        (good_did, bad_did),
    )
    cur.execute(
        f"DELETE FROM url_state_current_{bad_shard:03d} WHERE domain_id=%s",
        (bad_did,),
    )

    # url_event_counter has no domain_id column; match via the URLs we
    # just inserted into good_shard.
    cur.execute(
        f"""
        INSERT INTO url_event_counter_{good_shard:03d} AS g
          (url, event_date, num_scheduled, num_fetch_ok, num_fetch_fail,
           num_content_update, accounted)
        SELECT b.url, b.event_date, b.num_scheduled, b.num_fetch_ok,
               b.num_fetch_fail, b.num_content_update, b.accounted
        FROM url_event_counter_{bad_shard:03d} b
        JOIN url_state_current_{good_shard:03d} c ON c.url = b.url
        WHERE c.domain_id = %s
        ON CONFLICT (url, event_date) DO UPDATE
          SET num_scheduled      = g.num_scheduled      + EXCLUDED.num_scheduled,
              num_fetch_ok       = g.num_fetch_ok       + EXCLUDED.num_fetch_ok,
              num_fetch_fail     = g.num_fetch_fail     + EXCLUDED.num_fetch_fail,
              num_content_update = g.num_content_update + EXCLUDED.num_content_update,
              -- OR, not AND: accounted=TRUE means "still needs rolloff".
              -- AND would silently mark pending events as done and leave
              -- 90d counters inflated.
              accounted          = g.accounted OR EXCLUDED.accounted
        """,
        (good_did,),
    )
    cur.execute(
        f"""
        DELETE FROM url_event_counter_{bad_shard:03d} b
        USING url_state_current_{good_shard:03d} c
        WHERE b.url = c.url AND c.domain_id = %s
        """,
        (good_did,),
    )

    cur.execute(
        f"""
        INSERT INTO content_feature_current_{good_shard:03d} AS g
          (url, domain_id, fetched_at, content_length, content_hash, num_links)
        SELECT url, %s, fetched_at, content_length, content_hash, num_links
        FROM content_feature_current_{bad_shard:03d}
        WHERE domain_id = %s
        ON CONFLICT (url) DO UPDATE
          SET domain_id      = EXCLUDED.domain_id,
              fetched_at     = EXCLUDED.fetched_at,
              content_length = EXCLUDED.content_length,
              content_hash   = EXCLUDED.content_hash,
              num_links      = EXCLUDED.num_links
          WHERE EXCLUDED.fetched_at > g.fetched_at
        """,
        (good_did, bad_did),
    )
    cur.execute(
        f"DELETE FROM content_feature_current_{bad_shard:03d} WHERE domain_id=%s",
        (bad_did,),
    )

    cur.execute(
        """
        INSERT INTO domain_stats_daily AS g
          (domain_id, event_date, shard_id, num_scheduled, num_fetch_ok,
           num_fetch_fail, num_content_update, fail_reasons)
        SELECT %s, event_date, %s, num_scheduled, num_fetch_ok,
               num_fetch_fail, num_content_update, fail_reasons
        FROM domain_stats_daily
        WHERE domain_id = %s
        ON CONFLICT (domain_id, event_date) DO UPDATE
          SET num_scheduled      = g.num_scheduled      + EXCLUDED.num_scheduled,
              num_fetch_ok       = g.num_fetch_ok       + EXCLUDED.num_fetch_ok,
              num_fetch_fail     = g.num_fetch_fail     + EXCLUDED.num_fetch_fail,
              num_content_update = g.num_content_update + EXCLUDED.num_content_update,
              fail_reasons       = g.fail_reasons || EXCLUDED.fail_reasons
        """,
        (good_did, good_shard, bad_did),
    )
    cur.execute("DELETE FROM domain_stats_daily WHERE domain_id=%s", (bad_did,))

    cur.execute("DELETE FROM domain_state WHERE domain_id=%s", (bad_did,))

    return {"url_current": n_cur, "feat_current": n_feat, "stats_daily": n_stats}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", default=True)
    p.add_argument("--execute", dest="dry_run", action="store_false",
                   help="Actually perform the migration")
    p.add_argument("--domain-like", default=None,
                   help="Optional SQL LIKE filter on domain_state.domain, "
                        "e.g. '%%.wikipedia.org' to limit scope")
    args = p.parse_args()

    overrides, split_subdomains = load_sharding_config(INGEST_CONFIG, SPLIT_CONFIG)
    conn = psycopg2.connect(**CRAWLERDB)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        if args.domain_like:
            cur.execute(
                "SELECT domain, shard_id, domain_id FROM domain_state WHERE domain LIKE %s ORDER BY domain",
                (args.domain_like,),
            )
        else:
            cur.execute(
                "SELECT domain, shard_id, domain_id FROM domain_state ORDER BY domain"
            )
        rows = cur.fetchall()

        dirty = []
        unparseable = 0
        skipped_junk = 0
        for domain, shard_id, did in rows:
            if not DNS_HOST_RE.match(domain or ""):
                skipped_junk += 1
                continue
            canon = canonical_domain(domain)
            if canon is None:
                unparseable += 1
                continue
            if canon == domain:
                continue
            dirty.append((domain, shard_id, did, canon))

        log.info("scanned=%d dirty=%d unparseable=%d junk=%d",
                 len(rows), len(dirty), unparseable, skipped_junk)
        if not dirty:
            return

        totals = {"url_current": 0, "feat_current": 0, "stats_daily": 0, "domains": 0}
        for bad_domain, bad_shard, bad_did, canon in dirty:
            canon_shard = domain_to_shard(canon, overrides, split_subdomains)
            if args.dry_run:
                cur.execute("SELECT domain_id FROM domain_state WHERE domain=%s", (canon,))
                r = cur.fetchone()
                canon_did = int(r[0]) if r else -1
            else:
                canon_did = ensure_canonical(cur, canon, canon_shard)

            stats = merge_one(
                cur,
                bad=(bad_domain, bad_shard, bad_did),
                good=(canon, canon_shard, canon_did),
                dry_run=args.dry_run,
            )
            for k in ("url_current", "feat_current", "stats_daily"):
                totals[k] += stats[k]
            totals["domains"] += 1

            if not args.dry_run:
                conn.commit()

        tag = "[DRY-RUN] " if args.dry_run else ""
        log.info(
            "%sdomains=%d url_current=%d feat_current=%d stats_daily=%d",
            tag, totals["domains"], totals["url_current"],
            totals["feat_current"], totals["stats_daily"],
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

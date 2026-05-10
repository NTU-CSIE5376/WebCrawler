"""
Backfill `golden_parent_patrol_state` from live-observed parent evidence.

Scans every `url_state_current_{shard}` for rows where
`source = SOURCE_GOLDEN` AND `discovered_from IS NOT NULL` — i.e. URLs the
crawler organically discovered that later turned out to be golden — groups
them by normalized parent URL, and inserts a `live_observed` patrol row for
each parent that first-discovered at least one golden child.

Idempotent. On conflict the script:
  - upgrades a `wat_exact` row to `live_observed` (live evidence is stronger),
  - merges the alias set,
  - refreshes `lifetime_golden_child_count` from the live count,
  - leaves operational fields (status, cadence_bucket, next_patrol_at,
    counters) untouched so a rerun does not perturb in-flight patrols.

Newly enrolled rows bootstrap `last_eval_batch_id = MAX(metric_batches.id)`
so future delayed evaluations only credit/blame batches that opened
*after* enrollment (avoids retroactive miss penalties).

Usage:
    uv run scripts/backfill_golden_parent_patrol.py [--dry-run] [--limit-shards N]
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from pathlib import Path

import psycopg2
import tldextract

try:
    from scripts.constants import CRAWLERDB, METRICDB, NUM_SHARDS, SOURCE_GOLDEN
except ModuleNotFoundError:
    from constants import CRAWLERDB, METRICDB, NUM_SHARDS, SOURCE_GOLDEN

from libs.db.sharding.key import compute_shard, load_sharding_config
from libs.patrol.normalize import normalize_parent_url


INGEST_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_ingest/config/ingest.yaml"
)
SPLIT_CONFIG = INGEST_CONFIG.parent / "shard_split.yaml"

# Inline copies of golden_inject's helpers to avoid scripts/-as-package import
# gymnastics; both live in golden_inject.py too.
def extract_domain(url: str) -> str | None:
    e = tldextract.extract(url)
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


def domain_to_shard(domain: str, overrides, split_subdomains) -> int:
    return compute_shard(domain, NUM_SHARDS, overrides, split_subdomains)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def fetch_max_metric_batch_id(metric_conn) -> int:
    with metric_conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM metric_batches")
        return int(cur.fetchone()[0])


def scan_shard_for_live_parents(
    crawler_conn, shard_id: int
) -> list[tuple[str, int]]:
    """Returns (raw_parent_url, distinct_child_count) per raw parent URL."""
    table = f"url_state_current_{shard_id:03d}"
    with crawler_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT discovered_from, COUNT(DISTINCT url) AS children
            FROM {table}
            WHERE source = %s
              AND discovered_from IS NOT NULL
            GROUP BY discovered_from
            """,
            (SOURCE_GOLDEN,),
        )
        return [(row[0], int(row[1])) for row in cur.fetchall()]


class ParentAggregate:
    __slots__ = (
        "parent_key",
        "raw_urls",
        "child_count",
        "parent_domain",
        "shard_id",
    )

    def __init__(self, parent_key: str):
        self.parent_key = parent_key
        self.raw_urls: list[str] = []
        self.child_count = 0
        self.parent_domain: str | None = None
        self.shard_id: int | None = None


def aggregate_by_parent_key(
    rows: list[tuple[str, int]],
    overrides,
    split_subdomains,
) -> dict[str, ParentAggregate]:
    out: dict[str, ParentAggregate] = {}
    for raw_url, child_count in rows:
        if not raw_url:
            continue
        domain = extract_domain(raw_url)
        if not domain:
            continue
        key = normalize_parent_url(raw_url)
        if not key:
            continue
        agg = out.setdefault(key, ParentAggregate(key))
        if raw_url not in agg.raw_urls:
            agg.raw_urls.append(raw_url)
        agg.child_count += child_count
        if agg.parent_domain is None:
            agg.parent_domain = domain
            agg.shard_id = domain_to_shard(domain, overrides, split_subdomains)
    return out


UPSERT_SQL = """
INSERT INTO golden_parent_patrol_state (
    parent_key, fetch_url, aliases, parent_domain, shard_id,
    source_type,
    lifetime_golden_child_count,
    last_eval_batch_id,
    metadata_json
)
VALUES (
    %(parent_key)s, %(fetch_url)s, %(aliases)s, %(parent_domain)s, %(shard_id)s,
    'live_observed',
    %(lifetime_golden_child_count)s,
    %(last_eval_batch_id)s,
    %(metadata_json)s
)
ON CONFLICT (parent_key) DO UPDATE SET
    -- live_observed is a strict upgrade over wat_exact; if existing is
    -- already live_observed the assignment is a no-op.
    source_type = 'live_observed',
    -- merge alias sets: keep order, dedupe.
    aliases = (
        SELECT ARRAY(
            SELECT DISTINCT v FROM unnest(
                golden_parent_patrol_state.aliases || EXCLUDED.aliases
            ) AS v
        )
    ),
    -- refresh count from live observation; live count is authoritative.
    lifetime_golden_child_count = EXCLUDED.lifetime_golden_child_count,
    -- only set last_eval_batch_id on first enrollment; if non-NULL keep it.
    last_eval_batch_id = COALESCE(
        golden_parent_patrol_state.last_eval_batch_id,
        EXCLUDED.last_eval_batch_id
    ),
    parent_domain = EXCLUDED.parent_domain,
    shard_id = EXCLUDED.shard_id,
    updated_at = NOW()
"""


def upsert(
    crawler_conn,
    aggregates: dict[str, ParentAggregate],
    last_eval_batch_id: int,
    dry_run: bool,
) -> tuple[int, int]:
    if not aggregates:
        return 0, 0

    inserted = 0
    if dry_run:
        for key, agg in aggregates.items():
            log.info(
                "[DRY-RUN] would upsert %s (children=%d, aliases=%d, shard=%s)",
                key,
                agg.child_count,
                len(agg.raw_urls),
                agg.shard_id,
            )
        return len(aggregates), 0

    with crawler_conn.cursor() as cur:
        for agg in aggregates.values():
            fetch_url = sorted(agg.raw_urls)[0]
            cur.execute(
                UPSERT_SQL,
                {
                    "parent_key": agg.parent_key,
                    "fetch_url": fetch_url,
                    "aliases": agg.raw_urls,
                    "parent_domain": agg.parent_domain,
                    "shard_id": agg.shard_id,
                    "lifetime_golden_child_count": agg.child_count,
                    "last_eval_batch_id": last_eval_batch_id,
                    "metadata_json": None,
                },
            )
            inserted += 1
    crawler_conn.commit()
    return inserted, 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Backfill golden_parent_patrol_state with live-observed parents."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Aggregate but do not write to crawlerdb",
    )
    parser.add_argument(
        "--limit-shards",
        type=int,
        default=None,
        help="Only scan the first N shards (smoke testing).",
    )
    args = parser.parse_args()

    overrides, split_subdomains = load_sharding_config(INGEST_CONFIG, SPLIT_CONFIG)

    metric_conn = psycopg2.connect(**METRICDB)
    try:
        crawler_conn = psycopg2.connect(**CRAWLERDB)
    except Exception:
        metric_conn.close()
        raise

    try:
        last_eval_batch_id = fetch_max_metric_batch_id(metric_conn)
        log.info(
            "bootstrap last_eval_batch_id from metric_batches: %d",
            last_eval_batch_id,
        )

        max_shard = NUM_SHARDS if args.limit_shards is None else args.limit_shards

        # Aggregate across all shards before upserting so a parent that
        # appears in multiple shards (split_etld1 case) collapses into one
        # patrol_state row with the union of aliases.
        all_rows: list[tuple[str, int]] = []
        scanned_shards = 0
        for shard_id in range(max_shard):
            rows = scan_shard_for_live_parents(crawler_conn, shard_id)
            scanned_shards += 1
            all_rows.extend(rows)

        log.info(
            "scanned %d shards; %d raw discovered_from groups",
            scanned_shards,
            len(all_rows),
        )

        aggregates = aggregate_by_parent_key(
            all_rows, overrides, split_subdomains
        )
        log.info("aggregated to %d parent_keys", len(aggregates))

        upserted, _ = upsert(
            crawler_conn, aggregates, last_eval_batch_id, args.dry_run
        )

        if args.dry_run:
            log.info(
                "[DRY-RUN] would upsert %d parent_keys (no DB writes)",
                upserted,
            )
        else:
            log.info("Done: upserted %d parent_keys", upserted)

    except Exception:
        try:
            crawler_conn.rollback()
        except Exception:
            pass
        raise
    finally:
        metric_conn.close()
        crawler_conn.close()


if __name__ == "__main__":
    main()

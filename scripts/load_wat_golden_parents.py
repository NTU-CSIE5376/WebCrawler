"""
Load WAT-exact golden parent evidence into `golden_parent_patrol_state`.

The `wat_exact` source is the Common Crawl WAT scan output: a directory of
`chunks/wat_chunk_*.csv.gz` files where each row records one
(parent_url -> golden child_url) edge observed in a CC WAT snapshot. This
script aggregates those edges by normalized parent URL and inserts them as
`wat_exact` rows in the patrol watchlist.

WAT is *weaker* evidence than live observation: the scan is a historical
snapshot, the parent page may no longer exist, the link may have been
removed. The patrol cron will discover those failures via the
`consecutive_fetch_fail` backstop and `cold` → retire path. So this loader
inserts conservatively:

  - ON CONFLICT with an existing `live_observed` row: merge alias set only;
    do NOT overwrite source_type, count, or operational state.
  - ON CONFLICT with an existing `wat_exact` row: refresh count and aliases
    from the new WAT artifact (latest scan wins).

Idempotent: rerunning against the same artifact directory yields the same
final state.

Usage:
    uv run scripts/load_wat_golden_parents.py --wat-dir /path/to/wat_exact_run [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import psycopg2
import tldextract

try:
    from scripts.constants import CRAWLERDB, METRICDB, NUM_SHARDS
except ModuleNotFoundError:
    from constants import CRAWLERDB, METRICDB, NUM_SHARDS

from libs.db.sharding.key import compute_shard, load_sharding_config
from libs.patrol.normalize import normalize_parent_url


INGEST_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_ingest/config/ingest.yaml"
)
SPLIT_CONFIG = INGEST_CONFIG.parent / "shard_split.yaml"


def extract_domain(url: str) -> str | None:
    e = tldextract.extract(url)
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


def domain_to_shard(domain: str, overrides, split_subdomains) -> int:
    return compute_shard(domain, NUM_SHARDS, overrides, split_subdomains)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def iter_chunk_rows(wat_dir: Path) -> Iterable[tuple[str, str]]:
    """Yields (parent_url, child_url) per chunk row.

    Each chunk is a gzipped CSV with header
    `parent_url,parent_domain,child_url,child_domain,link_path,anchor_text,wat_path`.
    Only parent/child columns are used here.
    """
    chunks_dir = wat_dir / "chunks"
    if not chunks_dir.is_dir():
        raise FileNotFoundError(f"WAT chunks directory missing: {chunks_dir}")

    chunk_files = sorted(chunks_dir.glob("wat_chunk_*.csv.gz"))
    if not chunk_files:
        raise FileNotFoundError(f"No wat_chunk_*.csv.gz under {chunks_dir}")

    for cf in chunk_files:
        with gzip.open(cf, "rt", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                p = row.get("parent_url")
                c = row.get("child_url")
                if not p or not c:
                    continue
                yield p, c


def read_manifest(wat_dir: Path) -> dict:
    manifest_path = wat_dir / "manifest.json"
    if manifest_path.is_file():
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("failed to read manifest.json: %s", e)
    return {}


class WatParentAggregate:
    __slots__ = ("parent_key", "raw_urls", "child_urls", "parent_domain", "shard_id")

    def __init__(self, parent_key: str):
        self.parent_key = parent_key
        self.raw_urls: list[str] = []
        self.child_urls: set[str] = set()
        self.parent_domain: str | None = None
        self.shard_id: int | None = None


def aggregate(
    rows: Iterable[tuple[str, str]],
    overrides,
    split_subdomains,
) -> dict[str, WatParentAggregate]:
    out: dict[str, WatParentAggregate] = {}
    seen_alias_per_key: dict[str, set] = defaultdict(set)

    for raw_parent, child in rows:
        if not raw_parent or not child:
            continue
        domain = extract_domain(raw_parent)
        if not domain:
            continue
        key = normalize_parent_url(raw_parent)
        if not key:
            continue
        agg = out.setdefault(key, WatParentAggregate(key))
        if raw_parent not in seen_alias_per_key[key]:
            seen_alias_per_key[key].add(raw_parent)
            agg.raw_urls.append(raw_parent)
        agg.child_urls.add(child)
        if agg.parent_domain is None:
            agg.parent_domain = domain
            agg.shard_id = domain_to_shard(domain, overrides, split_subdomains)

    return out


# ON CONFLICT semantics:
#   * source_type: keep 'live_observed' if existing; otherwise switch to
#     'wat_exact'.
#   * lifetime_golden_child_count: keep live count if existing was
#     live_observed; otherwise replace with the new WAT count.
#   * aliases: union of existing and new (de-duped).
#   * fetch_url, parent_domain, shard_id: only set when first inserted, do
#     not touch existing — WAT cannot improve on what live observation
#     already chose.
#   * last_eval_batch_id: keep existing; new wat enrollments fall back to
#     EXCLUDED (caller passes MAX(metric_batches.id)).
#   * operational counters / next_patrol_at: untouched.
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
    'wat_exact',
    %(lifetime_golden_child_count)s,
    %(last_eval_batch_id)s,
    %(metadata_json)s
)
ON CONFLICT (parent_key) DO UPDATE SET
    source_type = CASE
        WHEN golden_parent_patrol_state.source_type = 'live_observed'
            THEN 'live_observed'
        ELSE 'wat_exact'
    END,
    lifetime_golden_child_count = CASE
        WHEN golden_parent_patrol_state.source_type = 'live_observed'
            THEN golden_parent_patrol_state.lifetime_golden_child_count
        ELSE EXCLUDED.lifetime_golden_child_count
    END,
    aliases = (
        SELECT ARRAY(
            SELECT DISTINCT v FROM unnest(
                golden_parent_patrol_state.aliases || EXCLUDED.aliases
            ) AS v
        )
    ),
    last_eval_batch_id = COALESCE(
        golden_parent_patrol_state.last_eval_batch_id,
        EXCLUDED.last_eval_batch_id
    ),
    -- merge metadata_json so multiple WAT runs accumulate provenance
    metadata_json = COALESCE(
        golden_parent_patrol_state.metadata_json, '{}'::jsonb
    ) || COALESCE(EXCLUDED.metadata_json, '{}'::jsonb),
    updated_at = NOW()
"""


def fetch_max_metric_batch_id(metric_conn) -> int:
    with metric_conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM metric_batches")
        return int(cur.fetchone()[0])


def upsert(
    crawler_conn,
    aggregates: dict[str, WatParentAggregate],
    last_eval_batch_id: int,
    metadata: dict,
    dry_run: bool,
) -> int:
    if dry_run:
        for key, agg in aggregates.items():
            log.info(
                "[DRY-RUN] would upsert %s (children=%d, aliases=%d, shard=%s)",
                key,
                len(agg.child_urls),
                len(agg.raw_urls),
                agg.shard_id,
            )
        return len(aggregates)

    inserted = 0
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
                    "lifetime_golden_child_count": len(agg.child_urls),
                    "last_eval_batch_id": last_eval_batch_id,
                    "metadata_json": json.dumps(metadata) if metadata else None,
                },
            )
            inserted += 1
    crawler_conn.commit()
    return inserted


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Load WAT-exact golden parent evidence into "
            "golden_parent_patrol_state."
        )
    )
    parser.add_argument(
        "--wat-dir",
        type=Path,
        required=True,
        help="Path to a wat_exact artifact directory (must contain chunks/).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Aggregate but do not write to crawlerdb",
    )
    args = parser.parse_args()

    if not args.wat_dir.is_dir():
        raise SystemExit(f"WAT dir not found: {args.wat_dir}")

    manifest = read_manifest(args.wat_dir)
    metadata_provenance = {
        "wat_dir": args.wat_dir.name,
        "wat_crawl_id": manifest.get("crawl_id"),
        "wat_seen_at": None,  # caller may extend this if scan timestamp is available
    }

    overrides, split_subdomains = load_sharding_config(INGEST_CONFIG, SPLIT_CONFIG)

    log.info("aggregating chunks under %s", args.wat_dir)
    aggregates = aggregate(
        iter_chunk_rows(args.wat_dir),
        overrides,
        split_subdomains,
    )
    log.info("aggregated %d distinct parent_keys", len(aggregates))

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

        upserted = upsert(
            crawler_conn,
            aggregates,
            last_eval_batch_id,
            metadata_provenance,
            args.dry_run,
        )

        if args.dry_run:
            log.info("[DRY-RUN] would upsert %d parent_keys", upserted)
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

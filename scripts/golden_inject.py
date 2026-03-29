"""
Golden Set Injection Script

Injects golden set URLs older than 4 weeks into crawlerdb, giving the
crawler time to discover them naturally before force-injecting.

Usage:
    python scripts/golden_inject.py [--dry-run]
"""

import argparse
import hashlib
import logging
from urllib.parse import urlparse

import psycopg2

NUM_SHARDS = 256
INJECT_AFTER_WEEKS = 4

CRAWLERDB = dict(host="172.16.191.1", port=5432, user="crawler", password="crawler", dbname="crawlerdb")
METRICDB = dict(host="172.16.191.1", port=5433, user="metric", password="metric", dbname="metricdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def domain_to_shard(domain: str) -> int:
    h = hashlib.md5((domain or "unknown").encode("utf-8")).hexdigest()
    return int(h, 16) % NUM_SHARDS


def extract_domain(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        return parsed.hostname
    except Exception:
        return None


def fetch_injectable_batch_ids(metric_cur) -> list[int]:
    metric_cur.execute(
        "SELECT id FROM metric_batches WHERE created_at <= NOW() - %s * INTERVAL '1 week'",
        (INJECT_AFTER_WEEKS,),
    )
    return [r[0] for r in metric_cur.fetchall()]


def fetch_urls_by_batches(metric_cur, batch_ids: list[int]) -> list[dict]:
    metric_cur.execute(
        """
        SELECT u.id, u.url
        FROM metric_url u
        JOIN metric_queries q ON u.query_id = q.id
        WHERE q.batch_id = ANY(%s)
          AND u.is_discovered = FALSE
        """,
        (batch_ids,),
    )
    rows = metric_cur.fetchall()
    log.info("Fetched %d URLs from %d batch(es) older than %d weeks", len(rows), len(batch_ids), INJECT_AFTER_WEEKS)
    return [{"id": r[0], "url": r[1]} for r in rows]


def ensure_domain(crawler_cur, domain: str, shard_id: int) -> tuple[int, float]:
    crawler_cur.execute(
        """
        INSERT INTO domain_state (domain, shard_id)
        VALUES (%s, %s)
        ON CONFLICT (domain) DO NOTHING
        """,
        (domain, shard_id),
    )
    crawler_cur.execute(
        "SELECT domain_id, COALESCE(domain_score, 0.0) FROM domain_state WHERE domain = %s",
        (domain,),
    )
    row = crawler_cur.fetchone()
    return int(row[0]), float(row[1])


def inject_url(crawler_cur, url: str, domain_id: int, shard_id: int, domain_score: float) -> bool:
    tcur = f"url_state_current_{shard_id:03d}"
    crawler_cur.execute(
        f"""
        INSERT INTO {tcur} (url, domain_id, domain_score)
        VALUES (%s, %s, %s)
        ON CONFLICT (url) DO NOTHING
        RETURNING url
        """,
        (url, domain_id, domain_score),
    )
    return crawler_cur.fetchone() is not None


def mark_discovered(metric_cur, ids: list[int]):
    if not ids:
        return
    metric_cur.execute(
        "UPDATE metric_url SET is_discovered = TRUE WHERE id = ANY(%s)",
        (ids,),
    )
    log.info("Marked %d URLs as discovered in metricdb", len(ids))


def main():
    parser = argparse.ArgumentParser(description="Inject golden set URLs into crawlerdb")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without writing")
    args = parser.parse_args()

    metric_conn = psycopg2.connect(**METRICDB)
    try:
        crawler_conn = psycopg2.connect(**CRAWLERDB)
    except Exception:
        metric_conn.close()
        raise

    try:
        metric_cur = metric_conn.cursor()
        crawler_cur = crawler_conn.cursor()

        batch_ids = fetch_injectable_batch_ids(metric_cur)
        if not batch_ids:
            log.info("No batches older than %d weeks", INJECT_AFTER_WEEKS)
            return

        urls = fetch_urls_by_batches(metric_cur, batch_ids)
        if not urls:
            log.info("Nothing to inject")
            return

        injected_ids = []
        skipped = 0
        failed = 0

        # Cache domain -> (domain_id, shard_id, domain_score)
        domain_cache: dict[str, tuple[int, int, float]] = {}

        for rec in urls:
            url = rec["url"]
            domain = extract_domain(url)
            if not domain:
                log.warning("Cannot parse domain from URL: %s", url)
                failed += 1
                continue

            if domain not in domain_cache:
                shard_id = domain_to_shard(domain)
                if args.dry_run:
                    domain_cache[domain] = (0, shard_id, 0.0)
                else:
                    domain_id, domain_score = ensure_domain(crawler_cur, domain, shard_id)
                    domain_cache[domain] = (domain_id, shard_id, domain_score)

            domain_id, shard_id, domain_score = domain_cache[domain]

            if args.dry_run:
                log.info("[DRY-RUN] Would inject: %s -> shard %03d", url, shard_id)
                injected_ids.append(rec["id"])
                continue

            if inject_url(crawler_cur, url, domain_id, shard_id, domain_score):
                injected_ids.append(rec["id"])
            else:
                skipped += 1

        if not args.dry_run:
            mark_discovered(metric_cur, injected_ids)
            crawler_conn.commit()
            metric_conn.commit()

        log.info(
            "Done: %d injected, %d already existed, %d failed",
            len(injected_ids), skipped, failed,
        )

    except Exception:
        crawler_conn.rollback()
        metric_conn.rollback()
        raise
    finally:
        metric_conn.close()
        crawler_conn.close()


if __name__ == "__main__":
    main()

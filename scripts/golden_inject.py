"""
Golden Set Injection Script

Injects golden set URLs older than 4 weeks into crawlerdb, giving the
crawler time to discover them naturally before force-injecting.

Usage:
    python scripts/golden_inject.py [--dry-run]
"""

import argparse
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import psycopg2
import tldextract

from constants import NUM_SHARDS, CRAWLERDB, METRICDB, SOURCE_GOLDEN
from libs.db.sharding.key import compute_shard, load_sharding_config
from libs.db.sharding.router import ShardRouter
from libs.urls import host_of, normalize_hostname, strip_srsltid
from golden_inject_batch import ConcurrentDeleteError, GOLDEN_URL_SCORE, GoldenURL, inject_batch

INJECT_AFTER_WEEKS = 4
MAX_URL_LEN = 2500
DEFAULT_COMMIT_INTERVAL = 1000
DEFAULT_STATE_DIR = Path("/data/ipc/maintenance/golden_inject")
PLAN_VERSION = 1
COUNT_KEYS = ("injected", "marked", "marked_variant")
INGEST_CONFIG = (
    Path(__file__).resolve().parents[1]
    / "containers/scheduler_ingest/config/ingest.yaml"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def domain_to_shard(domain: str, overrides: dict[str, int], split_subdomains: set[str] | None = None) -> int:
    return compute_shard(domain, NUM_SHARDS, overrides, split_subdomains)


def extract_domain(url: str) -> str | None:
    # Must match crawler spider's _extract_domain (eTLD+1) so golden and
    # natural-discovery rows share the same domain_id / shard.
    e = tldextract.extract(url)
    if not e.suffix or not e.domain:
        return None
    return f"{e.domain}.{e.suffix}"


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
        ORDER BY u.id
        """,
        (batch_ids,),
    )
    rows = metric_cur.fetchall()
    log.info(
        "Fetched %d URLs from %d batch(es) older than %d weeks",
        len(rows),
        len(batch_ids),
        INJECT_AFTER_WEEKS,
    )
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


INSERTED = "inserted"
MARKED = "marked"
MARKED_VARIANT = "marked_variant"


def inject_url(
    crawler_cur, url: str, domain_id: int, shard_id: int, domain_score: float,
    variant: str | None = None,
) -> str:
    # New URL: insert into current and mirror into history (see db_ops.process_link).
    # Existing URL: update source so the row is identifiable as golden set membership,
    # even if the crawler discovered it naturally first. No history snapshot is written
    # for source-only updates. Returns INSERTED, MARKED or MARKED_VARIANT.
    #
    # `variant` is the same page under another URL (the golden URL as copied, still
    # carrying `srsltid`). A URL with and without `srsltid` counts as one page: if
    # only the variant row exists, mark that row instead of adding a second row.
    #
    # url_score/url_score_updated_at are stamped on both new and existing rows so the
    # offerer lifts goldens to the top of their domain's pick order: the ranker prefers
    # url_score_updated_at IS NOT NULL first, then url_score DESC, so goldens were being
    # buried under freshly inline-scored natural URLs on busy domains.
    tcur = f"url_state_current_{shard_id:03d}"
    # xmax is not available on all PG-wire-compatible targets (e.g. YSQL), so
    # "newly inserted vs. pre-existing" is determined from a pre-upsert
    # existence check rather than RETURNING ... (xmax = 0).
    candidates = [url] if not variant or variant == url else [url, variant]
    crawler_cur.execute(f"SELECT url FROM {tcur} WHERE url = ANY(%s)", (candidates,))
    present = {row[0] for row in crawler_cur.fetchall()}
    result = MARKED if url in present else INSERTED
    if result == INSERTED and variant in present:
        url, result = variant, MARKED_VARIANT
    crawler_cur.execute(
        f"""
        INSERT INTO {tcur}
            (url, domain_id, domain_score, source, url_score, url_score_updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (url) DO UPDATE SET
            source = EXCLUDED.source,
            url_score = EXCLUDED.url_score,
            url_score_updated_at = EXCLUDED.url_score_updated_at
        """,
        (url, domain_id, domain_score, SOURCE_GOLDEN, GOLDEN_URL_SCORE),
    )
    if result != INSERTED:
        return result

    thist = f"url_state_history_{shard_id:03d}"
    crawler_cur.execute(
        f"""
        INSERT INTO {thist} (url, domain_id, domain_score, source)
        VALUES (%s, %s, %s, %s)
        """,
        (url, domain_id, domain_score, SOURCE_GOLDEN),
    )
    return INSERTED


@dataclass(frozen=True)
class InjectionPlan:
    batch_ids: list[int]
    records: list[GoldenURL]
    skipped: list[dict]
    input_count: int
    normalized: int
    fingerprint: str


def prepare_plan(urls, batch_ids, overrides, split_subdomains) -> InjectionPlan:
    """Use the normal pipeline's full-host routing, preserving URL contents."""
    router = ShardRouter(NUM_SHARDS, 16, overrides, split_subdomains)
    records, skipped = [], []
    normalized = 0
    for rec in urls:
        metric_id, raw = int(rec["id"]), rec["url"]
        try:
            if not isinstance(raw, str):
                raise ValueError("URL is not text")
            url = strip_srsltid(raw)
            if len(url) > MAX_URL_LEN:
                raise ValueError("URL exceeds length limit")
            if urlsplit(url).scheme.lower() not in {"http", "https"} or not host_of(url):
                raise ValueError("URL needs an HTTP(S) hostname")
            if any(ord(c) < 32 or ord(c) == 127 for c in url):
                raise ValueError("URL contains raw control characters")
            # Validate the hostname only. Do not canonicalize path/query/version.
            normalize_hostname(host_of(url))
            domain, shard_id, _ = router.route(url)
        except (ValueError, UnicodeError) as exc:
            skipped.append({"metric_url_id": metric_id, "reason": str(exc)})
            continue
        variant = raw if raw != url else None
        normalized += variant is not None
        records.append(GoldenURL(metric_id, url, variant, domain, shard_id))
    # Fewer shards/locks per transaction, and deterministic replay after failure.
    records.sort(key=lambda r: (r.shard_id, r.domain, r.metric_id, r.url, r.variant or ""))
    identity = {
        "version": PLAN_VERSION,
        "target": {k: CRAWLERDB.get(k) for k in ("host", "port", "dbname", "user", "options")},
        "write_policy": {"source": SOURCE_GOLDEN, "url_score": GOLDEN_URL_SCORE},
        "batch_ids": sorted(batch_ids),
        "overrides": overrides,
        "split_subdomains": sorted(split_subdomains),
        "records": [asdict(r) for r in records],
        "skipped": skipped,
    }
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False,
                                           separators=(",", ":")).encode()).hexdigest()
    return InjectionPlan(sorted(batch_ids), records, skipped, len(urls), normalized, fingerprint)


def _atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def injection_lock(state_dir: Path):
    """Cron and manual runs use the same persistent directory and lock inode."""
    state_dir.mkdir(parents=True, exist_ok=True)
    with (state_dir / "run.lock").open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _close(conn) -> None:
    if conn is not None:
        try:
            conn.close()
        except Exception as exc:
            log.warning("Connection cleanup failed: %s", exc)


def _connect(config, *, read_only=False):
    params = dict(config)
    params.setdefault("connect_timeout", 10)
    params.setdefault("keepalives", 1)
    params.setdefault("keepalives_idle", 30)
    params.setdefault("keepalives_interval", 5)
    params.setdefault("keepalives_count", 3)
    options = params.get("options", "")
    options += " -c statement_timeout=120000 -c lock_timeout=10000"
    if read_only:
        options += " -c default_transaction_read_only=on"
    params["options"] = options
    params.setdefault("application_name", "golden_inject")
    return psycopg2.connect(**params)


def _transient(exc) -> bool:
    if isinstance(exc, ConcurrentDeleteError):
        return True
    code = getattr(exc, "pgcode", None)
    if code:
        return code.startswith("08") or code in {
            "40001", "40P01", "55P03", "57014", "57P01", "57P02", "57P03", "53300", "YB004",
        }
    return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))


def _retry_delay(attempt):
    time.sleep(min(30.0, 2.0 ** attempt) + random.uniform(0, 0.25))


def _read(config, operation, max_retries):
    for attempt in range(max_retries + 1):
        conn = None
        try:
            conn = _connect(config, read_only=True)
            return operation(conn)
        except Exception as exc:
            _close(conn)
            conn = None
            if not _transient(exc) or attempt == max_retries:
                raise
            log.warning("Source read retry %d/%d: %s", attempt + 1, max_retries,
                        str(exc).splitlines()[0])
            _retry_delay(attempt)
        finally:
            _close(conn)


def load_plan(max_retries) -> InjectionPlan:
    def metric(conn):
        with conn.cursor() as cur:
            batches = fetch_injectable_batch_ids(cur)
            return batches, fetch_urls_by_batches(cur, batches) if batches else []

    batch_ids, urls = _read(METRICDB, metric, max_retries)
    overrides, splits = _read(CRAWLERDB,
                             lambda c: load_sharding_config(INGEST_CONFIG, c), max_retries)
    return prepare_plan(urls, batch_ids, overrides, splits)


def _checkpoint(path, plan):
    state = json.loads(path.read_text()) if path.exists() else None
    if state is not None:
        if (not isinstance(state, dict)
                or state.get("version") != PLAN_VERSION or type(state.get("complete")) is not bool
                or type(state.get("cursor")) is not int or state["cursor"] < 0
                or not isinstance(state.get("fingerprint"), str)
                or not isinstance(state.get("counts"), dict)
                or any(type(state["counts"].get(k)) is not int or state["counts"][k] < 0
                       for k in COUNT_KEYS)
                or sum(state["counts"][k] for k in COUNT_KEYS) != state["cursor"]):
            raise ValueError("Invalid golden injection checkpoint; inspect it before restarting")
        if state["fingerprint"] == plan.fingerprint and not state["complete"]:
            if state["cursor"] > len(plan.records):
                raise ValueError("Checkpoint cursor is outside its input plan")
            log.info("Resuming committed progress at %d/%d URLs", state["cursor"], len(plan.records))
            return state
        if state["fingerprint"] != plan.fingerprint:
            log.warning("Source or routing changed; restarting the input scan with idempotent writes")
    # A completed weekly run starts fresh next time, so removed rows can be restored.
    return {"version": PLAN_VERSION, "fingerprint": plan.fingerprint, "cursor": 0,
            "counts": dict.fromkeys(COUNT_KEYS, 0), "complete": False,
            "started_at": time.time()}


def _write_batch(records, cache, conn, max_retries):
    for attempt in range(max_retries + 1):
        try:
            if conn is None or conn.closed:
                conn = _connect(CRAWLERDB)
            with conn.cursor() as cur:
                result = inject_batch(cur, records, cache)
            conn.commit()
            return conn, result
        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception as rollback_err:
                    log.warning("Rollback failed after batch error: %s", rollback_err)
            _close(conn)
            conn = None
            if not _transient(exc) or attempt == max_retries:
                raise
            log.warning("Batch retry %d/%d (SQLSTATE %s): %s", attempt + 1, max_retries,
                        getattr(exc, "pgcode", None), str(exc).splitlines()[0])
            _retry_delay(attempt)
    raise AssertionError("Unreachable retry state")


def run_plan(plan, state_dir, *, commit_interval=1000, max_retries=8, batch_pause=0.5):
    checkpoint = state_dir / "checkpoint.json"
    state = _checkpoint(checkpoint, plan)
    _atomic_json(checkpoint, state)  # Record identity before creating any database output.
    cache, conn = {}, None
    try:
        while state["cursor"] < len(plan.records):
            lo = state["cursor"]
            batch = plan.records[lo:lo + commit_interval]
            conn, result = _write_batch(batch, cache, conn, max_retries)
            # These IDs are now committed; failed attempts never enter this cache.
            cache.update(result.domains)
            state["cursor"] += len(batch)
            for key in COUNT_KEYS:
                state["counts"][key] += result.counts[key]
            state["updated_at"] = time.time()
            # An I/O error here must fail visibly, outside the database retry loop.
            # The previous checkpoint remains safe: replay uses actual INSERT RETURNING.
            _atomic_json(checkpoint, state)
            log.info("Committed progress: %d/%d URLs; new=%d marked=%d variants=%d",
                     state["cursor"], len(plan.records), *(state["counts"][k] for k in COUNT_KEYS))
            if batch_pause and state["cursor"] < len(plan.records):
                time.sleep(batch_pause)
        # Catch a routing migration during the run instead of declaring a stale plan complete.
        overrides, splits = _read(CRAWLERDB,
                                 lambda c: load_sharding_config(INGEST_CONFIG, c), max_retries)
        current = prepare_plan(
            [{"id": r.metric_id, "url": r.variant or r.url} for r in plan.records],
            plan.batch_ids, overrides, splits)
        if [(r.url, r.variant, r.domain, r.shard_id, r.metric_id) for r in current.records] != [
                (r.url, r.variant, r.domain, r.shard_id, r.metric_id) for r in plan.records]:
            raise RuntimeError("Routing changed during injection; rerun to reconcile the new plan")
        state["complete"] = True
        state["completed_at"] = time.time()
        _atomic_json(checkpoint, state)
        summary = {"status": "completed", "input_rows": plan.input_count,
                   "processed": state["cursor"], "skipped": plan.skipped,
                   "skipped_count": len(plan.skipped),
                   "normalized": plan.normalized, "batch_ids": plan.batch_ids,
                   "fingerprint": plan.fingerprint, **state["counts"]}
        _atomic_json(state_dir / "latest-run.json", summary)
        log.info("Done: processed=%d new=%d marked=%d variants=%d skipped=%d; report=%s",
                 state["cursor"], *(state["counts"][k] for k in COUNT_KEYS), len(plan.skipped),
                 state_dir / "latest-run.json")
        return summary
    finally:
        _close(conn)


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def nonnegative_float(value):
    number = float(value)
    if not 0 <= number < float("inf"):
        raise argparse.ArgumentTypeError("must be finite and nonnegative")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description="Inject historical golden URLs into crawlerdb")
    parser.add_argument("--dry-run", action="store_true", help="Plan with read-only connections; no writes/checkpoint")
    parser.add_argument("--commit-interval", type=positive_int, default=DEFAULT_COMMIT_INTERVAL)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR,
                        help="Shared persistent checkpoint/lock directory; use the same path for cron and manual runs")
    parser.add_argument("--max-retries", type=nonnegative_int, default=8)
    parser.add_argument("--batch-pause", type=nonnegative_float, default=0.5)
    args = parser.parse_args(argv)
    context = nullcontext(True) if args.dry_run else injection_lock(args.state_dir)
    with context as acquired:
        if not acquired:
            log.info("skipped_locked: another golden injection is already running")
            return {"status": "skipped_locked"}
        plan = load_plan(args.max_retries)
        log.info("Plan: inputs=%d valid=%d skipped=%d normalized=%d batches=%s",
                 plan.input_count, len(plan.records), len(plan.skipped), plan.normalized, plan.batch_ids)
        if plan.skipped:
            log.warning("Skipped %d invalid input records; first metric_url IDs: %s",
                        len(plan.skipped), [r["metric_url_id"] for r in plan.skipped[:5]])
        if args.dry_run:
            summary = {"status": "dry_run", "input_rows": plan.input_count,
                       "valid_rows": len(plan.records), "skipped": plan.skipped,
                       "normalized": plan.normalized, "batch_ids": plan.batch_ids,
                       "fingerprint": plan.fingerprint}
            log.info("Dry run: %s", json.dumps(summary, ensure_ascii=False, sort_keys=True))
            return summary
        return run_plan(plan, args.state_dir, commit_interval=args.commit_interval,
                        max_retries=args.max_retries, batch_pause=args.batch_pause)


if __name__ == "__main__":
    main()

"""
Daily Request Success Rate

Computes the crawler request success rate for one day from Loki logs and
writes it to metricdb.crawler_stat_total.request_success_rate.

The rate mirrors the Grafana "Success rate" panel:
    success_rate = end(status < 400)
                   / (end(all) + fail(fail_reason not HttpError.*))

robots-blocked records (fail_reason "IgnoreRequest Forbidden by robots.txt")
never reached the downloader, but they are HttpError-free fails and would be
excluded here only if their fail_reason matches the filter. They are kept out
of the denominator the same way the dashboard keeps them out: HttpError.* is
the only excluded class, so adjust the filter if robots noise distorts the rate.

Loki cannot serve a single 24h instant query for this volume, so the day is
summed over 24 hourly windows client-side.

Usage:
    python scripts/request_success_rate_daily.py [--date YYYY-MM-DD] [--dry-run]

--date defaults to yesterday (Asia/Taipei). Dates are interpreted in UTC+8 to
match how crawler_stat_total.stat_date is bucketed.
"""

import argparse
import logging
import urllib.parse
import urllib.request
import json
from datetime import datetime, date, timedelta, timezone

import psycopg2

from constants import METRICDB

LOKI_URL = "http://localhost:3100/loki/api/v1/query"
TZ = timezone(timedelta(hours=8))
QUERY_TIMEOUT = 60

_END = '{service="crawler", event="request.end"}'
NUM_EXPR = f'sum(count_over_time({_END} | json | status < 400 [1h]))'
END_EXPR = f'sum(count_over_time({_END}[1h]))'
FAIL_EXPR = (
    'sum(count_over_time({service="crawler", event="request.fail"} '
    '| json | fail_reason !~ "HttpError.*" [1h]))'
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def loki_count(expr: str, at_ns: int) -> float:
    url = LOKI_URL + "?" + urllib.parse.urlencode({"query": expr, "time": str(at_ns)})
    with urllib.request.urlopen(url, timeout=QUERY_TIMEOUT) as resp:
        data = json.load(resp)
    result = data["data"]["result"]
    return float(result[0]["value"][1]) if result else 0.0


def compute_success_rate(day: date) -> tuple[float, int, int]:
    """Return (success_rate, numerator, denominator) for the given day (UTC+8)."""
    start = datetime(day.year, day.month, day.day, tzinfo=TZ)
    num = end_total = fail = 0.0
    for hour in range(24):
        at_ns = int((start + timedelta(hours=hour + 1)).timestamp()) * 1_000_000_000
        num += loki_count(NUM_EXPR, at_ns)
        end_total += loki_count(END_EXPR, at_ns)
        fail += loki_count(FAIL_EXPR, at_ns)
    denom = end_total + fail
    rate = num / denom if denom else 0.0
    return rate, int(num), int(denom)


def write_rate(conn, day: date, rate: float) -> int:
    cur = conn.cursor()
    cur.execute(
        "ALTER TABLE crawler_stat_total "
        "ADD COLUMN IF NOT EXISTS request_success_rate DOUBLE PRECISION"
    )
    cur.execute(
        "UPDATE crawler_stat_total SET request_success_rate = %s WHERE stat_date = %s",
        (rate, day),
    )
    return cur.rowcount


def main():
    parser = argparse.ArgumentParser(
        description="Write daily Loki crawler success rate to metricdb"
    )
    parser.add_argument(
        "--date",
        help="Day to compute, YYYY-MM-DD (default: yesterday, Asia/Taipei)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print without writing to metricdb",
    )
    args = parser.parse_args()

    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        day = (datetime.now(TZ) - timedelta(days=1)).date()

    rate, num, denom = compute_success_rate(day)
    log.info(
        "date=%s success_rate=%.4f numerator=%d denominator=%d",
        day, rate, num, denom,
    )
    if denom == 0:
        log.warning("No request.end/fail logs for %s; skipping write", day)
        return
    if args.dry_run:
        log.info("Dry run; not writing")
        return

    conn = psycopg2.connect(**METRICDB)
    try:
        updated = write_rate(conn, day, rate)
        if updated == 0:
            log.warning(
                "No crawler_stat_total row for stat_date=%s; rate not stored", day
            )
            conn.rollback()
        else:
            conn.commit()
            log.info("Updated request_success_rate for stat_date=%s", day)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Check the ingest trap filter's audit log against the golden set.

The ingestors write every normalized / dropped discovery link to
/data/ipc/trap_filter/ingestor_NN/YYYY-MM-DD.jsonl.gz (see
containers/scheduler_ingest/ingestor/trap_filter.py). This script streams those
logs and reports, per rule:

  * drop events, and how many distinct golden URLs were dropped
    (false kills: golden_dropped / golden_total)
  * golden URLs that were normalized: not a kill (the stripped URL is still
    inserted), listed so the stripping can be reviewed

Golden URLs are matched by their normalized form too: metricdb keeps them as
copied from search results (often with `srsltid` / `utm_*`), while the crawler
discovers the clean page URL. A logged link hits a golden URL when the link or
its normalized form equals the golden URL or the golden URL's normalized form.
Only the golden set is held in memory; the logs are streamed.

Golden set: metricdb `metric_url` (read-only, scripts/constants.METRICDB), or a
text/CSV file with one URL per line (first column).

Usage:
    python scripts/trap_filter_golden_check.py --since 2026-11-01 --out /tmp/trap_check
    python scripts/trap_filter_golden_check.py --golden-file golden_urls.csv --log-dir ./logs
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from containers.scheduler_ingest.ingestor.trap_filter import read_audit_log  # noqa: E402
from libs.frontier.url_normalize import strip_params  # noqa: E402


def load_golden_from_file(path: Path) -> set[str]:
    golden: set[str] = set()
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.reader(fh):
            if row and row[0].startswith(("http://", "https://")):
                golden.add(row[0].strip())
    return golden


def load_golden_from_metricdb() -> set[str]:
    import psycopg2

    try:
        from scripts.constants import METRICDB
    except ModuleNotFoundError:
        from constants import METRICDB

    conn = psycopg2.connect(**METRICDB, options="-c default_transaction_read_only=on")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT url FROM metric_url WHERE url LIKE 'http%'")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def golden_index(golden: set[str]) -> dict[str, str]:
    """Map each golden URL and its normalized form to the golden URL."""
    index = {strip_params(g): g for g in golden}
    index.update({g: g for g in golden})
    return index


def match_golden(event: dict, index: dict[str, str]) -> str | None:
    url = event.get("url") or ""
    for key in (url, event.get("normalized_url"), strip_params(url)):
        if key and key in index:
            return index[key]
    return None


def _in_range(event: dict, since: str | None, until: str | None) -> bool:
    day = (event.get("ts") or "")[:10]
    return (since is None or day >= since) and (until is None or day <= until)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log-dir", type=Path, default=Path("/data/ipc/trap_filter"))
    ap.add_argument("--golden-file", type=Path, help="one URL per line; default: metricdb metric_url")
    ap.add_argument("--since", help="YYYY-MM-DD, inclusive")
    ap.add_argument("--until", help="YYYY-MM-DD, inclusive")
    ap.add_argument("--out", type=Path, help="directory for summary.csv / golden_hits.csv")
    args = ap.parse_args()

    golden = load_golden_from_file(args.golden_file) if args.golden_file else load_golden_from_metricdb()
    if not golden:
        sys.exit("golden set is empty")
    index = golden_index(golden)

    drop_events: Counter[str] = Counter()
    golden_dropped: dict[str, set[str]] = defaultdict(set)   # rule -> golden urls
    golden_normalized: dict[str, str] = {}                   # golden url -> stripped url
    hit_rows: dict[tuple[str, str], dict] = {}
    totals: Counter[str] = Counter()

    for ev in read_audit_log(args.log_dir):
        if not _in_range(ev, args.since, args.until):
            continue
        action = ev.get("action")
        totals[action] += 1
        url, norm = ev.get("url"), ev.get("normalized_url")
        hit = match_golden(ev, index)
        if action == "drop":
            rules = ev.get("rules") or ["?"]
            drop_events.update(rules)
            if hit:
                for rule in rules:
                    golden_dropped[rule].add(hit)
                hit_rows.setdefault((hit, "drop"), {
                    "golden_url": hit, "action": "drop", "rules": "|".join(rules),
                    "logged_url": url, "normalized_url": norm or "",
                    "discovered_from": ev.get("discovered_from") or "", "first_ts": ev.get("ts"),
                })
        elif action == "normalize" and hit:
            golden_normalized.setdefault(hit, norm or "")
            hit_rows.setdefault((hit, "normalize"), {
                "golden_url": hit, "action": "normalize", "rules": "",
                "logged_url": url, "normalized_url": norm or "",
                "discovered_from": ev.get("discovered_from") or "", "first_ts": ev.get("ts"),
            })

    all_dropped = set().union(*golden_dropped.values()) if golden_dropped else set()
    n = len(golden)
    summary = [
        {"rule": rule, "drop_events": drop_events[rule],
         "golden_dropped": len(golden_dropped[rule]),
         "golden_false_kill_rate": f"{len(golden_dropped[rule]) / n:.6%}"}
        for rule in sorted(drop_events, key=lambda r: -drop_events[r])
    ]
    summary.append({"rule": "ALL_DROP", "drop_events": totals["drop"],
                    "golden_dropped": len(all_dropped),
                    "golden_false_kill_rate": f"{len(all_dropped) / n:.6%}"})
    summary.append({"rule": "NORMALIZE (not a kill)", "drop_events": totals["normalize"],
                    "golden_dropped": len(golden_normalized),
                    "golden_false_kill_rate": f"{len(golden_normalized) / n:.6%}"})

    print(f"golden URLs: {n:,}   log events: drop {totals['drop']:,}, normalize {totals['normalize']:,}")
    print(f"{'rule':<26}{'drop_events':>14}{'golden_hit':>12}{'rate':>14}")
    for row in summary:
        print(f"{row['rule']:<26}{row['drop_events']:>14,}{row['golden_dropped']:>12,}"
              f"{row['golden_false_kill_rate']:>14}")
    by_host = Counter(urlsplit(u).hostname for u in all_dropped)
    if by_host:
        print("top hosts of dropped golden URLs:", ", ".join(f"{h} {c}" for h, c in by_host.most_common(10)))

    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
        with (args.out / "summary.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(summary[0]))
            w.writeheader()
            w.writerows(summary)
        with (args.out / "golden_hits.csv").open("w", newline="") as fh:
            fields = ["golden_url", "action", "rules", "logged_url", "normalized_url",
                      "discovered_from", "first_ts"]
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(hit_rows.values())
        print(f"wrote {args.out}/summary.csv, {args.out}/golden_hits.csv")


if __name__ == "__main__":
    main()

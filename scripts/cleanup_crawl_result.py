"""Delete ``/data/ipc/crawl_result`` folders only after every consumer has read them.

Today a root cron on Host2 deletes crawl_result files older than 8 hours,
whether or not they were processed. When the router or ingestor falls more
than 8 h behind (it did on 2026-11-04 and 11-05, after the move to YB), folders
are deleted before they are read, and those crawl results and outlinks are lost
without any error: FolderReader just skips the missing folders.

This script replaces the age-only rule with a progress-based one:

  crawl_result/crawler_NN/<date>/<HHMM>   is read by router NN
  crawl_result/ingestor_NN/<date>/<HHMM>  is read by ingestor NN and extractor NN
                                          (router and sitemap_patroller write it)

A folder is deleted when
  * every consumer's progress file (``/data/ipc/progress/<svc>/NN.json``) is at
    or past it, and it is at least ``--min-age-hours`` old (default 8, which
    keeps HTML around as long as the current cron does), or
  * it is older than ``--max-age-hours`` (default 48) even though it was not
    consumed. This is the disk safety net for a stopped consumer; each such
    folder is logged as UNCONSUMED so the loss is visible.

A base dir whose consumer has no progress file is left alone until the safety
net applies. Folder names are UTC (FolderReader/current_interval), so ages are
computed from the name, not mtime.

Dry run by default; pass ``--apply`` to delete. Suggested cron (Host2, root),
replacing the 8 h ``find ... -delete`` job:

  */30 * * * * cd /opt/WebCrawler && python3 -m scripts.cleanup_crawl_result --apply \\
      >> /var/log/crawl_result_cleanup.log 2>&1

Examples:
  python -m scripts.cleanup_crawl_result                        # dry run
  python -m scripts.cleanup_crawl_result --apply
  python -m scripts.cleanup_crawl_result --apply --min-age-hours 2 --max-age-hours 72
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from libs.config.loader import load_yaml, require

DEFAULT_CONFIG = "containers/scheduler_ingest/config/ingest.yaml"
DATE_RE = re.compile(r"^\d{8}$")
TIME_RE = re.compile(r"^\d{4}$")


@dataclass(frozen=True)
class Stage:
    """One crawl_result base dir and the progress files of every process that
    reads it."""
    base_dir: Path
    progress_files: tuple[Path, ...]


@dataclass(frozen=True)
class Decision:
    folder: Path
    consumed: bool
    age: timedelta


def _template_dir(template: str) -> tuple[Path, re.Pattern]:
    """Split ``/x/crawl_result/ingestor_{id:02d}`` into its parent and a regex
    that matches child dir names and captures the id."""
    path = Path(template)
    name = path.name
    m = re.fullmatch(r"(.*)\{id(?::0?(\d+)d)?\}(.*)", name)
    if not m:
        raise ValueError(f"template has no {{id}} in its last component: {template}")
    prefix, _, suffix = m.groups()
    return path.parent, re.compile(re.escape(prefix) + r"(\d+)" + re.escape(suffix) + r"$")


def discover_stages(raw: dict) -> list[Stage]:
    router = require(raw, "router")
    ingestor = require(raw, "ingestor")
    extractor = require(raw, "extractor")

    consumers = {
        # base-dir template -> progress templates of its readers
        str(require(router, "crawler_dir_template")): [str(require(router, "progress_template"))],
        str(require(ingestor, "result_dir_template")): [
            str(require(ingestor, "progress_template")),
            str(require(extractor, "progress_template")),
        ],
    }
    extractor_dir = str(require(extractor, "result_dir_template"))
    if extractor_dir not in consumers:
        raise ValueError("extractor.result_dir_template must equal ingestor.result_dir_template")

    stages: list[Stage] = []
    for dir_template, progress_templates in consumers.items():
        parent, pattern = _template_dir(dir_template)
        if not parent.is_dir():
            continue
        for child in sorted(parent.iterdir()):
            m = pattern.match(child.name)
            if not m or child.is_symlink() or not child.is_dir():
                continue
            worker_id = int(m.group(1))
            stages.append(Stage(
                base_dir=child,
                progress_files=tuple(Path(t.format(id=worker_id)) for t in progress_templates),
            ))
    return stages


def read_progress(path: Path) -> tuple[str, str] | None:
    """(date, time) a consumer has finished, as written by libs.ipc.Progress."""
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    date, time_ = state.get("date"), state.get("time")
    if not date or not time_:
        return None
    return str(date), str(time_)


def watermark(stage: Stage) -> tuple[str, str] | None:
    """The newest folder every consumer has finished; None when any consumer
    has no usable progress file."""
    marks = [read_progress(p) for p in stage.progress_files]
    if any(m is None for m in marks):
        return None
    return min(marks)


def _folders(base: Path) -> Iterator[tuple[str, str, Path]]:
    if base.is_symlink():
        return
    for date_dir in sorted(base.iterdir()):
        if date_dir.is_symlink() or not (date_dir.is_dir() and DATE_RE.match(date_dir.name)):
            continue
        for time_dir in sorted(date_dir.iterdir()):
            if not time_dir.is_symlink() and time_dir.is_dir() and TIME_RE.match(time_dir.name):
                yield date_dir.name, time_dir.name, time_dir


def plan(
    stage: Stage,
    now: datetime,
    min_age: timedelta,
    max_age: timedelta | None,
) -> list[Decision]:
    mark = watermark(stage)
    out: list[Decision] = []
    for date, time_, folder in _folders(stage.base_dir):
        try:
            started = datetime.strptime(date + time_, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        age = now - started
        if age < min_age:
            # Sorted ascending: everything after this is newer still.
            break
        consumed = mark is not None and (date, time_) <= mark
        if consumed or (max_age is not None and age >= max_age):
            out.append(Decision(folder=folder, consumed=consumed, age=age))
    return out


@contextmanager
def _directory_fd(path: Path) -> Iterator[int]:
    """Open a directory without following symlinks in any path component."""
    if ".." in path.parts:
        raise ValueError("parent path components are not allowed")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path.anchor or ".", flags)
    try:
        for part in path.parts[1:] if path.is_absolute() else path.parts:
            child_fd = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child_fd
        yield fd
    finally:
        os.close(fd)


def _remove_folder(base: Path, folder: Path) -> None:
    """Delete only a date/time child, anchored to no-follow directory FDs.

    Discovery checks alone cannot protect against a directory being replaced
    by a symlink after planning. FD-relative rmtree also protects its own
    traversal against symlink replacement while deleting descendants.
    """
    parts = folder.relative_to(base).parts
    if len(parts) != 2 or not DATE_RE.fullmatch(parts[0]) or not TIME_RE.fullmatch(parts[1]):
        raise ValueError("folder must be a date/time child of its cleanup base")
    with _directory_fd(base) as base_fd:
        date_fd = os.open(parts[0], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=base_fd)
        try:
            shutil.rmtree(parts[1], dir_fd=date_fd)
        finally:
            os.close(date_fd)


def _remove_empty_date_dirs(base: Path) -> None:
    with _directory_fd(base) as base_fd:
        for name in os.listdir(base_fd):
            if not DATE_RE.fullmatch(name):
                continue
            try:
                if stat.S_ISDIR(os.stat(name, dir_fd=base_fd, follow_symlinks=False).st_mode):
                    os.rmdir(name, dir_fd=base_fd)  # only succeeds when empty
            except OSError:
                pass


def run(
    stages: list[Stage],
    now: datetime,
    min_age: timedelta,
    max_age: timedelta | None,
    apply: bool,
    log=print,
) -> dict[str, int]:
    totals = {"consumed": 0, "unconsumed": 0, "errors": 0}
    for stage in stages:
        mark = watermark(stage)
        decisions = plan(stage, now, min_age, max_age)
        n_consumed = sum(d.consumed for d in decisions)
        log(
            f"{stage.base_dir}: watermark={'/'.join(mark) if mark else 'MISSING'} "
            f"delete_consumed={n_consumed} delete_unconsumed={len(decisions) - n_consumed}"
        )
        for d in decisions:
            if not d.consumed:
                log(f"  UNCONSUMED {d.folder} age={d.age.total_seconds() / 3600:.1f}h")
            if apply:
                try:
                    _remove_folder(stage.base_dir, d.folder)
                except (OSError, ValueError) as e:
                    totals["errors"] += 1
                    log(f"  ERROR {d.folder}: {e}")
                    continue
            totals["consumed" if d.consumed else "unconsumed"] += 1
        if apply:
            try:
                _remove_empty_date_dirs(stage.base_dir)
            except (OSError, ValueError) as e:
                totals["errors"] += 1
                log(f"  ERROR {stage.base_dir}: {e}")
    return totals


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--min-age-hours", type=float, default=8.0,
                    help="never delete a folder younger than this, consumed or not (default 8)")
    ap.add_argument("--max-age-hours", type=float, default=48.0,
                    help="delete folders older than this even if unconsumed; 0 disables (default 48)")
    ap.add_argument("--apply", action="store_true", help="delete; without it, only report")
    args = ap.parse_args(argv)

    if args.min_age_hours < 0 or args.max_age_hours < 0:
        ap.error("ages must be non-negative")
    max_age = timedelta(hours=args.max_age_hours) if args.max_age_hours > 0 else None
    min_age = timedelta(hours=args.min_age_hours)
    if max_age is not None and max_age < min_age:
        ap.error("--max-age-hours must be >= --min-age-hours")

    now = datetime.now(timezone.utc)
    stages = discover_stages(load_yaml(args.config))
    print(f"{now.isoformat()} {'APPLY' if args.apply else 'DRY RUN'} stages={len(stages)}")
    totals = run(stages, now, min_age, max_age, args.apply)
    verb = "deleted" if args.apply else "would delete"
    print(
        f"{verb}: consumed={totals['consumed']} unconsumed={totals['unconsumed']} "
        f"errors={totals['errors']}"
    )
    return 1 if totals["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())

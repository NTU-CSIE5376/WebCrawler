"""Ingest-side frontier filter for newly discovered links (status == "new").

Applied before links reach the DB, in this order:
  1. normalize: strip tracking and named session-id params
     (libs.frontier.url_normalize). The same page otherwise re-enters the
     frontier under a new URL on every visit.
  2. drop: discard links that hit a high-confidence trap rule
     (libs.frontier.trap_rules.HIGH_CONFIDENCE), checked on the normalized URL.

Only discovery records pass through here; crawl results keep their URL.
Normalization never touches the host, so shard_id / domain_id stay valid.

Every normalized or dropped link is appended to a daily gzip JSONL audit log
(<log_dir>/YYYY-MM-DD.jsonl.gz) so the decisions can be checked against the
golden set later (scripts/trap_filter_golden_check.py). Each append is its own
gzip member; `zcat` and gzip.open read the concatenation transparently.
"""
from __future__ import annotations

import gzip
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

from libs.frontier.trap_rules import HIGH_CONFIDENCE, match_rules
from libs.frontier.url_normalize import strip_params


LOGGER = logging.getLogger("ingestor")


@dataclass(frozen=True)
class TrapFilterConfig:
    enabled: bool = True
    normalize: bool = True
    drop: bool = True
    # `sid=` / `s=` holding a 32-hex token: often a session, sometimes a search
    # id (saskatchewan.ca), so off by default.
    normalize_generic_session_tokens: bool = False
    drop_rules: frozenset[str] = HIGH_CONFIDENCE
    log_dir: str | None = None


@dataclass
class TrapFilterStats:
    dropped: int = 0
    normalized: int = 0
    by_rule: Counter = field(default_factory=Counter)


class TrapFilter:
    def __init__(
        self,
        cfg: TrapFilterConfig,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self.cfg = cfg
        self._now = now

    def apply(self, recs: list[dict]) -> tuple[list[dict], TrapFilterStats]:
        """Return (kept records, stats). Normalized records are copies with a
        new `url`; dropped records are left out."""
        stats = TrapFilterStats()
        if not self.cfg.enabled:
            return recs, stats

        ts = self._now().replace(microsecond=0)
        ts_iso = ts.isoformat()
        kept: list[dict] = []
        events: list[dict] = []
        for rec in recs:
            url = rec.get("url")
            if not url:
                kept.append(rec)
                continue
            new_url = url
            if self.cfg.normalize:
                new_url = strip_params(
                    url, generic_session_tokens=self.cfg.normalize_generic_session_tokens,
                )
            hits = []
            if self.cfg.drop:
                hits = [r for r in match_rules(new_url) if r in self.cfg.drop_rules]
            if new_url == url and not hits:
                kept.append(rec)
                continue

            event = {"ts": ts_iso, "action": "drop" if hits else "normalize", "url": url}
            if new_url != url:
                event["normalized_url"] = new_url
            if hits:
                event["rules"] = hits
                stats.dropped += 1
                stats.by_rule.update(hits)
            else:
                stats.normalized += 1
                kept.append({**rec, "url": new_url})
            event["domain_id"] = rec.get("domain_id")
            event["shard_id"] = rec.get("shard_id")
            event["discovered_from"] = rec.get("discovered_from")
            events.append(event)

        if events and self.cfg.log_dir:
            self._write_log(ts, events)
        return kept, stats

    def _write_log(self, ts: datetime, events: list[dict]) -> None:
        # The audit log is for offline review; losing it must not stop ingest.
        try:
            log_dir = Path(self.cfg.log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(log_dir / f"{ts:%Y-%m-%d}.jsonl.gz", "at", encoding="utf-8") as fh:
                for event in events:
                    fh.write(json.dumps(event, ensure_ascii=False) + "\n")
        except OSError as e:
            LOGGER.error(
                "ingest.trap_filter_log_error",
                extra={"event": "ingest.trap_filter_log_error", "error": str(e),
                       "lost_events": len(events)},
            )


def read_audit_log(log_dir: Path) -> Iterator[dict]:
    """Yield every event under `log_dir` (recursively), oldest file first."""
    for path in sorted(log_dir.rglob("*.jsonl.gz"), key=lambda p: (p.name, str(p))):
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
        except (EOFError, gzip.BadGzipFile, json.JSONDecodeError) as e:
            # A process killed mid-append leaves a truncated last member.
            LOGGER.warning("trap_filter audit log %s truncated: %s", path, e)

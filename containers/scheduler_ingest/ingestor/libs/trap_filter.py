"""Ingest-side frontier filter for newly discovered links (status == "new").

Applied before links reach the DB, in this order:
  1. normalize: strip an approved set of params (default only `srsltid`,
     the same policy golden_inject applies via libs.urls.strip_srsltid;
     libs.frontier.url_normalize.strip_named_params). The same page otherwise
     re-enters the frontier under a new URL on every visit.
  2. shadow normalize: compute what the full strip_params (all tracking params,
     every named session param, 32-hex `sid` / `s` tokens) would remove beyond
     step 1 and only log it, so the remaining params can be validated before
     they are stripped for real.
  3. drop: discard links that hit a high-confidence trap rule
     (libs.frontier.trap_rules.HIGH_CONFIDENCE), checked on the URL from step 1.

Only discovery records pass through here; crawl results keep their URL.
Normalization never touches the host, so shard_id / domain_id stay valid.

Every dropped, normalized or would-be-normalized link is appended to a daily
gzip JSONL audit log (<log_dir>/YYYY-MM-DD.jsonl.gz) so the decisions can be
checked against the golden set later (scripts/trap_filter_golden_check.py).
`action` is what happened to the link: "drop", "normalize", or
"would_normalize" (kept as-is, shadow only). Bounded batches append gzip
members; `zcat` and gzip.open read the concatenation transparently.
"""
from __future__ import annotations

import atexit
import gzip
import json
import logging
import math
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

import pyarrow as pa
import pyarrow.compute as pc

from libs.frontier import URLPolicy, evaluate
from libs.frontier.trap_rules import DEFAULT_NORMALIZE_PARAMS


LOGGER = logging.getLogger("ingestor")


@dataclass(frozen=True)
class TrapFilterConfig(URLPolicy):
    log_dir: str | None = None
    audit_batch_size: int = 4096
    audit_max_buffer_bytes: int = 1024 * 1024
    audit_flush_interval_seconds: float = 1.0

    def __post_init__(self):
        if (self.audit_batch_size <= 0 or self.audit_max_buffer_bytes <= 0
                or not math.isfinite(self.audit_flush_interval_seconds)
                or self.audit_flush_interval_seconds <= 0):
            raise ValueError("audit batch, byte and flush interval limits must be positive and finite")


class _AuditLogWriter:
    """A bounded payload buffer with one lazy idle-flush thread.

    Writes hold the condition lock: a slow filesystem applies backpressure,
    rather than accumulating an unbounded queue of detached batches.
    """
    def __init__(self, cfg: TrapFilterConfig):
        self.cfg = cfg
        self._condition = threading.Condition()
        self._lines: list[bytes] = []
        self._bytes = 0
        self._date: str | None = None
        self._deadline: float | None = None
        self._thread: threading.Thread | None = None
        self._stopping = False

    def write(self, ts: datetime, events: list[dict]) -> None:
        date = ts.strftime("%Y-%m-%d")
        for event in events:
            line = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
            with self._condition:
                if self._stopping:
                    raise RuntimeError("audit writer is closed")
                if self._lines and (date != self._date
                        or self._bytes + len(line) > self.cfg.audit_max_buffer_bytes):
                    self._flush_locked()
                if len(line) > self.cfg.audit_max_buffer_bytes:
                    # Never retain an oversized event in the bounded queue.
                    self._append(date, [line])
                    continue
                if not self._lines:
                    self._date = date
                    self._deadline = time.monotonic() + self.cfg.audit_flush_interval_seconds
                self._lines.append(line)
                self._bytes += len(line)
                if (len(self._lines) >= self.cfg.audit_batch_size
                        or self._bytes >= self.cfg.audit_max_buffer_bytes):
                    self._flush_locked()
                elif len(self._lines) == 1:
                    if self._thread is None:
                        self._thread = threading.Thread(
                            target=self._run, name="discovery_audit_flush", daemon=True,
                        )
                        atexit.register(self.close)
                        self._thread.start()
                    self._condition.notify_all()

    def _append(self, date: str, lines: list[bytes]) -> None:
        # Audit output remains best effort, as before. Report a failed batch
        # and discard it so an unavailable filesystem cannot grow the buffer.
        try:
            log_dir = Path(self.cfg.log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            with gzip.open(log_dir / f"{date}.jsonl.gz", "ab") as fh:
                fh.writelines(lines)
        except OSError as exc:
            LOGGER.error(
                "ingest.trap_filter_log_error",
                extra={"event": "ingest.trap_filter_log_error", "error": str(exc),
                       "lost_events": len(lines)},
            )

    def _flush_locked(self) -> None:
        if not self._lines:
            return
        lines, date = self._lines, self._date
        self._lines = []
        self._bytes = 0
        self._date = self._deadline = None
        self._append(date, lines)

    def _run(self) -> None:
        with self._condition:
            while not self._stopping:
                if not self._lines:
                    self._condition.wait()
                    continue
                remaining = self._deadline - time.monotonic()
                if remaining > 0:
                    self._condition.wait(timeout=remaining)
                else:
                    self._flush_locked()

    def flush(self) -> None:
        with self._condition:
            self._flush_locked()
            self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            self._stopping = True
            self._flush_locked()
            self._condition.notify_all()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join()
            atexit.unregister(self.close)


@dataclass
class TrapFilterStats:
    dropped: int = 0
    normalized: int = 0
    would_normalize: int = 0
    by_rule: Counter = field(default_factory=Counter)


class TrapFilter:
    def __init__(
        self,
        cfg: TrapFilterConfig,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self.cfg = cfg
        self._now = now
        self._audit = _AuditLogWriter(cfg) if cfg.log_dir else None

    @staticmethod
    def _event(rec, decision, ts):
        new_url, shadow_url, hits = decision
        url = rec["url"]
        action = "drop" if hits else "normalize" if new_url != url else "would_normalize"
        event = {"ts": ts.isoformat(), "action": action, "url": url}
        if new_url != url:
            event["normalized_url"] = new_url
        if shadow_url is not None:
            event["would_normalize_url"] = shadow_url
        if hits:
            event["rules"] = hits
        event["domain_id"] = rec.get("domain_id")
        event["shard_id"] = rec.get("shard_id")
        event["discovered_from"] = rec.get("discovered_from")
        return event

    def evaluate_urls(self, urls):
        return evaluate(urls, self.cfg, backend='pyarrow')

    def record_decisions(self, records, decisions):
        """Count policy actions and materialize only affected audit rows."""
        dropped = decisions['dropped']
        changed = pc.fill_null(pc.not_equal(decisions['url'], decisions['normalized_url']), False)
        shadow = pc.is_valid(decisions['shadow_url'])
        stats = TrapFilterStats(
            dropped=pc.sum(dropped).as_py() or 0,
            normalized=pc.sum(pc.and_(changed, pc.invert(dropped))).as_py() or 0,
            would_normalize=pc.sum(shadow).as_py() or 0,
        )
        rule_names = decisions.schema.names[3:-1]
        stats.by_rule.update({name:pc.sum(decisions[name]).as_py() or 0 for name in rule_names
                              if pc.any(decisions[name]).as_py()})
        affected = pc.or_(dropped,pc.or_(changed,shadow))
        if self.cfg.log_dir and pc.any(affected).as_py():
            metadata = records.filter(affected)
            actions = decisions.filter(affected)
            frame = metadata
            for name in actions.schema.names[1:]:
                frame = frame.append_column('__policy_'+name,actions[name])
            ts = self._now().replace(microsecond=0)
            events = []
            for record in frame.to_pylist():
                hits = [name for name in rule_names if record['__policy_'+name]]
                decision = (record['__policy_normalized_url'],record['__policy_shadow_url'],hits)
                events.append(self._event(record,decision,ts))
            self._write_log(ts,events)
        return stats

    def apply(self, recs: list[dict]) -> tuple[list[dict], TrapFilterStats]:
        urls = pa.array([record.get('url') for record in recs],type=pa.string())
        decisions = self.evaluate_urls(urls)
        metadata = pa.table({'url':urls})
        if self.cfg.log_dir:
            for name, typ in [('domain_id',pa.int64()),('shard_id',pa.int64()),('discovered_from',pa.string())]:
                metadata = metadata.append_column(name,pa.array([record.get(name) for record in recs],type=typ))
        stats = self.record_decisions(metadata,decisions)
        kept = [record if url==record.get('url') else {**record,'url':url}
                for record,url,drop in zip(recs,decisions['normalized_url'].to_pylist(),decisions['dropped'].to_pylist())
                if not drop]
        return kept,stats

    def apply_one(self, record):
        kept, _ = self.apply([record])
        return kept[0] if kept else None

    def _write_log(self, ts: datetime, events: list[dict]) -> None:
        self._audit.write(ts, events)

    def flush(self) -> None:
        if self._audit is not None:
            self._audit.flush()

    def close(self) -> None:
        if self._audit is not None:
            self._audit.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


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

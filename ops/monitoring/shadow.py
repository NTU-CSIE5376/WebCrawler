"""Evaluate generated alerts without notifications; persist costs and state changes.

Only Prometheus/Loki instant-query GETs are implemented. There is no Grafana,
Discord or database client, and no notification credential is read. This is a
threshold/pending observer, not a replica of Grafana's notification grouping or
missing-series lifecycle. Round budgets are checked between serial requests.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import ipaddress
import json
import math
import os
import re
import sys
import threading
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

try:
    from .render_alerting import LOKI_UID, PROMETHEUS_UID, build_provisioning
except ImportError:
    from render_alerting import LOKI_UID, PROMETHEUS_UID, build_provisioning

MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_STATE_BYTES = 2 * 1024 * 1024
GROUPS = {"fast": 60, "symptoms": 300, "trends": 600}


def seconds(value: str) -> int:
    match = re.fullmatch(r"([0-9]+)([smh])", value)
    if not match:
        raise ValueError("unsupported duration")
    return int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]


def base_url(value: str) -> str:
    """Accept private IPs, loopback and local service names; never credentials."""
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        valid = (
            parsed.scheme in {"http", "https"}
            and host
            and parsed.username is None
            and parsed.password is None
            and parsed.path in {"", "/"}
            and not parsed.query
            and not parsed.fragment
            and parsed.port is not None
        )
        if valid:
            try:
                valid = ipaddress.ip_address(host).is_private
            except ValueError:
                valid = bool(re.fullmatch(r"[a-zA-Z0-9-]+", host)) or host.endswith(
                    (".localhost", ".local", ".internal")
                )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("use a private metrics endpoint with an explicit port")
    return value.rstrip("/")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise HTTPError(request.full_url, code, "redirect refused", headers, fp)


class QueryClient:
    def __init__(self, prometheus_url: str, loki_url: str, timeout_seconds: float = 5):
        if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 8:
            raise ValueError("query timeout must be within eight seconds")
        self.urls = {
            PROMETHEUS_UID: base_url(prometheus_url) + "/api/v1/query",
            LOKI_UID: base_url(loki_url) + "/loki/api/v1/query",
        }
        self.timeout = timeout_seconds
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def query(self, datasource: str, expression: str, now: float) -> dict:
        url = (
            self.urls[datasource]
            + "?"
            + urlencode(
                {"query": expression, "time": str(now), "timeout": f"{self.timeout}s"}
            )
        )
        request = Request(url, method="GET", headers={"Accept": "application/json"})
        with self.opener.open(request, timeout=self.timeout) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("metrics response exceeded limit")
        value = json.loads(body)
        if not isinstance(value, dict) or value.get("status") != "success":
            raise ValueError("metrics query failed")
        data = value["data"]
        if data["resultType"] != "vector" or not isinstance(data["result"], list):
            raise ValueError("instant vector required")
        return data


def _private_file(path: Path, mode: int):
    return os.open(path, mode | os.O_NOFOLLOW | os.O_CREAT, 0o600)


class Journal:
    def __init__(self, directory: Path, max_bytes: int = 10 * 1024 * 1024):
        if max_bytes < 1024 or max_bytes > 100 * 1024 * 1024:
            raise ValueError("journal limit must be 1 KiB to 100 MiB")
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.lstat()
        if directory.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("output directory must be private and owned by this user")
        self.directory, self.max_bytes = directory, max_bytes
        self.path = directory / "journal.jsonl"
        self.lock_fd = _private_file(directory / "observer.lock", os.O_WRONLY)
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.lock_fd)
            raise ValueError("another observer owns this output directory") from None

    def load(self) -> dict:
        path = self.directory / "state.json"
        try:
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
                raw = stream.read(MAX_STATE_BYTES + 1)
        except FileNotFoundError:
            return {
                "version": 1,
                "rules": {},
                "counters": {},
                "started_at": time.time(),
            }
        if len(raw) > MAX_STATE_BYTES:
            raise ValueError("observer state exceeded limit")
        state = json.loads(raw)
        if state.get("version") != 1 or not isinstance(state.get("rules"), dict):
            raise ValueError("invalid observer state")
        if not isinstance(state.get("counters"), dict):
            raise ValueError("invalid observer counters")
        return state

    def save(self, state: dict) -> None:
        body = json.dumps(state, allow_nan=False).encode()
        if len(body) > MAX_STATE_BYTES:
            raise ValueError("observer state exceeded limit")
        temporary = self.directory / "state.json.tmp"
        with os.fdopen(
            _private_file(temporary, os.O_WRONLY | os.O_TRUNC), "wb"
        ) as stream:
            stream.write(body)
        temporary.replace(self.directory / "state.json")

    def append(self, row: dict) -> None:
        body = (json.dumps(row, allow_nan=False, separators=(",", ":")) + "\n").encode()
        if len(body) > self.max_bytes:
            raise ValueError("journal record exceeded rotation limit")
        if self.path.exists() and self.path.stat().st_size + len(body) > self.max_bytes:
            for older, newer in (
                ("journal.jsonl.1", "journal.jsonl.2"),
                ("journal.jsonl", "journal.jsonl.1"),
            ):
                source = self.directory / older
                if source.is_symlink():
                    raise ValueError("symlink in journal")
                if source.exists():
                    source.replace(self.directory / newer)
        with os.fdopen(
            _private_file(self.path, os.O_WRONLY | os.O_APPEND), "wb"
        ) as stream:
            stream.write(body)

    def close(self) -> None:
        os.close(self.lock_fd)


def report(directory: Path, now: float | None = None) -> dict:
    """Read cumulative counters, which survive journal rotation; never query HTTP."""
    path = directory / "state.json"
    if directory.is_symlink() or path.is_symlink():
        raise ValueError("symlink in report path")
    with path.open("rb") as stream:
        raw = stream.read(MAX_STATE_BYTES + 1)
    if len(raw) > MAX_STATE_BYTES:
        raise ValueError("observer state exceeded limit")
    state = json.loads(raw)
    now = time.time() if now is None else now
    summaries = {}
    active = set(state.get("active_rule_uids", []))
    for uid, counts in state["counters"].items():
        if uid not in active:
            continue
        saved = state["rules"].get(uid, {})
        statuses = {}
        for instance in saved.get("instances", {}).values():
            status = instance["status"]
            statuses[status] = statuses.get(status, 0) + 1
        summaries[uid] = {
            **counts,
            "title": saved.get("title"),
            "last_known_instances": statuses,
            "last_evaluated_at": saved.get("last_evaluated_at"),
            "observed_since": saved.get("observed_since"),
            "fingerprint": saved.get("fingerprint"),
            "coverage": saved.get("coverage", "warmup"),
            "last_skipped_at": saved.get("last_skipped_at"),
            "sample_stale": saved.get("last_evaluated_at") is None
            or (
                now - saved["last_evaluated_at"]
                > 1.5 * saved.get("interval_seconds", 60) + 20
            ),
        }
    return {
        "started_at": state["started_at"],
        "last_round_at": state.get("last_round_at"),
        "reported_at": now,
        "observer_stale": state.get("last_round_at") is None
        or (now - state["last_round_at"] > 180),
        "active_groups": state.get("active_groups", []),
        "active_rule_count": len(active),
        "excluded_historical_rules": sorted(set(state["counters"]) - active),
        "rules": summaries,
        "note": "Firing episodes are per alert instance, not Discord messages; partial coverage and unknown evaluations remain separate.",
    }


class Evaluator:
    def __init__(
        self,
        documents: dict,
        client: QueryClient,
        journal: Journal,
        *,
        groups: tuple[str, ...] = ("fast",),
        clock=time.time,
        monotonic=time.monotonic,
        round_budget_seconds: float = 20,
    ):
        if not groups or not set(groups) <= GROUPS.keys():
            raise ValueError("groups must select fast, symptoms and/or trends")
        if (
            not math.isfinite(round_budget_seconds)
            or not 0 < round_budget_seconds <= 60
        ):
            raise ValueError("round budget must be within sixty seconds")
        self.client, self.journal = client, journal
        self.clock, self.monotonic, self.budget = clock, monotonic, round_budget_seconds
        self._lock = threading.Lock()
        self.state = journal.load()
        previously_active = set(self.state.get("active_rule_uids", []))
        self.rules = {}
        now = clock()
        for group in documents["webcrawler-rules.json"]["groups"]:
            kind = group["name"].removeprefix("webcrawler-")
            if kind not in groups:
                continue
            interval = seconds(group["interval"])
            if interval != GROUPS[kind]:
                raise ValueError("unexpected shadow evaluation interval")
            for rule in group["rules"]:
                self.rules[rule["uid"]] = (rule, interval)
                saved = self.state["rules"].setdefault(rule["uid"], {})
                fingerprint = hashlib.sha256(
                    json.dumps(
                        {
                            "query": rule["data"][0],
                            "threshold": rule["data"][2],
                            "pending": rule["for"],
                            "labels": rule["labels"],
                            "interval": interval,
                        },
                        sort_keys=True,
                    ).encode()
                ).hexdigest()
                if saved.get("fingerprint") not in {None, fingerprint} or (
                    saved and rule["uid"] not in previously_active
                ):
                    saved.clear()
                    self.state["counters"][rule["uid"]] = {}
                saved["fingerprint"] = fingerprint
                saved["title"] = rule["title"]
                saved["interval_seconds"] = interval
                saved.setdefault("observed_since", now)
                saved.setdefault("coverage", "warmup")
                saved.setdefault("instances", {})
                # Warm up before the first expensive query. --once obeys this too.
                saved.setdefault("next_due", now if kind == "fast" else now + interval)
                self.state["counters"].setdefault(rule["uid"], {})
        self.state["active_groups"] = list(groups)
        self.state["active_rule_uids"] = sorted(self.rules)

    def _count(self, uid: str, key: str, increment: float = 1) -> None:
        counts = self.state["counters"][uid]
        counts[key] = counts.get(key, 0) + increment

    def _record(self, uid: str, row: dict) -> None:
        self.journal.append({"rule": uid, **row})

    def _instances(
        self,
        rule: dict,
        interval: int,
        now: float,
        data: dict | None,
        reason: str | None,
    ) -> None:
        uid = rule["uid"]
        saved = self.state["rules"][uid]
        previous = saved["instances"]
        interrupted = saved.pop("pending_interrupted", False)
        current = {}
        threshold = rule["data"][2]["model"]["conditions"][0]["evaluator"]["params"][0]
        pending = seconds(rule["for"])
        samples = [] if data is None else data["result"]
        if reason is None and not samples:
            reason = "no_data"
            self._count(uid, "no_data")
        if reason:
            samples = [{"metric": {}, "value": [now, "0"]}]
        for sample in samples:
            metric = sample["metric"]
            if not isinstance(metric, dict) or len(metric) > 32:
                raise ValueError("invalid metric labels")
            labels = {k: v for k, v in metric.items() if k != "__name__"}
            labels.update(rule["labels"])
            if any(not isinstance(v, str) or len(v) > 256 for v in labels.values()):
                raise ValueError("metric label value is invalid")
            identity = hashlib.sha256(
                json.dumps(labels, sort_keys=True).encode()
            ).hexdigest()[:24]
            value = float(sample["value"][1])
            if not math.isfinite(value):
                raise ValueError("nonfinite metric sample")
            old = previous.get(identity, {})
            gap = now - old.get("at", now) > interval * 1.5
            since = old.get("since", now)
            if reason:
                status = "unknown" if reason == "budget_skipped" else "firing"
                since = now
            elif value <= threshold:
                status, since = "normal", now
            else:
                # Unknown coverage cannot prove a pending duration, but it also
                # cannot prove an existing firing incident resolved/restarted.
                continuing_firing = old.get("status") == "firing" and not old.get(
                    "reason"
                )
                if not continuing_firing and (
                    gap
                    or interrupted
                    or old.get("reason")
                    or old.get("status") not in {"pending", "firing"}
                ):
                    since = now
                status = (
                    "firing"
                    if continuing_firing or now - since >= pending
                    else "pending"
                )
            current[identity] = {
                "status": status,
                "since": since,
                "at": now,
                "reason": reason,
                "labels": labels,
            }
            if status != old.get("status") or reason != old.get("reason"):
                self._count(uid, "transitions")
                if status == "firing" and old.get("status") != "firing":
                    self._count(uid, "firing_episodes")
                self._record(
                    uid,
                    {
                        "event": "transition",
                        "at": now,
                        "instance": identity,
                        "from": old.get("status"),
                        "to": status,
                        "reason": reason,
                        "labels": labels,
                    },
                )
        for identity, old in previous.items():
            if identity not in current and old["status"] != "missing":
                self._count(uid, "transitions")
                self._record(
                    uid,
                    {
                        "event": "transition",
                        "at": now,
                        "instance": identity,
                        "from": old["status"],
                        "to": "missing",
                        "reason": "series_disappeared",
                        "labels": old["labels"],
                    },
                )
        saved["instances"] = current
        saved["coverage"] = reason or "observed"

    def run_due(self, now: float | None = None) -> int:
        """Run serial queries; another concurrent invocation is rejected."""
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("shadow evaluation already running")
        try:
            now = self.clock() if now is None else now
            started, cache, evaluated = self.monotonic(), {}, 0
            due = sorted(
                self.rules, key=lambda uid: self.state["rules"][uid]["next_due"]
            )
            for uid in due:
                rule, interval = self.rules[uid]
                saved = self.state["rules"][uid]
                if saved["next_due"] > now:
                    continue
                if self.monotonic() - started >= self.budget:
                    self._count(uid, "skipped")
                    saved["coverage"] = "unknown"
                    saved["pending_interrupted"] = True
                    saved["last_skipped_at"] = now
                    self._record(
                        uid,
                        {"event": "evaluation", "at": now, "reason": "budget_skipped"},
                    )
                    # Keep the oldest due time, so it goes first next round.
                    continue
                query = rule["data"][0]
                key = (query["datasourceUid"], query["model"]["expr"])
                cached = key in cache
                query_started = self.monotonic()
                if not cached:
                    try:
                        cache[key] = (self.client.query(*key, now), None)
                    except Exception:
                        cache[key] = (None, "query_error")
                data, reason = cache[key]
                elapsed = self.monotonic() - query_started
                self._count(uid, "evaluations")
                self._count(uid, "query_seconds", elapsed)
                if reason:
                    self._count(uid, "query_errors")
                stats = {} if data is None else data.get("stats", {})
                summary = stats.get("summary", {}) if isinstance(stats, dict) else {}
                if not isinstance(summary, dict):
                    summary = {}
                cost = {}
                for source, target in (
                    ("totalBytesProcessed", "bytes_processed"),
                    ("totalLinesProcessed", "lines_processed"),
                ):
                    value = summary.get(source, 0) if not cached else 0
                    if (
                        isinstance(value, (int, float))
                        and math.isfinite(value)
                        and value >= 0
                    ):
                        cost[target] = value
                        self._count(uid, target, value)
                try:
                    self._instances(rule, interval, now, data, reason)
                except (ValueError, TypeError, KeyError, IndexError, OverflowError):
                    reason = "query_error"
                    self._count(uid, "query_errors")
                    self._instances(rule, interval, now, None, reason)
                self._record(
                    uid,
                    {
                        "event": "evaluation",
                        "at": now,
                        "reason": reason,
                        "elapsed_seconds": elapsed,
                        "cached": cached,
                        **cost,
                    },
                )
                saved["next_due"] = now + interval
                saved["last_evaluated_at"] = now
                evaluated += 1
            self.state["last_round_at"] = now
            self.journal.save(self.state)
            return evaluated
        finally:
            self._lock.release()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--prometheus-url")
    parser.add_argument("--loki-url")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--groups", default="fast", help="fast,symptoms,trends; default fast only"
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args(argv)
    journal = None
    try:
        if args.report:
            print(json.dumps(report(args.output_dir), ensure_ascii=False, indent=2))
            return 0
        if not args.config or not args.prometheus_url or not args.loki_url:
            parser.error(
                "evaluation requires --config, --prometheus-url and --loki-url"
            )
        config = json.loads(args.config.read_text())
        documents = build_provisioning(config)
        client = QueryClient(args.prometheus_url, args.loki_url)
        journal = Journal(args.output_dir)
        evaluator = Evaluator(
            documents, client, journal, groups=tuple(args.groups.split(","))
        )
        while True:
            round_started = time.monotonic()
            count = evaluator.run_due()
            print(
                json.dumps(
                    {
                        "event": "shadow_round",
                        "evaluated_rules": count,
                        "notifications": False,
                    }
                ),
                flush=True,
            )
            if args.once:
                break
            time.sleep(max(1, 60 - (time.monotonic() - round_started)))
        return 0
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError, TypeError, KeyError):
        print(
            "Shadow observer failed; check local config/state and private metrics endpoints.",
            file=sys.stderr,
        )
        return 1
    finally:
        if journal is not None:
            journal.close()


if __name__ == "__main__":
    raise SystemExit(main())

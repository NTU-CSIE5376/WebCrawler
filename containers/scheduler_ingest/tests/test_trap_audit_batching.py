import contextlib
from datetime import datetime, timedelta, timezone
import gzip
import json
import threading

import pytest

from containers.scheduler_ingest.ingestor import trap_filter as module
from containers.scheduler_ingest.ingestor.trap_filter import TrapFilter, TrapFilterConfig, read_audit_log


NOW = datetime(2026, 11, 28, 12, tzinfo=timezone.utc)


def record(i):
    return dict(url=f"https://example.test/p/{i}?srsltid=跟踪", status="new",
                domain_id=7, shard_id=3, discovered_from="https://example.test/parent")


@pytest.fixture
def filters(tmp_path):
    made = []

    def make(name="audit", now=lambda: NOW, **kwargs):
        kwargs.setdefault("audit_flush_interval_seconds", 60)
        obj = TrapFilter(TrapFilterConfig(log_dir=str(tmp_path / name), **kwargs), now=now)
        made.append(obj)
        return obj

    yield make
    for obj in made:
        obj.close()


@pytest.fixture
def writes(monkeypatch):
    real = gzip.open
    batches = []
    completed = threading.Event()

    @contextlib.contextmanager
    def opened(path, mode="rb", **kwargs):
        with real(path, mode, **kwargs) as stream:
            if mode == "ab":
                class Recorder:
                    def writelines(self, lines):
                        lines = list(lines)
                        batches.append((str(path), lines))
                        stream.writelines(lines)
                yield Recorder()
            else:
                yield stream
        if mode == "ab":
            completed.set()

    monkeypatch.setattr(module.gzip, "open", opened)
    return batches, completed


def test_event_limit_and_shutdown_preserve_order_and_every_field(filters, writes, tmp_path):
    f = filters(audit_batch_size=4)
    for i in range(3):
        f.apply([record(i)])
    assert writes[0] == []
    f.apply([record(3)])
    assert len(writes[0]) == 1
    f.apply([record(4)])
    f.close()
    f.close()  # Idempotent explicit/atexit/service shutdown.
    events = list(read_audit_log(tmp_path / "audit"))
    assert len(writes[0]) == 2
    assert [e["url"] for e in events] == [record(i)["url"] for i in range(5)]
    assert events[0] == dict(ts=NOW.isoformat(), action="normalize", url=record(0)["url"],
                            normalized_url="https://example.test/p/0", domain_id=7,
                            shard_id=3, discovered_from="https://example.test/parent")


def test_byte_cap_uses_encoded_bytes_and_never_accumulates_oversized_events(filters, writes, tmp_path):
    f = filters(audit_batch_size=100, audit_max_buffer_bytes=100)
    # Three short UTF-8 events exceed 100 bytes collectively. A fourth event
    # exceeds the entire budget and must be emitted alone, not queued.
    events = [{"url": "例子" * 4, "n": i} for i in range(3)] + [{"url": "大" * 100}]
    f._write_log(NOW, events)
    f.close()
    batches = writes[0]
    assert len(batches) >= 3
    for _, lines in batches:
        assert sum(map(len, lines)) <= 100 or len(lines) == 1 and len(lines[0]) > 100
    assert list(read_audit_log(tmp_path / "audit")) == events


def test_idle_timer_flushes_without_another_record_or_db_poll(filters, writes, tmp_path):
    f = filters(audit_flush_interval_seconds=0.02)
    f.apply([record(0)])
    # No second apply/flush call: simulates discovery blocked in a DB call.
    assert writes[1].wait(3), "audit timer failed to publish an idle tail"
    assert len(list(read_audit_log(tmp_path / "audit"))) == 1


def test_day_rollover_uses_original_event_date(filters, tmp_path):
    now = [NOW]
    f = filters(now=lambda: now[0])
    f.apply([record(0)])
    now[0] += timedelta(days=1)
    f.apply([record(1)])
    f.close()
    for day, i in [(NOW, 0), (now[0], 1)]:
        with gzip.open(tmp_path / "audit" / f"{day:%Y-%m-%d}.jsonl.gz", "rt") as stream:
            events = [json.loads(line) for line in stream]
        assert [e["url"] for e in events] == [record(i)["url"]]
        assert events[0]["ts"] == day.isoformat()


def test_timer_and_threshold_flushes_do_not_duplicate_or_reorder_events(filters, tmp_path):
    f = filters(audit_batch_size=17, audit_max_buffer_bytes=4096,
                audit_flush_interval_seconds=0.001)
    for i in range(500):
        f.apply([record(i)])
    f.close()
    assert [e["url"] for e in read_audit_log(tmp_path / "audit")] == [record(i)["url"] for i in range(500)]


def test_failed_batch_is_reported_and_does_not_poison_later_writes(filters, monkeypatch, caplog, tmp_path):
    f = filters(audit_batch_size=2)
    real = gzip.open
    calls = []

    def failed_once(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise OSError("disk unavailable")
        return real(*args, **kwargs)

    monkeypatch.setattr(module.gzip, "open", failed_once)
    kept, stats = f.apply([record(0), record(1)])
    assert len(kept) == 2 and stats.normalized == 2
    errors = [r for r in caplog.records if getattr(r, "event", "") == "ingest.trap_filter_log_error"]
    assert len(errors) == 1 and errors[0].lost_events == 2
    f.apply([record(2), record(3)])
    f.close()
    assert [e["url"] for e in read_audit_log(tmp_path / "audit")] == [record(2)["url"], record(3)["url"]]


def test_batching_preserves_filter_outputs_and_audits(filters, tmp_path):
    rows = [record(0), dict(record(1), url="https://example.test/x/x/x/"),
            dict(record(2), url="https://example.test/p?utm_source=x"),
            dict(record(3), url="https://example.test/clean")]
    outputs = []
    logs = []
    for name, size in [("immediate", 1), ("batched", 4096)]:
        f = filters(name, audit_batch_size=size)
        outputs.append([f.apply([row]) for row in rows])
        f.close()
        logs.append(list(read_audit_log(tmp_path / name)))
    assert outputs[0] == outputs[1]
    assert logs[0] == logs[1]


@pytest.mark.parametrize("setting,value", [("audit_batch_size", 0), ("audit_max_buffer_bytes", -1),
                                         ("audit_flush_interval_seconds", 0),
                                         ("audit_flush_interval_seconds", float("nan")),
                                         ("audit_flush_interval_seconds", float("inf"))])
def test_invalid_limits_fail_at_startup(setting, value):
    with pytest.raises(ValueError, match="positive and finite"):
        TrapFilterConfig(**{setting: value})


def test_yaml_overrides_audit_limits():
    from containers.scheduler_ingest.ingestor.main import _load_trap_filter
    f = _load_trap_filter({"trap_filter": dict(audit_batch_size=16000,
                          audit_max_buffer_bytes=2048, audit_flush_interval_seconds=2.5)}, 0)
    assert f.cfg.audit_batch_size == 16000
    assert f.cfg.audit_max_buffer_bytes == 2048
    assert f.cfg.audit_flush_interval_seconds == 2.5
    f.close()


def test_discovery_failure_still_flushes_audit_tail(tmp_path):
    from containers.scheduler_ingest.tests.test_split_ingestors import setup, link
    f = TrapFilter(TrapFilterConfig(log_dir=str(tmp_path / "audit"), audit_flush_interval_seconds=60),
                   now=lambda: NOW)
    svc, _, _, _ = setup(tmp_path, trap_filter=f)
    svc.add(link("https://example.test/a?srsltid=1"))
    svc.db.process_batch.side_effect = RuntimeError("DB failed")
    with pytest.raises(RuntimeError, match="DB failed"):
        svc.close()
    assert len(list(read_audit_log(tmp_path / "audit"))) == 1


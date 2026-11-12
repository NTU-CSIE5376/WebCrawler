"""Daily maintenance must not repeat a completed pass after service recreation."""
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from containers.scheduler_control import daily_state
from containers.scheduler_control.accounting import service as accounting
from containers.scheduler_control.gc import service as gc


class StopLoop(BaseException):
    pass


class Clock(datetime):
    current = datetime(2026, 11, 12, 12, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):
        assert tz is UTC
        return cls.current.astimezone(tz)


@pytest.fixture(params=["gc", "accounting"])
def service_case(request, tmp_path, monkeypatch):
    module = gc if request.param == "gc" else accounting
    Clock.current = datetime(2026, 11, 12, 12, tzinfo=UTC)
    monkeypatch.setattr(module, "datetime", Clock)
    common = {
        "total_shards": 1, "batch_size": 1, "run_hour_utc": 4, "run_minute_utc": 0,
        "check_interval_sec": 60, "catch_up_on_start": True,
        "state_path": str(tmp_path / f"{request.param}.json"),
    }
    if module is gc:
        cfg = gc.FrontierGCConfig(**common, frontier_cap=100, yield_floor=.01,
                                  sample_percent=.5, stale_pending_days=60,
                                  frozen_pending_days=7)
        cls = gc.FrontierGCService
    else:
        cfg = accounting.CounterRolloffConfig(**common, event_retention_days=90,
                                              history_retention_days=30)
        cls = accounting.CounterRolloffService
    session = Mock(side_effect=AssertionError("A state test must not touch the database"))
    return module, cls, cfg, session, request.param


def one_loop(service, module, monkeypatch):
    monkeypatch.setattr(module.time, "sleep", lambda seconds: (_ for _ in ()).throw(StopLoop()))
    with pytest.raises(StopLoop):
        service.run_forever()


def test_same_day_restart_skips_completed_pass(service_case, monkeypatch):
    module, cls, cfg, session, name = service_case
    first = cls(cfg, session)
    first.run_once = Mock()
    one_loop(first, module, monkeypatch)
    first.run_once.assert_called_once()

    restarted = cls(cfg, session)
    restarted.run_once = Mock()
    one_loop(restarted, module, monkeypatch)
    restarted.run_once.assert_not_called()
    session.assert_not_called()
    assert daily_state.load_last_success_day(
        cfg.state_path, service=name, today_utc=Clock.current.date(),
    ) == date(2026, 11, 12)


def test_next_day_runs_after_its_utc_schedule(service_case, monkeypatch):
    module, cls, cfg, session, _ = service_case
    first = cls(cfg, session)
    first.run_once = Mock()
    first.run_once_and_record()
    Clock.current += timedelta(days=1)
    restarted = cls(cfg, session)
    restarted.run_once = Mock()
    one_loop(restarted, module, monkeypatch)
    restarted.run_once.assert_called_once()


def test_next_day_before_schedule_waits(service_case, monkeypatch):
    module, cls, cfg, session, _ = service_case
    first = cls(cfg, session)
    first.run_once = Mock()
    first.run_once_and_record()
    Clock.current = datetime(2026, 11, 13, 3, 59, tzinfo=UTC)
    restarted = cls(cfg, session)
    restarted.run_once = Mock()
    one_loop(restarted, module, monkeypatch)
    restarted.run_once.assert_not_called()


def test_failed_run_does_not_mark_completion(service_case, monkeypatch):
    module, cls, cfg, session, _ = service_case
    service = cls(cfg, session)
    service.run_once = Mock(side_effect=RuntimeError("Pass failed"))
    one_loop(service, module, monkeypatch)
    assert not Path(cfg.state_path).exists()
    assert service._last_run_for_day is None


def test_failed_run_preserves_previous_completion(service_case, monkeypatch):
    module, cls, cfg, session, name = service_case
    yesterday = Clock.current.date() - timedelta(days=1)
    daily_state.save_last_success_day(cfg.state_path, service=name, day_utc=yesterday)
    original = Path(cfg.state_path).read_bytes()
    service = cls(cfg, session)
    service.run_once = Mock(side_effect=RuntimeError("Pass failed"))
    one_loop(service, module, monkeypatch)
    assert Path(cfg.state_path).read_bytes() == original
    assert service._last_run_for_day == yesterday


def test_missing_or_unconfigured_state_keeps_catch_up(service_case, monkeypatch):
    module, cls, cfg, session, _ = service_case
    for candidate in (cfg, replace(cfg, state_path=None)):
        service = cls(candidate, session)
        service.run_once = Mock()
        one_loop(service, module, monkeypatch)
        service.run_once.assert_called_once()
        Path(cfg.state_path).unlink(missing_ok=True)


@pytest.mark.parametrize("kind", ["malformed", "future", "wrong_service", "compact_date"])
def test_bad_state_fails_before_any_database_work(service_case, kind):
    _, cls, cfg, session, name = service_case
    payload = {"version": 1, "service": name, "last_success_day": "2026-11-12"}
    if kind == "future":
        payload["last_success_day"] = "2026-11-13"
    elif kind == "wrong_service":
        payload["service"] = "other"
    elif kind == "compact_date":
        payload["last_success_day"] = "20261112"
    Path(cfg.state_path).write_text("not json" if kind == "malformed" else json.dumps(payload))
    with pytest.raises(daily_state.DailyStateError):
        cls(cfg, session)
    session.assert_not_called()


def test_completion_uses_run_start_day_across_midnight(service_case):
    _, cls, cfg, session, name = service_case
    Clock.current = datetime(2026, 11, 12, 23, 59, tzinfo=UTC)
    service = cls(cfg, session)

    def finish_tomorrow():
        Clock.current += timedelta(minutes=2)

    service.run_once = finish_tomorrow
    service.run_once_and_record()
    assert Clock.current.date() == date(2026, 11, 13)
    assert daily_state.load_last_success_day(
        cfg.state_path, service=name, today_utc=Clock.current.date(),
    ) == date(2026, 11, 12)


def test_completion_uses_utc_date_not_taipei_date(service_case):
    _, cls, cfg, session, name = service_case
    Clock.current = datetime(2026, 11, 13, 0, 30, tzinfo=timezone(timedelta(hours=8)))
    service = cls(cfg, session)
    service.run_once = Mock()
    service.run_once_and_record()
    assert daily_state.load_last_success_day(
        cfg.state_path, service=name, today_utc=date(2026, 11, 12),
    ) == date(2026, 11, 12)


def test_persistence_failure_does_not_repeat_work_in_same_process(service_case, monkeypatch):
    module, cls, cfg, session, _ = service_case
    service = cls(cfg, session)
    service.run_once = Mock()
    monkeypatch.setattr(module, "save_last_success_day", Mock(side_effect=daily_state.DailyStateError("full")))
    one_loop(service, module, monkeypatch)
    one_loop(service, module, monkeypatch)
    service.run_once.assert_called_once()


def test_state_write_is_atomic_and_fsyncs_file_and_directory(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "gc.json"
    observed = []
    original_fsync = daily_state.os.fsync

    def fsync(fd):
        observed.append(fd)
        original_fsync(fd)

    monkeypatch.setattr(daily_state.os, "fsync", fsync)
    daily_state.save_last_success_day(str(target), service="gc", day_utc=date(2026, 11, 12))
    assert len(observed) == 2
    assert target.stat().st_mode & 0o777 == 0o600
    assert [entry.name for entry in target.parent.iterdir()] == ["gc.json"]


def test_failed_atomic_write_preserves_previous_state(tmp_path, monkeypatch):
    target = tmp_path / "gc.json"
    daily_state.save_last_success_day(str(target), service="gc", day_utc=date(2026, 11, 11))
    original = target.read_bytes()
    monkeypatch.setattr(daily_state.os, "replace", Mock(side_effect=OSError("full")))
    with pytest.raises(daily_state.DailyStateError):
        daily_state.save_last_success_day(str(target), service="gc", day_utc=date(2026, 11, 12))
    assert target.read_bytes() == original
    assert [entry.name for entry in target.parent.iterdir()] == ["gc.json"]


def test_malformed_state_error_does_not_disclose_contents(tmp_path):
    target = tmp_path / "gc.json"
    target.write_text(json.dumps({"version": 1, "service": "gc", "last_success_day": "private-token"}))
    with pytest.raises(daily_state.DailyStateError) as exc:
        daily_state.load_last_success_day(str(target), service="gc", today_utc=date(2026, 11, 12))
    assert "private-token" not in str(exc.value)
    assert exc.value.__suppress_context__


@pytest.mark.parametrize("name", ["gc", "accounting"])
def test_main_wires_state_path_and_explicit_once_completion(name, monkeypatch):
    if name == "gc":
        from containers.scheduler_control.gc import main
        service_name = "FrontierGCService"
    else:
        from containers.scheduler_control.accounting import main
        service_name = "CounterRolloffService"
    root = Path(__file__).resolve().parents[1]
    observed = {}
    monkeypatch.setattr(main, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(main, "create_engine", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "sessionmaker", lambda **kwargs: None)

    class Service:
        def __init__(self, *, cfg, Session):
            observed["state_path"] = cfg.state_path

        def run_once_and_record(self):
            observed["recorded"] = True

    monkeypatch.setattr(main, service_name, Service)
    monkeypatch.setattr("sys.argv", [name, "--config",
                        str(root / "containers/scheduler_control/config/control.yaml"), "--once"])
    main.main()
    assert observed == {"state_path": f"/data/ipc/progress/daily/{name}.json", "recorded": True}

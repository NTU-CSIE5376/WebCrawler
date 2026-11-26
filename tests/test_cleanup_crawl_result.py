from __future__ import annotations

import json
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from scripts import cleanup_crawl_result as cleanup

NOW = datetime(2026, 11, 5, 20, 0, tzinfo=timezone.utc)


@pytest.fixture
def frozen_clock(monkeypatch):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(cleanup, "datetime", FrozenDatetime)


@pytest.fixture
def ipc(tmp_path):
    base = tmp_path / "ipc"
    cfg = {
        "router": {
            "crawler_dir_template": str(base / "crawl_result/crawler_{id:02d}"),
            "progress_template": str(base / "progress/router/{id:02d}.json"),
        },
        "ingestor": {
            "result_dir_template": str(base / "crawl_result/ingestor_{id:02d}"),
            "progress_template": str(base / "progress/ingestor/{id:02d}.json"),
        },
        "extractor": {
            "result_dir_template": str(base / "crawl_result/ingestor_{id:02d}"),
            "progress_template": str(base / "progress/extractor/{id:02d}.json"),
        },
    }
    cfg_path = tmp_path / "ingest.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    return base, cfg, cfg_path


def _folder(base, kind, worker, hours_ago):
    t = NOW - timedelta(hours=hours_ago)
    t = t.replace(minute=t.minute // 10 * 10)
    d = base / "crawl_result" / f"{kind}_{worker:02d}" / t.strftime("%Y%m%d") / t.strftime("%H%M")
    d.mkdir(parents=True)
    (d / "x.jsonl").write_text("{}\n")
    return d


def _progress(base, svc, worker, hours_ago):
    t = NOW - timedelta(hours=hours_ago)
    t = t.replace(minute=t.minute // 10 * 10)
    p = base / "progress" / svc / f"{worker:02d}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"date": t.strftime("%Y%m%d"), "time": t.strftime("%H%M")}))


def _run(cfg, apply=True, min_h=2, max_h=0):
    stages = cleanup.discover_stages(cfg)
    lines = []
    totals = cleanup.run(
        stages, NOW, timedelta(hours=min_h),
        timedelta(hours=max_h) if max_h else None, apply, log=lines.append,
    )
    return totals, lines


def test_unconsumed_folder_survives_past_eight_hours(ipc):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=10)       # router is 10 h behind
    read = _folder(base, "crawler", 0, hours_ago=12)
    unread = _folder(base, "crawler", 0, hours_ago=9)
    totals, _ = _run(cfg)
    assert not read.exists()
    assert unread.exists()
    assert totals == {"consumed": 1, "unconsumed": 0, "errors": 0}


def test_consumed_but_young_folder_is_kept(ipc):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=0.5)
    young = _folder(base, "crawler", 0, hours_ago=1)
    old = _folder(base, "crawler", 0, hours_ago=9)
    _run(cfg)
    assert young.exists()
    assert not old.exists()


def test_ingestor_dir_waits_for_both_ingestor_and_extractor(ipc):
    base, cfg, _ = ipc
    _progress(base, "ingestor", 3, hours_ago=1)
    _progress(base, "extractor", 3, hours_ago=11)    # extractor behind
    f = _folder(base, "ingestor", 3, hours_ago=10)
    g = _folder(base, "ingestor", 3, hours_ago=12)
    _run(cfg)
    assert f.exists()
    assert not g.exists()


def test_missing_progress_keeps_everything_until_safety_net(ipc):
    base, cfg, _ = ipc
    _progress(base, "ingestor", 5, hours_ago=1)      # no extractor progress
    kept = _folder(base, "ingestor", 5, hours_ago=20)
    dropped = _folder(base, "ingestor", 5, hours_ago=50)
    totals, lines = _run(cfg, max_h=48)
    assert kept.exists()
    assert not dropped.exists()
    assert totals["unconsumed"] == 1
    assert any("UNCONSUMED" in line for line in lines)
    assert any("watermark=MISSING" in line for line in lines)


def test_safety_net_can_be_disabled(ipc):
    base, cfg, _ = ipc
    f = _folder(base, "crawler", 1, hours_ago=100)
    _run(cfg, max_h=0)
    assert f.exists()


def test_dry_run_deletes_nothing(ipc):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=1)
    f = _folder(base, "crawler", 0, hours_ago=12)
    totals, _ = _run(cfg, apply=False)
    assert f.exists()
    assert totals["consumed"] == 1


def test_other_dirs_are_ignored(ipc):
    base, cfg, _ = ipc
    other = base / "crawl_result" / "manual" / "20200101" / "0000"
    other.mkdir(parents=True)
    odd = base / "crawl_result" / "crawler_00" / "notes"
    odd.mkdir(parents=True)
    _run(cfg, max_h=1)
    assert other.exists()
    assert odd.exists()


def test_main_dry_run_by_default(ipc, capsys):
    base, _, cfg_path = ipc
    _progress(base, "router", 0, hours_ago=1)
    f = _folder(base, "crawler", 0, hours_ago=1000)
    assert cleanup.main(["--config", str(cfg_path)]) == 0
    assert f.exists()
    assert "DRY RUN" in capsys.readouterr().out


@pytest.mark.parametrize("progress", ["missing", "behind"])
def test_main_defaults_delete_consumed_at_two_hours_but_never_force_unread(ipc, frozen_clock, progress):
    base, _, cfg_path = ipc
    _progress(base, "router", 0, hours_ago=0)
    young = _folder(base, "crawler", 0, hours_ago=1)
    at_boundary = _folder(base, "crawler", 0, hours_ago=2)
    unread = _folder(base, "crawler", 1, hours_ago=1000)
    if progress == "behind":
        _progress(base, "router", 1, hours_ago=1001)

    assert cleanup.main(["--config", str(cfg_path), "--apply"]) == 0

    assert young.exists()
    assert not at_boundary.exists()
    assert unread.exists()


def test_production_cron_keeps_unread_data_and_uses_shared_ipc_service(ipc, frozen_clock):
    base, _, cfg_path = ipc
    cron = Path(__file__).resolve().parents[1] / "ops/production/cron.d/crawl-result-cleanup"
    jobs = [line for line in cron.read_text().splitlines()
            if line and not line.startswith("#") and "=" not in line]
    assert len(jobs) == 1
    fields = shlex.split(jobs[0])
    assert fields[:6] == ["*/30", "*", "*", "*", "*", "root"]
    assert fields[6:14] == ["/usr/bin/docker", "exec", "-w", "/app", "scheduler_ingest", "python", "-m",
                           "scripts.cleanup_crawl_result"]
    args = fields[14:fields.index(">>")]

    _progress(base, "router", 0, hours_ago=0)
    consumed = _folder(base, "crawler", 0, hours_ago=2)
    unread = _folder(base, "crawler", 1, hours_ago=1000)
    assert cleanup.main(["--config", str(cfg_path), *args]) == 0
    assert not consumed.exists()
    assert unread.exists()


def test_max_age_must_cover_min_age(ipc):
    _, _, cfg_path = ipc
    with pytest.raises(SystemExit):
        cleanup.main(["--config", str(cfg_path), "--min-age-hours", "10", "--max-age-hours", "5"])


@pytest.mark.parametrize("level", ["worker", "date", "time"])
def test_symlink_directories_never_delete_outside_data(ipc, tmp_path, level):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=1)
    folder = _folder(base, "crawler", 0, hours_ago=12)
    outside = tmp_path / "outside"
    target = {"worker": folder.parent.parent, "date": folder.parent, "time": folder}[level]
    target.rename(outside)
    target.symlink_to(outside, target_is_directory=True)

    totals, _ = _run(cfg)

    assert (outside / folder.relative_to(target) / "x.jsonl").exists()
    assert target.is_symlink()
    assert totals == {"consumed": 0, "unconsumed": 0, "errors": 0}


@pytest.mark.parametrize("level", ["worker", "date", "time"])
def test_symlink_swap_after_planning_cannot_escape_cleanup_root(ipc, tmp_path, monkeypatch, level):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=1)
    folder = _folder(base, "crawler", 0, hours_ago=12)
    target = {"worker": folder.parent.parent, "date": folder.parent, "time": folder}[level]
    outside = tmp_path / "outside"
    victim = outside / folder.relative_to(target)
    victim.mkdir(parents=True)
    (victim / "unrelated.txt").write_text("not crawler output")
    original_plan = cleanup.plan

    def swap_after_plan(*args, **kwargs):
        decisions = original_plan(*args, **kwargs)
        target.rename(target.with_name(target.name + ".saved"))
        target.symlink_to(outside, target_is_directory=True)
        return decisions

    monkeypatch.setattr(cleanup, "plan", swap_after_plan)
    totals, lines = _run(cfg)

    assert (victim / "unrelated.txt").read_text() == "not crawler output"
    assert target.is_symlink()
    assert totals["consumed"] == 0
    assert totals["errors"] >= 1
    assert any("ERROR" in line for line in lines)


def test_date_swap_during_delete_uses_open_directory_not_replacement(ipc, tmp_path, monkeypatch):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=1)
    folder = _folder(base, "crawler", 0, hours_ago=12)
    date_dir = folder.parent
    saved = date_dir.with_name(date_dir.name + ".saved")
    outside = tmp_path / "outside"
    victim = outside / folder.name
    victim.mkdir(parents=True)
    (victim / "unrelated.txt").write_text("not crawler output")
    original_rmtree = cleanup.shutil.rmtree

    def swap_before_rmtree(*args, **kwargs):
        date_dir.rename(saved)
        date_dir.symlink_to(outside, target_is_directory=True)
        return original_rmtree(*args, **kwargs)

    monkeypatch.setattr(cleanup.shutil, "rmtree", swap_before_rmtree)
    totals, _ = _run(cfg)

    assert (victim / "unrelated.txt").exists()
    assert not (saved / folder.name).exists()
    assert totals == {"consumed": 1, "unconsumed": 0, "errors": 0}


def test_delete_refuses_a_planned_folder_outside_its_base(ipc, tmp_path, monkeypatch):
    base, cfg, _ = ipc
    _progress(base, "router", 0, hours_ago=1)
    _folder(base, "crawler", 0, hours_ago=12)
    outside = tmp_path / "outside/20261105/0800"
    outside.mkdir(parents=True)
    (outside / "unrelated.txt").write_text("not crawler output")
    monkeypatch.setattr(cleanup, "plan", lambda *args: [
        cleanup.Decision(outside, consumed=True, age=timedelta(hours=12)),
    ])

    totals, _ = _run(cfg)

    assert (outside / "unrelated.txt").exists()
    assert totals == {"consumed": 0, "unconsumed": 0, "errors": 1}

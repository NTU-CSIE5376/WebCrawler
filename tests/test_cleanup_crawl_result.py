from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
import yaml

from scripts import cleanup_crawl_result as cleanup

NOW = datetime(2026, 11, 5, 20, 0, tzinfo=timezone.utc)


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


def _run(cfg, apply=True, min_h=8, max_h=48):
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
    young = _folder(base, "crawler", 0, hours_ago=2)
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
    totals, lines = _run(cfg)
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


def test_max_age_must_cover_min_age(ipc):
    _, _, cfg_path = ipc
    with pytest.raises(SystemExit):
        cleanup.main(["--config", str(cfg_path), "--min-age-hours", "10", "--max-age-hours", "5"])

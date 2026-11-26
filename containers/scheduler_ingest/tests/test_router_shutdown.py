"""Stop requests must leave a complete folder and its checkpoint together."""
import json
import os
from pathlib import Path
import signal
import sys
import subprocess
import time
from unittest.mock import Mock

import pytest

from containers.scheduler_ingest.router import main as router_main
from containers.scheduler_ingest.tests.test_router_service import router, _record, _output_lines


def _setup_main(monkeypatch, svc):
    handlers = {}
    monkeypatch.setattr(router_main.signal, "signal", lambda sig, handler: handlers.setdefault(sig, handler))
    monkeypatch.setattr(router_main, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(router_main, "load_router_config", lambda *args: svc.cfg)
    monkeypatch.setattr(router_main, "RouterService", lambda cfg: svc)
    monkeypatch.setattr(sys, "argv", ["router", "--config", "unused", "--router-id", str(svc.cfg.router_id)])
    dispose = Mock(wraps=svc.engine.dispose)
    monkeypatch.setattr(svc.engine, "dispose", dispose)
    return handlers, dispose


def _ready_folders(svc):
    for bucket in ("0000", "0010"):
        folder = Path(svc.cfg.crawler_dir_template.format(id=svc.cfg.router_id)) / "20000101" / bucket
        folder.mkdir(parents=True)
        record = _record("https://known.example/" + bucket, ["https://known.example/a"])
        (folder / "part.jsonl").write_text(json.dumps(record) + "\n")


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_stop_finishes_current_folder_and_saves_checkpoint(router, monkeypatch, stop_signal):
    svc, _, root = router
    _ready_folders(svc)
    handlers, dispose = _setup_main(monkeypatch, svc)
    process = svc.process_folder

    def stop_during_folder(folder):
        handlers[stop_signal](stop_signal, None)
        process(folder)

    monkeypatch.setattr(svc, "process_folder", stop_during_folder)
    router_main.main()
    checkpoint = json.loads(Path(svc.cfg.progress_template.format(id=svc.cfg.router_id)).read_text())
    assert checkpoint == {"date": "20000101", "time": "0000"}
    assert len([r for r in _output_lines(root) if r["status"] == "ok"]) == 1
    dispose.assert_called_once_with()


def test_idle_stop_does_not_wait_for_full_scan_interval(router, monkeypatch):
    svc, _, _ = router
    handlers, dispose = _setup_main(monkeypatch, svc)
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(router_main.time, "sleep", sleep)
    router_main.main()
    assert len(sleeps) == 1 and 0 < sleeps[0] <= 1
    dispose.assert_called_once_with()


def test_failed_folder_does_not_advance_checkpoint(router, monkeypatch):
    svc, _, _ = router
    _ready_folders(svc)
    _, dispose = _setup_main(monkeypatch, svc)
    monkeypatch.setattr(svc, "process_folder", Mock(side_effect=RuntimeError("folder failed")))
    with pytest.raises(RuntimeError, match="folder failed"):
        router_main.main()
    assert not Path(svc.cfg.progress_template.format(id=svc.cfg.router_id)).exists()
    dispose.assert_called_once_with()


@pytest.mark.skipif(sys.platform == "win32", reason="Requires POSIX stop signals")
@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT])
def test_real_signal_drains_one_folder_without_starting_next(tmp_path, stop_signal):
    repo = Path(__file__).resolve().parents[3]
    helper = Path(__file__).parent / "helpers/router_shutdown_runner.py"
    for bucket in ("0000", "0010"):
        (tmp_path / "crawler_00/20000101" / bucket).mkdir(parents=True)
    process = subprocess.Popen(
        [sys.executable, str(helper), str(tmp_path)], cwd=repo,
        env={**os.environ, "PYTHONPATH": str(repo)},
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 15
        while not (tmp_path / "started").exists():
            assert process.poll() is None, "Router exited before processing a folder"
            assert time.monotonic() < deadline, "Router did not start in time"
            time.sleep(.01)
        process.send_signal(stop_signal)
        (tmp_path / "finish").touch()
        _, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, stderr.decode()
        assert (tmp_path / "output").read_text().splitlines() == ["0000"]
        assert json.loads((tmp_path / "progress_00.json").read_text()) == {
            "date": "20000101", "time": "0000",
        }
        assert (tmp_path / "disposed").exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)

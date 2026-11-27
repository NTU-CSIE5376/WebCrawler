"""Linux must clean parser children after parent crashes and stop timeouts."""
import configparser
import ctypes
import errno
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import crawler.spiders.parser as module  # noqa: E402


def test_parent_death_signal_is_armed_before_initializing_parser(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    prctl = Mock(return_value=0)
    monkeypatch.setattr(ctypes, "CDLL", Mock(return_value=SimpleNamespace(prctl=prctl)))
    monkeypatch.setattr(os, "getppid", lambda: 1234)
    monkeypatch.setattr(signal, "signal", Mock())
    parser = Mock()
    monkeypatch.setattr(module, "HtmlParser", parser)
    module._init_parser(1234)
    prctl.assert_called_once_with(1, signal.SIGKILL, 0, 0, 0)
    parser.assert_called_once_with()


def test_parent_already_dead_during_startup_kills_child(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ctypes, "CDLL", Mock(return_value=SimpleNamespace(prctl=Mock(return_value=0))))
    monkeypatch.setattr(os, "getppid", lambda: 1)
    kill = Mock(side_effect=SystemExit)
    monkeypatch.setattr(os, "kill", kill)
    with pytest.raises(SystemExit):
        module._init_parser(1234)
    kill.assert_called_once_with(os.getpid(), signal.SIGKILL)


def test_parent_death_protection_fails_closed_when_prctl_fails(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(ctypes, "CDLL", Mock(return_value=SimpleNamespace(prctl=Mock(return_value=-1))))
    monkeypatch.setattr(ctypes, "get_errno", lambda: errno.EPERM)
    with pytest.raises(OSError) as error:
        module._init_parser(1234)
    assert error.value.errno == errno.EPERM


@pytest.mark.skipif(sys.platform != "linux", reason="Linux parent-death signals and child reaping")
@pytest.mark.parametrize("mode", ["parent_sigkill", "supervisor_timeout"])
def test_parser_children_exit_when_parent_is_killed(tmp_path, mode):
    root = Path(__file__).resolve().parents[3]
    helper = Path(__file__).parent / "helpers/html_parser_lifecycle_runner.py"
    env = {**os.environ, "PYTHONPATH": f"{root}:{root / 'containers/crawler'}"}
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0  # PR_GET_CHILD_SUBREAPER
    assert libc.prctl(36, 1, 0, 0, 0) == 0  # Adopt/reap our orphaned test children.
    process = None
    info = None
    remaining_children = set()
    controller = shutil.which("supervisorctl")
    supervisor = shutil.which("supervisord")
    try:
        if mode == "supervisor_timeout":
            if not controller or not supervisor:
                pytest.skip("Supervisor is not installed")
            # Exercise the actual crawler stop policy, shortening only its timeout.
            config = configparser.ConfigParser(interpolation=None)
            assert config.read(root / "containers/crawler/supervisord.conf")
            programs = [config[name] for name in ("program:crawler_default", "program:crawler_canary")]
            for program in programs:
                assert not program.getboolean("stopasgroup")
                assert program.getboolean("killasgroup")
                assert program["stopsignal"] == "TERM"
                assert program.getint("stopwaitsecs") == 60
            conf = tmp_path / "supervisord.conf"
            conf.write_text(f'''[supervisord]
nodaemon=true
logfile={tmp_path}/supervisor.log
pidfile={tmp_path}/supervisor.pid
[unix_http_server]
file={tmp_path}/supervisor.sock
[rpcinterface:supervisor]
supervisor.rpcinterface_factory=supervisor.rpcinterface:make_main_rpcinterface
[supervisorctl]
serverurl=unix://{tmp_path}/supervisor.sock
[program:probe]
command={sys.executable} {helper} {tmp_path}
startsecs=0
autorestart=false
stopsignal={programs[0]['stopsignal']}
stopwaitsecs=1
stopasgroup={programs[0]['stopasgroup']}
killasgroup={programs[0]['killasgroup']}
stdout_logfile={tmp_path}/parent.log
stderr_logfile={tmp_path}/parent-error.log
''')
            process = subprocess.Popen([supervisor, "-c", str(conf)], env=env,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            process = subprocess.Popen([sys.executable, str(helper), str(tmp_path)], env=env,
                                       start_new_session=True, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while not (tmp_path / "parent.json").exists():
            assert process.poll() is None, "Test parent exited before children were ready"
            assert time.monotonic() < deadline, "Test children did not become ready"
            time.sleep(.02)
        info = json.loads((tmp_path / "parent.json").read_text())
        remaining_children = set(info["children"])
        assert len(info["workers"]) == 2
        if mode == "supervisor_timeout":
            stopped = subprocess.run([controller, "-c", str(conf), "stop", "probe"],
                                     capture_output=True, text=True, timeout=10)
            assert stopped.returncode == 0, stopped.stdout + stopped.stderr
        else:
            process.kill()
            assert process.wait(timeout=5) == -signal.SIGKILL
        # As subreaper, collect actual exit status instead of mistaking zombies
        # for live workers or depending on the container init's reaping speed.
        for pid in info["children"]:
            deadline = time.monotonic() + 10
            while True:
                reaped, status = os.waitpid(pid, os.WNOHANG)
                if reaped:
                    remaining_children.discard(pid)
                    if pid in info["workers"]:
                        assert os.WIFSIGNALED(status)
                        assert os.WTERMSIG(status) == signal.SIGKILL
                    break
                assert time.monotonic() < deadline, f"Child {pid} survived parent death"
                time.sleep(.02)
    finally:
        for pid in remaining_children:
            try:
                reaped, _ = os.waitpid(pid, os.WNOHANG)
                if not reaped:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
        if process is not None and process.poll() is None:
            if mode == "supervisor_timeout":
                subprocess.run([controller, "-c", str(conf), "shutdown"],
                               capture_output=True, timeout=10)
            else:
                process.kill()
            process.wait(timeout=10)
        assert libc.prctl(36, previous.value, 0, 0, 0) == 0

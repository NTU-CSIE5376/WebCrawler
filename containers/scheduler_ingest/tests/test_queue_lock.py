import subprocess
import sys

import pytest

from containers.scheduler_ingest.api.queue_lock import queue_lock


def locked_by_another_process(directory):
    code = """
import fcntl, sys
with open(sys.argv[1], 'a') as handle:
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print('locked')
    else:
        print('unlocked')
"""
    return subprocess.run(
        [sys.executable, "-c", code, str(directory / ".lock")],
        check=True, capture_output=True, text=True, timeout=5,
    ).stdout.strip() == "locked"


def test_queue_lock_excludes_other_processes_and_releases_after_failure(tmp_path):
    with pytest.raises(RuntimeError):
        with queue_lock(tmp_path):
            assert locked_by_another_process(tmp_path)
            raise RuntimeError("worker failed")
    assert not locked_by_another_process(tmp_path)
    assert (tmp_path / ".lock").exists()

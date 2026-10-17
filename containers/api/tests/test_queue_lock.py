import subprocess
import sys

import pytest

from libs.ipc.manual_bulk_crawl import batch_claim, queue_lock


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


def test_batch_claim_skips_missing_and_owned_files(tmp_path):
    batch = tmp_path / "batch.json"
    with batch_claim(batch) as owned:
        assert not owned
    batch.write_text("[]")
    with batch_claim(batch) as owned:
        assert owned
        with batch_claim(batch) as other:
            assert not other
    with batch_claim(batch) as owned:
        assert owned


def test_batch_claim_is_recoverable_after_process_death(tmp_path):
    batch = tmp_path / "batch.json"
    batch.write_text("[]")
    code = """
from pathlib import Path
import sys
from libs.ipc.manual_bulk_crawl import batch_claim
with batch_claim(Path(sys.argv[1])) as owned:
    print(owned, flush=True)
    sys.stdin.read()
"""
    process = subprocess.Popen([sys.executable, "-c", code, str(batch)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "True"
        with batch_claim(batch) as owned:
            assert not owned
    finally:
        process.kill()
        process.wait(timeout=5)
    with batch_claim(batch) as owned:
        assert owned
    assert batch.read_text() == "[]"

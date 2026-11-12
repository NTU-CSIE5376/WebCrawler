import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("mode", ["normal", "failure", "redirect"])
def test_restart_recovery_through_real_downloader_and_item_pipeline(tmp_path, mode):
    repository = Path(__file__).resolve().parents[3]
    helper = Path(__file__).with_name("helpers") / "restart_recovery_runner.py"
    environment = dict(os.environ)
    environment["NO_PROXY"] = "127.0.0.1,localhost"
    environment["PYTHONPATH"] = os.pathsep.join((str(repository), str(repository / "containers" / "crawler")))
    process = subprocess.run([sys.executable, str(helper), str(tmp_path), mode],
                             check=False,
                             cwd=repository / "containers" / "crawler", env=environment,
                             text=True, capture_output=True, timeout=30)
    assert process.returncode == 0, process.stdout[-5000:] + process.stderr[-5000:]
    proofs = [json.loads(line) for line in process.stdout.splitlines() if line.startswith('{"proof_restart_recovery"')]
    assert len(proofs) == 1 and proofs[0]["deadline_respected"]
    assert proofs[0]["records"] == 2 and proofs[0]["completion_receipt"] == "completed"

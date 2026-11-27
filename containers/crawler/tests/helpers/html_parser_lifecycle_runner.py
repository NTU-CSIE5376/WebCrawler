"""A disposable crawler parent whose parser children are busy during a crash."""
import json
import os
from pathlib import Path
import signal
import sys
import time


def hold(root):
    Path(root, f"worker-{os.getpid()}").write_text("ready")
    time.sleep(60)


def main(root):
    from crawler.spiders.parser import HtmlParserPool

    pool = HtmlParserPool(2)
    for _ in range(2):
        pool._executor.submit(hold, str(root))
    deadline = time.monotonic() + 15
    while len(list(root.glob("worker-*"))) != 2:
        if time.monotonic() > deadline:
            raise RuntimeError("Parser children did not become ready")
        time.sleep(.01)
    # Force Supervisor to use its timeout/SIGKILL path in the stop test.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    ready = root / "parent.tmp"
    ready.write_text(json.dumps({
        "pid": os.getpid(), "workers": list(pool._executor._processes),
        "children": [int(pid) for pid in Path(f"/proc/{os.getpid()}/task/{os.getpid()}/children").read_text().split()],
    }))
    ready.replace(root / "parent.json")
    while True:
        signal.pause()


if __name__ == "__main__":
    main(Path(sys.argv[1]))

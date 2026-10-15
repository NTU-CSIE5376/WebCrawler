"""Serialize infrequent submissions and delivery across worker processes."""
from contextlib import contextmanager, suppress
import fcntl
import os
from pathlib import Path


@contextmanager
def queue_lock(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    # Keep this inode permanently: unlinking lock files allows two callers
    # to lock different inodes and both believe they own the queue.
    descriptor = os.open(directory / ".lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        # Closing also releases the lock after worker death. Cleanup errors
        # must not turn an already accepted batch into a reported rejection.
        with suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        with suppress(OSError):
            os.close(descriptor)

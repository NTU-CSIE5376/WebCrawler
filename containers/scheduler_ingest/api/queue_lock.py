"""Coordinate publication, batch ownership and shard updates across processes."""
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


@contextmanager
def batch_claim(batch: Path):
    """Claim an immutable accepted file without waiting for another worker.

    Keep the file in pending until delivery finishes. Process death releases
    ownership automatically, so another dispatcher can retry the same file.
    """
    try:
        descriptor = os.open(batch, os.O_RDONLY)
    except FileNotFoundError:
        yield False
        return
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        # A previous owner may have removed the file after we opened it.
        try:
            current = batch.stat()
            opened = os.fstat(descriptor)
            owned = (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino)
        except FileNotFoundError:
            owned = False
        yield owned
    finally:
        with suppress(OSError):
            os.close(descriptor)

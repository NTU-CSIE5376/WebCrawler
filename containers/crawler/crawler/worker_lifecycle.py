"""Signal handling shared by spawned parser and output workers."""
import os
import signal
import sys


def initialize_worker(parent_pid: int):
    if sys.platform == "linux":
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        prctl = libc.prctl
        prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                          ctypes.c_ulong, ctypes.c_ulong]
        prctl.restype = ctypes.c_int
        if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        # Parent death before prctl() is not delivered retrospectively.
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGKILL)
    # Supervisor sends TERM to the parent, then group KILL after its timeout.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

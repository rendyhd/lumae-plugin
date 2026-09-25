"""Fake analyzers for the LUM-018 isolation tests (P3-8).

They run inside the analysis child, which imports them by module name through
the parent's ``sys.path``. Each takes the analyzed path first, like the real
analyzers.
"""
import os
import signal
import time


def _note_pid(pid_file):
    if pid_file:
        with open(pid_file, "w", encoding="ascii") as out:
            out.write(str(os.getpid()))


def sleep_forever(path, pid_file=None):
    """A decoder stuck in native code: never returns, never checks a deadline."""
    _note_pid(pid_file)
    time.sleep(3600)


def ignore_term_and_sleep(path, pid_file=None):
    """Hung and deaf to SIGTERM: only SIGKILL ends it."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _note_pid(pid_file)
    time.sleep(3600)


def raise_invalid_data(path):
    import av

    raise av.error.InvalidDataError(
        1094995529, "Invalid data found when processing input", "avcodec_send_packet()",
    )


def raise_value_error(path):
    raise ValueError(f"cannot read {path}")


def exit_hard(path):
    os._exit(3)


def die_by_signal(path):
    os.kill(os.getpid(), signal.SIGSEGV)
    time.sleep(10)


def raise_memory_error(path):
    raise MemoryError()


def describe_process(path):
    """What the child can see: its pid, its sockets, its environment names."""
    sockets = []
    for name in os.listdir("/proc/self/fd") if os.path.isdir("/proc/self/fd") else ():
        try:
            target = os.readlink(f"/proc/self/fd/{name}")
        except OSError:
            continue
        if target.startswith("socket:"):
            sockets.append(target)
    return {"pid": os.getpid(), "sockets": sockets, "environment": sorted(os.environ)}


def echo(path, **kwargs):
    return {"path": path, "kwargs": kwargs, "pid": os.getpid(), "pair": (1, b"\x00\xff")}

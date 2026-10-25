"""Stand-ins for the plugin in an ``action_isolation`` child (tests only).

``load`` is a loader: the child imports this module by name (it is on the
parent's ``sys.path``) and calls the entries below instead of the plugin's.
``load_plugin`` imports the real plugin under the tests' host stub.
"""
import contextlib
import os
import signal
import time


class AnalysisProjectionDeferred(RuntimeError):
    def __init__(self, message, reason):
        super().__init__(message)
        self.reason = reason


def load(request):
    import action_child_fakes

    return action_child_fakes, contextlib.nullcontext()


def broken(request):
    raise ImportError("no AudioMuse here")


def load_plugin(request):
    import test_lumae_analysis  # noqa: F401  (installs the plugin.api host stub)

    return test_lumae_analysis.load_plugin(), contextlib.nullcontext()


def echo(**call):
    return {"call": call, "pid": os.getpid(), "parent": os.getppid(), "nice": os.nice(0)}


def deferred(reason):
    raise AnalysisProjectionDeferred("identity not settled", reason)


def boom():
    raise KeyError("no such source")


def noisy():
    print("a stray line on stdout")
    return {"status": "complete"}


def crash():
    os.kill(os.getpid(), signal.SIGKILL)


def slow(seconds):
    time.sleep(seconds)
    return {"status": "complete"}


def big(megabytes):
    held = bytearray(megabytes * 1024 * 1024)
    return {"held": len(held)}

"""Hard wall-clock limit for one media-file analysis (LUM-018).

The waveform (``loudness.analyze_file``) and edge (``edge_profiles.
analyze_edge_file``) analyzers decode with PyAV/FFmpeg. Their soft deadline is
checked only between decoded blocks, so a decoder stuck inside native code
cannot be interrupted in the task's own process. ``run_isolated`` runs the
analyzer in a child process and kills the child when it has not answered in
time.

Host limits. AudioMuse puts no wall-clock limit on a plugin task. RQ-era hosts
enqueued plugin tasks with an RQ ``job_timeout`` of -1 ("no timeout"). The
database task queue that replaced RQ (AudioMuse bd23c90f6, 2026-08-06, and
still at 8aa1639c) has no per-task timeout at all, which is why
``enqueue_bounded`` discards its ``timeout`` argument. This limit is the only
wall-clock bound on one file. A host cancel still ends the child: the child
stays in the job's process group, which the host's cancel kills, and it dies
with its parent (``PR_SET_PDEATHSIG`` on Linux, a parent watchdog elsewhere).

Design:

* ``subprocess`` (fork and exec of ``sys.executable``), never a bare ``fork``:
  the parent may run threads and hold a database connection. ``close_fds``
  (the default) keeps every descriptor except the three pipes out of the child,
  so it can neither see nor use the parent's connection.
* The child loads this file and the analyzer modules through a stand-in package
  for the plugin directory. The plugin ``__init__`` (Flask, the host API, the
  database) is never imported there.
* One worker per parent process is started on first use and reused for later
  files: starting one costs about 1 s (numpy, scipy.signal and av imports),
  about as much as a whole 4-minute waveform analysis. A killed or crashed
  worker is reaped and replaced on the next call. A call made while another
  thread uses the worker gets a one-off worker of its own.
* Only the path and keyword arguments go to the child, and only the result
  comes back, as one JSON line (bytes, tuples and dataclasses are tagged). No
  audio crosses the process boundary, so nothing is held twice.
* The analyzers keep their soft deadline (default 15 minutes). The hard limit
  kills the child ``HARD_LIMIT_HEADROOM_SECONDS`` after ``limit_seconds``, with
  SIGTERM and then SIGKILL after ``TERM_GRACE_SECONDS``, so an analysis that is
  still making progress stops at its soft deadline first.
* A target the child cannot import by name (a closure, as tests inject), a
  frozen build (``sys.executable`` is the application, not Python), a
  non-POSIX platform and a worker that cannot start run in-process: soft
  deadline only, no hard limit. The last case is logged.

Failures carry a LUM-007 retry category (``failure_category``) and safe
diagnostics (``DIAGNOSTIC_FIELDS``): container, codec, sample rate, channel
layout, byte size and the decode position at failure. Values are allowlisted
and validated; exception text, which can hold a path, never leaves the child.

This file must import only the standard library at module level: the child
loads it on its own, outside the plugin package.
"""

import atexit
import base64
import dataclasses
import importlib
import inspect
import json
import logging
import math
import os
import re
import select
import signal
import subprocess
import sys
import threading
import time
import types


DEFAULT_LIMIT_SECONDS = 15 * 60
MIN_LIMIT_SECONDS = 60
MAX_LIMIT_SECONDS = 24 * 60 * 60
HARD_LIMIT_HEADROOM_SECONDS = 30
TERM_GRACE_SECONDS = 5
STARTUP_TIMEOUT_SECONDS = 120
PROGRESS_INTERVAL_SECONDS = 1.0
MAX_MESSAGE_BYTES = 16 * 1024 * 1024

MEDIA_ERROR = "media_error"
ANALYSIS_TIMEOUT = "analysis_timeout"
ANALYSIS_CRASH = "analysis_crash"

# The child's analyzers are loaded under this name, not the plugin's.
CHILD_PACKAGE = "_lumae_analysis_child"
_EXIT_AFTER_CRASH = 70
_TAG = "__lumae_type__"
_PR_SET_PDEATHSIG = 1

logger = logging.getLogger("lumae_analysis.isolation")

# Matched with fullmatch: no "/", "\\" or ":", so no path or URL fits.
_TOKEN = re.compile(r"[A-Za-z0-9_.,+-]{1,64}")
_LAYOUT = re.compile(r"[A-Za-z0-9_.,+() -]{1,32}")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
_MODULE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*")


def _int_field(low, high):
    def check(value):
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return value if low <= value <= high else None
    return check


def _float_field(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    if not math.isfinite(value) or value < 0:
        return None
    return round(value, 3)


def _pattern_field(pattern):
    def check(value):
        return value if isinstance(value, str) and pattern.fullmatch(value) else None
    return check


def _choice_field(*choices):
    def check(value):
        return value if value in choices else None
    return check


# Everything a stored diagnostic may contain. Unknown keys and values that fail
# their check are dropped, so no path, URL, tag or exception text gets through.
DIAGNOSTIC_FIELDS = {
    "analyzer": _pattern_field(_IDENTIFIER),
    "category": _pattern_field(_IDENTIFIER),
    "phase": _choice_field("start", "open", "decode"),
    "container": _pattern_field(_TOKEN),
    "codec": _pattern_field(_TOKEN),
    "sample_rate": _int_field(0, 10_000_000),
    "channel_layout": _pattern_field(_LAYOUT),
    "channels": _int_field(0, 1024),
    "byte_size": _int_field(0, 2**62),
    "decode_position_seconds": _float_field,
    "decoded_frames": _int_field(0, 2**62),
    "error_type": _pattern_field(_IDENTIFIER),
    "errno": _int_field(-(2**31), 2**31 - 1),
    "exit_code": _int_field(-255, 255),
    "signal": _pattern_field(_IDENTIFIER),
    "elapsed_seconds": _float_field,
    "limit_seconds": _int_field(0, 2**31),
}


def safe_diagnostics(raw):
    """Only allowlisted, validated fields; None when nothing is left."""
    if not isinstance(raw, dict):
        return None
    safe = {}
    for key, check in DIAGNOSTIC_FIELDS.items():
        if key in raw and raw[key] is not None:
            value = check(raw[key])
            if value is not None:
                safe[key] = value
    return safe or None


class IsolatedAnalysisError(RuntimeError):
    """An analysis failure reported by, or observed on, the child process.

    ``str()`` carries only the category and the exception class name.
    """

    def __init__(self, category, error_type=None, diagnostics=None, pid=None):
        self.category = category
        self.error_type = error_type
        self.diagnostics = safe_diagnostics(
            {**(diagnostics or {}), "category": category}
        )
        self.pid = pid
        super().__init__(f"{category} ({error_type})" if error_type else category)


def _mro_names(exc):
    return {cls.__name__ for cls in type(exc).__mro__}


def failure_category(exc):
    """The LUM-007 retry category of an analysis failure.

    Matches class names along the MRO, so the child's copies of the plugin
    exceptions and the parent's classify alike. Only PyAV ``InvalidDataError``
    (AVERROR_INVALIDDATA, e.g. from ``avcodec_send_packet()``) is a decoder
    data error; every other exception keeps the category it had before.
    """
    if isinstance(exc, IsolatedAnalysisError):
        return exc.category
    names = _mro_names(exc)
    if "SilentAudioError" in names:
        return "silent_audio"
    if names & {"ProfileAnalysisTimeout", "EdgeAnalysisTimeout"}:
        return ANALYSIS_TIMEOUT
    if "ProfileResourceLimitError" in names:
        return "resource_limit"
    if "InvalidDataError" in names:
        return MEDIA_ERROR
    if isinstance(exc, MemoryError):
        return ANALYSIS_CRASH
    if isinstance(exc, (ValueError, EOFError)):
        return "unsupported_media"
    return "analysis_error"


def failure_diagnostics(exc):
    return getattr(exc, "diagnostics", None) if isinstance(exc, IsolatedAnalysisError) else None


EDGE_FAILURE_REASONS = {
    MEDIA_ERROR: "edge-media-error",
    ANALYSIS_TIMEOUT: "edge-analysis-timeout",
    ANALYSIS_CRASH: "edge-analysis-crash",
}
EDGE_REASON_MAX = 160
_EDGE_DIAGNOSTIC_KEYS = (
    ("container", "fmt"), ("codec", "codec"), ("sample_rate", "sr"),
    ("channel_layout", "layout"), ("byte_size", "bytes"),
    ("decode_position_seconds", "pos_s"), ("decoded_frames", "frames"),
    ("phase", "phase"), ("error_type", "err"), ("exit_code", "exit"),
    ("signal", "sig"), ("elapsed_seconds", "elapsed_s"),
)


def edge_failure_reason(exc):
    """``edge_profile_jobs.last_error``: the reason, then safe diagnostics.

    Edge jobs have no category column; every failure backs off the same way.
    The new LUM-018 categories get their own reason, everything else keeps
    ``edge-analysis-unavailable``. Diagnostics follow as ``key=value`` words.
    """
    reason = EDGE_FAILURE_REASONS.get(failure_category(exc), "edge-analysis-unavailable")
    diagnostics = failure_diagnostics(exc) or {}
    words = [
        f"{short}={str(diagnostics[key]).replace(' ', '_')}"
        for key, short in _EDGE_DIAGNOSTIC_KEYS if key in diagnostics
    ]
    return " ".join([reason, *words])[:EDGE_REASON_MAX]


def normalize_limit(raw):
    """The configured hard limit in seconds, clamped; invalid means default."""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT_SECONDS
    if value <= 0:
        return DEFAULT_LIMIT_SECONDS
    return min(max(value, MIN_LIMIT_SECONDS), MAX_LIMIT_SECONDS)


# ---------------------------------------------------------------- encoding


def _encode(value, plugin_prefix):
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {_TAG: "bytes", "b64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, tuple):
        return {_TAG: "tuple", "items": [_encode(item, plugin_prefix) for item in value]}
    if isinstance(value, list):
        return [_encode(item, plugin_prefix) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value) or _TAG in value:
            raise TypeError("analysis results must be JSON objects with string keys")
        return {key: _encode(item, plugin_prefix) for key, item in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        cls = type(value)
        return {
            _TAG: "dataclass",
            "class": _module_ref(cls.__module__, cls.__qualname__, plugin_prefix),
            "fields": {
                field.name: _encode(getattr(value, field.name), plugin_prefix)
                for field in dataclasses.fields(value)
            },
        }
    item = getattr(value, "item", None)
    if type(value).__module__ == "numpy" and callable(item):
        return _encode(item(), plugin_prefix)
    raise TypeError(f"cannot return {type(value).__name__} from an isolated analysis")


def _decode(value, plugin_package):
    if isinstance(value, list):
        return [_decode(item, plugin_package) for item in value]
    if not isinstance(value, dict):
        return value
    kind = value.get(_TAG)
    if kind is None:
        return {key: _decode(item, plugin_package) for key, item in value.items()}
    if kind == "bytes":
        return base64.b64decode(value["b64"])
    if kind == "tuple":
        return tuple(_decode(item, plugin_package) for item in value["items"])
    if kind == "dataclass":
        # Only the plugin's own result dataclasses: the child decodes untrusted
        # media, so its answer must not name an arbitrary callable to invoke.
        ref = value["class"]
        cls = _resolve(ref, plugin_package) if ref.get("plugin") else None
        if not (isinstance(cls, type) and dataclasses.is_dataclass(cls)):
            raise ValueError("isolated analysis returned an unknown class")
        return cls(**{key: _decode(item, plugin_package) for key, item in value["fields"].items()})
    raise ValueError("unknown value in an isolated analysis result")


def _module_ref(module, name, plugin_prefix):
    if plugin_prefix and module.startswith(plugin_prefix + "."):
        return {"plugin": True, "module": module[len(plugin_prefix) + 1:], "name": name}
    return {"plugin": False, "module": module, "name": name}


def _resolve(ref, plugin_package):
    module = ref["module"]
    name = ref["name"]
    if not _IDENTIFIER.fullmatch(name) or not _MODULE.fullmatch(module):
        raise ValueError("invalid isolated analysis target")
    if ref.get("plugin"):
        module = f"{plugin_package}.{module}"
    return getattr(importlib.import_module(module), name)


def _child_target(target, plugin_package):
    """A reference the child can import, or None (closures, lambdas, patches)."""
    module = getattr(target, "__module__", None)
    name = getattr(target, "__qualname__", None)
    if not module or not name or not _IDENTIFIER.fullmatch(name):
        return None
    loaded = sys.modules.get(module)
    if loaded is None or getattr(loaded, name, None) is not target:
        return None
    if module == "__main__":
        return None
    return _module_ref(module, name, plugin_package)


# ---------------------------------------------------------------- child side


class DecodeProbe:
    """Observer the analyzers call; records safe stream facts and the position.

    ``opened`` runs once the audio stream is selected, ``decoded`` for every
    decoded frame. Neither may raise into the analyzer.
    """

    def __init__(self, send=None):
        self._send = send
        self._last_sent = time.monotonic()
        self.facts = {"phase": "open"}
        self.frames = 0
        self.position = None
        self._rate = 0

    def opened(self, container, stream):
        try:
            facts = self.facts
            facts["phase"] = "decode"
            codec = getattr(stream, "codec_context", None)
            facts["container"] = getattr(getattr(container, "format", None), "name", None)
            facts["codec"] = getattr(codec, "name", None)
            self._rate = int(getattr(codec, "sample_rate", 0) or 0)
            facts["sample_rate"] = self._rate
            layout = getattr(codec, "layout", None)
            facts["channel_layout"] = getattr(layout, "name", None)
            facts["channels"] = len(getattr(layout, "channels", ()) or ())
            self._report(force=True)
        except Exception:
            pass

    def decoded(self, frame):
        try:
            samples = int(frame.samples)
            self.frames += samples
            rate = int(frame.sample_rate or self._rate or 0)
            start = frame.time
            if start is not None and rate:
                self.position = float(start) + samples / rate
            elif self._rate:
                self.position = self.frames / self._rate
            self._report()
        except Exception:
            pass

    def snapshot(self):
        facts = dict(self.facts)
        facts["decoded_frames"] = self.frames
        if self.position is not None:
            facts["decode_position_seconds"] = self.position
        return facts

    def _report(self, force=False):
        now = time.monotonic()
        if self._send is None or (not force and now - self._last_sent < PROGRESS_INTERVAL_SECONDS):
            return
        self._last_sent = now
        self._send({"event": "progress", "diagnostics": safe_diagnostics(self.snapshot())})


def _accepts(func, name):
    try:
        return name in inspect.signature(func).parameters
    except (TypeError, ValueError):
        return False


def _serve(request, send):
    probe = DecodeProbe(send)
    target_ref = request["target"]
    try:
        target = _resolve(target_ref, CHILD_PACKAGE)
        kwargs = dict(request.get("kwargs") or {})
        if _accepts(target, "observer"):
            kwargs["observer"] = probe
        value = target(request["path"], **kwargs)
        return {"event": "result", "value": _encode(value, CHILD_PACKAGE)}, False
    except Exception as exc:
        category = failure_category(exc)
        facts = probe.snapshot()
        facts["error_type"] = type(exc).__name__
        code = getattr(exc, "errno", None)
        if "FFmpegError" in _mro_names(exc) and isinstance(code, int):
            facts["errno"] = code
        message = {
            "event": "error",
            "category": category,
            "error_type": type(exc).__name__,
            "diagnostics": safe_diagnostics(facts),
        }
        # A MemoryError may leave the worker unusable: report, then exit.
        return message, category == ANALYSIS_CRASH


def _bind_to_parent_death(parent_pid):
    bound = False
    if sys.platform.startswith("linux"):
        try:
            import ctypes

            libc = ctypes.CDLL(None, use_errno=True)  # libc is in the global namespace
            bound = libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGKILL), 0, 0, 0) == 0
        except Exception:
            bound = False
    if os.getppid() != parent_pid:
        os._exit(0)
    if not bound:
        def watch():
            while os.getppid() == parent_pid:
                time.sleep(1.0)
            os._exit(0)

        threading.Thread(target=watch, name="lumae-parent-watch", daemon=True).start()


def _stand_in_package(plugin_dir):
    package = types.ModuleType(CHILD_PACKAGE)
    package.__path__ = [plugin_dir]
    package.__package__ = CHILD_PACKAGE
    sys.modules[CHILD_PACKAGE] = package
    return package


def _child_main():
    """Worker loop: one JSON request per line on stdin, JSON lines back.

    The protocol uses a private copy of the original stdout; file descriptor 1
    is pointed at stderr so stray prints cannot corrupt it.
    """
    protocol = os.fdopen(os.dup(1), "wb", buffering=0)
    os.dup2(2, 1)
    lock = threading.Lock()

    def send(message):
        data = json.dumps(message, separators=(",", ":"), allow_nan=True).encode("utf-8") + b"\n"
        with lock:
            protocol.write(data)

    hello = json.loads(sys.stdin.buffer.readline() or b"{}")
    _bind_to_parent_death(int(hello.get("parent_pid") or os.getppid()))
    try:
        import faulthandler

        faulthandler.enable()
    except Exception:
        pass
    sys.path[:] = [entry for entry in hello.get("sys_path", sys.path) if entry]
    _stand_in_package(hello["plugin_dir"])
    for module in hello.get("preload", ()):
        try:
            importlib.import_module(module if "." in module else f"{CHILD_PACKAGE}.{module}")
        except Exception:
            pass
    send({"event": "ready", "pid": os.getpid()})
    for line in sys.stdin.buffer:
        if not line.strip():
            continue
        message, fatal = _serve(json.loads(line), send)
        try:
            send(message)
        except Exception:
            os._exit(_EXIT_AFTER_CRASH)
        if fatal:
            os._exit(_EXIT_AFTER_CRASH)
    os._exit(0)


# ---------------------------------------------------------------- parent side


_BOOTSTRAP = (
    "import sys\n"
    "if sys.path and sys.path[0] == '':\n"
    "    del sys.path[0]\n"
    "import importlib.util\n"
    "spec = importlib.util.spec_from_file_location('_lumae_analysis_isolation', sys.argv[1])\n"
    "module = importlib.util.module_from_spec(spec)\n"
    "sys.modules[spec.name] = module\n"
    "spec.loader.exec_module(module)\n"
    "module._child_main()\n"
)
_PRELOAD = ("loudness", "edge_profiles", "av")
# The child needs no credentials: it reads one local file and computes.
_SECRET_ENV = re.compile(
    r"PASSWORD|PASSWD|SECRET|TOKEN|CREDENTIAL|API_?KEY|PRIVATE_?KEY|ACCESS_?KEY"
    r"|DATABASE_URL|DSN|AUTH|^PG|^POSTGRES",
    re.IGNORECASE,
)


def _child_environment():
    return {name: value for name, value in os.environ.items() if not _SECRET_ENV.search(name)}


class _WorkerUnavailable(Exception):
    pass


class _WorkerGone(Exception):
    """The worker exited or broke its protocol before answering."""


def _signal_name(number):
    try:
        return signal.Signals(number).name
    except (ValueError, AttributeError):
        return f"SIG{number}"


class _Worker:
    def __init__(self, startup_timeout):
        command = [sys.executable, "-c", _BOOTSTRAP, os.path.abspath(__file__)]
        try:
            # No preexec_fn and no new session: safe with threads, and the
            # host's cancel (a process-group SIGKILL) still reaches the child.
            self.proc = subprocess.Popen(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                bufsize=0, close_fds=True, env=_child_environment(),
            )
        except OSError as exc:
            raise _WorkerUnavailable(type(exc).__name__) from exc
        self.pid = self.proc.pid
        self.owner_pid = os.getpid()
        self._buffer = b""
        path = [os.getcwd() if entry == "" else entry for entry in sys.path]
        try:
            self._write({
                "parent_pid": os.getpid(),
                "plugin_dir": os.path.dirname(os.path.abspath(__file__)),
                "sys_path": path,
                "preload": _PRELOAD,
            })
            ready = self.read(time.monotonic() + startup_timeout)
        except _WorkerGone as exc:
            self.kill(0)
            raise _WorkerUnavailable("worker exited during startup") from exc
        if ready is None or ready.get("event") != "ready":
            self.kill(0)
            raise _WorkerUnavailable("worker did not start in time")

    def alive(self):
        return self.owner_pid == os.getpid() and self.proc.poll() is None

    def _write(self, message):
        data = json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"
        try:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as exc:
            raise _WorkerGone() from exc

    def send(self, message):
        self._write(message)

    def read(self, deadline):
        """The next message, or None at ``deadline``. Raises ``_WorkerGone``."""
        fd = self.proc.stdout.fileno()
        # poll, not select: a busy host process can hold descriptors >= 1024.
        poller = select.poll()
        poller.register(fd, select.POLLIN | select.POLLHUP | select.POLLERR)
        while b"\n" not in self._buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if not poller.poll(max(1, math.ceil(remaining * 1000))):
                continue  # timed out (or a signal): the loop re-checks the deadline
            chunk = os.read(fd, 65536)
            if not chunk:
                raise _WorkerGone()
            self._buffer += chunk
            if len(self._buffer) > MAX_MESSAGE_BYTES:
                raise _WorkerGone()
        line, self._buffer = self._buffer.split(b"\n", 1)
        try:
            message = json.loads(line)
        except ValueError as exc:
            raise _WorkerGone() from exc
        if not isinstance(message, dict):
            raise _WorkerGone()
        return message

    def exit_facts(self, wait_seconds=5.0):
        """Reap the exited worker (killing it if it lingers); exit code or signal."""
        try:
            code = self.proc.wait(timeout=wait_seconds)
        except subprocess.TimeoutExpired:
            self.kill(0)
            code = self.proc.returncode
        self._close_pipes()
        if code is None:
            return {}
        if code < 0:
            return {"signal": _signal_name(-code)}
        return {"exit_code": code}

    def kill(self, grace):
        """SIGTERM, then SIGKILL after ``grace`` seconds; always reaps."""
        if self.owner_pid != os.getpid():
            return
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=max(0.0, grace))
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            except OSError:
                pass
        self._close_pipes()

    def close(self, wait_seconds=2.0):
        """Ask an idle worker to exit (EOF on stdin); kill it if it does not."""
        if self.owner_pid != os.getpid():
            return
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=wait_seconds)
        except subprocess.TimeoutExpired:
            self.kill(0)
        self._close_pipes()

    def _close_pipes(self):
        for pipe in (self.proc.stdin, self.proc.stdout):
            try:
                pipe.close()
            except (OSError, ValueError):
                pass


_pool_lock = threading.Lock()
_pooled = None
# Set when a worker could not start: this process then analyzes in-process
# instead of paying the start-up timeout again for every file.
_unavailable = None


def _forget_after_fork():
    # A forked copy of the parent must not talk to (or hold open) its worker.
    # Closing the pipe objects (not their descriptors) keeps a later finalizer
    # from closing a reused descriptor number.
    global _pool_lock, _pooled
    _pool_lock = threading.Lock()
    worker, _pooled = _pooled, None
    if worker is not None:
        worker._close_pipes()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_forget_after_fork)


def shutdown():
    """Stop the pooled worker and forget a start failure (exit, tests).

    Never waits for an analysis in progress: a busy worker is killed.
    """
    global _pooled, _unavailable
    idle = _pool_lock.acquire(timeout=1.0)
    try:
        worker, _pooled = _pooled, None
        _unavailable = None
    finally:
        if idle:
            _pool_lock.release()
    if worker is not None:
        if idle:
            worker.close()
        else:
            worker.kill(0)


def _isolation_available():
    return (
        os.name == "posix"
        and hasattr(select, "poll")
        and bool(sys.executable)
        and not getattr(sys, "frozen", False)
        and _unavailable is None
    )


def _checkout(startup_timeout):
    """(worker, pooled): the pooled worker, or a one-off when it is busy."""
    global _pooled
    if not _pool_lock.acquire(blocking=False):
        return _Worker(startup_timeout), False
    try:
        worker = _pooled
        if worker is not None and not worker.alive():
            if worker.owner_pid == os.getpid():
                worker.exit_facts(0.1)
            worker = _pooled = None
        if worker is None:
            worker = _pooled = _Worker(startup_timeout)
    except BaseException:
        _pool_lock.release()
        raise
    return worker, True


def _checkin(worker, pooled, broken, grace):
    """Return the worker. A broken one is killed and reaped, never reused.

    This is the one place a worker is killed: SIGTERM, then SIGKILL after
    ``grace`` seconds.
    """
    global _pooled
    if broken:
        worker.kill(grace)
    if not pooled:
        if not broken:
            worker.close()
        return
    if broken and _pooled is worker:
        _pooled = None
    _pool_lock.release()


def _identifier_or(value, default):
    return value if isinstance(value, str) and _IDENTIFIER.fullmatch(value) else default


def _byte_size(path):
    try:
        return os.stat(path).st_size
    except (OSError, TypeError, ValueError):
        return None


def run_isolated(target, path, *, limit_seconds, headroom_seconds=None,
                 term_grace_seconds=None, startup_timeout_seconds=None, **kwargs):
    """``target(path, **kwargs)`` in the worker, killed after the hard limit.

    Returns the target's result. A failure in the child raises
    ``IsolatedAnalysisError`` with its category (``failure_category``):
    exceptions keep their category, the hard limit is ``analysis_timeout``, and
    a worker that dies without answering (non-zero exit, a signal, a
    MemoryError) is ``analysis_crash``. Targets that cannot run in the child run
    in-process and raise their own exceptions (see the module docstring).
    ``kwargs`` must be JSON-serializable.
    """
    global _unavailable
    plugin_package = __package__ or ""
    reference = _child_target(target, plugin_package) if plugin_package else None
    if reference is None or not _isolation_available():
        return target(path, **kwargs)
    headroom = HARD_LIMIT_HEADROOM_SECONDS if headroom_seconds is None else headroom_seconds
    grace = TERM_GRACE_SECONDS if term_grace_seconds is None else term_grace_seconds
    startup = STARTUP_TIMEOUT_SECONDS if startup_timeout_seconds is None else startup_timeout_seconds
    request = {"target": reference, "path": os.fspath(path), "kwargs": kwargs}
    facts = {"analyzer": reference["module"].rsplit(".", 1)[-1], "phase": "start"}
    size = _byte_size(path)
    if size is not None:
        facts["byte_size"] = size

    for attempt in (1, 2):
        try:
            worker, pooled = _checkout(startup)
        except _WorkerUnavailable as exc:
            _unavailable = str(exc)
            logger.warning(
                "lumae_analysis analysis worker unavailable (%s); this process "
                "analyzes in-process without the hard time limit", exc,
            )
            return target(path, **kwargs)
        broken = True
        try:
            try:
                worker.send(request)
            except _WorkerGone:
                if attempt == 1:
                    continue  # an idle worker had died; start a fresh one
                facts.update(worker.exit_facts(0.5))
                raise IsolatedAnalysisError(ANALYSIS_CRASH, None, facts, worker.pid)
            started = time.monotonic()
            kill_at = started + max(0.0, float(limit_seconds)) + max(0.0, float(headroom))
            while True:
                try:
                    message = worker.read(kill_at)
                except _WorkerGone:
                    # Exited (or broke the protocol) without an answer.
                    facts.update(worker.exit_facts())
                    facts["elapsed_seconds"] = time.monotonic() - started
                    raise IsolatedAnalysisError(ANALYSIS_CRASH, None, facts, worker.pid)
                if message is None:
                    # Hard limit: the worker is killed on the way out.
                    facts["elapsed_seconds"] = time.monotonic() - started
                    facts["limit_seconds"] = int(limit_seconds)
                    raise IsolatedAnalysisError(ANALYSIS_TIMEOUT, None, facts, worker.pid)
                event = message.get("event")
                reported = message.get("diagnostics")
                if isinstance(reported, dict):
                    facts.update(reported)
                if event == "progress":
                    continue
                if event == "error":
                    category = _identifier_or(message.get("category"), "analysis_error")
                    facts["elapsed_seconds"] = time.monotonic() - started
                    if category == ANALYSIS_CRASH:
                        # The worker exits after reporting a MemoryError.
                        facts.update(worker.exit_facts())
                    else:
                        broken = False
                    raise IsolatedAnalysisError(
                        category, _identifier_or(message.get("error_type"), None),
                        facts, worker.pid,
                    )
                if event != "result":
                    raise IsolatedAnalysisError(ANALYSIS_CRASH, None, facts, worker.pid)
                broken = False
                try:
                    return _decode(message.get("value"), plugin_package)
                except Exception as exc:
                    raise IsolatedAnalysisError(
                        "analysis_error", type(exc).__name__, facts, worker.pid,
                    ) from None
        finally:
            _checkin(worker, pooled, broken, grace)
    raise AssertionError("unreachable")  # pragma: no cover


atexit.register(shutdown)

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

Two ways to start the child, with the same limits, classification and
diagnostics:

* Fork path (``_use_fork``): when the calling process has one Python thread,
  the one running, the child is an ``os.fork()`` of it. AudioMuse runs every
  job in a freshly forked, single-threaded process, so this is the host's
  path. The child already has the analyzers imported and costs a fork (about
  5 ms), not a new interpreter. It inherits every descriptor, database sockets
  included, and never uses or closes one: closing a psycopg2 connection would
  send Terminate on the parent's socket. It resets the parent's Python signal
  handlers, turns the garbage collector off (no inherited object is finalized
  there), binds itself to the parent's death, writes one JSON answer to a pipe
  and leaves with ``os._exit`` (no atexit handlers, finalizers or stdio
  flushes). Native threads (OpenBLAS, ONNX Runtime) are not counted; the child
  uses neither their locks nor their pools.
* Exec path (``_Worker``), for a process with more threads (the web tier,
  threaded tests): ``subprocess`` (fork and exec of ``sys.executable``), never
  a bare ``fork`` of a threaded process. ``close_fds`` keeps every descriptor
  but its three pipes out of the child. The child loads this file and the
  analyzers through a stand-in package for the plugin directory; the plugin
  ``__init__`` (Flask, the host API, the database) is never imported there.
  Starting one costs about 1 s (mostly ``import scipy.signal``), so one worker
  is pooled and reused for later files. It acknowledges each request; a worker
  that died while idle (before it acknowledged) is replaced and the file sent
  again, uncounted. A killed or crashed worker is reaped and replaced. The
  pooled worker is reused only by the thread that started it, because its
  ``PR_SET_PDEATHSIG`` fires when that thread exits; a call made while another
  thread uses it gets a one-off worker. Its environment omits credential-like
  variables. That is hygiene, not a security boundary: it runs as the same
  user and could read the parent's ``/proc/<pid>/environ``.
* Only the path and keyword arguments go to the child, and only the result
  comes back, as one JSON line (bytes, tuples and dataclasses are tagged). No
  audio crosses the process boundary, so nothing is held twice.
* ``limit_seconds`` is also the analyzer's soft deadline: it is passed with
  each request as ``deadline_seconds`` to a target that takes that argument,
  so a changed setting applies to the next file without restarting the pooled
  worker. The soft deadline is checked between decoded frames and ends an
  analysis that is still making progress (``analysis_timeout``). The child is
  killed ``HARD_LIMIT_HEADROOM_SECONDS`` later, with SIGTERM and then SIGKILL
  after ``TERM_GRACE_SECONDS``: only a call that never returns to Python
  reaches the kill.
* A target the child cannot import by name (a closure, as tests inject), a
  frozen build (``sys.executable`` is the application, not Python), a
  non-POSIX platform and a worker that cannot start run in-process: the soft
  deadline (still ``limit_seconds``) only, no hard limit and no failure
  diagnostics. The last case is logged.

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
import gc
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
    if (
        not _IDENTIFIER.fullmatch(name) or not _MODULE.fullmatch(module)
        or any(part.startswith("__") for part in (*module.split("."), name))
    ):
        # No dunder part: "__init__" would run the plugin package in the child.
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
    if module == "__main__" or module == plugin_package:
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


def _error_message(exc, probe):
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
    # A MemoryError may leave the process unusable: report, then exit.
    return message, category == ANALYSIS_CRASH


def _run_target(target, path, kwargs, send, plugin_prefix):
    """(message, fatal) for one analysis; runs in the child on both paths."""
    probe = DecodeProbe(send)
    try:
        kwargs = dict(kwargs or {})
        if _accepts(target, "observer"):
            kwargs["observer"] = probe
        value = target(path, **kwargs)
        return {"event": "result", "value": _encode(value, plugin_prefix)}, False
    except Exception as exc:
        return _error_message(exc, probe)


def _serve(request, send):
    try:
        target = _resolve(request["target"], CHILD_PACKAGE)
    except Exception as exc:
        return _error_message(exc, DecodeProbe())
    return _run_target(target, request["path"], request.get("kwargs"), send, CHILD_PACKAGE)


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
    protocol = os.dup(1)
    os.dup2(2, 1)
    lock = threading.Lock()

    def send(message):
        with lock:
            _write_all(protocol, _line(message))

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
        # Taken: from here on, the worker dying is this request's crash.
        send({"event": "accepted"})
        message, fatal = _serve(json.loads(line), send)
        try:
            send(message)
        except Exception:
            os._exit(_EXIT_AFTER_CRASH)
        if fatal:
            os._exit(_EXIT_AFTER_CRASH)
    os._exit(0)


def _line(message):
    return json.dumps(message, separators=(",", ":"), allow_nan=True).encode("utf-8") + b"\n"


def _write_all(fd, data):
    """``os.write`` until every byte is written (a pipe write can be partial)."""
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _enable_faulthandler():
    try:
        import faulthandler

        faulthandler.enable()
    except Exception:
        pass


def _default_signal_handlers():
    """Default dispositions for every signal the parent handles in Python.

    A fork inherits the parent's Python handlers (the host worker's SIGTERM
    handler, SIGINT's KeyboardInterrupt); the kill must not reach them.
    """
    for number in signal.valid_signals():
        try:
            handler = signal.getsignal(number)
        except (ValueError, OSError):
            continue
        if callable(handler) or number in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(number, signal.SIG_DFL)
            except (ValueError, OSError):
                pass


def _fork_child_main(read_fd, write_fd, parent_pid, run):
    """The fork path's child: run one analysis, report, ``os._exit``. Never returns.

    It shares every descriptor the parent had, database sockets included, and
    must never use or close one: closing a psycopg2 connection here would send
    Terminate on the parent's socket. ``os._exit`` skips atexit handlers,
    finalizers and stdio flushes, and the garbage collector is off, so no
    inherited object is finalized in this process.
    """
    code = _EXIT_AFTER_CRASH
    try:
        gc.disable()
        os.close(read_fd)
        _default_signal_handlers()
        _bind_to_parent_death(parent_pid)
        _enable_faulthandler()

        def send(message):
            _write_all(write_fd, _line(message))

        message, fatal = run(send)
        send(message)
        code = _EXIT_AFTER_CRASH if fatal else 0
    except BaseException:
        code = _EXIT_AFTER_CRASH
    finally:
        os._exit(code)


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
# Hygiene, not a security boundary: the worker runs as the same user and could
# read /proc/<parent>/environ. It simply has no use for credentials.
_SECRET_ENV = re.compile(
    r"PASSWORD|PASSWD|SECRET|TOKEN|CREDENTIAL|API_?KEY|PRIVATE_?KEY|ACCESS_?KEY"
    r"|DATABASE_URL|DSN|AUTH|^PG|^POSTGRES",
    re.IGNORECASE,
)
# The fork path; tests and the benchmark switch it off to use the exec path.
FORK_FAST_PATH = True


def _child_environment():
    return {name: value for name, value in os.environ.items() if not _SECRET_ENV.search(name)}


class _WorkerUnavailable(Exception):
    pass


class _WorkerGone(Exception):
    """The child exited or broke its protocol before answering."""


def _signal_name(number):
    try:
        return signal.Signals(number).name
    except (ValueError, AttributeError):
        return f"SIG{number}"


def _exit_facts_of(code):
    if code is None:
        return {}
    if code < 0:
        return {"signal": _signal_name(-code)}
    return {"exit_code": code}


class _LineReader:
    """JSON lines from a pipe, each read bounded by a monotonic deadline."""

    def __init__(self, fd):
        self.fd = fd
        self._buffer = b""
        # poll, not select: a busy host process can hold descriptors >= 1024.
        self._poller = select.poll()
        self._poller.register(fd, select.POLLIN | select.POLLHUP | select.POLLERR)

    def read(self, deadline):
        """The next message, or None at ``deadline``. Raises ``_WorkerGone``."""
        while b"\n" not in self._buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if not self._poller.poll(max(1, math.ceil(remaining * 1000))):
                continue  # timed out (or a signal): the loop re-checks the deadline
            chunk = os.read(self.fd, 65536)
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


class _Worker:
    """The exec path: a pooled ``sys.executable`` worker serving many files."""

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
        # PR_SET_PDEATHSIG fires when this thread exits, not the process.
        self.creator = threading.get_ident()
        self.healthy = False
        self._reader = _LineReader(self.proc.stdout.fileno())
        path = [os.getcwd() if entry == "" else entry for entry in sys.path]
        try:
            self.send({
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

    def send(self, message):
        try:
            _write_all(self.proc.stdin.fileno(), _line(message))
        except (OSError, ValueError) as exc:
            raise _WorkerGone() from exc

    def read(self, deadline):
        return self._reader.read(deadline)

    def exit_facts(self, wait_seconds=5.0):
        """Reap the exited worker (killing it if it lingers); exit code or signal."""
        try:
            code = self.proc.wait(timeout=wait_seconds)
        except subprocess.TimeoutExpired:
            self.kill(0)
            code = self.proc.returncode
        self._close_pipes()
        return _exit_facts_of(code)

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


class _ForkedChild:
    """The fork path: one analysis in a fork of this single-threaded process."""

    def __init__(self, run):
        read_fd, write_fd = os.pipe()
        parent_pid = os.getpid()
        try:
            pid = os.fork()
        except OSError:
            os.close(read_fd)
            os.close(write_fd)
            raise
        if pid == 0:
            _fork_child_main(read_fd, write_fd, parent_pid, run)
        os.close(write_fd)
        self.pid = pid
        self.healthy = False
        self._fd = read_fd
        self._reader = _LineReader(read_fd)
        self._code = None
        self._reaped = False
        # A pidfd (Linux 5.3+) becomes readable the moment the child exits, so
        # reaping waits exactly as long as needed. Unreaped, the pid is ours.
        self._pidfd = None
        if hasattr(os, "pidfd_open"):
            try:
                self._pidfd = os.pidfd_open(pid)
            except OSError:
                pass

    def read(self, deadline):
        return self._reader.read(deadline)

    def _poll(self, block=False):
        """True once the child is reaped (by this process or by SIGCHLD=SIG_IGN)."""
        if not self._reaped:
            try:
                pid, status = os.waitpid(self.pid, 0 if block else os.WNOHANG)
            except ChildProcessError:
                self._reaped = True  # reaped elsewhere: exit status unknown
            else:
                if pid:
                    self._reaped = True
                    self._code = os.waitstatus_to_exitcode(status)
        return self._reaped

    def _wait(self, seconds):
        deadline = time.monotonic() + max(0.0, seconds)
        pause = 0.0005
        while not self._poll():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if self._pidfd is not None:
                poller = select.poll()
                poller.register(self._pidfd, select.POLLIN)
                poller.poll(max(1, math.ceil(remaining * 1000)))
            else:
                time.sleep(min(pause, remaining))
                pause = min(pause * 2, 0.02)
        return True

    def alive(self):
        return not self._poll()

    def exit_facts(self, wait_seconds=5.0):
        if not self._wait(wait_seconds):
            self.kill(0)
        self._close()
        return _exit_facts_of(self._code)

    def kill(self, grace):
        """SIGTERM, then SIGKILL after ``grace`` seconds; always reaps."""
        if not self._poll():
            for number, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, None)):
                try:
                    os.kill(self.pid, number)
                except ProcessLookupError:
                    pass
                if wait is None:
                    self._poll(block=True)
                elif self._wait(wait):
                    break
        self._close()

    def close(self):
        """Reap a child that answered (it exits right after); kill a lingering one."""
        if not self._wait(5.0):
            self.kill(0)
        self._close()

    def _close(self):
        for name in ("_fd", "_pidfd"):
            fd = getattr(self, name)
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, name, None)


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
        and not getattr(sys, "frozen", False)
    )


def _use_fork():
    """The fork path needs a process with one Python thread, and it on the main thread.

    Another Python thread could hold a lock (logging, an import) that the
    fork child would inherit locked. Native threads (OpenBLAS, ONNX Runtime)
    are not counted: the child uses neither their locks nor their pools, and a
    child that deadlocks anyway is killed at the hard limit.
    """
    return (
        FORK_FAST_PATH
        and hasattr(os, "fork")
        and threading.active_count() == 1
        and threading.current_thread() is threading.main_thread()
    )


def _checkout(startup_timeout):
    """(worker, pooled): the pooled worker, or a one-off when it is busy.

    The pooled worker is reused only by the thread that started it: its
    ``PR_SET_PDEATHSIG`` fires when that thread exits. Another thread gets a
    fresh pooled worker (the idle one is stopped).
    """
    global _pooled
    if not _pool_lock.acquire(blocking=False):
        if _unavailable is not None:
            raise _WorkerUnavailable(_unavailable)
        return _Worker(startup_timeout), False
    try:
        worker = _pooled
        if worker is not None and not (
            worker.creator == threading.get_ident() and worker.alive()
        ):
            _pooled = None
            if worker.owner_pid == os.getpid():
                worker.kill(0)
            worker = None
        if worker is None:
            if _unavailable is not None:
                raise _WorkerUnavailable(_unavailable)
            worker = _pooled = _Worker(startup_timeout)
    except BaseException:
        _pool_lock.release()
        raise
    worker.healthy = False
    return worker, True


def _checkin(worker, pooled, grace):
    """Return the worker. A broken one is killed and reaped, never reused.

    Workers are killed here and only here: SIGTERM, then SIGKILL after
    ``grace`` seconds.
    """
    global _pooled
    if not worker.healthy:
        worker.kill(grace)
    if not pooled:
        if worker.healthy:
            worker.close()
        return
    if not worker.healthy and _pooled is worker:
        _pooled = None
    _pool_lock.release()


def _identifier_or(value, default):
    return value if isinstance(value, str) and _IDENTIFIER.fullmatch(value) else default


def with_deadline(target, kwargs, deadline_seconds):
    """``kwargs`` plus ``deadline_seconds`` when ``target`` takes that argument.

    An explicit ``deadline_seconds`` in ``kwargs`` wins. Targets without the
    argument (test doubles) are called exactly as before.
    """
    if (
        deadline_seconds is None
        or "deadline_seconds" in kwargs
        or not _accepts(target, "deadline_seconds")
    ):
        return kwargs
    return {**kwargs, "deadline_seconds": deadline_seconds}


def _byte_size(path):
    try:
        return os.stat(path).st_size
    except (OSError, TypeError, ValueError):
        return None


def _await_answer(child, facts, limit_seconds, headroom, plugin_package, accepted):
    """The child's answer, or ``IsolatedAnalysisError``. Same on both paths.

    Sets ``child.healthy`` when the child answered and may be reused. Raises
    ``_WorkerGone`` when an exec worker died before it accepted the request.
    """
    started = time.monotonic()
    kill_at = started + max(0.0, float(limit_seconds)) + max(0.0, float(headroom))
    while True:
        try:
            message = child.read(kill_at)
        except _WorkerGone:
            if not accepted:
                raise
            # Exited (or broke the protocol) without an answer.
            facts.update(child.exit_facts())
            facts["elapsed_seconds"] = time.monotonic() - started
            raise IsolatedAnalysisError(ANALYSIS_CRASH, None, facts, child.pid)
        if message is None:
            # Hard limit: the child is killed on the way out.
            facts["elapsed_seconds"] = time.monotonic() - started
            facts["limit_seconds"] = int(limit_seconds)
            raise IsolatedAnalysisError(ANALYSIS_TIMEOUT, None, facts, child.pid)
        event = message.get("event")
        if event == "accepted":
            accepted = True
            continue
        reported = message.get("diagnostics")
        if isinstance(reported, dict):
            facts.update(reported)
        if event == "progress":
            continue
        if event == "error":
            category = _identifier_or(message.get("category"), "analysis_error")
            facts["elapsed_seconds"] = time.monotonic() - started
            if category == ANALYSIS_CRASH:
                # The child exits after reporting a MemoryError.
                facts.update(child.exit_facts())
            else:
                child.healthy = True
            raise IsolatedAnalysisError(
                category, _identifier_or(message.get("error_type"), None),
                facts, child.pid,
            )
        if event != "result":
            raise IsolatedAnalysisError(ANALYSIS_CRASH, None, facts, child.pid)
        child.healthy = True
        try:
            return _decode(message.get("value"), plugin_package)
        except Exception as exc:
            raise IsolatedAnalysisError(
                "analysis_error", type(exc).__name__, facts, child.pid,
            ) from None


def _run_forked(target, path, kwargs, plugin_package, facts, limit_seconds, headroom, grace):
    child = _ForkedChild(
        lambda send: _run_target(target, path, kwargs, send, plugin_package)
    )
    try:
        return _await_answer(child, facts, limit_seconds, headroom, plugin_package, True)
    finally:
        if child.healthy:
            child.close()
        else:
            child.kill(grace)


def _run_exec(target, path, kwargs, reference, plugin_package, facts, limit_seconds,
              headroom, grace, startup):
    global _unavailable
    request = {"target": reference, "path": path, "kwargs": kwargs}
    for attempt in (1, 2):
        try:
            worker, pooled = _checkout(startup)
        except _WorkerUnavailable as exc:
            if _unavailable is None:
                _unavailable = str(exc)
                logger.warning(
                    "lumae_analysis analysis worker unavailable (%s); this process "
                    "analyzes in-process without the hard time limit", exc,
                )
            return target(path, **kwargs)
        try:
            try:
                worker.send(request)
                return _await_answer(
                    worker, facts, limit_seconds, headroom, plugin_package, False,
                )
            except _WorkerGone:
                # Gone before it accepted the request (killed while idle, e.g.
                # by the OOM killer): replace it and send once more, uncounted.
                if attempt == 1:
                    continue
                facts.update(worker.exit_facts(0.5))
                raise IsolatedAnalysisError(ANALYSIS_CRASH, None, facts, worker.pid)
        finally:
            _checkin(worker, pooled, grace)
    raise AssertionError("unreachable")  # pragma: no cover


def run_isolated(target, path, *, limit_seconds, headroom_seconds=None,
                 term_grace_seconds=None, startup_timeout_seconds=None, **kwargs):
    """``target(path, **kwargs)`` in a child process, killed after the hard limit.

    ``limit_seconds`` is the soft deadline (``deadline_seconds``, for a target
    that takes it); the hard limit is ``limit_seconds`` plus
    ``headroom_seconds``. The child is a fork of this process when it has one
    Python thread (``_use_fork``), else the pooled exec worker. Returns the
    target's result. A failure in the child raises ``IsolatedAnalysisError``
    with its category (``failure_category``): exceptions keep their category,
    the hard limit is ``analysis_timeout``, and a child that dies without
    answering (non-zero exit, a signal, a MemoryError) is ``analysis_crash``.
    Targets that cannot run in the child run in-process and raise their own
    exceptions (see the module docstring). ``kwargs`` must be JSON-serializable.
    """
    kwargs = with_deadline(target, kwargs, limit_seconds)
    plugin_package = __package__ or ""
    reference = _child_target(target, plugin_package) if plugin_package else None
    if reference is None or not _isolation_available():
        return target(path, **kwargs)
    headroom = HARD_LIMIT_HEADROOM_SECONDS if headroom_seconds is None else headroom_seconds
    grace = TERM_GRACE_SECONDS if term_grace_seconds is None else term_grace_seconds
    startup = STARTUP_TIMEOUT_SECONDS if startup_timeout_seconds is None else startup_timeout_seconds
    path = os.fspath(path)  # both paths hand the analyzer a str
    facts = {"analyzer": reference["module"].rsplit(".", 1)[-1], "phase": "start"}
    size = _byte_size(path)
    if size is not None:
        facts["byte_size"] = size
    if _use_fork():
        return _run_forked(target, path, kwargs, plugin_package, facts,
                           limit_seconds, headroom, grace)
    return _run_exec(target, path, kwargs, reference, plugin_package, facts,
                     limit_seconds, headroom, grace, startup)


atexit.register(shutdown)

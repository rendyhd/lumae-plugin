"""Runner plan R5: catalogue work in a child process.

A catalogue refresh, its first preparation, an analysis run's finalization,
the projection, the identity recheck and a relationship build step load whole
catalogues. In AudioMuse's web process they held its interpreter for seconds
and kept the memory after they ended (RN.8, 132k tracks: a refresh that
changed nothing took the request p95 from 37 to 421 ms and the process from
289 MB to 1.36 GB). In a runner turn each runs in a child process started for
it instead (``__init__._call_action``):

* ``sys.executable`` started fresh (fork and exec, ``close_fds``, no
  ``preexec_fn``), as ``analysis_isolation``'s decoding workers, so it is safe
  in a threaded process; bound to the death of the thread that started it and
  running at ``NICE``, so AudioMuse's requests come first.
* It imports the installed plugin as AudioMuse does (the ``audiomuse_plugins``
  namespace over ``config.PLUGINS_DIR``), with AudioMuse's own ``config``,
  ``database`` and provider modules, inside a bare Flask application context,
  so ``get_db``, the settings and the server bindings behave as in the parent.
  It keeps the parent's environment: it needs the database.
* It calls one entry of the plugin (``child_action``), answers one JSON line
  and exits; its memory goes with it. The parent keeps the journal, the turn
  and the slice, and waits.
* An exception comes back as ``ChildRaised`` (type name, text, ``reason``).
  A child that cannot start (no AudioMuse layout, a failed import, no ready
  line within ``STARTUP_TIMEOUT_SECONDS``) raises ``ChildUnavailable``, and the
  caller runs the action in its own thread. A child that dies before it
  answers (the kernel's OOM killer, a crash) or runs past
  ``HARD_LIMIT_SECONDS`` fails the action, which retries with its own back-off:
  running that work in the web process instead is what this avoids.

Standard library only at module level: the child loads this file by path
before anything else.
"""

import datetime
import decimal
import importlib
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time

# The plugin namespace AudioMuse imports plugins under (``plugin.api.NAMESPACE``).
NAMESPACE = "audiomuse_plugins"
NICE = 10
STARTUP_TIMEOUT_SECONDS = 120
HARD_LIMIT_SECONDS = 2 * 3600
TERM_GRACE_SECONDS = 5
MAX_MESSAGE_BYTES = 4 * 1024 * 1024
CHILD_NAME = "_lumae_action_isolation"
_PR_SET_PDEATHSIG = 1

logger = logging.getLogger("lumae_analysis.action_isolation")

_BOOTSTRAP = (
    "import sys\n"
    "if sys.path and sys.path[0] == '':\n"
    "    del sys.path[0]\n"
    "import importlib.util\n"
    f"spec = importlib.util.spec_from_file_location({CHILD_NAME!r}, sys.argv[1])\n"
    "module = importlib.util.module_from_spec(spec)\n"
    "sys.modules[spec.name] = module\n"
    "spec.loader.exec_module(module)\n"
    "module._child_main()\n"
)


class ChildUnavailable(Exception):
    """No child process could run the action; the caller runs it itself."""


class ChildRaised(RuntimeError):
    """The action raised in the child: its exception's type name and text."""

    def __init__(self, type_name, message, reason=None):
        super().__init__(f"{type_name}: {message}")
        self.type_name = type_name
        self.message = message
        self.reason = reason


class ChildFailed(RuntimeError):
    """The child died before it answered, or ran past ``HARD_LIMIT_SECONDS``."""


def host_layout():
    """``(plugin id, AudioMuse's directory)`` when this code runs as AudioMuse
    loaded it (``audiomuse_plugins.<id>``), else None (tests, a copy imported
    elsewhere)."""
    parts = (__package__ or "").split(".")
    if len(parts) != 2 or parts[0] != NAMESPACE:
        return None
    path = getattr(sys.modules.get("plugin.api"), "__file__", None)
    if not path:
        return None
    return parts[1], os.path.dirname(os.path.dirname(os.path.abspath(path)))


def available():
    """Whether ``run`` can start a child here: AudioMuse's layout, POSIX, and
    ``sys.executable`` a Python (not a frozen application)."""
    return os.name == "posix" and not getattr(sys, "frozen", False) and host_layout() is not None


def run(entry, call, *, loader=None, hard_limit=None, startup_timeout=None):
    """``plugin.<entry>(**call)`` in a child process; its JSON-safe result.

    ``loader`` (``"module:function"``, tests only) replaces the AudioMuse
    import: the function gets the request and returns ``(plugin, context)``.
    """
    if loader is None:
        layout = host_layout() if available() else None
        if layout is None:
            raise ChildUnavailable("not running inside AudioMuse")
        plugin_id, host_dir = layout
    else:
        plugin_id, host_dir = None, None
    child = _Child(startup_timeout or STARTUP_TIMEOUT_SECONDS)
    try:
        child.send({
            "parent_pid": os.getpid(),
            "sys_path": [os.getcwd() if entry_path == "" else entry_path for entry_path in sys.path],
            "plugin_id": plugin_id,
            "host_dir": host_dir,
            "loader": loader,
            "nice": NICE,
            "entry": entry,
            "call": call,
        })
        ready = child.read(time.monotonic() + child.startup_timeout)
        if ready is None:
            raise ChildUnavailable("the child did not start in time")
        if ready.get("event") == "unavailable":
            raise ChildUnavailable(str(ready.get("reason") or "the child could not load the plugin"))
        if ready.get("event") != "ready":
            raise ChildUnavailable("the child broke the protocol while starting")
        answer = child.read(time.monotonic() + (hard_limit or HARD_LIMIT_SECONDS))
        if answer is None:
            raise ChildFailed(f"{entry} ran longer than {int(hard_limit or HARD_LIMIT_SECONDS)} s")
    except _ChildGone:
        child.kill()
        facts = child.exit_facts()
        if not child.started:
            raise ChildUnavailable(f"the child exited while starting{facts}") from None
        raise ChildFailed(f"the child exited before it answered{facts}") from None
    finally:
        child.kill(wait=TERM_GRACE_SECONDS)
    if answer.get("event") == "raised":
        raise ChildRaised(str(answer.get("type") or "Exception"), str(answer.get("message") or ""),
                          answer.get("reason"))
    if answer.get("event") != "result":
        raise ChildFailed("the child broke the protocol")
    return answer.get("result")


class _ChildGone(Exception):
    pass


class _Child:
    def __init__(self, startup_timeout):
        self.startup_timeout = startup_timeout
        self.started = False
        try:
            # No preexec_fn and no new session: safe with threads, and the
            # host's cancel (a process-group SIGKILL) still reaches the child.
            self.proc = subprocess.Popen(
                [sys.executable, "-c", _BOOTSTRAP, os.path.abspath(__file__)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0, close_fds=True,
            )
        except OSError as exc:
            raise ChildUnavailable(type(exc).__name__) from exc
        self._buffer = b""

    def send(self, message):
        try:
            self.proc.stdin.write(_line(message))
            self.proc.stdin.flush()
        except OSError as exc:
            raise _ChildGone() from exc

    def read(self, deadline):
        """The next message, or None at ``deadline``. Raises ``_ChildGone``."""
        import select

        fd = self.proc.stdout.fileno()
        poller = select.poll()  # poll, not select: a busy host can hold descriptors >= 1024
        poller.register(fd, select.POLLIN | select.POLLHUP | select.POLLERR)
        while b"\n" not in self._buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if not poller.poll(int(min(remaining, 60.0) * 1000) + 1):
                continue
            chunk = os.read(fd, 65536)
            if not chunk:
                raise _ChildGone()
            self._buffer += chunk
            if len(self._buffer) > MAX_MESSAGE_BYTES:
                raise _ChildGone()
        line, self._buffer = self._buffer.split(b"\n", 1)
        try:
            message = json.loads(line)
        except ValueError as exc:
            raise _ChildGone() from exc
        if not isinstance(message, dict):
            raise _ChildGone()
        if message.get("event") == "ready":
            self.started = True
        return message

    def kill(self, wait=0.0):
        """After ``wait`` seconds for it to exit by itself, SIGTERM, then
        SIGKILL after ``TERM_GRACE_SECONDS``; always reaps."""
        if wait and self.proc.poll() is None:
            try:
                self.proc.wait(timeout=wait)
            except subprocess.TimeoutExpired:
                pass
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=TERM_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            except OSError:
                pass
        for stream in (self.proc.stdin, self.proc.stdout):
            try:
                stream.close()
            except OSError:
                pass

    def exit_facts(self):
        code = self.proc.poll()
        if code is None:
            return ""
        if code < 0:
            try:
                return f" ({signal.Signals(-code).name})"
            except ValueError:
                return f" (signal {-code})"
        return f" (exit code {code})"


# ---------------------------------------------------------------- child side


def _line(message):
    return json.dumps(message, separators=(",", ":"), default=_plain).encode("utf-8") + b"\n"


def _plain(value):
    """What JSON cannot hold, as the journal's summaries keep it."""
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return None
    return str(value)


def _write_all(fd, data):
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _bind_to_parent_death(parent_pid):
    bound = False
    if sys.platform.startswith("linux"):
        try:
            import ctypes

            libc = ctypes.CDLL(None, use_errno=True)
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

        threading.Thread(target=watch, name="lumae-action-parent-watch", daemon=True).start()


def _load_host(request):
    """The installed plugin, imported as AudioMuse imports it, and a Flask
    application context for its ``get_db``."""
    import types

    import config
    from flask import Flask

    module = sys.modules.get(NAMESPACE)
    if module is None:
        module = types.ModuleType(NAMESPACE)
        module.__path__ = []
        module.__package__ = NAMESPACE
        sys.modules[NAMESPACE] = module
    if config.PLUGINS_DIR not in module.__path__:
        module.__path__.append(config.PLUGINS_DIR)
    lib_dir = os.path.join(config.PLUGINS_DIR, "_lib")
    if os.path.isdir(lib_dir) and lib_dir not in sys.path:
        sys.path.append(lib_dir)
    plugin = importlib.import_module(f"{NAMESPACE}.{request['plugin_id']}")
    return plugin, Flask(f"{request['plugin_id']}_action").app_context()


def _load(request):
    if request.get("loader"):
        module_name, _, function = str(request["loader"]).partition(":")
        return getattr(importlib.import_module(module_name), function)(request)
    host_dir = request.get("host_dir")
    if host_dir and host_dir not in sys.path:
        sys.path.insert(0, host_dir)
    return _load_host(request)


def _close_db():
    """The context's database connection, closed before exiting (a dropped
    socket is logged by PostgreSQL)."""
    try:
        from flask import g

        db = g.pop("db", None)
        if db is not None:
            db.close()
    except Exception:
        pass


def _child_main():
    """One request on stdin; ``ready`` (or ``unavailable``), then one answer."""
    protocol = os.dup(1)
    os.dup2(2, 1)  # a stray print cannot corrupt the protocol

    def send(message):
        _write_all(protocol, _line(message))

    request = json.loads(sys.stdin.buffer.readline() or b"{}")
    _bind_to_parent_death(int(request.get("parent_pid") or os.getppid()))
    nice = request.get("nice")
    if isinstance(nice, int) and 0 < nice <= 19:
        try:
            os.nice(nice)
        except (AttributeError, OSError):
            pass
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr,
        format="[%(levelname)s]-[%(asctime)s]-%(name)s (child %(process)d): %(message)s",
        datefmt="%d-%m-%Y %H-%M-%S",
    )
    sys.path[:] = [entry for entry in request.get("sys_path", sys.path) if entry]
    try:
        plugin, context = _load(request)
    except BaseException as exc:  # noqa: BLE001 - the parent runs the action itself
        send({"event": "unavailable", "reason": f"{type(exc).__name__}: {exc}"[:500]})
        os._exit(0)
    send({"event": "ready", "pid": os.getpid()})
    try:
        with context:
            try:
                result = getattr(plugin, request["entry"])(**(request.get("call") or {}))
                answer = {"event": "result", "result": result}
            except Exception as exc:
                answer = {"event": "raised", "type": type(exc).__name__,
                          "message": str(exc)[:2000], "reason": getattr(exc, "reason", None)}
            finally:
                _close_db()
        send(answer)
    finally:
        logging.shutdown()
        os._exit(0)

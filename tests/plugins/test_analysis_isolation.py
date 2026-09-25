"""P3-8 / LUM-018: file analysis in a child process with a hard wall-clock limit.

Unit tests drive ``analysis_isolation.run_isolated`` with fake analyzers from
``analysis_isolation_fakes`` (the child imports them by name) and with the real
analyzers on synthesized FLAC. The end-to-end tests run the task entry points
against the production schema (``migrated_db``) and read what the failure path
recorded.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import numpy as np
import psutil
import pytest

import analysis_isolation_fakes as fakes
from test_lumae_analysis import load_plugin, plugin_client
from plugins.LumaeAnalysis import analysis_isolation as iso
from plugins.LumaeAnalysis import edge_profile_store as store
from plugins.LumaeAnalysis import edge_profiles as edge
from plugins.LumaeAnalysis import loudness
from plugins.LumaeAnalysis import profile_publication as publication


SOURCE = "catalog-a"
SERVER = "server-a"
P = "plugin_lumae_analysis__"
SECRET = "SECRET_LIBRARY_DIR"
EDGE_ARGS = dict(catalog_instance_id=SOURCE, track_id="track-a",
                 media_revision="sha256:" + "a" * 64)
FAST = dict(limit_seconds=1, headroom_seconds=0, term_grace_seconds=1)

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX process isolation")


@pytest.fixture(autouse=True, scope="module")
def _stop_worker_at_end():
    yield
    iso.shutdown()


def _gone(pid, within=5.0):
    """The process no longer exists (or is only an unreaped zombie)."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return True
        except psutil.NoSuchProcess:
            return True
        time.sleep(0.05)
    return False


def _flac(directory, seconds=3.0, rate=44100, name="Artist - Title.flac"):
    import av

    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    frames = int(seconds * rate)
    t = np.arange(frames) / rate
    mono = 0.3 * np.sin(2 * np.pi * 220 * t) + 0.05 * np.random.default_rng(7).standard_normal(frames)
    pcm = (np.stack([mono, mono * 0.9]) * 20000).astype(np.int16)
    with av.open(str(path), "w") as container:
        stream = container.add_stream("flac", rate=rate)
        stream.layout = "stereo"
        for offset in range(0, frames, 4096):
            chunk = np.ascontiguousarray(pcm[:, offset:offset + 4096].T.reshape(1, -1))
            frame = av.AudioFrame.from_ndarray(chunk, format="s16", layout="stereo")
            frame.sample_rate = rate
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode(None):
            container.mux(packet)
    return path


def _corrupt(path):
    """Zero 512 bytes mid-file: FFmpeg's FLAC decoder raises InvalidDataError."""
    data = bytearray(path.read_bytes())
    middle = len(data) // 2
    data[middle:middle + 512] = bytes(512)
    path.write_bytes(bytes(data))
    return path


def _no_secret(*values):
    text = json.dumps(values, default=str)
    assert SECRET not in text
    assert "/" not in json.dumps([v for v in values if isinstance(v, dict)])


# ---------------------------------------------------------------- the limit


def test_hard_limit_kills_a_hung_analysis_and_leaves_no_process(tmp_path):
    media = tmp_path / SECRET / "hung.flac"
    media.parent.mkdir()
    media.write_bytes(b"x" * 1234)
    started = time.monotonic()
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        iso.run_isolated(fakes.sleep_forever, media, **FAST)
    elapsed = time.monotonic() - started
    error = raised.value
    assert error.category == "analysis_timeout"
    assert iso.failure_category(error) == "analysis_timeout"
    assert _gone(error.pid), "the hung child must be killed"
    assert error.pid not in [child.pid for child in psutil.Process().children(recursive=True)]
    assert elapsed < 1 + 1 + 5  # startup, the limit, then the kill
    diagnostics = error.diagnostics
    assert diagnostics["limit_seconds"] == 1
    assert diagnostics["elapsed_seconds"] >= 1
    assert diagnostics["byte_size"] == 1234
    assert diagnostics["analyzer"] == "analysis_isolation_fakes"
    assert diagnostics["phase"] == "start"
    _no_secret(diagnostics, str(error))


def test_sigterm_is_followed_by_sigkill_after_the_grace(tmp_path):
    media = tmp_path / "deaf.flac"
    media.write_bytes(b"x")
    pid_file = tmp_path / "pid"
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        iso.run_isolated(fakes.ignore_term_and_sleep, media, limit_seconds=1,
                         headroom_seconds=0, term_grace_seconds=0.5, pid_file=str(pid_file))
    assert raised.value.category == "analysis_timeout"
    assert int(pid_file.read_text()) == raised.value.pid
    assert _gone(raised.value.pid)


def test_worker_is_reused_and_replaced_after_a_kill(tmp_path):
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    first = iso.run_isolated(fakes.describe_process, media, limit_seconds=60)
    again = iso.run_isolated(fakes.describe_process, media, limit_seconds=60)
    assert first["pid"] == again["pid"] != os.getpid()
    with pytest.raises(iso.IsolatedAnalysisError):
        iso.run_isolated(fakes.sleep_forever, media, **FAST)
    replaced = iso.run_isolated(fakes.describe_process, media, limit_seconds=60)
    assert replaced["pid"] != first["pid"]
    assert _gone(first["pid"])


def test_worker_dies_with_its_parent(tmp_path):
    """A host cancel or OOM kill of the job process takes the child with it."""
    pid_file = tmp_path / "pid"
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    plugin_dir = Path(iso.__file__).resolve().parent
    parent = textwrap.dedent(f"""
        import sys, types
        sys.path.insert(0, {str(Path(fakes.__file__).resolve().parent)!r})
        package = types.ModuleType("p38_parent")
        package.__path__ = [{str(plugin_dir)!r}]
        sys.modules["p38_parent"] = package
        from p38_parent import analysis_isolation as iso
        import analysis_isolation_fakes as fakes
        iso.run_isolated(fakes.sleep_forever, {str(media)!r}, limit_seconds=600,
                         pid_file={str(pid_file)!r})
    """)
    process = subprocess.Popen([sys.executable, "-c", parent])
    try:
        deadline = time.monotonic() + 30
        while not pid_file.exists() or not pid_file.read_text():
            assert time.monotonic() < deadline and process.poll() is None
            time.sleep(0.05)
        worker = int(pid_file.read_text())
        assert psutil.pid_exists(worker)
        process.send_signal(signal.SIGKILL)
        process.wait()
        assert _gone(worker)
    finally:
        if process.poll() is None:
            process.kill()


# ---------------------------------------------------------------- categories


def test_decoder_data_error_in_the_child_is_media_error(tmp_path):
    media = tmp_path / SECRET / "bad.flac"
    media.parent.mkdir()
    media.write_bytes(b"x")
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        iso.run_isolated(fakes.raise_invalid_data, media, limit_seconds=60)
    error = raised.value
    assert error.category == "media_error"
    assert error.diagnostics["error_type"] == "InvalidDataError"
    assert error.diagnostics["errno"] == 1094995529
    _no_secret(error.diagnostics, str(error))
    # The worker survives an ordinary failure and serves the next file.
    assert iso.run_isolated(fakes.describe_process, media, limit_seconds=60)["pid"] == error.pid


def test_real_corrupt_flac_is_media_error_with_decoder_diagnostics(tmp_path):
    media = _corrupt(_flac(tmp_path / SECRET))
    for analyzer, kwargs in ((loudness.analyze_file, {}), (edge.analyze_edge_file, EDGE_ARGS)):
        with pytest.raises(iso.IsolatedAnalysisError) as raised:
            iso.run_isolated(analyzer, media, limit_seconds=60, **kwargs)
        error = raised.value
        assert error.category == "media_error"
        diagnostics = error.diagnostics
        assert {key: diagnostics[key] for key in (
            "container", "codec", "sample_rate", "channel_layout", "channels",
            "byte_size", "phase", "error_type",
        )} == {
            "container": "flac", "codec": "flac", "sample_rate": 44100,
            "channel_layout": "stereo", "channels": 2,
            "byte_size": media.stat().st_size, "phase": "decode",
            "error_type": "InvalidDataError",
        }
        assert 0 < diagnostics["decode_position_seconds"] < 3
        assert 0 < diagnostics["decoded_frames"] < 3 * 44100
        _no_secret(diagnostics, str(error))


@pytest.mark.parametrize("fake,facts", [
    (fakes.exit_hard, {"exit_code": 3}),
    (fakes.die_by_signal, {"signal": "SIGSEGV"}),
    (fakes.raise_memory_error, {"error_type": "MemoryError", "exit_code": 70}),
])
def test_child_crash_is_analysis_crash(tmp_path, fake, facts):
    media = tmp_path / "crash.flac"
    media.write_bytes(b"x")
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        iso.run_isolated(fake, media, limit_seconds=60)
    error = raised.value
    assert error.category == "analysis_crash"
    assert {key: error.diagnostics.get(key) for key in facts} == facts
    assert _gone(error.pid)
    # A fresh worker replaces the dead one.
    assert iso.run_isolated(fakes.describe_process, media, limit_seconds=60)["pid"] != error.pid


def test_child_exceptions_keep_their_previous_categories(tmp_path):
    media = tmp_path / SECRET / "x.flac"
    media.parent.mkdir()
    media.write_bytes(b"x")
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        iso.run_isolated(fakes.raise_value_error, media, limit_seconds=60)
    assert raised.value.category == "unsupported_media"
    _no_secret(raised.value.diagnostics, str(raised.value))  # the message named the path


@pytest.mark.parametrize("exc,category", [
    (loudness.SilentAudioError("x"), "silent_audio"),
    (loudness.ProfileAnalysisTimeout("x"), "analysis_timeout"),
    (loudness.ProfileResourceLimitError("x"), "resource_limit"),
    (edge.EdgeAnalysisTimeout("x"), "analysis_timeout"),
    (edge.EdgeProfileError("x"), "unsupported_media"),
    (ValueError("x"), "unsupported_media"),
    (EOFError("x"), "unsupported_media"),
    (MemoryError(), "analysis_crash"),
    (RuntimeError("x"), "analysis_error"),
    (iso.IsolatedAnalysisError("analysis_crash"), "analysis_crash"),
])
def test_failure_category(exc, category):
    assert iso.failure_category(exc) == category


def test_failure_category_of_pyav_decoder_errors():
    av = pytest.importorskip("av")
    assert iso.failure_category(av.error.InvalidDataError(1, "bad", "avcodec_send_packet()")) == "media_error"
    # Other FFmpeg errors keep their earlier category.
    assert iso.failure_category(av.error.ValueError(22, "invalid")) == "unsupported_media"
    assert iso.failure_category(av.error.MemoryError(12, "oom")) == "analysis_crash"


# ---------------------------------------------------------------- results


def test_real_analyzers_return_identical_results_through_the_child(tmp_path):
    media = _flac(tmp_path)
    expected = loudness.analyze_file(str(media))
    assert iso.run_isolated(loudness.analyze_file, media, limit_seconds=60) == expected
    expected_edge = edge.analyze_edge_file(str(media), **EDGE_ARGS)
    assert iso.run_isolated(edge.analyze_edge_file, media, limit_seconds=60, **EDGE_ARGS) == expected_edge


def test_results_round_trip_tuples_bytes_and_kwargs(tmp_path):
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    result = iso.run_isolated(fakes.echo, media, limit_seconds=60, flag=True, name="n")
    assert result["path"] == str(media)
    assert result["kwargs"] == {"flag": True, "name": "n"}
    assert result["pair"] == (1, b"\x00\xff")


def test_closures_run_in_process(tmp_path):
    assert iso.run_isolated(lambda path: ("in-process", os.getpid()), tmp_path,
                            limit_seconds=60) == ("in-process", os.getpid())


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc/self/fd")
def test_child_has_no_parent_descriptor_or_credentials(tmp_path, monkeypatch):
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    monkeypatch.setenv("P38_DB_PASSWORD", "secret")
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:secret@db/x")
    monkeypatch.setenv("P38_PLAIN_SETTING", "kept")
    iso.shutdown()  # the next worker starts with this environment
    left, right = socket.socketpair()
    left.set_inheritable(True)  # only close_fds keeps it out of the child
    try:
        inode = f"socket:[{os.fstat(left.fileno()).st_ino}]"
        seen = iso.run_isolated(fakes.describe_process, media, limit_seconds=60)
    finally:
        left.close()
        right.close()
        iso.shutdown()
    assert inode not in seen["sockets"]
    assert "P38_PLAIN_SETTING" in seen["environment"]
    assert "P38_DB_PASSWORD" not in seen["environment"]
    assert "DATABASE_URL" not in seen["environment"]


# ------------------------------------------- the setting is the soft deadline


@pytest.fixture
def small_limits(monkeypatch):
    """The real setting path, in seconds instead of minutes.

    ``fakes.OLD_DEFAULT_DEADLINE`` (2 s) stands in for the analyzers' former
    fixed 900 s; a setting of 5 s stands in for raising it to 1,200 s.
    """
    mod = load_plugin()
    settings = {}
    monkeypatch.setattr(iso, "MIN_LIMIT_SECONDS", 1)
    monkeypatch.setattr(iso, "HARD_LIMIT_HEADROOM_SECONDS", 1)
    monkeypatch.setattr(iso, "TERM_GRACE_SECONDS", 1)
    monkeypatch.setattr(mod, "get_setting",
                        lambda key, default=None: settings.get(key, default))

    def set_limit(seconds):
        settings["analysis_time_limit_seconds"] = str(seconds)
        assert mod.analysis_time_limit_seconds() == seconds

    return mod, set_limit


def test_raised_setting_gives_a_progressing_analysis_more_time(small_limits, tmp_path):
    mod, set_limit = small_limits
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    set_limit(1)
    worker = mod.run_file_analysis(fakes.describe_process, media)["pid"]
    set_limit(5)  # above the old default: 3 s of work now completes
    result = mod.run_file_analysis(fakes.progressing, media)
    assert isinstance(result, loudness.AnalysisResult)
    assert result.duration_ms >= fakes.PROGRESS_SECONDS * 1000 * 0.5
    # The setting travels with each request: the pooled worker was not restarted.
    assert mod.run_file_analysis(fakes.describe_process, media)["pid"] == worker


def test_lowered_setting_stops_a_progressing_analysis_at_the_soft_deadline(small_limits, tmp_path):
    mod, set_limit = small_limits
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    set_limit(1)  # below the old default: 1.8 s of work no longer fits
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        mod.run_file_analysis(fakes.progressing, media, work_seconds=1.8)
    error = raised.value
    assert error.category == "analysis_timeout"
    assert error.error_type == "ProfileAnalysisTimeout"  # the soft deadline, not the kill
    assert "limit_seconds" not in error.diagnostics
    assert mod.run_file_analysis(fakes.describe_process, media)["pid"] == error.pid


def test_lowered_setting_kills_a_hung_analysis_after_the_headroom(small_limits, tmp_path):
    mod, set_limit = small_limits
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    set_limit(1)
    with pytest.raises(iso.IsolatedAnalysisError) as raised:
        mod.run_file_analysis(fakes.sleep_forever, media)
    error = raised.value
    assert error.category == "analysis_timeout" and error.error_type is None
    assert error.diagnostics["limit_seconds"] == 1
    assert 1 + 1 <= error.diagnostics["elapsed_seconds"] < 1 + 1 + 3
    assert _gone(error.pid)


def test_with_deadline_only_for_targets_that_take_one():
    def takes(path, deadline_seconds=900):
        return deadline_seconds

    assert iso.with_deadline(takes, {}, 60) == {"deadline_seconds": 60}
    assert iso.with_deadline(takes, {"deadline_seconds": 5}, 60) == {"deadline_seconds": 5}
    assert iso.with_deadline(lambda path: None, {"a": 1}, 60) == {"a": 1}
    assert iso.with_deadline(takes, {}, None) == {}
    assert iso.run_isolated(takes, "unused", limit_seconds=42) == 42  # in-process path too
    # The real analyzers take it, so production files get the configured deadline.
    assert iso.with_deadline(loudness.analyze_file, {}, 1200) == {"deadline_seconds": 1200}
    assert iso.with_deadline(edge.analyze_edge_file, EDGE_ARGS, 1200) == {
        **EDGE_ARGS, "deadline_seconds": 1200}


def test_worker_pipes_above_fd_setsize_work(tmp_path):
    """A busy host process can hold 1024+ descriptors; select() fails there."""
    resource = pytest.importorskip("resource")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if max(soft, hard if hard != resource.RLIM_INFINITY else 4096) < 1200:
        pytest.skip("RLIMIT_NOFILE too low")
    if soft < 1200:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1200, hard))
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    held = []
    iso.shutdown()
    try:
        while not held or held[-1] < 1030:
            held.append(os.open(os.devnull, os.O_RDONLY))
        assert iso.run_isolated(fakes.describe_process, media, limit_seconds=60)["pid"] != os.getpid()
        with pytest.raises(iso.IsolatedAnalysisError) as raised:
            iso.run_isolated(fakes.sleep_forever, media, **FAST)
        assert raised.value.category == "analysis_timeout"
    finally:
        iso.shutdown()
        for fd in held:
            os.close(fd)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


# ---------------------------------------------------------------- diagnostics


def test_safe_diagnostics_allowlist():
    safe = iso.safe_diagnostics({
        "container": "mov,mp4,m4a,3gp,3g2,mj2", "codec": "/music/Artist/secret.flac",
        "channel_layout": "5.1(side)", "sample_rate": 44100, "byte_size": -1,
        "decode_position_seconds": float("nan"), "error_type": "flac\n",
        "title": "Song Title", "path": "/music/a.flac", "signal": "SIGSEGV",
    })
    assert safe == {"container": "mov,mp4,m4a,3gp,3g2,mj2", "channel_layout": "5.1(side)",
                    "sample_rate": 44100, "signal": "SIGSEGV"}
    assert iso.safe_diagnostics({"url": "http://x"}) is None


def test_edge_failure_reason_is_compact_and_safe(tmp_path):
    error = iso.IsolatedAnalysisError("media_error", "InvalidDataError", {
        "container": "flac", "codec": "flac", "sample_rate": 44100,
        "channel_layout": "2 channels", "byte_size": 358855,
        "decode_position_seconds": 1.1493, "decoded_frames": 50688, "phase": "decode",
        "error_type": "InvalidDataError",
    })
    reason = iso.edge_failure_reason(error)
    assert reason == ("edge-media-error fmt=flac codec=flac sr=44100 layout=2_channels "
                      "bytes=358855 pos_s=1.149 frames=50688 phase=decode err=InvalidDataError")
    assert len(reason) <= iso.EDGE_REASON_MAX
    assert iso.edge_failure_reason(ValueError(str(tmp_path))) == "edge-analysis-unavailable"
    assert iso.edge_failure_reason(iso.IsolatedAnalysisError("analysis_crash")).startswith(
        "edge-analysis-crash")


@pytest.mark.parametrize("raw,expected", [
    (None, 900), ("", 900), ("abc", 900), (0, 900), (-5, 900),
    ("120", 120), (5, 60), (10 ** 9, 86400), (1800, 1800),
])
def test_time_limit_setting(monkeypatch, raw, expected):
    mod = load_plugin()
    monkeypatch.setattr(
        mod, "get_setting",
        lambda key, default=None: raw if key == "analysis_time_limit_seconds" else default,
    )
    assert mod.analysis_time_limit_seconds() == expected


def test_public_failure_reasons_are_the_1_2_5_vocabulary():
    assert publication.public_failure_reason("media_error") == "unsupported_media"
    assert publication.public_failure_reason("analysis_crash") == "analysis_error"
    for code in publication.SAFE_FAILURES - {"media_error", "analysis_crash"}:
        assert publication.public_failure_reason(code) == code
    assert publication.public_failure_reason(None) is None


# ---------------------------------------------------------------- end to end


def _seed(db, *tracks):
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status)
                VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')""",
            (SOURCE, SERVER),
        )
        for track in tracks:
            cur.execute(
                f"""INSERT INTO {P}catalog_tracks
                    (catalog_instance_id, published_generation, track_id, title,
                     metadata_fp, media_fp, analysis_eligible, payload,
                     first_seen_at, last_seen_at)
                    VALUES (%s, 1, %s, %s, 'metadata', 'rev-a', TRUE, '{{}}'::jsonb,
                            now(), now())""",
                (SOURCE, track, track),
            )
    db.commit()


def _row(db, track):
    with db.cursor() as cur:
        cur.execute(
            f"""SELECT status, last_error, retry_category, retry_count,
                       retry_after IS NOT NULL, failure_diagnostics
                  FROM {P}source_profiles
                 WHERE catalog_instance_id=%s AND track_id=%s""",
            (SOURCE, track),
        )
        row = cur.fetchone()
    db.commit()
    return row


def _eligible(mod, track):
    return track in mod.find_backfill_ids(25, catalog_instance_id=SOURCE, server_id=SERVER)


def _make_due(db, track):
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}source_profiles SET retry_after=now()-interval '1 second' "
            "WHERE catalog_instance_id=%s AND track_id=%s",
            (SOURCE, track),
        )
    db.commit()


@pytest.fixture
def task_db(migrated_db, monkeypatch):
    mod = load_plugin()
    _seed(migrated_db, "track-a", "track-b")
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "_schedule_edge_upgrade", lambda *a, **k: None)
    monkeypatch.setattr(
        mod, "resolve_profile_source",
        lambda **_: {"catalog_instance_id": SOURCE, "server_id": SERVER},
    )
    return migrated_db


def _serve_file(mod, monkeypatch, path):
    monkeypatch.setattr(mod, "load_track_file", lambda *a, **k: {
        "file_path": str(path), "media_signature": "catalog-media:rev-a", "cleanup_path": None,
    })


def test_task_records_a_hard_limit_kill_as_analysis_timeout(task_db, monkeypatch, tmp_path):
    mod = load_plugin()
    media = tmp_path / SECRET / "hung.flac"
    media.parent.mkdir()
    media.write_bytes(b"x" * 2048)
    _serve_file(mod, monkeypatch, media)
    monkeypatch.setattr(mod, "analyze_file", fakes.sleep_forever)
    monkeypatch.setattr(mod, "analysis_time_limit_seconds", lambda: 1)
    monkeypatch.setattr(iso, "HARD_LIMIT_HEADROOM_SECONDS", 0)
    monkeypatch.setattr(iso, "TERM_GRACE_SECONDS", 1)
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]

    outcome = mod.analyze_one_track("track-a", SOURCE, SERVER, token)

    assert outcome == {"track_id": "track-a", "status": "failed"}
    status, last_error, category, count, cooling, diagnostics = _row(task_db, "track-a")
    assert (status, last_error, category, count, cooling) == (
        "failed", "analysis_timeout", "analysis_timeout", 1, True)
    assert diagnostics["category"] == "analysis_timeout"
    assert diagnostics["limit_seconds"] == 1 and diagnostics["byte_size"] == 2048
    _no_secret(diagnostics)
    # Transient: retried once the cooldown is due, and the wake-up sees it.
    assert mod.next_profile_retry_at(SOURCE, db=task_db) is not None
    assert not _eligible(mod, "track-a")
    _make_due(task_db, "track-a")
    assert _eligible(mod, "track-a")


def test_task_records_a_real_decoder_error_as_media_error(task_db, monkeypatch, tmp_path):
    mod = load_plugin()
    media = _corrupt(_flac(tmp_path / SECRET))
    _serve_file(mod, monkeypatch, media)
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]

    assert mod.analyze_one_track("track-a", SOURCE, SERVER, token)["status"] == "failed"

    status, last_error, category, count, cooling, diagnostics = _row(task_db, "track-a")
    assert (status, last_error, category, count, cooling) == (
        "failed", "media_error", "media_error", 1, False)
    assert {key: diagnostics[key] for key in (
        "container", "codec", "sample_rate", "channel_layout", "byte_size", "phase")} == {
        "container": "flac", "codec": "flac", "sample_rate": 44100,
        "channel_layout": "stereo", "byte_size": media.stat().st_size, "phase": "decode",
    }
    assert diagnostics["decode_position_seconds"] > 0
    _no_secret(diagnostics)
    # A revision failure: not retried for the same media...
    _make_due(task_db, "track-a")
    assert not _eligible(mod, "track-a")
    # ...and clients still see the 1.2.5 reason for a decoder rejection.
    response = plugin_client(mod).get("/api/profiles?ids=track-a")
    assert response.get_json()["failed"] == [{"track_id": "track-a", "reason": "unsupported_media"}]
    # A later success clears the diagnostics with the retry state.
    good = _flac(tmp_path / "good")
    _serve_file(mod, monkeypatch, good)
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]
    assert mod.analyze_one_track("track-a", SOURCE, SERVER, token)["status"] == "ready"
    assert _row(task_db, "track-a") == ("ready", None, None, 0, False, None)


def test_task_records_a_child_crash_as_analysis_crash(task_db, monkeypatch, tmp_path):
    mod = load_plugin()
    media = tmp_path / "crash.flac"
    media.write_bytes(b"x")
    _serve_file(mod, monkeypatch, media)
    monkeypatch.setattr(mod, "analyze_file", fakes.die_by_signal)
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]

    assert mod.analyze_one_track("track-a", SOURCE, SERVER, token)["status"] == "failed"

    status, last_error, category, count, cooling, diagnostics = _row(task_db, "track-a")
    assert (status, category, count, cooling) == ("failed", "analysis_crash", 1, True)
    assert diagnostics["signal"] == "SIGSEGV"
    response = plugin_client(mod).get("/api/profiles?ids=track-a")
    assert response.get_json()["failed"] == [{"track_id": "track-a", "reason": "analysis_error"}]
    _make_due(task_db, "track-a")
    assert _eligible(mod, "track-a")


@pytest.mark.parametrize("limit,status,category", [
    (5, "ready", None),               # raised: more time than the old default
    (1, "failed", "analysis_timeout"),  # lowered: stopped by the soft deadline
])
def test_task_soft_deadline_follows_the_setting(task_db, small_limits, monkeypatch, tmp_path,
                                                limit, status, category):
    mod, set_limit = small_limits
    media = tmp_path / "a.flac"
    media.write_bytes(b"x")
    _serve_file(mod, monkeypatch, media)
    monkeypatch.setattr(mod, "analyze_file", fakes.progressing)
    set_limit(limit)
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]

    assert mod.analyze_one_track("track-a", SOURCE, SERVER, token)["status"] == status

    row = _row(task_db, "track-a")
    assert (row[0], row[2]) == (status, category)
    if category:
        assert row[5]["error_type"] == "ProfileAnalysisTimeout"
    else:
        assert row[5] is None


@pytest.mark.parametrize("code", sorted(publication.SAFE_FAILURES))
def test_retry_selection_sql_agrees_with_the_python_categories(task_db, code):
    """SQL selects a due failure exactly when complete_attempt armed its cooldown."""
    mod = load_plugin()
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]
    assert publication.complete_attempt(
        task_db, SOURCE, "track-a", token, object(), "failed", None,
        None, 1, 1, failure_code=code,
    )
    transient = code in publication.TRANSIENT_FAILURES
    assert _row(task_db, "track-a")[2:5] == (code, 1, transient)
    assert (mod.next_profile_retry_at(SOURCE, db=task_db) is not None) is transient
    _make_due(task_db, "track-a")
    assert _eligible(mod, "track-a") is transient


def test_edge_task_records_the_category_and_safe_diagnostics(task_db, monkeypatch, tmp_path):
    mod = load_plugin()
    media = _corrupt(_flac(tmp_path / SECRET))
    token = publication.admit_attempts(task_db, SOURCE, ["track-a"])["track-a"]
    assert publication.complete_attempt(
        task_db, SOURCE, "track-a", token, loudness.analyze_file(str(_flac(tmp_path / "ok"))),
        "ready", None, "catalog-media:rev-a", 1, 1,
    )
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    _serve_file(mod, monkeypatch, media)
    jobs, _ = store.claim_edge_jobs(task_db, SOURCE, ["track-a"])

    assert mod.analyze_edges_task(jobs, SOURCE, SERVER) == [{"track_id": "track-a", "status": "failed"}]

    with task_db.cursor() as cur:
        cur.execute(f"SELECT status, last_error FROM {P}edge_profile_jobs WHERE track_id='track-a'")
        status, reason = cur.fetchone()
    task_db.commit()
    assert status == "failed"
    assert reason.startswith("edge-media-error fmt=flac codec=flac sr=44100 layout=stereo bytes=")
    assert "err=InvalidDataError" in reason and "pos_s=" in reason
    assert SECRET not in reason and "/" not in reason

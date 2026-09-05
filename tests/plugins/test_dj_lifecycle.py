"""DJ lifecycle guarantees exercised with independent PostgreSQL sessions."""

import importlib
from dataclasses import replace
from types import SimpleNamespace
import json
import logging
import threading
import pytest
from test_dj_analysis import postgres_db
from test_lumae_analysis import load_plugin, plugin_client
from test_dj_analysis_v3 import model_output


@pytest.fixture
def lifecycle(postgres_db, monkeypatch):
    db = postgres_db
    mod = load_plugin()
    jobs = mod.dj_jobs
    with db.cursor() as cur:
        cur.execute(
            "CREATE TABLE cron (name TEXT,task_type TEXT UNIQUE,cron_expr TEXT,enabled BOOLEAN)"
        )
        cur.execute(
            "INSERT INTO plugin_lumae_analysis__catalog_sources VALUES('catalog-a')"
        )
        cur.execute(
            "INSERT INTO plugin_lumae_analysis__source_profiles VALUES('catalog-a','track-a','ready','signature-a'),('catalog-a','track-b','ready','signature-b')"
        )
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    for version in (2, 3):
        jobs.migrate(db, version)
    mod.dj_maintenance.migrate(db)
    db.commit()
    others = []

    def connect():
        import psycopg2, os

        value = psycopg2.connect(os.environ["LUMAE_POSTGRES_TEST_DSN"])
        with value.cursor() as cur:
            cur.execute(f"SET search_path TO {schema},public")
        value.commit()
        others.append(value)
        return value

    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setenv("LUMAE_DJ_WORKER", "1")
    monkeypatch.setenv("LUMAE_DJ_HOST_CONTRACT", mod.dj_maintenance.HOST_CONTRACT)
    try:
        yield SimpleNamespace(db=db, mod=mod, jobs=jobs, connect=connect)
    finally:
        for value in others:
            value.close()
        jobs.release_execution(db)


def payload(jobs, job, calibration="uncalibrated-v1", padding=""):
    value = {
        key: job[key] for key in ("catalog_instance_id", "track_id", "media_revision")
    }
    value.update(
        schema_version=job["analysis_version"],
        method=jobs.contract(job["analysis_version"]).method,
        representation_id="sha256:" + "b" * 64,
        vocal_risk={"calibration": {"cache_key": calibration}},
        padding=padding,
    )
    value["analysis_digest"] = jobs.analysis_digest(value)
    return value


@pytest.mark.parametrize("version", [2, 3])
def test_live_owner_cannot_be_reclaimed_and_cancel_fences_publication(
    lifecycle, version
):
    x = lifecycle
    other = x.connect()
    x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"])
    job = x.jobs.claim_next(x.db)
    assert x.jobs.claim_next(other) is None
    assert x.jobs.claim_next(x.db) is None
    assert not x.jobs.cancelled(x.db, job)
    assert x.db.get_transaction_status() == 0
    assert x.jobs.cancel_jobs(other, version, "catalog-a", ["track-a"]) == ["track-a"]
    assert x.jobs.cancelled(x.db, job)
    assert not x.jobs.publish(x.db, job, payload(x.jobs, job), "signature-a")


def test_connection_loss_reclaims_with_new_fencing_token(lifecycle):
    x = lifecycle
    owner = x.connect()
    x.jobs.request_jobs(x.db, 3, "catalog-a", ["track-a"])
    old = x.jobs.claim_next(owner)
    owner.close()
    current = x.jobs.claim_next(x.db)
    assert current["job_token"] != old["job_token"]
    assert current["attempts"] == 2
    assert not x.jobs.publish(
        x.db,
        dict(old, release_lock_on_finish=False),
        payload(x.jobs, old),
        "signature-a",
    )
    assert x.jobs.publish(x.db, current, payload(x.jobs, current), "signature-a")


@pytest.mark.parametrize("version", [2, 3])
def test_retry_backoff_terminal_cache_force_and_source_invalidation(lifecycle, version):
    x = lifecycle
    x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"])
    job = x.jobs.claim_next(x.db)
    x.jobs.finish(x.db, job, "failed", "rss_cap_exceeded")
    assert x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"])["blocked"]
    assert x.jobs.claim_next(x.db) is None
    spec = x.jobs.contract(version)
    with x.db.cursor() as cur:
        cur.execute(f"UPDATE {spec.jobs} SET next_retry_at=now()-interval '1 second'")
    x.db.commit()
    job = x.jobs.claim_next(x.db)
    assert job["attempts"] == 2
    x.jobs.finish(x.db, job, "unsupported", "source_duration_unsupported")
    assert x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"])["blocked"]
    assert x.jobs.backfill_candidates(x.db, version, "catalog-a") == ["track-b"]
    with x.db.cursor() as cur:
        cur.execute(
            "UPDATE plugin_lumae_analysis__source_profiles SET media_signature='new' WHERE track_id='track-a'"
        )
    x.db.commit()
    assert "track-a" in x.jobs.backfill_candidates(x.db, version, "catalog-a")
    assert x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"])["accepted"]
    job = x.jobs.claim_next(x.db)
    assert job["attempts"] == 1
    x.jobs.finish(x.db, job, "cancelled")
    assert x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"])["blocked"]
    assert x.jobs.request_jobs(x.db, version, "catalog-a", ["track-a"], force=True)[
        "accepted"
    ]


def test_concurrent_absent_row_requests_coalesce(lifecycle):
    x = lifecycle
    other = x.connect()
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def request(db):
        try:
            barrier.wait(3)
            results.append(x.jobs.request_jobs(db, 3, "catalog-a", ["track-a"]))
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=request, args=(other,))
    worker.start()
    request(x.db)
    worker.join(5)
    assert not worker.is_alive() and not errors
    assert sum(len(r["accepted"]) for r in results) == 1
    assert sum(len(r["queued"]) for r in results) == 1


def test_promotion_preserves_request_and_attempts(lifecycle):
    x = lifecycle
    first = x.jobs.request_jobs(x.db, 3, "catalog-a", ["track-a"])["accepted"][0]
    assert x.jobs.request_jobs(
        x.db, 3, "catalog-a", ["track-a"], priority_tier="boundary"
    )["promoted"] == ["track-a"]
    with x.db.cursor() as cur:
        cur.execute(
            f"SELECT job_token,priority,attempts FROM {x.jobs.contract(3).jobs}"
        )
        row = cur.fetchone()
    assert row == (first["job_token"], 100, 0)


def test_read_and_backfill_invalidate_producer_calibration_and_bound_bytes(lifecycle):
    x = lifecycle
    x.jobs.request_jobs(x.db, 3, "catalog-a", ["track-a", "track-b"])
    for track in ("track-a", "track-b"):
        job = x.jobs.claim_next(x.db)
        assert job["track_id"] == track
        assert x.jobs.publish(
            x.db,
            job,
            payload(x.jobs, job, padding="x" * 1500),
            "signature-" + track[-1],
        )
    state = x.jobs.read_analysis(
        x.db, 3, "catalog-a", ["track-a", "track-b"], max_bytes=2500
    )
    assert len(state["ready"]) == 1 and len(state["next_ids"]) == 1
    assert x.jobs.read_analysis(x.db, 3, "catalog-a", state["next_ids"])["ready"]
    assert not x.jobs.read_analysis(
        x.db, 3, "catalog-a", ["track-a"], calibration_key="new"
    )["ready"]
    assert "track-a" in x.jobs.backfill_candidates(
        x.db, 3, "catalog-a", calibration_key="new"
    )
    assert x.jobs.request_jobs(
        x.db, 3, "catalog-a", ["track-a"], calibration_key="new"
    )["accepted"]
    with x.db.cursor() as cur:
        cur.execute(
            f"UPDATE {x.jobs.contract(3).analyses} SET payload=jsonb_set(payload,'{{method}}','\"old\"')"
        )
    x.db.commit()
    assert not x.jobs.read_analysis(x.db, 3, "catalog-a", ["track-b"])["ready"]
    assert x.jobs.request_jobs(x.db, 3, "catalog-a", ["track-b"])["accepted"]


def make_host(x, **changes):
    service = x.mod.dj_service
    runtime = importlib.import_module("plugins.LumaeAnalysis.dj_runtime")
    raw = runtime.RawDjEvidence(
        runtime._freeze(model_output()),
        None,
        "b" * 64,
        192,
        runtime._freeze({"sample_rate": 48000, "decoded_frames": 9216000}),
        runtime._freeze({}),
    )
    values = dict(
        get_db=lambda: x.db,
        paused=lambda: False,
        capability=lambda: {"worker_available": True},
        prepare=lambda: {"worker_available": True},
        gate_open=lambda *args: True,
        resolve_source=lambda **kwargs: {"server_id": "server-a"},
        load_track=lambda *args, **kwargs: {
            "file_path": "fake",
            "media_signature": "signature-a",
            "cleanup_path": "temp",
        },
        remove_download=lambda path: None,
        model_path=lambda: "beat",
        yamnet_path=lambda: "yamnet",
        calibration=lambda: None,
        enqueue=lambda: None,
        enabled=lambda: True,
        remove_models=lambda *args: [],
        setup_lock=lambda: (x.db, True),
        release_setup_lock=lambda *args: None,
        logger=logging.getLogger("test_dj_service"),
        extract=lambda *args, **kwargs: raw,
    )
    values.update(changes)
    return service.WorkerHost(**values)


def test_worker_reuses_inference_for_both_versions_and_cleans_source(lifecycle):
    x = lifecycle
    calls = []
    clean = []
    host = make_host(x, remove_download=clean.append)
    extract = host.extract
    host = replace(host, extract=lambda *args, **kw: calls.append(1) or extract())
    for v in (2, 3):
        x.jobs.request_jobs(x.db, v, "catalog-a", ["track-a"])
    result = x.mod.dj_service.run_worker(host)
    assert result["status"] == result["companion_status"] == "ready"
    assert len(calls) == 1 and clean == ["temp"]
    assert not x.jobs.owns_execution(x.db)
    for v in (2, 3):
        assert x.jobs.read_analysis(x.db, v, "catalog-a", ["track-a"])["ready"]


def test_reconcile_retries_failed_dispatch_and_profile_deferral(lifecycle):
    x = lifecycle
    calls = []

    def enqueue():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("queue down")

    host = make_host(x, enqueue=enqueue, gate_open=lambda *args: False)
    result, status = x.mod.dj_service.request_analysis(
        host, 3, {"catalog_instance_id": "catalog-a"}, ["track-a"]
    )
    assert status == 202 and result["accepted"] == ["track-a"] and result["deferred"]
    assert x.mod.dj_service.reconcile(host)["status"] == "waiting"
    with x.db.cursor() as cur:
        cur.execute(
            f"UPDATE {x.mod.table('dj_control')} SET next_dispatch_at=now()-interval '1 second'"
        )
    x.db.commit()
    assert x.mod.dj_service.reconcile(host)["status"] == "queued"
    assert x.mod.dj_service.run_worker(host)["reason"] == "profile_work_pending"
    assert x.mod.dj_service.reconcile(host)["status"] == "queued"
    assert (
        x.mod.dj_service.run_worker(replace(host, gate_open=lambda *args: True))[
            "status"
        ]
        == "ready"
    )


def test_reconcile_wakes_setup_without_tracks(lifecycle):
    x = lifecycle
    calls = []
    host = make_host(
        x,
        capability=lambda: {"worker_available": False},
        enqueue=lambda: calls.append(1),
    )
    assert x.mod.dj_service.reconcile(host)["status"] == "queued"
    assert calls == [1]


def test_remove_models_waits_for_analysis_and_runs_on_worker(lifecycle):
    x = lifecycle
    other = x.connect()
    removed = []
    x.jobs.request_jobs(x.db, 3, "catalog-a", ["track-a"])
    job = x.jobs.claim_next(other)
    token = x.mod.dj_maintenance.request_removal(x.db)
    host = make_host(
        x,
        enabled=lambda: False,
        remove_models=lambda *paths: removed.append(paths) or ["beat", "yamnet"],
    )
    assert x.mod.dj_service.run_worker(host)["reason"] == "worker_busy"
    assert not removed and x.jobs.cancelled(other, job)
    x.jobs.finish(other, job, "cancelled")
    assert x.mod.dj_service.run_worker(host)["status"] == "models_removed"
    assert removed == [("beat", "yamnet")]
    assert x.mod.dj_maintenance.state(x.db)["status"] == "complete"
    with x.db.cursor() as cur:
        cur.execute(
            "SELECT enabled FROM cron WHERE task_type=%s",
            (x.mod.dj_maintenance.TASK_TYPE,),
        )
        assert cur.fetchone() == (False,)


def test_v3_cancel_route_updates_running_database_job(lifecycle, monkeypatch):
    x = lifecycle
    other = x.connect()
    x.jobs.request_jobs(x.db, 3, "catalog-a", ["track-a"])
    job = x.jobs.claim_next(other)
    monkeypatch.setattr(
        x.mod,
        "resolve_profile_source",
        lambda **kw: {"catalog_instance_id": "catalog-a"},
    )
    response = plugin_client(x.mod).post(
        "/api/dj/v3/analysis/cancel", json={"ids": ["track-a"]}
    )
    assert response.status_code == 200 and response.json["cancelled"] == ["track-a"]
    assert x.jobs.cancelled(other, job)


def test_unattested_host_never_enqueues_heavy_work(lifecycle, monkeypatch):
    x = lifecycle
    monkeypatch.setattr(
        x.mod, "read_dj_worker_capability", lambda *args: {"worker_available": True}
    )
    monkeypatch.setattr(
        x.mod,
        "enqueue_bounded",
        lambda *args, **kw: pytest.fail("unattested host must not enqueue"),
    )
    with pytest.raises(RuntimeError, match="contract"):
        x.mod._enqueue_dj_worker()


def test_polling_is_wall_time_bounded_and_failure_is_not_ignored():
    from plugins.LumaeAnalysis.dj_control import CancellationPoller

    now = [0.0]
    calls = []
    poll = CancellationPoller(
        lambda: calls.append(now[0]) or False, clock=lambda: now[0]
    )
    for _ in range(10000):
        assert not poll()
    assert len(calls) == 1
    now[0] = 0.5
    assert not poll()
    assert len(calls) == 2
    poll(force=True)
    assert len(calls) == 3

    def failed():
        raise ConnectionError("ownership database lost")

    with pytest.raises(ConnectionError):
        CancellationPoller(failed)()


def test_optional_retention_waits_30_days_and_preserves_returning_tracks(lifecycle):
    x = lifecycle
    storage = importlib.import_module("plugins.LumaeAnalysis.optional_storage")
    edge = importlib.import_module("plugins.LumaeAnalysis.edge_profile_store")
    edge.migrate_edge_profiles(x.db)
    with x.db.cursor() as cur:
        cur.execute(
            "CREATE TABLE plugin_lumae_analysis__catalog_state(catalog_instance_id TEXT,published_generation BIGINT)"
        )
        cur.execute(
            "CREATE TABLE plugin_lumae_analysis__catalog_tracks(catalog_instance_id TEXT,published_generation BIGINT,track_id TEXT,available BOOLEAN)"
        )
        cur.execute(
            "INSERT INTO plugin_lumae_analysis__catalog_state VALUES('catalog-a',1)"
        )
    storage.migrate(x.db)
    x.db.commit()
    for track in ("track-a", "track-b"):
        x.jobs.request_jobs(x.db, 3, "catalog-a", [track])
        job = x.jobs.claim_next(x.db)
        x.jobs.publish(x.db, job, payload(x.jobs, job), "signature-" + track[-1])
    assert not any(storage.prune(x.db).values())
    x.db.commit()
    with x.db.cursor() as cur:
        cur.execute(
            f"UPDATE {x.jobs.contract(3).analyses} SET orphaned_at=now()-interval '31 days'"
        )
        cur.execute(
            "INSERT INTO plugin_lumae_analysis__catalog_tracks VALUES('catalog-a',1,'track-a',TRUE)"
        )
    assert storage.prune(x.db)["dj_analyses_v3"] == 1
    x.db.commit()
    assert x.jobs.read_analysis(x.db, 3, "catalog-a", ["track-a"])["ready"]
    assert not x.jobs.read_analysis(x.db, 3, "catalog-a", ["track-b"])["ready"]


def test_model_removal_settings_only_records_worker_command(lifecycle, monkeypatch):
    x = lifecycle
    monkeypatch.setattr(x.mod, "set_setting", lambda *args: None)
    monkeypatch.setattr(
        x.mod,
        "render_settings",
        lambda message=None, error=None: message or error or "settings",
    )
    monkeypatch.setattr(
        x.mod,
        "remove_dj_models",
        lambda *args: pytest.fail("Flask cannot remove worker model files"),
    )
    monkeypatch.setattr(x.mod.dj_service, "dispatch", lambda host: False)
    response = plugin_client(x.mod).post(
        "/settings", data={"action": "remove_dj_models"}
    )
    assert response.status_code == 200
    assert x.mod.dj_maintenance.state(x.db)["status"] == "pending"
    assert "queued on the dedicated worker" in response.text

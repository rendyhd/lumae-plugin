"""P3-6 (LUM-007): retry gaps, on PostgreSQL.

* A stale transition (an attempt or occurrence abandoned, not a failed
  analysis) clears ``retry_category`` and ``failure_diagnostics``: a row that
  failed, was retried and became stale is selected again when its media
  returns, instead of being stranded with no retry and no wake. The attempt
  count is kept.
* A maintenance pause, a legacy-job migration and an aborted batch release
  their attempts without using one up.
* Every cooldown is the slot for the attempts used: 60 s, 300 s, 1800 s.
* A release locks its rows in track-ID order, as admission does, so the two
  cannot deadlock in either order.
* The scheduler predicates are shared (``profile_publication``) and select
  what the inline predicates selected before the extraction.

Every lock wait is bounded (``lock_timeout``), so a deadlock or block fails.
"""
import threading

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from pg_helpers import connect  # noqa: E402
from test_lumae_analysis import load_plugin  # noqa: E402
from test_catalog_publication_lock_postgres import _source, _wait_until_waiting  # noqa: E402
from test_database_state_postgres import PROFILE_ROWS, fixture_db  # noqa: E402,F401
from plugins.LumaeAnalysis import (  # noqa: E402
    catalog_enrichment,
    profile_publication as publication,
)
from plugins.LumaeAnalysis.profile_publication import (  # noqa: E402
    RETRY_ARMED_SQL,
    RETRY_DELAYS_SECONDS,
    RETRY_LIMIT,
    backfill_due_sql,
    scheduler_params,
)


SOURCE = "catalog-a"
SERVER = "server-a"
P = "plugin_lumae_analysis__"
LOCK_TIMEOUT = "15s"


def _bound(connection):
    with connection.cursor() as cur:
        cur.execute(f"SET lock_timeout='{LOCK_TIMEOUT}'")
        cur.execute("SET statement_timeout='60s'")
    connection.commit()
    return connection


@pytest.fixture
def db(migrated_db, monkeypatch):
    """One active, complete source; the plugin reads it through ``get_db``."""
    _bound(migrated_db)
    with migrated_db.cursor() as cur:
        _source(cur, SOURCE, SERVER, generation=1)
        catalog_enrichment._profile_stream_state(cur, SOURCE, for_update=True)
    migrated_db.commit()
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    return migrated_db


def _sql(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else None
    db.commit()
    return rows


def _track(db, track, fp="rev-a", available=True):
    _sql(db, f"""INSERT INTO {P}catalog_tracks
                 (catalog_instance_id, published_generation, track_id, title, metadata_fp,
                  media_fp, analysis_eligible, available, payload, first_seen_at,
                  last_seen_at)
                 VALUES (%s, 1, %s, %s, 'meta', %s, TRUE, %s, '{{}}'::jsonb, now(), now())
                 ON CONFLICT (catalog_instance_id, published_generation, track_id)
                 DO UPDATE SET media_fp=EXCLUDED.media_fp, available=EXCLUDED.available""",
         (SOURCE, track, track, fp, available))


def _row(db, track):
    return _sql(db, f"""SELECT status, retry_category, retry_count, retry_after IS NOT NULL,
                               failure_diagnostics
                          FROM {P}source_profiles
                         WHERE catalog_instance_id=%s AND track_id=%s""",
                (SOURCE, track))[0]


def _cooldown(db, track):
    """Seconds from the last release/failure to ``retry_after``."""
    return _sql(db, f"""SELECT round(extract(epoch FROM retry_after - analyzed_at))
                          FROM {P}source_profiles
                         WHERE catalog_instance_id=%s AND track_id=%s""",
                (SOURCE, track))[0][0]


def _make_due(db, track):
    _sql(db, f"UPDATE {P}source_profiles SET retry_after=now()-interval '1 second' "
             "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track))


def _eligible(track):
    return track in load_plugin().find_backfill_ids(
        25, catalog_instance_id=SOURCE, server_id=SERVER)


class _Result:
    sample_rate, duration_ms, ref_lufs = 48000, 1234, -12.5
    start_ramp_blob, end_ramp_blob = b"wave", b"tail"


def _complete(db, track, token, status="ready"):
    return publication.complete_attempt(
        db, SOURCE, track, token, _Result(), status, None, "catalog-media:rev-a", 1, 1,
        failure_code=None if status == "ready" else "analysis_error",
        diagnostics=None if status == "ready" else {"stage": "decode"},
    )


# ---------------------------------------------------------------------------
# 1. A stale transition does not strand a retried failure
# ---------------------------------------------------------------------------
def _missing_fingerprint(db, token):
    _track(db, "stuck", fp=None)
    assert not _complete(db, "stuck", token)
    _track(db, "stuck", fp="rev-a")


def _media_changed_and_back(db, _token):
    _track(db, "stuck", fp="rev-b")
    with db.cursor() as cur:
        assert publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [("track", "stuck", "upsert")]) == 1
    db.commit()
    _track(db, "stuck", fp="rev-a")


def _epoch_rebase(db, _token):
    with db.cursor() as cur:
        publication.invalidate_catalog_changes(cur, SOURCE, 1, [], full_reconcile=True)
    db.commit()


def _orphaned_and_back(db, _token):
    _track(db, "stuck", available=False)
    assert publication.withdraw_orphaned_profiles(db, SOURCE) == ["stuck"]
    _track(db, "stuck", available=True)


@pytest.mark.parametrize("transition", [
    _missing_fingerprint, _media_changed_and_back, _epoch_rebase, _orphaned_and_back,
], ids=lambda transition: transition.__name__.strip("_"))
def test_a_stale_transition_requeues_a_retried_failure(db, transition):
    """The integrity probe "stranded retry", inverted: published, failed
    once (a transient category, diagnostics), due, admitted, then abandoned
    by a stale transition while the media stays the same."""
    mod = load_plugin()
    _track(db, "stuck")
    _track(db, "control")
    assert _complete(db, "stuck", publication.admit_attempts(db, SOURCE, ["stuck"])["stuck"])
    assert _complete(db, "stuck", publication.admit_attempts(db, SOURCE, ["stuck"])["stuck"],
                     status="failed")
    assert _row(db, "stuck") == ("failed", "analysis_error", 1, True, {"stage": "decode"})
    _make_due(db, "stuck")
    assert _eligible("stuck")
    token = publication.admit_attempts(db, SOURCE, ["stuck"])["stuck"]
    transition(db, token)
    # Stale, no category or diagnostics left; the used attempt still counts.
    assert _row(db, "stuck")[:3] == ("stale", None, 1)
    assert _row(db, "stuck")[4] is None
    assert _eligible("stuck") and _eligible("control")
    # Nothing to wake for: it is due now.
    assert mod.next_profile_retry_at(SOURCE, db=db) is None
    # The next failure uses the second attempt, with its own cooldown.
    token = publication.admit_attempts(db, SOURCE, ["stuck"])["stuck"]
    assert _complete(db, "stuck", token, status="failed")
    assert _row(db, "stuck")[:4] == ("failed", "analysis_error", 2, True)
    assert _cooldown(db, "stuck") == RETRY_DELAYS_SECONDS[1]
    assert not _eligible("stuck")


# ---------------------------------------------------------------------------
# 2. A release that never tried the analysis uses no attempt
# ---------------------------------------------------------------------------
def _paused_batch(mod, monkeypatch, ids, tokens):
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    mod.analyze_tracks_task(ids, SOURCE, SERVER, "background", tokens)


def _paused_track(mod, monkeypatch, ids, tokens):
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    for track in ids:
        mod.analyze_one_track(track, SOURCE, SERVER, tokens[track])


def _paused_during_batch(mod, monkeypatch, ids, tokens):
    checks = iter([False] + [True] * 10)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: next(checks))
    monkeypatch.setattr(mod, "heartbeat_profile_backfill", lambda *_args, **_kwargs: None)
    result = mod.analyze_tracks_task(ids, SOURCE, SERVER, "background", tokens)
    assert result["attempted"] == 0


def _legacy_job(mod, monkeypatch, ids, tokens):
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    batch_size = mod.MAX_BACKFILL_BATCH_SIZE
    monkeypatch.setattr(mod, "MAX_BACKFILL_BATCH_SIZE", 1)
    mod.analyze_tracks_task(ids, SOURCE, SERVER, "background", tokens)
    monkeypatch.setattr(mod, "MAX_BACKFILL_BATCH_SIZE", batch_size)


@pytest.mark.parametrize("release", [
    _paused_batch, _paused_track, _paused_during_batch, _legacy_job,
], ids=lambda release: release.__name__.strip("_"))
def test_a_release_without_an_analysis_keeps_the_retry_budget(db, monkeypatch, release):
    """The integrity probe "pause burns attempts", inverted: more releases
    than RETRY_LIMIT, and every row is still retried after its cooldown."""
    mod = load_plugin()
    ids = ["p1", "p2"]
    for track in ids:
        _track(db, track)
    for _ in range(RETRY_LIMIT + 1):
        tokens = publication.admit_attempts(db, SOURCE, ids)
        release(mod, monkeypatch, ids, tokens)
        for track in ids:
            assert _row(db, track)[:4] == ("stale", "queue_unavailable", 0, True)
            assert _cooldown(db, track) == RETRY_DELAYS_SECONDS[0]
            _make_due(db, track)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    assert all(_eligible(track) for track in ids)


def test_a_failed_enqueue_still_uses_an_attempt(db):
    _track(db, "q")
    for used in range(1, RETRY_LIMIT + 1):
        tokens = publication.admit_attempts(db, SOURCE, ["q"])
        load_plugin().release_pending(["q"], catalog_instance_id=SOURCE, tokens=tokens)
        assert _row(db, "q")[:4] == ("stale", "queue_unavailable", used, used < RETRY_LIMIT)
        _make_due(db, "q")
    assert not _eligible("q")


# ---------------------------------------------------------------------------
# 3. Cooldown slots
# ---------------------------------------------------------------------------
def _admitted_with(db, track, used):
    _track(db, track)
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    _sql(db, f"UPDATE {P}source_profiles SET retry_count=%s "
             "WHERE catalog_instance_id=%s AND track_id=%s", (used, SOURCE, track))
    return token


@pytest.mark.parametrize("used, count_failure, slot", [
    (0, True, 0), (1, True, 1), (0, False, 0), (1, False, 1), (2, False, 2),
])
def test_a_release_cools_down_for_the_slot_of_its_attempt_count(
    db, used, count_failure, slot,
):
    assert RETRY_DELAYS_SECONDS == (60, 300, 1800)
    token = _admitted_with(db, "c", used)
    assert publication.release_attempts(
        db, SOURCE, {"c": token}, "reason", count_failure=count_failure) == 1
    assert _row(db, "c")[:3] == ("stale", "queue_unavailable", used + int(count_failure))
    assert _cooldown(db, "c") == RETRY_DELAYS_SECONDS[slot]


def test_the_last_counted_release_arms_no_cooldown(db):
    token = _admitted_with(db, "c", RETRY_LIMIT - 1)
    assert publication.release_attempts(db, SOURCE, {"c": token}, "reason") == 1
    assert _row(db, "c")[:4] == ("stale", "queue_unavailable", RETRY_LIMIT, False)


@pytest.mark.parametrize("used", [0, 1])
def test_a_recovered_claim_cools_down_for_its_slot(db, used):
    _admitted_with(db, "c", used)
    _sql(db, f"UPDATE {P}source_profiles SET analyzed_at=now()-interval '2 days' "
             "WHERE catalog_instance_id=%s AND track_id='c'", (SOURCE,))
    assert load_plugin().recover_stale_pending_profiles(SOURCE, db=db) == 1
    assert _row(db, "c")[:4] == ("stale", "queue_unavailable", used + 1, True)
    remaining = _sql(db, f"""SELECT extract(epoch FROM retry_after - now())
                               FROM {P}source_profiles WHERE track_id='c'""")[0][0]
    assert abs(float(remaining) - RETRY_DELAYS_SECONDS[used]) < 30


# ---------------------------------------------------------------------------
# 4. Admission and release lock rows in the same order
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("first", ["admission", "release"])
def test_concurrent_admission_and_release_do_not_deadlock(
    db, second_connection, first,
):
    """A third connection holds row ``a``. Admission (a, b) and a release
    handed its tokens as {b, a} queue behind it in either order. Locking
    unsorted, the release would hold b while admission, holding a, waits
    for b: a deadlock."""
    for track in ("a", "b"):
        _track(db, track)
    old = publication.admit_attempts(db, SOURCE, ["a", "b"])
    admitter = _bound(second_connection)
    releaser = _bound(connect(_sql(db, "SELECT current_schema()")[0][0]))
    holder = _bound(connect(_sql(db, "SELECT current_schema()")[0][0]))
    outcome = {}

    def admit():
        try:
            outcome["tokens"] = publication.admit_attempts(admitter, SOURCE, ["a", "b"])
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    def release():
        try:
            outcome["released"] = publication.release_attempts(
                releaser, SOURCE, {"b": old["b"], "a": old["a"]}, "reason")
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    workers = {
        "admission": (threading.Thread(target=admit), admitter.get_backend_pid()),
        "release": (threading.Thread(target=release), releaser.get_backend_pid()),
    }
    try:
        with holder.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {P}source_profiles WHERE catalog_instance_id=%s "
                        "AND track_id='a' FOR UPDATE", (SOURCE,))
        for name in [first] + [name for name in workers if name != first]:
            worker, pid = workers[name]
            worker.start()
            _wait_until_waiting(pid)
    finally:
        holder.commit()
        for worker, _pid in workers.values():
            worker.join(30)
        for connection in (releaser, holder):
            connection.rollback()
            connection.close()
    assert not any(worker.is_alive() for worker, _pid in workers.values())
    assert "error" not in outcome, outcome
    rows = {track: _row(db, track) for track in ("a", "b")}
    tokens = dict(_sql(db, f"SELECT track_id, attempt_token FROM {P}source_profiles "
                           "WHERE catalog_instance_id=%s", (SOURCE,)))
    # Admission always wins the rows in the end, with its own tokens.
    assert tokens == outcome["tokens"]
    assert all(row[0] == "pending" for row in rows.values())
    # The release changed both rows or neither, and counted what it changed.
    assert outcome["released"] in (0, 2)
    assert {row[2] for row in rows.values()} == {outcome["released"] // 2}
    assert outcome["released"] == (2 if first == "release" else 0)


# ---------------------------------------------------------------------------
# 5. The shared predicates select what the inline predicates selected
# ---------------------------------------------------------------------------
# fetch_backfill_rows(catalog_instance_id=...)'s WHERE clause and
# next_profile_retry_at's retry term before P3-6, verbatim but for named
# parameters (the literal ``3`` was RETRY_LIMIT).
_INLINE_DUE = """
    COALESCE(p.status, '') NOT IN
        ('pending', 'pending_interactive', 'deferred_no_media_revision')
    OR (p.status='deferred_no_media_revision'
        AND NULLIF(t.media_fp, '') IS NOT NULL)
   )
   AND (
        p.track_id IS NULL
        OR p.analyzer_ver IS NULL
        OR p.analyzer_ver < %(analyzer_version)s
        OR (p.status='stale' AND (
            p.retry_category IS NULL
            OR (p.retry_category='queue_unavailable'
                AND p.retry_count < 3 AND p.retry_after <= now())
            OR (NULLIF(t.media_fp, '') IS NOT NULL
                AND p.retry_media_signature IS NOT NULL
                AND p.retry_media_signature IS DISTINCT FROM
                    ('catalog-media:' || t.media_fp))
        ))
        OR (p.status='deferred_no_media_revision'
            AND NULLIF(t.media_fp, '') IS NOT NULL)
        OR (
            p.status='ready'
            AND NULLIF(t.media_fp, '') IS NOT NULL
            AND p.media_signature IS DISTINCT FROM
                ('catalog-media:' || COALESCE(t.media_fp, ''))
        )
        OR (p.status IN ('failed', 'skipped_no_file')
            AND (
                (NULLIF(t.media_fp, '') IS NOT NULL
                 AND p.retry_media_signature IS NOT NULL
                 AND p.retry_media_signature IS DISTINCT FROM
                     ('catalog-media:' || t.media_fp))
                OR (p.retry_analyzer_ver IS NOT NULL
                    AND p.retry_analyzer_ver < %(analyzer_version)s)
                OR (p.retry_profile_schema_ver IS NOT NULL
                    AND p.retry_profile_schema_ver < %(schema_version)s)
                OR (p.retry_category IS NULL AND p.retry_count=0)
                OR (p.retry_category = ANY(%(transient)s)
                    AND p.retry_count < %(retry_limit)s AND p.retry_after <= now())
            ))
   )"""
_INLINE_DUE = "((" + _INLINE_DUE + ")"
_INLINE_ARMED = """(p.status IN ('failed', 'skipped_no_file', 'stale')
                    AND p.retry_category = ANY(%(transient)s)
                    AND p.retry_count < 3 AND p.retry_after IS NOT NULL)"""


def _selected(db, predicate):
    return [row[0] for row in _sql(db, f"""
        SELECT t.track_id
          FROM {P}catalog_tracks t
          LEFT JOIN {P}source_profiles p
            ON p.catalog_instance_id=t.catalog_instance_id AND p.track_id=t.track_id
         WHERE t.catalog_instance_id=%(source)s AND t.published_generation=1
           AND t.available=TRUE AND t.analysis_eligible=TRUE AND {predicate}
         ORDER BY t.track_id""", {**scheduler_params(1, 1), "source": SOURCE})]


def test_the_shared_predicates_select_what_the_inline_ones_did(fixture_db, monkeypatch):
    """Over P3-10's fixture (every status, category, cooldown and limit)."""
    assert RETRY_LIMIT == 3
    due = sorted(track for track, _t, _p, _pub, state in PROFILE_ROWS if state == "due")
    assert _selected(fixture_db, _INLINE_DUE) == due
    assert _selected(fixture_db, backfill_due_sql()) == due
    assert _selected(fixture_db, backfill_due_sql("statement_timestamp()")) == due
    armed = _selected(fixture_db, _INLINE_ARMED)
    assert armed and _selected(fixture_db, RETRY_ARMED_SQL) == armed
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: fixture_db)
    assert [row[0] for row in mod.fetch_backfill_rows(
        10**6, catalog_instance_id=SOURCE, server_id=SERVER)] == due
    fixture_db.rollback()

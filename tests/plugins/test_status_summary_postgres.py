"""P2-1 (LUM-011): committed status summary and read-only GET routes.

Readiness used to count catalogue coverage and analysis links on every
``/api/catalog/health``, ``/settings/status`` and ``/database-state`` request.
It now reads counts committed by the work that publishes what they describe
(``status_model``). The pre-P2-1 live-query implementation is kept below,
verbatim, as the oracle.

* Equivalence: at every state of the fixture, reached the way the plugin
  reaches it (an AudioMuse or catalogue change, then the catalogue refresh or
  projection that consumes it), the summary-derived readiness equals the
  oracle's: every field, blockers and admission included. The summary readers
  are forbidden to fall back to a live count while this is checked.
* The counts are committed with the catalogue publication and the projection
  publication; a no-change projection writes nothing.
* GET routes perform zero writes when nothing changed: no INSERT, UPDATE,
  DELETE or row lock, no commit, and no transaction ID assigned.
* The migration is additive and idempotent.
"""

import re
import sys
import types
from datetime import datetime, timezone

import pytest

psycopg2 = pytest.importorskip("psycopg2")
import psycopg2.extensions  # noqa: E402

from test_lumae_analysis import (  # noqa: E402  (installs the plugin.api host stub)
    RefreshBridge,
    load_plugin,
    plugin_api_module,
    plugin_client,
)

from plugin.api import table  # noqa: E402
from plugins.LumaeAnalysis import (  # noqa: E402
    catalog,
    catalog_analysis,
    catalog_readiness,
    provider_identity_rekey,
    status_model,
)
from plugins.LumaeAnalysis.catalog_readiness import (  # noqa: E402  (unchanged helpers)
    ANALYSIS_SEMANTIC_CONTRACTS,
    _catalogue_admission,
    _detected_core_version,
    _policy_blockers,
    _stream_admission,
    _task_evidence,
)
from plugins.LumaeAnalysis.core_v3 import AudioMuseV3Adapter  # noqa: E402


P = "plugin_lumae_analysis__"
SERVER = "server-a"
COMPATIBILITY = types.SimpleNamespace(core_version="v3.1.1", adapter="v3_registry")

HOST_SCHEMA = """
CREATE TABLE music_servers (server_id TEXT PRIMARY KEY, name TEXT);
INSERT INTO music_servers VALUES ('server-a', 'Main');
CREATE TABLE score (item_id TEXT PRIMARY KEY, title TEXT, author TEXT, album TEXT,
    album_artist TEXT, tempo REAL, key TEXT, scale TEXT, mood_vector TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(), energy REAL, other_features TEXT,
    year INTEGER, rating INTEGER, file_path TEXT, duration DOUBLE PRECISION);
CREATE TABLE embedding (item_id TEXT PRIMARY KEY, embedding BYTEA);
CREATE TABLE clap_embedding (item_id TEXT PRIMARY KEY, embedding BYTEA);
CREATE TABLE track_server_map (item_id TEXT NOT NULL, server_id TEXT NOT NULL,
    provider_track_id TEXT NOT NULL, match_tier TEXT, file_path TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE UNIQUE INDEX ON track_server_map (server_id, provider_track_id);
CREATE TABLE chromaprint (server_id TEXT NOT NULL, provider_track_id TEXT NOT NULL,
    fingerprint BYTEA, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (server_id, provider_track_id));
CREATE TABLE map_projection_data (index_name VARCHAR(255) PRIMARY KEY,
    projection_data BYTEA NOT NULL, id_map_json TEXT NOT NULL,
    embedding_dimension INTEGER NOT NULL);
CREATE TABLE task_status (task_id TEXT PRIMARY KEY, parent_task_id TEXT,
    task_type TEXT, status TEXT, end_time DOUBLE PRECISION, details JSONB,
    timestamp TIMESTAMP);
"""


def t(name):
    return table(name)


class V3Adapter(AudioMuseV3Adapter):
    # No provider module: the provider-identity guard (an upstream ping) is
    # skipped, as in scripts/perf/proj_bench.py.
    provider_module = None


class Bridge(RefreshBridge):
    """The catalogue test bridge, bound to an AudioMuse 3 core."""

    core = V3Adapter()


# ---------------------------------------------------------------------------
# Oracle: pre-P2-1 catalog_readiness (phase/1-stop-the-bleeding e2f0c12),
# _coverage, _link_coverage and v3_release_readiness verbatim. Its helpers
# (_task_evidence, _policy_blockers, _stream_admission, _catalogue_admission)
# are unchanged by P2-1 and imported above.
# ---------------------------------------------------------------------------


def _coverage(db, source):
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT count(*) AS eligible_tracks,
                   count(m.provider_track_id) AS mapped_tracks,
                   count(CASE WHEN cp.fingerprint IS NOT NULL THEN 1 END)
                     AS fingerprinted_tracks,
                   max(EXTRACT(EPOCH FROM cp.updated_at)) AS latest_chromaprint_at
              FROM {t("catalog_tracks")} ct
              LEFT JOIN track_server_map m
                ON m.server_id=%s AND m.provider_track_id=ct.track_id
              LEFT JOIN chromaprint cp
                ON cp.server_id=m.server_id
               AND cp.provider_track_id=m.provider_track_id
             WHERE ct.catalog_instance_id=%s
               AND ct.published_generation=%s
               AND ct.available=TRUE
               AND ct.analysis_eligible=TRUE
            """,
            (
                source["server_id"],
                source["catalog_instance_id"],
                source["catalog"]["generation"],
            ),
        )
        row = cur.fetchone() or (0, 0, 0, None)
    finally:
        cur.close()
    eligible = int(row[0] or 0)
    mapped = int(row[1] or 0)
    fingerprinted = int(row[2] or 0)
    latest_chromaprint_at = float(row[3]) if row[3] is not None else None
    return {
        "eligible_track_count": eligible,
        "mapped_track_count": mapped,
        "missing_mapping_count": max(0, eligible - mapped),
        "chromaprint_track_count": fingerprinted,
        "chromaprint_missing_count": max(0, mapped - fingerprinted),
        "chromaprint_coverage": fingerprinted / mapped if mapped else 0.0,
        "latest_chromaprint_at_unix": latest_chromaprint_at,
    }


def _link_coverage(db, source, eligible_track_count=0):
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT count(*) FILTER (WHERE status='ready') AS ready_links,
                   count(*) FILTER (WHERE status='pending') AS pending_links,
                   count(*) FILTER (
                     WHERE status='suspect'
                        OR review_state IN ('needs_repair', 'needs_review')
                   ) AS suspect_links,
                   count(*) FILTER (WHERE status='missing') AS missing_links,
                   count(*) FILTER (
                     WHERE status='ready' AND evidence_complete=TRUE
                   ) AS verified_links,
                   count(*) FILTER (
                     WHERE status='ready' AND evidence_complete=FALSE
                   ) AS provisional_links
              FROM {t("track_analysis_links")}
             WHERE catalog_instance_id=%s AND projection_generation=%s
            """,
            (
                source["catalog_instance_id"],
                source.get("analysis", {}).get("generation", 0),
            ),
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0, 0)
    finally:
        cur.close()
    eligible = int(eligible_track_count or 0)
    ready = int(row[0] or 0)
    return {
        "ready_link_count": ready,
        "pending_link_count": int(row[1] or 0),
        "suspect_link_count": int(row[2] or 0),
        "missing_link_count": int(row[3] or 0),
        "evidence_complete_link_count": int(row[4] or 0),
        "verified_link_count": int(row[4] or 0),
        "provisional_link_count": int(row[5] or 0),
        "usable_analysis_coverage": ready / eligible if eligible else 0.0,
    }


def v3_release_readiness(
    db,
    compatibility,
    source,
    policy,
    acknowledgement=None,
    requested_mode=None,
):
    """Return automatic, source-scoped stream admission.

    The obsolete acknowledgement arguments remain accepted for one plugin
    release so older callers do not break. They never influence admission.
    """

    del acknowledgement, requested_mode
    detected_core_version = _detected_core_version(compatibility)
    base = {
        # These legacy fields remain additive for older app releases. They now
        # report the detected version rather than an allow-listed release.
        "qualified_core_version": detected_core_version,
        "detected_core_version": detected_core_version,
        "applicable": compatibility.adapter == "v3_registry",
        "status": "not_applicable",
        "ready": compatibility.adapter != "v3_registry",
        "fully_verified": compatibility.adapter != "v3_registry",
        "analysis_sync_allowed": compatibility.adapter != "v3_registry",
        "progressive_analysis": False,
        "verification_mode": None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "blockers": [],
    }
    if compatibility.adapter != "v3_registry":
        return base

    catalog_admission = _catalogue_admission(source)
    if not catalog_admission["admitted"]:
        analysis_admission = _stream_admission(
            False,
            ANALYSIS_SEMANTIC_CONTRACTS,
            ["catalog_not_ready"],
        )
        return {
            **base,
            "status": catalog_admission["status"],
            "blockers": list(catalog_admission["blockers"]),
            "admission": {
                "catalog": catalog_admission,
                "analysis": analysis_admission,
            },
        }

    try:
        coverage = _coverage(db, source)
        link_coverage = _link_coverage(
            db,
            source,
            coverage["eligible_track_count"],
        )
    except Exception:
        analysis_admission = _stream_admission(
            False,
            ANALYSIS_SEMANTIC_CONTRACTS,
            ["readiness_unavailable"],
        )
        return {
            **base,
            "status": "readiness_unavailable",
            "blockers": ["readiness_unavailable"],
            "admission": {
                "catalog": catalog_admission,
                "analysis": analysis_admission,
            },
        }
    try:
        tasks = _task_evidence(db)
    except Exception:
        tasks = {
            "analysis_before_cleaning": None,
            "cleaning": None,
            "analysis_after_cleaning": None,
            "upgrade_sequence_complete": False,
            "diagnostics_available": False,
        }

    cleaning = tasks.get("cleaning") or {}
    cleaning_time = cleaning.get("completed_at_unix")
    latest_chromaprint_at = coverage.get("latest_chromaprint_at_unix")
    chromaprint_complete_before_cleaning = bool(
        cleaning_time is not None
        and latest_chromaprint_at is not None
        and latest_chromaprint_at <= cleaning_time
    )
    task_order_complete = tasks["upgrade_sequence_complete"]
    tasks["chromaprint_complete_before_cleaning"] = chromaprint_complete_before_cleaning
    tasks["upgrade_sequence_complete"] = bool(
        task_order_complete and chromaprint_complete_before_cleaning
    )

    blockers = _policy_blockers(policy)
    admission_blockers = list(blockers)
    if source.get("analysis", {}).get("status") != "complete":
        blockers.append("analysis_projection_incomplete")
        admission_blockers.append("analysis_projection_incomplete")
    if coverage["mapped_track_count"] == 0:
        blockers.append("no_analysis_mappings")
        admission_blockers.append("no_analysis_mappings")
    else:
        if coverage["missing_mapping_count"]:
            blockers.append("analysis_mapping_incomplete")
        if coverage["chromaprint_missing_count"]:
            blockers.append("chromaprint_backfill_incomplete")
    if link_coverage["pending_link_count"]:
        blockers.append("analysis_links_pending")
    if link_coverage["suspect_link_count"]:
        blockers.append("analysis_links_need_repair")
    if link_coverage["missing_link_count"]:
        blockers.append("analysis_links_missing")
    if link_coverage["provisional_link_count"]:
        blockers.append("provisional_links_remaining")
    if (
        link_coverage["verified_link_count"] != coverage["eligible_track_count"]
        and not any(
            code in blockers
            for code in (
                "no_analysis_mappings",
                "analysis_mapping_incomplete",
                "analysis_links_pending",
                "analysis_links_need_repair",
                "analysis_links_missing",
                "provisional_links_remaining",
            )
        )
    ):
        blockers.append("sonic_evidence_incomplete")
    if policy.get("per_link_chromaprint_evidence_available") is not True:
        blockers.append("per_link_evidence_unavailable")
        admission_blockers.append("per_link_evidence_unavailable")

    analysis_sync_allowed = not admission_blockers
    ready = analysis_sync_allowed and not blockers
    if ready:
        status = "ready"
    elif analysis_sync_allowed:
        status = "progressive"
    else:
        status = "repair_incomplete"
    analysis_admission = _stream_admission(
        analysis_sync_allowed,
        ANALYSIS_SEMANTIC_CONTRACTS,
        admission_blockers,
        status,
    )
    return {
        **base,
        **coverage,
        **link_coverage,
        "status": status,
        "ready": ready,
        "fully_verified": ready,
        "analysis_sync_allowed": analysis_sync_allowed,
        "progressive_analysis": analysis_sync_allowed and not ready,
        "verification_mode": "automatic" if analysis_sync_allowed else None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "task_evidence": tasks,
        "blockers": blockers,
        "admission": {
            "catalog": catalog_admission,
            "analysis": analysis_admission,
        },
    }


oracle_readiness = v3_release_readiness


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def track_id(index):
    return f"tr{index:04d}"


def item_id(index):
    return f"it{index:04d}"


# Tracks 5 and 7 are further occurrences of the recordings of 4 and 6.
PARTNER = {5: 4, 7: 6}


def catalogue(indices, podcasts=(), ids=track_id):
    """Provider tracks; podcasts are published but not analysis eligible."""
    return {
        "tracks": [
            {
                "id": ids(index),
                "title": f"Song {PARTNER.get(index, index)}",
                "artist": f"Artist {PARTNER.get(index, index) % 5}",
                "duration": 180 + PARTNER.get(index, index) % 3,
                **({"type": "podcast"} if index in podcasts else {}),
            }
            for index in sorted(indices)
        ]
    }


def fingerprint_time(index):
    # Distinct and sub-second; tracks from 16 on are fingerprinted after the
    # cleaning task below.
    return f"2026-09-01 10:00:{index % 60:02d}.{123457 + index:06d}"


CLEANING_END = datetime(2026, 9, 1, 10, 0, 15, 500000, tzinfo=timezone.utc).timestamp()


class Host:
    """AudioMuse's side: mappings, analysed items and Chromaprints."""

    def __init__(self, db):
        self.db = db
        with db.cursor() as cur:
            cur.execute(HOST_SCHEMA)
            cur.execute(
                """INSERT INTO task_status (task_id, parent_task_id, task_type, status,
                       end_time, details, timestamp)
                   VALUES ('analysis-1', NULL, 'main_analysis', 'SUCCESS', %s, '{}', now()),
                          ('cleaning-1', NULL, 'cleaning', 'SUCCESS', %s, '{}', now()),
                          ('analysis-2', NULL, 'main_analysis', 'SUCCESS', %s, '{}', now())""",
                (CLEANING_END - 100, CLEANING_END, CLEANING_END + 100),
            )
        db.commit()

    def map(self, mapping, ids=track_id):
        """``{track index: item index}``; several tracks may share one item."""
        with self.db.cursor() as cur:
            for track, item in mapping.items():
                cur.execute(
                    """INSERT INTO track_server_map (item_id, server_id, provider_track_id,
                           match_tier) VALUES (%s, %s, %s, 'fp_4')
                       ON CONFLICT (server_id, provider_track_id)
                       DO UPDATE SET item_id=EXCLUDED.item_id""",
                    (item_id(item), SERVER, ids(track)),
                )
        self.db.commit()

    def analyse(self, items):
        with self.db.cursor() as cur:
            for item in items:
                cur.execute(
                    """INSERT INTO score (item_id, tempo, key, scale, mood_vector, energy,
                           other_features) VALUES (%s, %s, 'C', 'major', 'happy:0.5', 0.1,
                           'danceable:0.4') ON CONFLICT DO NOTHING""",
                    (item_id(item), 100.0 + item),
                )
                cur.execute(
                    "INSERT INTO embedding VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (item_id(item), bytes(range(8))),
                )
        self.db.commit()

    def fingerprint(self, prints, ids=track_id):
        """``{track index: fingerprint bytes}``."""
        with self.db.cursor() as cur:
            for track, value in prints.items():
                cur.execute(
                    """INSERT INTO chromaprint (server_id, provider_track_id, fingerprint,
                           updated_at) VALUES (%s, %s, %s, %s)
                       ON CONFLICT (server_id, provider_track_id)
                       DO UPDATE SET fingerprint=EXCLUDED.fingerprint,
                                     updated_at=EXCLUDED.updated_at""",
                    (SERVER, ids(track), value, fingerprint_time(track)),
                )
        self.db.commit()


@pytest.fixture
def evidence_config(monkeypatch):
    config = sys.modules["plugin.api"].config

    def apply(**values):
        defaults = {
            "CATALOGUE_ID_SCHEME_VERSION": 4,
            "CHROMAPRINT_COLLECTION_ENABLED": True,
            "CHROMAPRINT_GATE_ENABLED": True,
            "DUPLICATE_DISTANCE_THRESHOLD_COSINE": 0.01,
            "DURATION_TOLERANCE_SECONDS": 1.0,
            "CHROMAPRINT_MATCH_THRESHOLD": 0.8,
            "CHROMAPRINT_MIN_OVERLAP": 10,
        }
        defaults.update(values)
        for key, value in defaults.items():
            monkeypatch.setattr(config, key, value, raising=False)

    apply()
    # Deterministic stand-in for the host's Chromaprint comparison.
    chromaprint = types.ModuleType("tasks.chromaprint")
    chromaprint.chromaprints_agree = lambda left, right: left[:4] == right[:4]
    monkeypatch.setitem(sys.modules, "tasks", sys.modules.get("tasks") or types.ModuleType("tasks"))
    monkeypatch.setitem(sys.modules, "tasks.chromaprint", chromaprint)
    return apply


@pytest.fixture
def host(migrated_db, evidence_config):
    return Host(migrated_db)


def publish(db, indices, podcasts=(), ids=track_id):
    """The catalogue refresh: publication, or its no-change path."""
    return catalog.refresh_catalog(
        SERVER, db=db, bridge=Bridge(catalogue(indices, podcasts, ids))
    )


def project(db):
    return catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())


def current_source(db):
    source = catalog.resolve_catalog_source(db, server_id=SERVER)[0]
    db.commit()
    return source


def no_live_fallback(*_args, **_kwargs):
    raise AssertionError("readiness counted the library instead of reading the summary")


def assert_equivalent(db, monkeypatch):
    """Summary-derived readiness equals the live-query oracle, field for field."""
    source = current_source(db)
    policy = catalog_analysis.dedup_policy()
    expected = oracle_readiness(db, COMPATIBILITY, source, policy)
    db.commit()
    with monkeypatch.context() as patch:
        patch.setattr(status_model, "_live_row", no_live_fallback)
        actual = catalog_readiness.v3_release_readiness(db, COMPATIBILITY, source, policy)
    db.commit()
    assert expected["status"] != "readiness_unavailable"
    assert actual["blockers"] == expected["blockers"]
    assert actual["admission"] == expected["admission"]
    assert actual == expected
    return actual


def live_link_counts(db, generation):
    with db.cursor() as cur:
        cur.execute(status_model.link_counts_sql(), (current_source(db)["catalog_instance_id"], generation))
        row = cur.fetchone()
    db.commit()
    return tuple(row)


def stored_summary(db):
    with db.cursor() as cur:
        cur.execute(
            f"""SELECT a.summary_generation, a.link_count, a.ready_link_count,
                       a.pending_link_count, a.suspect_link_count, a.missing_link_count,
                       a.evidence_complete_link_count, s.coverage_generation,
                       s.eligible_track_count, s.mapped_track_count,
                       s.fingerprinted_track_count, s.latest_chromaprint_at
                  FROM {P}analysis_state a
                  LEFT JOIN {P}status_summary s USING (catalog_instance_id)"""
        )
        row = cur.fetchone()
    db.commit()
    return row


# ---------------------------------------------------------------------------
# Equivalence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("progressive", [True, False], ids=["progressive", "gate_off"])
def test_summary_readiness_equals_live_readiness_across_states(
    migrated_db, host, evidence_config, monkeypatch, progressive
):
    evidence_config(CHROMAPRINT_GATE_ENABLED=progressive)
    db = migrated_db
    tracks = set(range(24))
    podcasts = {22, 23}
    seen = set()

    # 1. A published catalogue; AudioMuse has analysed nothing yet.
    publish(db, tracks, podcasts)
    seen.add(assert_equivalent(db, monkeypatch)["status"])

    # 2. An AudioMuse run maps 0-15 (4 and 5 share one item, and so do 6 and
    # 7), analyses the items of 0-11 and fingerprints 0-3 and 6. The run ends
    # with the catalogue refresh (no catalogue change), then the projection.
    host.map({index: index for index in range(16)} | PARTNER)
    host.analyse(range(12))
    host.fingerprint({0: b"AAAA0", 1: b"BBBB1", 2: b"CCCC2", 3: b"DDDD3", 6: b"FFFF6"})
    assert publish(db, tracks, podcasts)["change_reason"] == "no_change"
    seen.add(assert_equivalent(db, monkeypatch)["status"])
    assert project(db)["generation"] == 1
    seen.add(assert_equivalent(db, monkeypatch)["status"])

    # 3. The pending items are analysed; 4/5 agree and 6/7 disagree.
    host.analyse(range(12, 16))
    host.fingerprint({4: b"EEEE4", 5: b"EEEE5", 7: b"GGGG7"})
    publish(db, tracks, podcasts)
    assert project(db)["changes"] > 0
    seen.add(assert_equivalent(db, monkeypatch)["status"])

    # 4. Only Chromaprint coverage changes: no link changes, so the
    # projection publishes nothing; the catalogue refresh recounts coverage.
    host.fingerprint({8: b"HHHH8", 9: b"IIII9"})
    assert publish(db, tracks, podcasts)["change_reason"] == "no_change"
    seen.add(assert_equivalent(db, monkeypatch)["status"])
    assert project(db).get("unchanged") is True
    seen.add(assert_equivalent(db, monkeypatch)["status"])

    # 4b. Chromaprint changes again and only a projection follows, as after a
    # provider-migration recheck. It publishes nothing, and the coverage it
    # consumed is recounted after it.
    host.fingerprint({10: b"JJJJ10", 11: b"KKKK11", 12: b"LLLL12"})
    assert project(db).get("unchanged") is True
    seen.add(assert_equivalent(db, monkeypatch)["status"])

    # 5. The catalogue changes: 2 is removed and 24 added (unmapped). Checked
    # after the publication and again after the projection.
    tracks = (tracks - {2}) | {24}
    assert publish(db, tracks, podcasts)["generation"] == 2
    seen.add(assert_equivalent(db, monkeypatch)["status"])
    project(db)
    seen.add(assert_equivalent(db, monkeypatch)["status"])

    # 6. Everything eligible is mapped, analysed and fingerprinted, and the
    # 6/7 group agrees: fully verified when the gate is on.
    eligible = sorted(tracks - podcasts)
    host.map({index: index for index in eligible if index not in (5, 7)})
    host.analyse(eligible)
    host.fingerprint({index: f"{index:04d}".encode() for index in eligible})
    host.fingerprint({4: b"EEEE4", 5: b"EEEE5", 6: b"GGGG6", 7: b"GGGG7"})
    publish(db, tracks, podcasts)
    project(db)
    final = assert_equivalent(db, monkeypatch)
    seen.add(final["status"])

    if progressive:
        assert final["status"] == "ready" and final["blockers"] == []
        assert {"repair_incomplete", "progressive", "ready"} <= seen
    else:
        assert final["status"] == "repair_incomplete"
        assert "chromaprint_gate_disabled" in final["blockers"]


def test_counts_are_committed_with_each_publication(migrated_db, host, monkeypatch):
    db = migrated_db
    tracks = set(range(10))
    first = publish(db, tracks)
    row = stored_summary(db)
    # A new source has no projection yet (generation 0, no links); its
    # catalogue coverage is counted right after the publication commits.
    assert row[:7] == (0, 0, 0, 0, 0, 0, 0)
    assert row[7:] == (first["generation"], 10, 0, 0, None)

    host.map({index: index for index in range(6)})
    host.analyse(range(4))
    host.fingerprint({0: b"AAAA", 1: b"BBBB"})
    result = project(db)
    links = live_link_counts(db, result["generation"])
    row = stored_summary(db)
    assert row[:7] == (result["generation"], *links)
    assert links == (10, 4, 2, 0, 4, 4)
    assert row[7:11] == (first["generation"], 10, 6, 2)

    # A no-change projection executes no write statement at all.
    statements = []

    class Recording(psycopg2.extensions.cursor):
        def execute(self, query, vars=None):
            statements.append(" ".join(str(query).split()))
            return super().execute(query, vars)

    db.cursor_factory = Recording
    try:
        assert project(db).get("unchanged") is True
    finally:
        db.cursor_factory = None
    # (The projection still locks its state row; that is not a write.)
    assert statements and not [
        sql for sql in statements if WRITE.search(re.sub(r"\bFOR UPDATE\b", "", sql))
    ]
    assert stored_summary(db) == row

    # The catalogue publication commits the new generation's coverage.
    second = publish(db, tracks | {10})
    assert second["generation"] == first["generation"] + 1
    assert stored_summary(db)[7:11] == (second["generation"], 11, 6, 2)


def test_readers_fall_back_to_a_live_count_for_an_unsummarized_generation(
    migrated_db, host, monkeypatch
):
    """A generation published without a summary (an older worker draining
    during an upgrade) is still answered exactly, by the same aggregate."""
    db = migrated_db
    publish(db, set(range(6)))
    host.map({0: 0, 1: 1})
    host.analyse([0])
    project(db)
    # Summaries of older generations, whose counts no longer apply.
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}analysis_state SET summary_generation=summary_generation - 1, "
            "ready_link_count=999, missing_link_count=999"
        )
        cur.execute(
            f"UPDATE {P}status_summary SET coverage_generation=coverage_generation - 1, "
            "eligible_track_count=999, mapped_track_count=999"
        )
    db.commit()
    source = current_source(db)
    policy = catalog_analysis.dedup_policy()
    expected = oracle_readiness(db, COMPATIBILITY, source, policy)
    db.commit()
    actual = catalog_readiness.v3_release_readiness(db, COMPATIBILITY, source, policy)
    assert actual == expected
    assert actual["eligible_track_count"] == 6 and actual["ready_link_count"] == 1

    # No summary at all is answered the same way.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}analysis_state SET summary_generation=NULL")
        cur.execute(f"DELETE FROM {P}status_summary")
    db.commit()
    assert catalog_readiness.v3_release_readiness(db, COMPATIBILITY, source, policy) == expected

    # Coverage counted for another server (a source summarized as the v2
    # legacy-default before its rebind) does not describe this one.
    status_model.refresh_status_summary(db, source["catalog_instance_id"], V3Adapter())
    assert status_model.read_summary(db, source)["coverage"] is not None
    db.commit()
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}status_summary SET coverage_server_id='legacy-default', "
            "mapped_track_count=0"
        )
    db.commit()
    assert status_model.read_summary(db, source)["coverage"] is None
    db.commit()
    assert catalog_readiness.v3_release_readiness(db, COMPATIBILITY, source, policy) == expected


# ---------------------------------------------------------------------------
# Read-only GET routes
# ---------------------------------------------------------------------------


WRITE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|ALTER|CREATE|DROP|LOCK|GRANT|VACUUM|ANALYZE)\b",
    re.IGNORECASE,
)


class RecordingConnection:
    """The host's request connection, recording statements and commits."""

    def __init__(self, db):
        self.db = db
        self.statements = []
        self.commits = 0
        recorder = self

        class Cursor(psycopg2.extensions.cursor):
            def execute(self, query, vars=None):
                recorder.statements.append(" ".join(str(query).split()))
                return super().execute(query, vars)

            def executemany(self, query, vars_list):
                recorder.statements.append(" ".join(str(query).split()))
                return super().executemany(query, vars_list)

        self.cursor_class = Cursor

    def cursor(self, *args, **kwargs):
        kwargs.setdefault("cursor_factory", self.cursor_class)
        return self.db.cursor(*args, **kwargs)

    def commit(self):
        self.commits += 1
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def __getattr__(self, name):
        return getattr(self.db, name)

    def reset(self):
        self.statements.clear()
        self.commits = 0

    def writes(self):
        return [sql for sql in self.statements if WRITE.search(sql)]

    def transaction_id(self):
        with self.db.cursor() as cur:
            cur.execute("SELECT txid_current_if_assigned()")
            return cur.fetchone()[0]


@pytest.fixture
def v3_routes(migrated_db, host, monkeypatch):
    """The plugin routes on an AudioMuse 3 host whose Navidrome answers pings."""
    mod = load_plugin()
    ping = {"version": "0.53.3 (abc1234)", "calls": 0}

    def navidrome_request(endpoint, timeout=None):
        ping["calls"] += 1
        return {"status": "ok", "serverVersion": ping["version"], "type": "navidrome"}

    navidrome = types.ModuleType("tasks.mediaserver.navidrome")
    navidrome._navidrome_request = navidrome_request
    mediaserver = types.ModuleType("tasks.mediaserver")
    mediaserver.navidrome = navidrome
    monkeypatch.setitem(sys.modules, "tasks.mediaserver", mediaserver)
    monkeypatch.setitem(sys.modules, "tasks.mediaserver.navidrome", navidrome)
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", "v3.1.1")
    monkeypatch.setattr(plugin_api_module, "active_server_id", lambda: SERVER, raising=False)
    monkeypatch.setattr(plugin_api_module, "use_server", lambda _server_id: _NoContext(), raising=False)
    monkeypatch.setattr(
        plugin_api_module,
        "list_servers",
        lambda: [{"server_id": SERVER, "name": "Main", "server_type": "navidrome"}],
        raising=False,
    )
    connection = RecordingConnection(migrated_db)
    from plugins.LumaeAnalysis import catalog_enrichment, collection_manager, reconcile

    for module in (mod, catalog_enrichment, collection_manager, reconcile):
        monkeypatch.setattr(module, "get_db", lambda: connection)

    tracks = set(range(12))
    publish(migrated_db, tracks)
    host.map({index: index for index in range(8)})
    host.analyse(range(6))
    host.fingerprint({0: b"AAAA", 1: b"BBBB"})
    publish(migrated_db, tracks)
    project(migrated_db)
    mod.refresh_status_snapshot(migrated_db)
    return types.SimpleNamespace(
        mod=mod, client=plugin_client(mod), connection=connection, ping=ping, db=migrated_db
    )


class _NoContext:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _get(routes, path):
    routes.db.commit()
    routes.connection.reset()
    response = routes.client.get(path)
    assert response.status_code == 200, response.get_data(as_text=True)[:500]
    return response


def test_get_routes_perform_zero_writes_when_nothing_changed(v3_routes, monkeypatch):
    routes = v3_routes
    # The first observation of the provider records it, and the route
    # commits that change itself.
    _get(routes, "/api/catalog/health")
    assert routes.connection.writes()
    assert routes.connection.commits == 1

    for path in ("/api/catalog/health", "/api/health", "/settings/status", "/database-state"):
        calls = routes.ping["calls"]
        response = _get(routes, path)
        assert routes.connection.writes() == [], path
        assert routes.connection.commits == 0, path
        assert routes.connection.transaction_id() is None, path
        if path == "/api/catalog/health":
            # The provider is still pinged on every request.
            assert routes.ping["calls"] == calls + 1
            readiness = response.get_json()["servers"][0]["v3_readiness"]
            source = current_source(routes.db)
            expected = oracle_readiness(
                routes.db, COMPATIBILITY, {**source, "supported": True},
                catalog_analysis.dedup_policy(),
            )
            routes.db.commit()
            assert readiness["blockers"] == expected["blockers"]
            assert readiness["ready_link_count"] == expected["ready_link_count"]


def test_readiness_routes_read_the_summary_not_the_library(v3_routes):
    routes = v3_routes
    _get(routes, "/api/catalog/health")
    for path in ("/api/catalog/health", "/settings/status"):
        _get(routes, path)
        scans = [
            sql for sql in routes.connection.statements
            if f"FROM {P}catalog_tracks" in sql or f"FROM {P}track_analysis_links" in sql
        ]
        assert scans == [], path
        assert any(f"{P}status_summary" in sql for sql in routes.connection.statements)


def test_a_changed_provider_version_is_written_once_and_committed_by_the_route(v3_routes):
    routes = v3_routes
    _get(routes, "/api/catalog/health")
    _get(routes, "/api/catalog/health")
    assert routes.connection.writes() == []

    routes.ping["version"] = "0.53.4 (def5678)"
    _get(routes, "/api/catalog/health")
    assert len(routes.connection.writes()) == 1
    assert routes.connection.commits == 1
    with routes.db.cursor() as cur:
        cur.execute(
            f"SELECT state, current_provider_version, previous_provider_version "
            f"FROM {P}provider_identity_transitions"
        )
        assert cur.fetchone() == ("normal", "0.53.4 (def5678)", "0.53.3 (abc1234)")
    routes.db.commit()

    _get(routes, "/api/catalog/health")
    assert routes.connection.writes() == []
    assert routes.connection.commits == 0


def test_catalog_health_leaves_an_applied_transitions_audiomuse_health_to_the_cron(
    v3_routes, monkeypatch
):
    routes = v3_routes
    routes.ping["version"] = "0.64.0"
    with routes.db.cursor() as cur:
        cur.execute(
            f"""UPDATE {P}provider_identity_transitions
                   SET state='applied', transition_id='transition-a',
                       current_provider_version='0.64.0',
                       last_checked_provider_version='0.64.0',
                       detection_reason='provider_ids_checked',
                       required_action='run_audiomuse_provider_migration',
                       audiomuse_health='repair_required'"""
        )
    routes.db.commit()
    inspections = []
    # Recorded, not raised: the route logs and swallows provider errors.
    monkeypatch.setattr(
        provider_identity_rekey,
        "refresh_audiomuse_health",
        lambda *args, **_kwargs: inspections.append(args[1:3]) or "ready",
    )

    response = _get(routes, "/api/catalog/health")

    assert inspections == []
    assert routes.connection.writes() == []
    assert routes.connection.commits == 0
    server = response.get_json()["servers"][0]
    assert server["provider_identity_transition"]["state"] == "applied"
    assert server["audiomuse_health"] == "repair_required"

    # The provider_identity_recheck cron is what inspects it.
    source = current_source(routes.db)["catalog_instance_id"]
    monkeypatch.setattr(routes.mod, "projection_reconcile_required", lambda *_args: False)
    monkeypatch.setattr(
        routes.mod, "analysis_projection_task", lambda server_id=None: {"status": "complete"}
    )
    monkeypatch.setattr(routes.mod, "refresh_audiomuse_health", provider_identity_rekey.refresh_audiomuse_health)
    result = routes.mod.provider_identity_recheck_task()
    assert inspections == [(source, SERVER)]
    assert result["results"][0]["projection_processed"] is True


# ---------------------------------------------------------------------------
# Waveform counts behind /settings/status
# ---------------------------------------------------------------------------


def _profiles(db, rows):
    source = current_source(db)["catalog_instance_id"]
    with db.cursor() as cur:
        cur.execute(f"SELECT track_id, media_fp FROM {P}catalog_tracks")
        media = dict(cur.fetchall())
        for index, status in rows.items():
            cur.execute(
                f"""INSERT INTO {P}source_profiles
                       (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                        start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                        media_signature, status)
                    VALUES (%s, %s, 48000, 1000, -14, '\\x00', '\\x00', 1, 1, %s, %s)
                    ON CONFLICT (catalog_instance_id, track_id)
                    DO UPDATE SET status=EXCLUDED.status""",
                (source, track_id(index), f"catalog-media:{media[track_id(index)]}", status),
            )
    db.commit()


def test_settings_waveform_counts_are_a_committed_snapshot(migrated_db, host, monkeypatch):
    mod = load_plugin()
    db = migrated_db
    monkeypatch.setattr(mod, "get_db", lambda: db)
    publish(db, set(range(8)), podcasts={7})
    _profiles(db, {0: "ready", 1: "ready", 2: "pending", 3: "failed"})
    source = current_source(db)

    def live():
        counts = mod.analysis_status_counts(
            catalog_instance_id=source["catalog_instance_id"], server_id=SERVER
        )
        db.commit()
        return counts

    assert mod.refresh_status_snapshot(db) == 1
    snapshot = status_model.profile_counts(db, current_source(db))
    db.commit()
    assert {key: value for key, value in snapshot.items() if key != "counted_at"} == live()
    assert live() == {
        "total_with_files": 7, "ready_current": 2, "pending": 1, "failed": 1,
        "skipped": 0, "needs_analysis": 3,
    }

    # Within the minimum age a snapshot is kept; after it, it is replaced.
    _profiles(db, {2: "ready"})
    assert mod.refresh_status_snapshot(db, source["catalog_instance_id"], min_age_seconds=3600) == 0
    assert status_model.profile_counts(db, current_source(db))["ready_current"] == 2
    db.commit()
    assert mod.refresh_status_snapshot(db, source["catalog_instance_id"]) == 1
    snapshot = status_model.profile_counts(db, current_source(db))
    db.commit()
    assert {key: value for key, value in snapshot.items() if key != "counted_at"} == live()

    # A new catalogue generation invalidates the snapshot until it is taken
    # again: the reader then counts live.
    publish(db, set(range(9)), podcasts={7})
    assert status_model.profile_counts(db, current_source(db)) is None
    db.commit()
    assert mod.refresh_status_snapshot(db) == 1
    snapshot = status_model.profile_counts(db, current_source(db))
    db.commit()
    assert {key: value for key, value in snapshot.items() if key != "counted_at"} == live()


def test_the_watchdog_tick_and_catalogue_cron_refresh_the_waveform_snapshot(
    migrated_db, host, monkeypatch
):
    mod = load_plugin()
    db = migrated_db
    monkeypatch.setattr(mod, "get_db", lambda: db)
    publish(db, set(range(4)))
    _profiles(db, {0: "ready"})
    for name in (
        "next_settled_analysis_run", "next_preparation_run", "next_relationship_run",
        "next_profile_backfill_run",
    ):
        monkeypatch.setattr(mod, name, lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_reconcile_schedule", lambda *_args, **_kwargs: None)

    assert mod.catalog_reconcile_task()["status"] == "current"
    assert status_model.profile_counts(db, current_source(db))["ready_current"] == 1
    db.commit()

    _profiles(db, {1: "ready"})
    monkeypatch.setattr(mod, "refresh_catalog", lambda server_id=None: publish(db, set(range(4))))
    monkeypatch.setattr(mod, "get_core_adapter", lambda: types.SimpleNamespace(
        mode="v3_registry", active_server_id=lambda: SERVER,
        list_servers=lambda: [{"server_id": SERVER}],
    ))
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(mod, "next_profile_retry_at", lambda *_args, **_kwargs: None)
    mod.catalog_refresh_task(SERVER)
    assert status_model.profile_counts(db, current_source(db))["ready_current"] == 2
    db.commit()


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------


SUMMARY_COLUMNS = tuple(name for name, _kind in status_model.ANALYSIS_SUMMARY_COLUMNS)


def _schema(db):
    with db.cursor() as cur:
        cur.execute(
            """SELECT table_name, column_name, data_type, is_nullable, column_default
                 FROM information_schema.columns WHERE table_schema=current_schema()
                ORDER BY table_name, column_name"""
        )
        columns = cur.fetchall()
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname=current_schema() ORDER BY indexname"
        )
        indexes = cur.fetchall()
    db.commit()
    return columns, indexes


def _state_rows(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT * FROM {P}analysis_state ORDER BY catalog_instance_id")
        state = cur.fetchall()
        cur.execute(f"SELECT * FROM {P}status_summary ORDER BY catalog_instance_id")
        summary = cur.fetchall()
    db.commit()
    return state, summary


def test_migration_is_additive_and_idempotent(migrated_db, host, run_plugin_migration):
    db = migrated_db
    publish(db, set(range(8)))
    host.map({index: index for index in range(5)})
    host.analyse(range(3))
    host.fingerprint({0: b"AAAA"})
    project(db)
    load_plugin().refresh_status_snapshot(db)
    schema = _schema(db)
    rows = _state_rows(db)

    # Re-running the install changes nothing and alters no table.
    statements = []

    class Recording(psycopg2.extensions.cursor):
        def execute(self, query, vars=None):
            statements.append(" ".join(str(query).split()))
            return super().execute(query, vars)

    db.cursor_factory = Recording
    try:
        run_plugin_migration(db)
    finally:
        db.cursor_factory = None
    assert _schema(db) == schema
    assert _state_rows(db) == rows
    assert not [
        sql for sql in statements
        if sql.startswith(f"ALTER TABLE {P}analysis_state") or f"{P}status_summary SET" in sql
    ]


def test_upgrade_adds_the_summary_and_fills_it_for_published_data(
    migrated_db, host, monkeypatch, run_plugin_migration
):
    db = migrated_db
    publish(db, set(range(8)), podcasts={6})
    host.map({index: index for index in range(5)} | {5: 4})
    host.analyse(range(4))
    host.fingerprint({0: b"AAAA", 4: b"EEEE", 5: b"EEEE"})
    project(db)
    expected = assert_equivalent(db, monkeypatch)
    # The pre-P2-1 schema: no summary columns and no summary table.
    with db.cursor() as cur:
        cur.execute(f"DROP TABLE {P}status_summary")
        cur.execute(
            f"ALTER TABLE {P}analysis_state "
            + ", ".join(f"DROP COLUMN {name}" for name in SUMMARY_COLUMNS)
        )
    db.commit()
    before = _schema(db)
    columns = ", ".join(_pre_p21_columns(before))
    with db.cursor() as cur:
        cur.execute(f"SELECT {columns} FROM {P}analysis_state")
        state_before = cur.fetchall()
    db.commit()

    run_plugin_migration(db)

    after = _schema(db)
    # Additive: every column and index survives unchanged; only the summary
    # columns and the summary table are new.
    assert set(before[0]) <= set(after[0])
    assert set(before[1]) <= set(after[1])
    added = {(row[0], row[1]) for row in set(after[0]) - set(before[0])}
    assert {(f"{P}analysis_state", name) for name in SUMMARY_COLUMNS} <= added
    assert {table_name for table_name, _column in added} == {
        f"{P}analysis_state", f"{P}status_summary"
    }
    with db.cursor() as cur:
        cur.execute(f"SELECT {columns} FROM {P}analysis_state")
        assert cur.fetchall() == state_before
    db.commit()
    # The install filled the summary for what was already published.
    assert assert_equivalent(db, monkeypatch) == expected


def _pre_p21_columns(schema):
    return [
        column for table_name, column, *_rest in schema[0]
        if table_name == f"{P}analysis_state"
    ]


# ---------------------------------------------------------------------------
# When the summaries are written
# ---------------------------------------------------------------------------


@pytest.fixture
def post_commit_probe(migrated_db, second_connection, monkeypatch):
    """Wrap the post-commit summary refresh and look at what is committed.

    From a second connection, each call records the committed link summary
    and projection generations, and whether the catalogue and projection state
    rows can be locked, i.e. nothing holds them while coverage is counted.
    """
    calls = []
    original = status_model.refresh_status_summary

    def probe(db, catalog_instance_id, adapter=None):
        record = {"source": catalog_instance_id}
        with second_connection.cursor() as cur:
            cur.execute(
                f"SELECT summary_generation, projection_generation FROM {P}analysis_state "
                "WHERE catalog_instance_id=%s",
                (catalog_instance_id,),
            )
            record["link_summary"] = cur.fetchone()
        second_connection.rollback()
        for table_name in ("catalog_state", "analysis_state"):
            try:
                with second_connection.cursor() as cur:
                    cur.execute(
                        f"SELECT 1 FROM {P}{table_name} WHERE catalog_instance_id=%s "
                        "FOR UPDATE NOWAIT",
                        (catalog_instance_id,),
                    )
                record[f"{table_name}_free"] = True
            except psycopg2.errors.LockNotAvailable:
                record[f"{table_name}_free"] = False
            second_connection.rollback()
        calls.append(record)
        return original(db, catalog_instance_id, adapter)

    for module in (status_model, catalog, catalog_analysis):
        monkeypatch.setattr(module, "refresh_status_summary", probe)
    return calls


def test_coverage_is_counted_after_every_refresh_and_projection_commits(
    migrated_db, host, post_commit_probe, monkeypatch
):
    db = migrated_db
    tracks = set(range(8))
    publish(db, tracks)  # publication
    host.map({index: index for index in range(5)})
    host.analyse(range(3))
    publish(db, tracks)  # no change
    project(db)  # publication
    host.fingerprint({0: b"AAAA"})
    assert project(db).get("unchanged") is True  # no change
    generation = current_source(db)["analysis"]["generation"]

    assert len(post_commit_probe) == 4
    for record in post_commit_probe:
        assert record["catalog_state_free"] and record["analysis_state_free"], record
    # The projection's link counts were committed with its publication, before
    # the recount that follows it.
    assert post_commit_probe[2]["link_summary"] == (generation, generation)
    assert_equivalent(db, monkeypatch)


def rekey_old_id(index):
    return f"{index + 1:032x}"


def rekey_new_id(index):
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id

    return canonicalize_navidrome_id(rekey_old_id(index)).value


def test_rekey_then_audiomuse_remap_then_unchanged_projection_equals_live(
    migrated_db, host, post_commit_probe, monkeypatch
):
    """A provider-ID rekey publishes both generations; AudioMuse migrates its
    mapping afterwards, and the recheck's projection publishes nothing."""
    db = migrated_db
    tracks = set(range(6))
    publish(db, tracks, ids=rekey_old_id)
    host.map({index: index for index in range(5)} | {4: 3}, ids=rekey_old_id)
    host.analyse(range(4))
    host.fingerprint({0: b"AAAA", 3: b"DDDD", 4: b"DDDD"}, ids=rekey_old_id)
    project(db)
    before = assert_equivalent(db, monkeypatch)
    source = current_source(db)
    target = catalog.normalize_provider_catalog(catalogue(tracks, ids=rekey_new_id), "navidrome")
    target_fp = provider_identity_rekey.target_scan_fingerprint(target)
    with db.cursor() as cur:
        cur.execute(
            f"""UPDATE {P}provider_identity_transitions
                   SET transition_id='transition-a', state='transition_pending',
                       previous_provider_version='0.63.0', current_provider_version='0.64.0',
                       baseline_catalog_generation=%s, baseline_analysis_generation=%s,
                       target_fingerprint=%s, target_scan_count=2
                 WHERE catalog_instance_id=%s""",
            (
                source["catalog"]["generation"],
                source["analysis"]["generation"],
                target_fp,
                source["catalog_instance_id"],
            ),
        )
    db.commit()
    post_commit_probe.clear()

    result = provider_identity_rekey.publish_provider_identity_rekey(
        db,
        catalog_instance_id=source["catalog_instance_id"],
        server_id=SERVER,
        normalized=target,
        target_fingerprint=target_fp,
        current_provider_version="0.64.0",
        adapter=V3Adapter(),
    )

    assert result["provider_identity_transition"]["audiomuse_health"] == "migration_required"
    rekeyed = current_source(db)
    generation = rekeyed["analysis"]["generation"]
    assert generation == source["analysis"]["generation"] + 1
    # The carried generation's link counts were committed with the rekey, and
    # coverage was recounted after it (AudioMuse still maps the old IDs).
    assert post_commit_probe == [
        {
            "source": source["catalog_instance_id"],
            "link_summary": (generation, generation),
            "catalog_state_free": True,
            "analysis_state_free": True,
        }
    ]
    migrated = assert_equivalent(db, monkeypatch)
    assert migrated["mapped_track_count"] == 0
    assert "no_analysis_mappings" in migrated["blockers"]

    # AudioMuse's provider migration moves its mapping and Chromaprints.
    with db.cursor() as cur:
        for index in tracks:
            for host_table in ("track_server_map", "chromaprint"):
                cur.execute(
                    f"UPDATE {host_table} SET provider_track_id=%s "
                    "WHERE server_id=%s AND provider_track_id=%s",
                    (rekey_new_id(index), SERVER, rekey_old_id(index)),
                )
    db.commit()
    assert project(db).get("unchanged") is True
    after = assert_equivalent(db, monkeypatch)
    assert after["mapped_track_count"] == before["mapped_track_count"] == 5
    assert after["blockers"] == before["blockers"]
    assert after["status"] == before["status"]


def _coverage_row(db):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT coverage_generation, coverage_server_id, eligible_track_count, "
            f"mapped_track_count, coverage_updated_at FROM {P}status_summary"
        )
        row = cur.fetchone()
    db.commit()
    return row


def test_newer_coverage_is_never_replaced_by_an_older_count(migrated_db, host, monkeypatch):
    db = migrated_db
    publish(db, set(range(6)))
    host.map({0: 0})
    host.analyse([0])
    project(db)
    generation = current_source(db)["catalog"]["generation"]
    host.fingerprint({0: b"AAAA"})  # coverage changes, links do not

    # A newer catalogue generation's summary (a publication that committed
    # while this count ran) is kept.
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}status_summary SET coverage_generation=%s, eligible_track_count=777",
            (generation + 1,),
        )
    db.commit()
    assert project(db).get("unchanged") is True
    row = _coverage_row(db)
    assert (row[0], row[2]) == (generation + 1, 777)

    # So is a later count of the same generation.
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}status_summary SET coverage_generation=%s, eligible_track_count=777, "
            "coverage_updated_at=now() + interval '1 day'",
            (generation,),
        )
    db.commit()
    assert project(db).get("unchanged") is True
    assert _coverage_row(db)[2] == 777

    # An older count is replaced.
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}status_summary SET coverage_updated_at=now() - interval '1 day'"
        )
    db.commit()
    assert project(db).get("unchanged") is True
    assert _coverage_row(db)[:4] == (generation, SERVER, 6, 1)
    assert_equivalent(db, monkeypatch)


def test_an_existing_link_summary_is_left_alone(migrated_db, host):
    db = migrated_db
    publish(db, set(range(4)))
    host.map({0: 0})
    host.analyse([0])
    project(db)
    source = current_source(db)
    generation = source["analysis"]["generation"]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}analysis_state SET ready_link_count=555")
        status_model.persist_analysis_summary(cur, source["catalog_instance_id"], generation)
        cur.execute(f"SELECT summary_generation, ready_link_count FROM {P}analysis_state")
        assert cur.fetchone() == (generation, 555)
    db.rollback()


def test_a_newer_waveform_snapshot_is_never_replaced(migrated_db, host, monkeypatch):
    mod = load_plugin()
    db = migrated_db
    monkeypatch.setattr(mod, "get_db", lambda: db)
    publish(db, set(range(4)))
    _profiles(db, {0: "ready"})
    assert mod.refresh_status_snapshot(db) == 1
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}status_summary SET profile_ready_count=444, "
            "profile_counted_at=now() + interval '1 day'"
        )
    db.commit()
    assert mod.refresh_status_snapshot(db) == 1
    assert status_model.profile_counts(db, current_source(db))["ready_current"] == 444
    db.commit()


def test_the_watchdog_tick_summarizes_unsummarized_publications(migrated_db, host, monkeypatch):
    mod = load_plugin()
    db = migrated_db
    monkeypatch.setattr(mod, "get_db", lambda: db)
    for name in (
        "next_settled_analysis_run", "next_preparation_run", "next_relationship_run",
        "next_profile_backfill_run",
    ):
        monkeypatch.setattr(mod, name, lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_reconcile_schedule", lambda *_args, **_kwargs: None)
    publish(db, set(range(6)))
    host.map({0: 0, 1: 1})
    host.analyse([0, 1])
    project(db)
    source = current_source(db)

    # A failed post-commit refresh, or coverage counted for the v2
    # legacy-default server before the source was rebound.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}analysis_state SET summary_generation=NULL")
        cur.execute(f"UPDATE {P}status_summary SET coverage_server_id='legacy-default'")
    db.commit()
    summary = status_model.read_summary(db, source)
    db.commit()
    assert summary["links"] is None and summary["coverage"] is None

    assert mod.catalog_reconcile_task()["status"] == "current"
    summary = status_model.read_summary(db, source)
    db.commit()
    assert summary["links"] is not None and summary["coverage"] is not None
    assert_equivalent(db, monkeypatch)

    # Only the server changed; the link summary and the coverage generation
    # are current. The tick still recounts it.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}status_summary SET coverage_server_id='legacy-default'")
        cur.execute(f"SELECT summary_generation = projection_generation FROM {P}analysis_state")
        assert cur.fetchone()[0] is True
    db.commit()
    assert _coverage_row(db)[0] == source["catalog"]["generation"]
    assert mod.catalog_reconcile_task()["status"] == "current"
    assert _coverage_row(db)[1] == SERVER
    assert_equivalent(db, monkeypatch)

    # Only active sources are counted: readiness stops at a pending rebind,
    # and the tick does not count coverage that nothing reads.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_sources SET rebind_status='rebind_required'")
        cur.execute(f"DELETE FROM {P}status_summary")
    db.commit()
    statements = []

    class Recording(psycopg2.extensions.cursor):
        def execute(self, query, vars=None):
            statements.append(" ".join(str(query).split()))
            return super().execute(query, vars)

    db.cursor_factory = Recording
    try:
        assert mod.catalog_reconcile_task()["status"] == "current"
    finally:
        db.cursor_factory = None
    assert statements
    assert not [sql for sql in statements if "JOIN track_server_map" in sql]
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {P}status_summary WHERE coverage_generation IS NOT NULL")
        assert cur.fetchone()[0] == 0
    db.commit()


def test_the_watchdog_backfill_holds_no_row_lock_while_counting(
    migrated_db, host, second_connection, monkeypatch
):
    """The tick's summary backfill commits each source it wrote before it
    counts the next source or the waveform profiles (~0.25 s each at full
    scale), so no analysis_state row lock or transaction ID is held meanwhile."""
    mod = load_plugin()
    db = migrated_db
    monkeypatch.setattr(mod, "get_db", lambda: db)
    publish(db, set(range(4)))
    host.map({0: 0})
    host.analyse([0])
    project(db)
    first = current_source(db)["catalog_instance_id"]
    second = "zzzz-second-source"  # sorts after the first (a UUID)
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id,
                   provider_type, server_name, is_default, rebind_status)
                VALUES (%s, 'server-b', 'navidrome', 'B', FALSE, 'active')""",
            (second,),
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state (catalog_instance_id, provider_type,
                   current_core_server_id, published_generation, catalog_epoch, status)
                VALUES (%s, 'navidrome', 'server-b', 0, 'epoch-b', 'not_initialized')
                ON CONFLICT (catalog_instance_id) DO NOTHING""",
            (second,),
        )
        cur.execute(
            f"""INSERT INTO {P}analysis_state (catalog_instance_id, projection_generation,
                   analysis_epoch, status)
                VALUES (%s, 0, 'analysis-b', 'not_initialized')
                ON CONFLICT (catalog_instance_id) DO NOTHING""",
            (second,),
        )
        # Both sources need a link summary written by the backfill.
        cur.execute(f"UPDATE {P}analysis_state SET summary_generation=NULL")
    db.commit()

    def rows_free():
        free = {}
        for catalog_instance_id in (first, second):
            try:
                with second_connection.cursor() as cur:
                    cur.execute(
                        f"SELECT 1 FROM {P}analysis_state WHERE catalog_instance_id=%s "
                        "FOR UPDATE NOWAIT",
                        (catalog_instance_id,),
                    )
                free[catalog_instance_id] = True
            except psycopg2.errors.LockNotAvailable:
                free[catalog_instance_id] = False
            finally:
                second_connection.rollback()
        return free

    def transaction_id():
        with db.cursor() as cur:
            cur.execute("SELECT txid_current_if_assigned()")
            return cur.fetchone()[0]

    seen = []
    summarize = status_model._refresh_summaries
    count = status_model.store_profile_counts

    def probe_summarize(cur, catalog_instance_id, with_coverage):
        seen.append(("summarize", catalog_instance_id, rows_free()))
        return summarize(cur, catalog_instance_id, with_coverage)

    def probe_count(cur, catalog_instance_id, sql, params):
        seen.append(("count", catalog_instance_id, rows_free(), transaction_id()))
        return count(cur, catalog_instance_id, sql, params)

    monkeypatch.setattr(status_model, "_refresh_summaries", probe_summarize)
    monkeypatch.setattr(status_model, "store_profile_counts", probe_count)

    mod.refresh_status_snapshot(db, publications=True)

    all_free = {first: True, second: True}
    assert seen[:3] == [
        ("summarize", first, all_free),
        # The first source's link summary was written and committed.
        ("summarize", second, all_free),
        ("count", first, all_free, None),
    ]
    assert seen[3][:3] == ("count", second, all_free)
    with db.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {P}analysis_state "
            "WHERE summary_generation IS NOT DISTINCT FROM projection_generation"
        )
        assert cur.fetchone()[0] == 2
    db.commit()


# ---------------------------------------------------------------------------
# Waveform snapshot refreshes around analysis
# ---------------------------------------------------------------------------


def test_the_song_hook_refreshes_the_waveform_snapshot_at_most_every_30_s(monkeypatch, tmp_path):
    mod = load_plugin()
    refreshes = []
    audio = tmp_path / "song.flac"
    audio.write_bytes(b"audio")
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: {
        "catalog_instance_id": "catalog-a", "server_id": SERVER,
    })
    monkeypatch.setattr(mod, "record_analysis_run", lambda *_args: None)
    monkeypatch.setattr(mod, "published_profile_current", lambda *_args: False)
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "mark_pending", lambda ids, *_args, **_kwargs: {ids[0]: "token"})
    monkeypatch.setattr(mod, "catalog_media_signature", lambda *_args, **_kwargs: "catalog-media:a")
    monkeypatch.setattr(mod, "analyze_file", lambda _path: object())
    monkeypatch.setattr(mod, "upsert_profile", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(mod, "_schedule_edge_upgrade", lambda *_args: None)
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda **kwargs: refreshes.append(kwargs))

    result = mod.analyze_song_hook(
        {"item_id": "track-a", "run_id": "run-a", "audio_path": str(audio)}
    )

    assert result == {"track_id": "track-a", "status": "ready"}
    assert refreshes == [
        {
            "catalog_instance_id": "catalog-a",
            "min_age_seconds": status_model.PROFILE_COUNTS_MIN_AGE_SECONDS,
        }
    ]
    assert status_model.PROFILE_COUNTS_MIN_AGE_SECONDS == 30


@pytest.mark.parametrize("priority", ["interactive", "background"])
def test_an_interactive_analysis_refreshes_the_waveform_snapshot_at_once(monkeypatch, priority):
    mod = load_plugin()
    refreshes = []
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "heartbeat_profile_backfill", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "profile_task_disposition", lambda *_args, **_kwargs: "analyze")
    monkeypatch.setattr(
        mod, "analyze_one_track",
        lambda track_id, **_kwargs: {"track_id": track_id, "status": "ready"},
    )
    monkeypatch.setattr(mod, "finalize_preparation_if_settled", lambda *_args: None)
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda **kwargs: refreshes.append(kwargs))

    summary = mod.analyze_tracks_task(
        ["track-a"], catalog_instance_id="catalog-a", server_id=SERVER,
        priority=priority, attempt_tokens={"track-a": "token"},
    )

    assert summary["ready"] == 1
    if priority == "interactive":
        # No throttle: the operator is waiting for this result.
        assert refreshes == [{"catalog_instance_id": "catalog-a"}]
    else:
        # A background batch runs in a watchdog tick, which refreshes after it.
        assert refreshes == []

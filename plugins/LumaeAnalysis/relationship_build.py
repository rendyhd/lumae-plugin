"""Checkpointed relationship preparation with a short atomic publication.

The session advisory lock owns computation, including across checkpoint commits.
Published rows are never used as scratch space. Input generations AND epochs pin
all checkpoints; the final transaction locks and revalidates their source rows.
"""

from contextlib import contextmanager
import io
import json
import logging
import time
import uuid

import numpy as np
from psycopg2.extras import execute_values

from .catalog import CatalogScanError, canonical_json, opaque_cursor

log = logging.getLogger("lumae.relationships")
BUILD_FORMAT_VERSION = 1
CHECKPOINT_SIZE = 16


def t(name):
    from plugin.api import table
    return table(name)


class InputsChanged(CatalogScanError):
    pass


def migrate_relationship_builds(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t('relationship_builds')} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t('relationship_state')}(catalog_instance_id) ON DELETE CASCADE,
            build_id TEXT NOT NULL,
            input_identity JSONB NOT NULL,
            inputs_ready BOOLEAN NOT NULL DEFAULT FALSE,
            phase TEXT NOT NULL DEFAULT 'fingerprints',
            track_count BIGINT NOT NULL DEFAULT 0,
            album_count BIGINT NOT NULL DEFAULT 0,
            artist_count BIGINT NOT NULL DEFAULT 0,
            progress JSONB NOT NULL DEFAULT '{{}}',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t('relationship_build_entities')} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('relationship_builds')}(catalog_instance_id) ON DELETE CASCADE,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            fingerprint BYTEA NOT NULL,
            payload JSONB,
            result_fp TEXT,
            PRIMARY KEY (catalog_instance_id, entity_type, entity_id)
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t('relationship_build_tracks')} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('relationship_builds')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            album_key TEXT,
            artist_key TEXT NOT NULL,
            input_order BIGINT NOT NULL,
            input_payload JSONB NOT NULL,
            embedding BYTEA NOT NULL,
            PRIMARY KEY (catalog_instance_id, track_id)
        )
    """)


def pack_entity(entity):
    """Store ndarray values as binary arrays, without pickle or precision loss."""
    arrays = {}

    def encode(value):
        if isinstance(value, np.ndarray):
            key = f"v{len(arrays)}"
            arrays[key] = value
            return {"__ndarray__": key}
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, dict):
            return {key: encode(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [encode(item) for item in value]
        return value

    # Catalogue canonical_json strips provider paths/URLs. Here 'path' is the
    # album's sonic trajectory, so use lossless JSON for our own typed metadata.
    metadata = json.dumps(encode(entity), sort_keys=True, separators=(',', ':'),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    stream = io.BytesIO()
    np.savez(stream, metadata=np.frombuffer(metadata, dtype=np.uint8), **arrays)
    return stream.getvalue()


def unpack_entity(blob):
    with np.load(io.BytesIO(bytes(blob)), allow_pickle=False) as archive:
        def decode(value):
            if isinstance(value, dict):
                if set(value) == {"__ndarray__"}:
                    return archive[value["__ndarray__"]].copy()
                return {key: decode(item) for key, item in value.items()}
            if isinstance(value, list):
                return [decode(item) for item in value]
            return value
        return decode(json.loads(archive["metadata"].tobytes().decode("utf-8")))


def input_identity(cur, source_id, lock=False):
    from .catalog_enrichment import RELATIONSHIP_ALGORITHM_VERSION
    cur.execute(f"""
        SELECT s.current_core_server_id, s.rebind_status,
               c.published_generation, c.catalog_epoch, c.status,
               a.projection_generation, a.analysis_epoch, a.status
          FROM {t('catalog_sources')} s
          JOIN {t('catalog_state')} c USING (catalog_instance_id)
          JOIN {t('analysis_state')} a USING (catalog_instance_id)
         WHERE s.catalog_instance_id=%s
         {'FOR SHARE OF s, c, a' if lock else ''}
    """, (source_id,))
    row = cur.fetchone()
    if (not row or row[1] != 'active' or row[4] != 'complete'
            or row[7] != 'complete' or int(row[2]) <= 0 or int(row[5]) <= 0):
        raise InputsChanged("Waiting for published catalogue and sonic analysis inputs")
    return {
        "server_id": row[0], "catalog_generation": int(row[2]), "catalog_epoch": row[3],
        "analysis_generation": int(row[5]), "analysis_epoch": row[6],
        "algorithm_version": RELATIONSHIP_ALGORITHM_VERSION,
        "build_format_version": BUILD_FORMAT_VERSION,
    }


class Build:
    def __init__(self, db, source_id, progress, batch_size, time_budget_seconds):
        self.db, self.source_id, self.callback = db, source_id, progress
        self.batch_size = max(1, min(int(batch_size), 1024))
        self.deadline = time.monotonic() + max(0.01, float(time_budget_seconds))
        self.work = 0
        self.diagnostics = {"timings_ms": {}, "counts": {}}
        self.active_phase = "claiming"

    @contextmanager
    def timed(self, phase):
        self.active_phase = phase
        started = time.monotonic()
        log.debug("relationship phase started source=%s phase=%s", self.source_id, phase)
        try:
            yield
        finally:
            elapsed = round((time.monotonic() - started) * 1000, 3)
            timings = self.diagnostics["timings_ms"]
            timings[phase] = round(timings.get(phase, 0) + elapsed, 3)
            log.debug("relationship phase finished source=%s phase=%s duration_ms=%s counts=%s",
                     self.source_id, phase, elapsed, self.diagnostics["counts"])

    def exhausted(self):
        return self.work >= self.batch_size or time.monotonic() >= self.deadline

    def report(self, phase, current=None, total=None):
        # Only call at committed boundaries: the existing callback commits get_db().
        if callable(self.callback):
            self.callback(phase, current=current, total=total)

    def checkpoint(self, cur, phase=None):
        if phase:
            self.phase = phase
        self.diagnostics["phase"] = self.active_phase
        self.diagnostics["build_id"] = self.build_id
        cur.execute(f"""
            UPDATE {t('relationship_builds')}
               SET phase=%s, progress=%s::jsonb, updated_at=now()
             WHERE catalog_instance_id=%s AND build_id=%s
        """, (self.phase, canonical_json(self.diagnostics), self.source_id, self.build_id))
        cur.execute(f"UPDATE {t('relationship_state')} SET updated_at=now() "
                    "WHERE catalog_instance_id=%s", (self.source_id,))
        self.db.commit()
        log.info("relationship checkpoint source=%s build=%s phase=%s timings_ms=%s counts=%s",
                 self.source_id, self.build_id, self.phase,
                 self.diagnostics['timings_ms'], self.diagnostics['counts'])

    def verify_inputs(self, cur, lock=False):
        if input_identity(cur, self.source_id, lock=lock) != self.identity:
            raise InputsChanged("Catalogue or sonic analysis changed; restarting relationship build")

    def initialize(self, cur):
        self.identity = input_identity(cur, self.source_id)
        cur.execute(f"SELECT build_id, input_identity, phase, progress FROM {t('relationship_builds')} "
                    "WHERE catalog_instance_id=%s", (self.source_id,))
        old = cur.fetchone()
        if old and old[1] == self.identity:
            self.build_id, self.phase = old[0], old[2]
            self.diagnostics = old[3]
            self.diagnostics.pop("error", None)
        else:
            cur.execute(f"DELETE FROM {t('relationship_builds')} WHERE catalog_instance_id=%s",
                        (self.source_id,))
            self.build_id, self.phase = str(uuid.uuid4()), "fingerprints"
            cur.execute(f"""
                INSERT INTO {t('relationship_builds')} (catalog_instance_id, build_id, input_identity)
                VALUES (%s, %s, %s::jsonb)
            """, (self.source_id, self.build_id, canonical_json(self.identity)))
            cur.execute(f"UPDATE {t('relationship_state')} SET started_at=now(), completed_at=NULL "
                        "WHERE catalog_instance_id=%s", (self.source_id,))
        cur.execute(f"UPDATE {t('relationship_state')} SET status='running', last_error=NULL, "
                    "started_at=COALESCE(started_at, now()), updated_at=now() "
                    "WHERE catalog_instance_id=%s", (self.source_id,))
        self.checkpoint(cur)

    def fingerprints(self, cur):
        from . import catalog_enrichment as e
        self.report("Loading relationship inputs")
        cur.execute(f"SELECT inputs_ready FROM {t('relationship_builds')} WHERE catalog_instance_id=%s",
                    (self.source_id,))
        inputs_ready = cur.fetchone()[0]
        if inputs_ready:
            with self.timed('input_checkpoint_loading'):
                cur.execute(f"SELECT input_payload, embedding FROM {t('relationship_build_tracks')} "
                            "WHERE catalog_instance_id=%s ORDER BY input_order", (self.source_id,))
                tracks = [{**payload, 'embedding': np.frombuffer(bytes(blob), dtype='<f4')}
                          for payload, blob in cur.fetchall()]
        else:
            with self.timed("input_loading"):
                tracks = e._load_relationship_inputs(cur, {
                    "catalog_instance_id": self.source_id,
                    "catalog": {"generation": self.identity["catalog_generation"]},
                    "analysis": {"generation": self.identity["analysis_generation"]},
                })
                self.verify_inputs(cur)
        self.db.commit()
        album_keys = {f"{r['artist'].lower()}::{r['album'].lower()}" for r in tracks if r['album']}
        artist_keys = {r['artist'].lower() for r in tracks}
        self.diagnostics['counts'].update(tracks=len(tracks), albums=len(album_keys), artists=len(artist_keys))
        # The mapping is generation-pinned and idempotent across a partial insert.
        # Each statement is bounded; no published rows are touched here.
        if not inputs_ready:
            with self.timed("input_staging"):
                self.stage_inputs(cur, tracks, album_keys, artist_keys)
        self.checkpoint(cur)
        cur.execute(f"SELECT entity_type, entity_id FROM {t('relationship_build_entities')} "
                    "WHERE catalog_instance_id=%s", (self.source_id,))
        completed = set(cur.fetchall())
        self.db.commit()
        total = len(album_keys) + len(artist_keys)
        done = len(completed)
        self.report("Calculating relationship fingerprints", done, total)
        iterator = e._iter_relationship_entities(tracks, completed)
        while True:
            with self.timed("fingerprints"):
                item = next(iterator, None)
            if item is None:
                self.verify_inputs(cur)
                self.checkpoint(cur, "scoring")
                return
            entity_type, entity = item
            cur.execute(f"""
                INSERT INTO {t('relationship_build_entities')}
                    (catalog_instance_id, entity_type, entity_id, fingerprint)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (catalog_instance_id, entity_type, entity_id) DO NOTHING
            """, (self.source_id, entity_type, entity['key'], pack_entity(entity)))
            self.work += 1
            done += 1
            self.diagnostics['counts']['fingerprints_done'] = done
            if done % CHECKPOINT_SIZE == 0 or self.exhausted():
                self.checkpoint(cur)
                self.report("Calculating relationship fingerprints", done, total)
            if self.exhausted():
                return

    def stage_inputs(self, cur, tracks, album_keys, artist_keys):
        """Snapshot normalized inputs once, keeping MusicNN vectors in binary."""
        with self.timed("input_staging_sql"):
            for offset in range(0, len(tracks), 500):
                execute_values(cur, f"""
                    INSERT INTO {t('relationship_build_tracks')}
                        (catalog_instance_id, track_id, album_key, artist_key,
                         input_order, input_payload, embedding) VALUES %s
                    ON CONFLICT (catalog_instance_id, track_id) DO NOTHING
                """, [(self.source_id, r['id'],
                       f"{r['artist'].lower()}::{r['album'].lower()}" if r['album'] else None,
                       r['artist'].lower(), r['order'],
                       json.dumps({key: value for key, value in r.items() if key != 'embedding'},
                                  ensure_ascii=False, allow_nan=False),
                       r['embedding'].tobytes()) for r in tracks[offset:offset+500]], page_size=500)
            cur.execute(f"UPDATE {t('relationship_builds')} SET track_count=%s, album_count=%s, "
                        "artist_count=%s, inputs_ready=TRUE WHERE catalog_instance_id=%s",
                        (len(tracks), len(album_keys), len(artist_keys), self.source_id))

    def score(self, cur, candidate_lookup):
        from . import catalog_enrichment as e
        with self.timed("checkpoint_loading"):
            cur.execute(f"SELECT entity_type, entity_id, fingerprint, result_fp "
                        f"FROM {t('relationship_build_entities')} WHERE catalog_instance_id=%s "
                        "ORDER BY entity_type, entity_id", (self.source_id,))
            rows = cur.fetchall()
            entities = {kind: {} for kind in ('album', 'artist')}
            pending = []
            for kind, key, blob, result_fp in rows:
                entities[kind][key] = unpack_entity(blob)
                if result_fp is None:
                    pending.append((kind, key))
            cur.execute(f"SELECT track_id, album_key, artist_key FROM {t('relationship_build_tracks')} "
                        "WHERE catalog_instance_id=%s", (self.source_id,))
            mapping = cur.fetchall()
            track_maps = {
                "album": {row[0]: row[1] for row in mapping if row[1]},
                "artist": {row[0]: row[2] for row in mapping},
            }
        self.db.commit()
        completed = {kind: sum(r[0] == kind and r[3] is not None for r in rows)
                     for kind in entities}
        for kind, key in pending:
            # Loading a large checkpoint can consume the time budget. Still
            # complete one bounded entity so repeated resumes cannot livelock.
            if self.work > 0 and self.exhausted():
                break
            entity = entities[kind][key]
            self.report(f"{'Albums' if kind == 'album' else 'Artists'}: finding similarities",
                        completed[kind], len(entities[kind]))
            with self.timed("candidate_lookup"):
                candidates = e._relationship_candidates(
                    entity, kind, entities[kind], track_maps[kind], candidate_lookup)
            with self.timed("scoring"):
                ranked = (e._rank_albums if kind == 'album' else e._rank_artists)(entity, candidates)
                payload = {"entity_type": kind, "entity_id": key,
                           "artist": entity['artist'], "coverItemId": entity['cover'],
                           "candidates": ranked, "algorithm_version": e.RELATIONSHIP_ALGORITHM_VERSION}
                if kind == 'album':
                    payload['album'] = entity['album']
            with self.timed("result_staging"):
                cur.execute(f"UPDATE {t('relationship_build_entities')} SET payload=%s::jsonb, result_fp=%s "
                            "WHERE catalog_instance_id=%s AND entity_type=%s AND entity_id=%s",
                            (canonical_json(payload), e._fingerprint(payload), self.source_id, kind, key))
            self.work += 1
            completed[kind] += 1
            self.diagnostics['counts'][f'{kind}s_done'] = completed[kind]
            # One completed entity is a useful restart boundary. It also keeps
            # the existing progress callback away from uncommitted writes.
            self.checkpoint(cur)
        if all(completed[kind] == len(entities[kind]) for kind in entities):
            self.checkpoint(cur, "publishing")

    def publish(self, cur):
        from . import catalog_enrichment as e
        self.report("Publishing relationship generation")
        with self.timed("publication"):
            # Input writers lock these rows too. Fail quickly on contention and
            # retry the completed staging data without rerunning the rankers.
            cur.execute("SET LOCAL lock_timeout = '2s'")
            self.verify_inputs(cur, lock=True)
            cur.execute(f"SELECT result_generation, epoch, head_seq FROM {t('relationship_state')} "
                        "WHERE catalog_instance_id=%s FOR UPDATE", (self.source_id,))
            previous_generation, epoch, head_seq = cur.fetchone()
            generation = int(previous_generation) + 1
            cur.execute(f"SELECT count(*), count(*) FILTER (WHERE result_fp IS NULL) "
                        f"FROM {t('relationship_build_entities')} WHERE catalog_instance_id=%s",
                        (self.source_id,))
            total, incomplete = cur.fetchone()
            counts = self.diagnostics['counts']
            if incomplete or total != counts['albums'] + counts['artists']:
                raise CatalogScanError("Relationship checkpoint is incomplete")
            # Generate the journal BEFORE replacing published results. A stable
            # ordering makes retries deterministic, with unchanged rows omitted.
            cur.execute(f"""
                WITH delta AS (
                    SELECT n.entity_type, n.entity_id, 'upsert'::text AS operation, n.payload
                      FROM {t('relationship_build_entities')} n
                      LEFT JOIN {t('relationship_results')} old
                        ON old.catalog_instance_id=n.catalog_instance_id
                       AND old.entity_type=n.entity_type AND old.entity_id=n.entity_id
                     WHERE n.catalog_instance_id=%s AND old.result_fp IS DISTINCT FROM n.result_fp
                    UNION ALL
                    SELECT old.entity_type, old.entity_id, 'delete'::text, NULL::jsonb
                      FROM {t('relationship_results')} old
                     WHERE old.catalog_instance_id=%s AND NOT EXISTS (
                        SELECT 1 FROM {t('relationship_build_entities')} n
                         WHERE n.catalog_instance_id=old.catalog_instance_id
                           AND n.entity_type=old.entity_type AND n.entity_id=old.entity_id)
                )
                INSERT INTO {t('relationship_changes')}
                    (catalog_instance_id, epoch, seq, generation, entity_type, entity_id, operation, payload)
                SELECT %s, %s, %s + row_number() OVER (ORDER BY entity_type, entity_id),
                       %s, entity_type, entity_id, operation, payload FROM delta
            """, (self.source_id, self.source_id, self.source_id, epoch, head_seq, generation))
            changes = cur.rowcount
            cur.execute(f"""
                INSERT INTO {t('relationship_results')}
                    (catalog_instance_id, entity_type, entity_id, result_generation, result_fp, payload)
                SELECT catalog_instance_id, entity_type, entity_id, %s, result_fp, payload
                  FROM {t('relationship_build_entities')} WHERE catalog_instance_id=%s
                ON CONFLICT (catalog_instance_id, entity_type, entity_id) DO UPDATE SET
                    result_generation=EXCLUDED.result_generation, result_fp=EXCLUDED.result_fp,
                    payload=EXCLUDED.payload, computed_at=now()
            """, (generation, self.source_id))
            cur.execute(f"DELETE FROM {t('relationship_results')} WHERE catalog_instance_id=%s "
                        "AND result_generation<>%s", (self.source_id, generation))
            cur.execute(f"""
                UPDATE {t('relationship_state')} SET relationship_schema_version=%s, algorithm_version=%s,
                    source_catalog_generation=%s, source_analysis_generation=%s,
                    result_generation=%s, head_seq=%s, status='complete', album_count=%s, artist_count=%s,
                    completed_at=now(), last_error=NULL, updated_at=now()
                 WHERE catalog_instance_id=%s
            """, (e.RELATIONSHIP_SCHEMA_VERSION, e.RELATIONSHIP_ALGORITHM_VERSION,
                  self.identity['catalog_generation'], self.identity['analysis_generation'],
                  generation, head_seq + changes, counts['albums'], counts['artists'], self.source_id))
            e.compact_change_journal(
                cur, catalog_instance_id=self.source_id, state_table='relationship_state',
                changes_table='relationship_changes', epoch_column='epoch', floor_column='floor_seq',
                epoch=epoch, head_seq=head_seq + changes,
                retention_limit=e.change_journal_retention_limit(total))
            self.diagnostics['result'] = {
                "catalog_instance_id": self.source_id, "status": "complete", "generation": generation,
                "album_count": counts['albums'], "artist_count": counts['artists'],
                "track_count": counts['tracks'], "changes": changes,
                "cursor": opaque_cursor(self.source_id, epoch, head_seq + changes),
            }
            # Published rows, cursor and completion marker commit together.
            cur.execute(f"UPDATE {t('relationship_builds')} SET phase='complete' "
                        "WHERE catalog_instance_id=%s", (self.source_id,))
        self.checkpoint(cur, 'complete')

    def queued(self, cur, reason='batch_complete'):
        self.checkpoint(cur)
        cur.execute(f"UPDATE {t('relationship_state')} SET status='queued', updated_at=now() "
                    "WHERE catalog_instance_id=%s", (self.source_id,))
        self.db.commit()
        return {"catalog_instance_id": self.source_id, "status": "queued", "reason": reason,
                "phase": self.phase, "progress": self.diagnostics['counts']}


def run_relationship_build(source_id, *, db, candidate_lookup, progress=None,
                           batch_size=128, time_budget_seconds=20):
    from . import catalog_enrichment as e
    build = Build(db, source_id, progress, batch_size, time_budget_seconds)
    cur = db.cursor()
    acquired = False
    try:
        cur.execute("SELECT pg_try_advisory_lock(hashtext('lumae.relationships'), hashtext(%s))", (source_id,))
        acquired = cur.fetchone()[0]
        db.commit()
        if not acquired:
            return {"status": "coalesced", "reason": "already_running"}
        build.initialize(cur)
        if build.phase == 'fingerprints':
            build.fingerprints(cur)
        if build.phase == 'scoring' and (build.work == 0 or not build.exhausted()):
            build.score(cur, candidate_lookup)
        if build.phase == 'publishing':
            build.publish(cur)
        if build.phase == 'complete':
            # Scratch storage is bounded to the active build, not a history of
            # full-library embeddings. Retain only counts/timings after success.
            cur.execute(f"UPDATE {t('relationship_state')} SET status='complete' WHERE catalog_instance_id=%s", (source_id,))
            db.commit()
            try:
                cur.execute(f"DELETE FROM {t('relationship_build_entities')} WHERE catalog_instance_id=%s", (source_id,))
                cur.execute(f"DELETE FROM {t('relationship_build_tracks')} WHERE catalog_instance_id=%s", (source_id,))
                db.commit()
            except Exception:
                db.rollback()
                log.warning("relationship checkpoint cleanup deferred source=%s", source_id, exc_info=True)
            return build.diagnostics['result']
        return build.queued(cur)
    except Exception as exc:
        db.rollback()
        code = getattr(exc, 'pgcode', None)
        message = getattr(getattr(exc, 'diag', None), 'message_primary', None) or str(exc)
        build.diagnostics['error'] = {"phase": build.active_phase, "sqlstate": code, "message": message[:1000]}
        status = ('queued' if isinstance(exc, InputsChanged) or code in ('55P03', '40P01')
                  else 'waiting_for_index' if isinstance(exc, e.RelationshipIndexUnavailable) else 'failed')
        log.warning("relationship build stopped source=%s phase=%s sqlstate=%s status=%s error=%s",
                    source_id, build.active_phase, code, status, message[:1000])
        # Rollback precedes diagnostics; progress must never commit partial publication.
        if hasattr(build, 'build_id'):
            cur.execute(f"UPDATE {t('relationship_builds')} SET progress=%s::jsonb, updated_at=now() "
                        "WHERE catalog_instance_id=%s AND build_id=%s",
                        (canonical_json(build.diagnostics), source_id, build.build_id))
        cur.execute(f"UPDATE {t('relationship_state')} SET status=%s, last_error=%s, updated_at=now() "
                    "WHERE catalog_instance_id=%s",
                    (status, f"{build.active_phase}{f' [SQLSTATE {code}]' if code else ''}: {message}"[:2000], source_id))
        db.commit()
        if status in ('queued', 'waiting_for_index'):
            return {"catalog_instance_id": source_id, "status": status, "reason": message[:1000]}
        raise
    finally:
        if acquired:
            # Release even on BaseException/worker cancellation; a dead session
            # also releases this lock automatically in PostgreSQL.
            db.rollback()
            try:
                cur.execute("SELECT pg_advisory_unlock(hashtext('lumae.relationships'), hashtext(%s))", (source_id,))
                db.commit()
            finally:
                cur.close()
        else:
            cur.close()

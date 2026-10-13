"""A catalogue refresh's own database work stays proportional (LA-1).

The analysis-run finalization the test instance timed on 2026-10-12 took
109 s, 102.7 s of it the catalogue refresh. On a 36k-track fixture with an
instant provider fetch, that refresh with 5,471 changed tracks took 86-103 s
in 1.7.0 and 34 s with these two fixes.

``_revision_lookup`` reads, for each changed track, what the generation the
publisher is writing says about it. That generation is the publisher's own,
uncommitted, so PostgreSQL has no statistics for it. 1.7.0 probed it once per
changed track; the planner took the album index and filtered the whole
generation for every probe. A 36k-track refresh with 5,471 changed tracks
spent 41-58 s in that one statement (119M buffer hits), most of the 109 s
analysis-run finalization seen on the test instance on 2026-10-12.
"""
import json

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin  # noqa: E402,F401  (installs the plugin.api stub)
from plugins.LumaeAnalysis import catalog, profile_publication  # noqa: E402

SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
TRACKS = 4000


class CapturingCursor:
    def __init__(self, cur):
        self._cur = cur
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        return self._cur.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._cur, name)


def _insert_generation(cur, generation, available=True):
    cur.execute(
        f"""
        INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id,
               album_id, title, metadata_fp, media_fp, payload, available,
               first_seen_at, last_seen_at)
        SELECT %s, %s, 'tr-' || lpad(n::text, 6, '0'), 'al-' || (n %% 40), 'Song', 'meta',
               'media-' || %s || '-' || n, '{{}}'::jsonb, %s, now(), now()
          FROM generate_series(1, %s) AS n
        """,
        (SOURCE, generation, generation, available, TRACKS),
    )


def _buffers(plan):
    node = plan[0]["Plan"]
    return int(node.get("Shared Hit Blocks", 0)) + int(node.get("Shared Read Blocks", 0))


def test_revision_lookup_reads_an_uncommitted_generation_in_one_pass(migrated_db):
    db = migrated_db
    with db.cursor() as cur:
        _insert_generation(cur, 1)
        cur.execute(f"ANALYZE {P}catalog_tracks")
    db.commit()

    cur = db.cursor()
    # The publisher's next generation, written in its own transaction: two
    # tracks gone from it, one unavailable in it.
    _insert_generation(cur, 2)
    cur.execute(
        f"DELETE FROM {P}catalog_tracks WHERE published_generation=2 "
        "AND track_id IN ('tr-000002', 'tr-000003')"
    )
    cur.execute(
        f"UPDATE {P}catalog_tracks SET available=FALSE "
        "WHERE published_generation=2 AND track_id='tr-000004'"
    )
    changed = {f"tr-{n:06d}": "upsert" for n in range(1, 1501)}
    changed["tr-000003"] = "delete"
    changed["tr-999999"] = "upsert"  # not in the generation at all
    capture = CapturingCursor(cur)

    track_ids, gone, media_fps = profile_publication._revision_lookup(
        capture, SOURCE, 2, changed)

    assert track_ids == list(changed)
    expected_gone = {"tr-000002", "tr-000003", "tr-000004", "tr-999999"}
    assert [track_id for track_id, flag in zip(track_ids, gone) if flag] == [
        track_id for track_id in changed if track_id in expected_gone]
    for track_id, media_fp in zip(track_ids, media_fps):
        if track_id in ("tr-000002", "tr-000003", "tr-999999"):
            assert media_fp is None
        else:
            assert media_fp == f"media-2-{int(track_id[3:])}"

    # Every statement the lookup ran, re-run under EXPLAIN in the same
    # transaction: one pass over the generation, not one per changed track.
    # 1.7.0 touched about TRACKS buffers per changed track (6M here).
    total = 0
    for sql, params in capture.statements:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
        plan = cur.fetchone()[0]
        total += _buffers(plan if isinstance(plan, list) else json.loads(plan))
    assert total < 20 * (TRACKS + len(changed)), total
    cur.close()
    db.rollback()


class CountingCursor(psycopg2.extensions.cursor):
    """Counts the statements the server runs: executemany runs one per row."""

    statements = 0

    def execute(self, sql, params=None):
        CountingCursor.statements += 1
        return super().execute(sql, params)

    def executemany(self, sql, params_list):
        params_list = list(params_list)
        CountingCursor.statements += len(params_list)
        return super().executemany(sql, params_list)


def test_a_generation_is_written_a_page_per_statement(migrated_db):
    # 1.7.0 wrote one INSERT per row: about 150k statements and 18 s for a
    # 36k-track generation with its artist credits and library memberships.
    db = migrated_db
    normalized = catalog.normalize_provider_catalog({
        "libraries": [{"id": "lib-1", "name": "Music"}],
        "tracks": [
            {"id": f"tr-{n}", "title": f"Song {n}", "album": f"Album {n % 50}",
             "albumId": f"al-{n % 50}", "artist": f"Artist {n % 30}",
             "artistId": f"ar-{n % 30}", "duration": 200, "musicFolderId": "lib-1"}
            for n in range(2500)
        ],
    }, "navidrome")
    cur = db.cursor(cursor_factory=CountingCursor)
    CountingCursor.statements = 0
    for entity_type in catalog.ENTITY_ORDER:
        catalog._insert_generation_rows(
            cur, entity_type, SOURCE, 1,
            normalized[catalog.ENTITY_COLLECTIONS[entity_type]], catalog.utc_now())
    catalog._insert_relationship_rows(cur, SOURCE, 1, normalized)
    rows = sum(len(normalized[catalog.ENTITY_COLLECTIONS[entity]])
               for entity in catalog.ENTITY_ORDER)
    rows += len(normalized["track_artists"]) + len(normalized["album_artists"])
    rows += len(normalized["entity_libraries"])

    cur.execute(f"SELECT count(*) FROM {P}catalog_tracks WHERE published_generation=1")
    assert cur.fetchone()[0] == 2500
    cur.execute(f"SELECT count(*) FROM {P}catalog_entity_libraries WHERE published_generation=1")
    assert cur.fetchone()[0] == len(normalized["entity_libraries"])
    # A page of up to 1,000 rows per statement, plus the track insert's
    # unaccent check and album-name read and the two counts above.
    assert rows > 5000
    assert CountingCursor.statements <= 20, (CountingCursor.statements, rows)
    cur.close()
    db.rollback()

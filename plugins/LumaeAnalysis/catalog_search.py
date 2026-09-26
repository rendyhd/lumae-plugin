"""The workbench's stored search text and its indexes (LUM-016, P3-5c).

``catalog_tracks.search_text`` holds ``lower(unaccent(concat_ws(' ', title,
artist_display, album_artist_display, album name)))``, the text the workbench
used to compute per request (``collection_library.search_text_sql``). It is
written with the rows when a catalogue generation is published
(``catalog._insert_generation_rows``); without ``unaccent`` it is only
lower-cased. ``catalog_state.search_text_generation`` and
``search_text_folded`` record which generation holds it and how it was
folded, and a request uses it only when that matches the published
generation and the request's own folding. The migration rewrites a stale
generation (an upgrade, or ``unaccent`` installed or removed since).

Indexes, all through the P2-5 helpers (bounded lock waits, no-op when
present):

- ``gin (search_text gin_trgm_ops) WHERE available`` when ``pg_trgm`` can be
  installed; otherwise search filters the generation's rows (logged once).
  It is scoped by catalogue and generation through the keyset btrees below,
  which the planner ANDs with it, and because old generations are pruned.
- keyset btrees on ``(catalog_instance_id, published_generation, lower(key),
  id)`` for tracks by title, albums by name and artists by name;
- ``(catalog_instance_id, published_generation, album_id)`` on tracks.
"""

from plugin.api import logger, table

from . import migrations
from .collection_library import ARTIST_KEY_SQL, search_text_sql


TRGM_INDEX = "idx_catalog_tracks_search_trgm"
KEYSET_INDEXES = (
    ("idx_catalog_tracks_title_keyset", "catalog_tracks",
     "(catalog_instance_id, published_generation, lower(title), track_id) WHERE available"),
    ("idx_catalog_tracks_artist_keyset", "catalog_tracks",
     f"(catalog_instance_id, published_generation, lower({ARTIST_KEY_SQL.replace('t.', '')}), "
     f"({ARTIST_KEY_SQL.replace('t.', '')})) WHERE available"),
    ("idx_catalog_albums_name_keyset", "catalog_albums",
     "(catalog_instance_id, published_generation, lower(name), album_id) WHERE available"),
    ("idx_catalog_tracks_album", "catalog_tracks",
     "(catalog_instance_id, published_generation, album_id)"),
)


def search_text_value(title, artist_display, album_artist_display, album_name):
    """``concat_ws(' ', ...)`` in Python: NULLs are skipped, empty strings kept."""
    parts = (title, artist_display, album_artist_display, album_name)
    return " ".join(str(part) for part in parts if part is not None)


def search_text_placeholder(folded):
    """The INSERT placeholder that folds ``search_text_value`` like the SQL."""
    return "lower(unaccent(%s))" if folded else "lower(%s)"


def fold_available(cur):
    """Whether ``unaccent(text)`` resolves here (quietly; see collection_library)."""
    cur.execute("SELECT to_regprocedure('unaccent(text)') IS NOT NULL")
    row = cur.fetchone()
    return bool(row and row[0])


def mark_search_text(cur, catalog_instance_id, generation, folded):
    """Record that ``generation`` holds search text folded as ``folded``."""
    cur.execute(
        f"UPDATE {table('catalog_state')} SET search_text_generation=%s, "
        "search_text_folded=%s WHERE catalog_instance_id=%s",
        (generation, bool(folded), catalog_instance_id),
    )


def refresh_search_text(cur, catalog_instance_id, generation, folded):
    """Rewrite one generation's search text; returns the rows changed."""
    tracks = table("catalog_tracks")
    album_name = (
        f"(SELECT al.name FROM {table('catalog_albums')} al "
        "WHERE al.catalog_instance_id=t.catalog_instance_id "
        "AND al.published_generation=t.published_generation "
        "AND al.album_id=t.album_id AND al.available)"
    )
    expression = search_text_sql(folded, "t", album_name)
    cur.execute(
        f"UPDATE {tracks} t SET search_text={expression} "
        "WHERE t.catalog_instance_id=%s AND t.published_generation=%s "
        f"AND t.search_text IS DISTINCT FROM {expression}",
        (catalog_instance_id, generation),
    )
    changed = cur.rowcount
    mark_search_text(cur, catalog_instance_id, generation, folded)
    return changed


def _trgm_opclass(cur):
    """``gin_trgm_ops``, schema-qualified, or None without pg_trgm."""
    cur.execute(
        """
        SELECT quote_ident(n.nspname) || '.gin_trgm_ops'
          FROM pg_opclass o
          JOIN pg_am a ON a.oid=o.opcmethod
          JOIN pg_namespace n ON n.oid=o.opcnamespace
         WHERE o.opcname='gin_trgm_ops' AND a.amname='gin'
         ORDER BY pg_opclass_is_visible(o.oid) DESC
         LIMIT 1
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def migrate_search_text(cur):
    """Columns, indexes and stale-generation backfill; a no-op when current."""
    tracks = table("catalog_tracks")
    state = table("catalog_state")
    migrations.ensure_columns(cur, tracks, "search_text TEXT")
    migrations.ensure_columns(
        cur, state, "search_text_generation BIGINT", "search_text_folded BOOLEAN")
    migrations.ensure_extension(cur, "unaccent")
    folded = fold_available(cur)
    cur.execute(
        f"SELECT catalog_instance_id, published_generation FROM {state} "
        "WHERE status='complete' AND (search_text_generation IS DISTINCT FROM "
        "published_generation OR search_text_folded IS DISTINCT FROM %s) "
        "ORDER BY catalog_instance_id",
        (folded,),
    )
    for catalog_instance_id, generation in cur.fetchall():
        refresh_search_text(cur, catalog_instance_id, generation, folded)
    for name, relation, columns in KEYSET_INDEXES:
        migrations.ensure_index(
            cur, f"CREATE INDEX IF NOT EXISTS {table(name)} ON {table(relation)} {columns}")
    opclass = _trgm_opclass(cur)
    if opclass is None and migrations.ensure_extension(cur, "pg_trgm"):
        opclass = _trgm_opclass(cur)
    if opclass is None:
        logger.warning(
            "Living Collections search has no trigram index: the PostgreSQL pg_trgm "
            "extension is not installed (a database owner can run CREATE EXTENSION "
            "pg_trgm); search reads the stored text of the catalogue generation instead"
        )
        return
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {table(TRGM_INDEX)} ON {tracks} "
        f"USING gin (search_text {opclass}) WHERE available",
    )

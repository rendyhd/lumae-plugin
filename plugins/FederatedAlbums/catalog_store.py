"""Bounded, owner-scoped catalogue reads and an approximate similarity shortlist.

Only the final, bounded shortlist materializes full dynamics fingerprints. The
LSH index is a candidate generator; the existing golden-tested scorer is the
final authority and its scoring contract is unchanged.
"""

import re
import numpy as np
from psycopg2.extras import DictCursor
from plugin.api import get_db, table

MAX_CANDIDATES = 512
# Fixed seed and dimensions form an internal index version, not an API score.
_PLANES = np.random.default_rng(0x464131).standard_normal((8, 8, 200))


def buckets(vector, probe=False):
    vector = np.asarray(vector, dtype=np.float64)
    if (
        vector.shape != (200,)
        or not np.isfinite(vector).all()
        or not np.linalg.norm(vector)
    ):
        raise ValueError("Expected a finite, nonzero 200D fingerprint")
    signs = (_PLANES @ vector) >= 0
    codes = (signs * (1 << np.arange(8))).sum(axis=1)
    output = []
    for band, code in enumerate(codes):
        output.append(int(band * 256 + code))
        if probe:
            output.extend(
                int(band * 256 + (int(code) ^ (1 << bit))) for bit in range(8)
            )
    return output


def migrate(db):
    with db.cursor() as cur:
        for name in ("albums", "remote_albums"):
            target = table(name)
            cur.execute(
                f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS buckets INTEGER[]"
            )
            cur.execute(
                f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS search_document TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', album || ' ' || artist)) STORED"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {target}_buckets_idx ON {target} USING GIN(buckets)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {target}_search_idx ON {target} USING GIN(search_document)"
            )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {table('connections')}_owner_idx ON {table('connections')}(owner,id)"
        )


def read(
    owner,
    *,
    remote=False,
    fingerprint=False,
    query=None,
    album_key=None,
    instance_id=None,
    vector=None,
    limit=50,
    exclude_owned=False,
):
    limit = max(1, min(MAX_CANDIDATES, int(limit)))
    alias = "r" if remote else "a"
    columns = ",".join(
        f"{alias}.{name}"
        for name in (
            "album_key",
            "album",
            "artist",
            "year",
            "track_count",
            "updated_at",
        )
    )
    if fingerprint:
        columns += f", {alias}.fingerprint"
    predicates = []
    params = []
    if remote:
        columns += ",r.remote_instance_id,c.name AS source_name,c.base_url"
        source = f"{table('remote_albums')} r JOIN {table('connections')} c ON c.id=r.connection_id"
        predicates.append("c.owner=%s")
        params.append(owner)
        if instance_id:
            predicates.append("r.remote_instance_id=%s")
            params.append(instance_id)
        if exclude_owned:
            predicates.append(
                f"NOT EXISTS(SELECT 1 FROM {table('albums')} owned WHERE owned.album_key=r.album_key)"
            )
    else:
        source = f"{table('albums')} a"
    if query:
        # Prefix token search with a parameterized tsquery, never raw SQL.
        tokens = re.findall(r"[^\W_]+", query[:200], flags=re.UNICODE)[:12]
        if not tokens:
            return []
        predicates.append(f"{alias}.search_document @@ to_tsquery('simple', %s)")
        params.append(" & ".join(token + ":*" for token in tokens))
    if album_key:
        predicates.append(f"{alias}.album_key=%s")
        params.append(album_key)
    if vector is not None:
        predicates.append(f"{alias}.buckets && %s")
        params.append(buckets(vector, probe=True))
    order = f"{alias}.album_key"
    if vector is not None:
        # Prefer candidates supported by several independent hash bands before
        # applying the transfer/CPU budget; ties retain deterministic order.
        order = f"(SELECT count(*) FROM unnest({alias}.buckets) AS entries(bucket) WHERE entries.bucket=ANY(%s)) DESC, {order}"
        params.append(buckets(vector, probe=True))
    where = " AND ".join(predicates) or "TRUE"
    with get_db().cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            f"SELECT {columns} FROM {source} WHERE {where} ORDER BY {order} LIMIT %s",
            (*params, limit),
        )
        return [dict(row) for row in cur.fetchall()]

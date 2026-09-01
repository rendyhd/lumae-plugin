"""Additive edge storage and independently retryable upgrade jobs.

Never changes the legacy profile's readiness. The legacy row is locked before
publishing a measurement, so a completed old job cannot attach to a new source.
"""
import uuid

from plugin.api import table

from .edge_profiles import METHOD, SCHEMA_VERSION, canonical_json, opaque_revision, profile_digest


def migrate_edge_profiles(db):
    cur = db.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('edge_profiles')} (
        catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        track_id TEXT NOT NULL,
        media_revision TEXT NOT NULL,
        representation_id TEXT NOT NULL,
        media_signature TEXT NOT NULL,
        profile_digest TEXT NOT NULL,
        payload JSONB NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (catalog_instance_id, track_id, media_revision, representation_id)
    )""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('edge_profile_jobs')} (
        catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        track_id TEXT NOT NULL,
        media_revision TEXT NOT NULL,
        job_token TEXT NOT NULL,
        status TEXT NOT NULL,
        last_error TEXT,
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (catalog_instance_id, track_id)
    )""")
    cur.close()


def edge_join(profile_alias='p'):
    # Internal signatures stay on the server. Only their opaque revision is public.
    return f"""LEFT JOIN LATERAL (
        SELECT e.payload FROM {table('edge_profiles')} e
         WHERE e.catalog_instance_id={profile_alias}.catalog_instance_id
           AND e.track_id={profile_alias}.track_id
           AND e.media_signature={profile_alias}.media_signature
         ORDER BY e.updated_at DESC, e.profile_digest LIMIT 1
    ) edge ON TRUE"""


def claim_edge_jobs(db, catalog_id, ids):
    """Bounded request; ready/current rows are retained and failed upgrades back off."""
    cur = db.cursor()
    cur.execute(f"""SELECT p.track_id, p.media_signature, edge.payload
        FROM {table('source_profiles')} p {edge_join()}
        WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s) AND p.status='ready'
        ORDER BY p.track_id""", (catalog_id, list(dict.fromkeys(ids))[:100]))
    rows = cur.fetchall()
    accepted, ready = [], []
    for track_id, signature, payload in rows:
        revision = opaque_revision(signature)
        if not revision:
            continue
        if payload and payload.get('schema_version') == SCHEMA_VERSION and payload.get('measurement', {}).get('method') == METHOD:
            ready.append(track_id)
            continue
        token = str(uuid.uuid4())
        cur.execute(f"""INSERT INTO {table('edge_profile_jobs')} AS job
            (catalog_instance_id, track_id, media_revision, job_token, status)
            VALUES (%s, %s, %s, %s, 'pending')
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                media_revision=EXCLUDED.media_revision, job_token=EXCLUDED.job_token,
                status='pending', last_error=NULL, updated_at=now()
            WHERE job.media_revision<>EXCLUDED.media_revision
               OR (job.status IN ('pending', 'running') AND job.updated_at < now()-interval '30 minutes')
               OR (job.status NOT IN ('pending', 'running') AND job.updated_at < now()-interval '6 hours')
            RETURNING job_token""", (catalog_id, track_id, revision, token))
        if cur.fetchone():
            accepted.append({'track_id': track_id, 'media_revision': revision, 'job_token': token})
    db.commit()
    cur.close()
    return accepted, ready


def update_edge_job(db, catalog_id, job, status, reason=None):
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_profile_jobs')} SET status=%s, last_error=%s, updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s AND job_token=%s
          AND status=ANY(%s)
        RETURNING job_token""", (status, str(reason)[:160] if reason else None, catalog_id,
                                  job['track_id'], job['media_revision'], job['job_token'],
                                  ['pending'] if status == 'running' else ['pending', 'running']))
    applied = cur.fetchone() is not None
    db.commit()
    cur.close()
    return applied


def publish_edge_profile(db, catalog_id, job, payload, signature):
    from .catalog_enrichment import record_profile_change, serialize_profile

    if (payload.get('schema_version') != SCHEMA_VERSION or payload.get('catalog_instance_id') != catalog_id or
            payload.get('track_id') != job['track_id'] or payload.get('media_revision') != job['media_revision'] or
            opaque_revision(signature) != job['media_revision'] or profile_digest(payload) != payload.get('profile_digest')):
        raise ValueError('edge publication identity/digest mismatch')
    cur = db.cursor()
    cur.execute(f"""SELECT track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                analyzer_ver, analyzed_at, media_signature FROM {table('source_profiles')}
        WHERE catalog_instance_id=%s AND track_id=%s AND status='ready' FOR UPDATE""",
                (catalog_id, job['track_id']))
    legacy = cur.fetchone()
    cur.execute(f"""SELECT job_token FROM {table('edge_profile_jobs')}
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s AND job_token=%s
          AND status IN ('pending', 'running') FOR UPDATE""",
                (catalog_id, job['track_id'], job['media_revision'], job['job_token']))
    current = cur.fetchone()
    if not legacy or legacy[8] != signature or not current:
        db.rollback()
        cur.close()
        return False
    cur.execute(f"DELETE FROM {table('edge_profiles')} WHERE catalog_instance_id=%s AND track_id=%s",
                (catalog_id, job['track_id']))
    cur.execute(f"""INSERT INTO {table('edge_profiles')}
        (catalog_instance_id, track_id, media_revision, representation_id, media_signature, profile_digest, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)""",
                (catalog_id, job['track_id'], job['media_revision'], payload['representation_id'],
                 signature, payload['profile_digest'], canonical_json(payload)))
    record_profile_change(cur, catalog_id, job['track_id'], 'ready', serialize_profile(*legacy, edge_profile=payload))
    cur.execute(f"""UPDATE {table('edge_profile_jobs')} SET status='ready', last_error=NULL, updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s""",
                (catalog_id, job['track_id'], job['job_token']))
    db.commit()
    cur.close()
    return True


def edge_backfill_candidates(db, catalog_id, after='', limit=100):
    cur = db.cursor()
    cur.execute(f"""SELECT p.track_id FROM {table('source_profiles')} p {edge_join()}
        LEFT JOIN {table('edge_profile_jobs')} j ON j.catalog_instance_id=p.catalog_instance_id AND j.track_id=p.track_id
        WHERE p.catalog_instance_id=%s AND p.status='ready' AND p.media_signature IS NOT NULL
          AND p.track_id>%s AND edge.payload IS NULL
          AND (j.track_id IS NULL OR j.updated_at < now()-interval '6 hours')
        ORDER BY p.track_id LIMIT %s""", (catalog_id, after, max(1, min(100, int(limit)))))
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids

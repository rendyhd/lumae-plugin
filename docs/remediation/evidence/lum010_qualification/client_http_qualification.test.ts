/** Actual frozen AudioMuseClient HTTP methods against the disposable host. */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import nodeFetch from 'node-fetch';
import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import type { SQLiteDatabase } from 'expo-sqlite';
import { AudioMuseClient } from '@services/audioMuseClient';
import { runProfileV2Bootstrap, selectProfileBootstrapMode } from '@services/profileBootstrapV2';
import { getProfileV2State } from '@store/profileBootstrapV2Repo';
import { getPublishedProfileCursor } from '@store/publishedProfileRepo';
import type { ProfilePublicationSource } from '@store/profilePublicationSource';

// This integration isolates the v2 transport/durable-store path. The separate
// LUM-009 admission tests exercise the live settings/provider proof machinery.
jest.mock('../profilePublicationSource', () => ({
  profilePublicationSourceStillAdmitted: jest.fn().mockResolvedValue(true),
}));

function database(filename = ':memory:') {
  let native = new DatabaseSync(filename);
  native.exec(`
    CREATE TABLE tracks (id TEXT PRIMARY KEY, media_fp TEXT, available INTEGER DEFAULT 1);
    CREATE TABLE mixramp_profiles (track_id TEXT PRIMARY KEY, source TEXT);
    CREATE TABLE published_waveform_profiles (
      source_key TEXT, track_id TEXT, sample_rate INTEGER, duration_ms INTEGER,
      ref_lufs REAL, start_ramp BLOB, end_ramp BLOB, analyzer_ver INTEGER,
      analyzed_at TEXT, PRIMARY KEY(source_key,track_id));
    CREATE TABLE published_profile_staging (
      source_key TEXT, run_id TEXT, track_id TEXT, media_revision TEXT, sample_rate INTEGER,
      duration_ms INTEGER, ref_lufs REAL, start_ramp BLOB, end_ramp BLOB,
      analyzer_ver INTEGER, analyzed_at TEXT,
      PRIMARY KEY(source_key,run_id,track_id));
    CREATE TABLE published_edge_staging (
      source_key TEXT, run_id TEXT, track_id TEXT, update_json TEXT,
      PRIMARY KEY(source_key,run_id,track_id));
    CREATE TABLE published_edge_profiles (
      source_key TEXT, track_id TEXT, media_revision TEXT,
      representation_id TEXT, profile_digest TEXT, payload_json TEXT,
      PRIMARY KEY(source_key,track_id,media_revision,representation_id));
    CREATE TABLE published_profile_invalidations (
      source_key TEXT, track_id TEXT, PRIMARY KEY(source_key,track_id));
    CREATE TABLE opportunistic_waveform_profiles AS
      SELECT * FROM published_waveform_profiles WHERE 0;
    CREATE TABLE opportunistic_edge_profiles AS
      SELECT * FROM published_edge_profiles WHERE 0;
    CREATE TABLE profile_publication_state (
      source_key TEXT PRIMARY KEY, catalog_instance_id TEXT, catalog_epoch TEXT,
      cursor TEXT, generation INTEGER, last_success_at TEXT);
    CREATE TABLE profile_publication_refresh (source_key TEXT PRIMARY KEY);
    CREATE TABLE profile_bootstrap_v2_state (
      source_key TEXT PRIMARY KEY, catalog_instance_id TEXT NOT NULL,
      catalog_epoch TEXT NOT NULL, account_identity TEXT NOT NULL,
      principal_binding TEXT NOT NULL,
      admission_scope TEXT NOT NULL, protocol_version INTEGER NOT NULL,
      schema_version INTEGER NOT NULL, session_token TEXT NOT NULL,
      run_id TEXT NOT NULL, profile_epoch TEXT NOT NULL,
      snapshot_cursor TEXT NOT NULL, snapshot_seq INTEGER NOT NULL,
      snapshot_count INTEGER NOT NULL, snapshot_rows_seen INTEGER NOT NULL,
      last_seq INTEGER NOT NULL, page_size INTEGER NOT NULL, phase TEXT NOT NULL,
      next_page_token TEXT, head_cursor TEXT, last_cursor TEXT,
      last_applied_phase TEXT, last_applied_page_token TEXT,
      imported INTEGER NOT NULL, expires_at TEXT NOT NULL,
      last_commit_at TEXT NOT NULL);
    INSERT INTO tracks(id) VALUES ('track-a'), ('track-b'), ('track-c'), ('track-d');
    INSERT INTO mixramp_profiles VALUES ('track-a','waveform'), ('track-b','waveform'), ('track-c','waveform'), ('track-d','waveform');
  `);
  let failState = false;
  let failCheckpoint = false;
  let failReleaseCleanup = false;
  let failBegin = false;
  const db = {
    runAsync: async (sql: string, args: SQLInputValue[] = []) => {
      if (failState && sql.includes('INSERT INTO profile_publication_state'))
        throw new Error('injected publication failure');
      if (failCheckpoint && sql.includes('UPDATE profile_bootstrap_v2_state SET phase='))
        throw new Error('injected checkpoint failure');
      if (failReleaseCleanup && sql.includes('DELETE FROM profile_bootstrap_v2_state') &&
          sql.includes("phase='release_pending'"))
        throw new Error('injected release cleanup failure');
      if (failBegin && sql.includes('INSERT OR REPLACE INTO profile_bootstrap_v2_state'))
        throw new Error('injected session persist failure');
      return native.prepare(sql).run(...args);
    },
    getAllAsync: async (sql: string, args: SQLInputValue[] = []) =>
      native.prepare(sql).all(...args),
    getFirstAsync: async (sql: string, args: SQLInputValue[] = []) =>
      native.prepare(sql).get(...args) ?? null,
    withTransactionAsync: async (work: () => Promise<void>) => {
      native.exec('BEGIN');
      try {
        await work();
        native.exec('COMMIT');
      } catch (error) {
        native.exec('ROLLBACK');
        throw error;
      }
    },
  } as unknown as SQLiteDatabase;
  const rows = (key: string) =>
    native
      .prepare(
        'SELECT track_id, ref_lufs FROM published_waveform_profiles WHERE source_key=? ORDER BY track_id',
      )
      .all(key);
  return {
    get native() { return native; },
    db,
    rows,
    reopen: () => {
      native.close();
      native = new DatabaseSync(filename);
    },
    fail: (enabled: boolean) => {
      failState = enabled;
    },
    failCheckpoint: (enabled: boolean) => {
      failCheckpoint = enabled;
    },
    failReleaseCleanup: (enabled: boolean) => {
      failReleaseCleanup = enabled;
    },
    failBegin: (enabled: boolean) => {
      failBegin = enabled;
    },
  };
}


const config = JSON.parse(readFileSync(process.env.LUM010_QUAL_RUNTIME!, 'utf8')) as {
  host_port: number; admin_password: string; work_dir: string;
};

function useRealHttpCookieJar(): () => void {
  const priorFetch = global.fetch;
  let cookie = '';
  global.fetch = (async (url: RequestInfo | URL, init?: RequestInit) => {
    const headers = { ...(init?.headers as Record<string, string> ?? {}) };
    if (cookie) headers.Cookie = cookie;
    const response = await nodeFetch(String(url), { ...init, headers, redirect: 'manual' } as never);
    const issued = response.headers.get('set-cookie');
    if (issued?.startsWith('audiomuse_jwt=')) cookie = issued.split(';', 1)[0];
    return response;
  }) as typeof fetch;
  return () => { global.fetch = priorFetch; };
}

function accountClient(): AudioMuseClient {
  return new AudioMuseClient({
    url: `http://127.0.0.1:${config.host_port}`,
    authMode: 'password', username: 'qual_admin', password: config.admin_password,
  });
}

it('uses account auth, v2 capability, and real server page/catchup/release', async () => {
  const restoreFetch = useRealHttpCookieJar();
  try {
    const client = accountClient();
    const login = await client.login('qual_admin', config.admin_password);
    expect(login).toEqual(expect.objectContaining({ success: true }));
    expect(login.principalBinding).toBeTruthy();
    expect(await selectProfileBootstrapMode(client, true)).toBe('v2');
    expect(await selectProfileBootstrapMode(client, false)).toBe('legacy');
    const checkpoint = JSON.parse(readFileSync(
      path.join(config.work_dir, 'http.checkpoint.json'), 'utf8',
    ));
    const source = checkpoint.created.catalog_instance_id;
    expect(typeof source).toBe('string');
    const created = await client.createProfileV2Session(source, 1);
    expect(created.principal_binding).toBe(login.principalBinding);
    const page = await client.getProfileV2SnapshotPage(
      source, created.session_token, created.next_page_token,
    );
    expect(page.profiles.length).toBe(1);
    const catchup = await client.getProfileV2CatchupPage(source, created.session_token, null);
    expect(catchup.has_more).toBe(false);
    await client.releaseProfileV2Session(source, created.session_token);
  } finally {
    restoreFetch();
  }
}, 30_000);

it('resumes the real server session from durable SQLite and atomically replaces historical rows', async () => {
  const restoreFetch = useRealHttpCookieJar();
  const checkpoint = JSON.parse(readFileSync(
    path.join(config.work_dir, 'http.checkpoint.json'), 'utf8',
  ));
  const owner: ProfilePublicationSource = {
    key: 'qual-profile-source',
    catalogInstanceId: checkpoint.created.catalog_instance_id,
    catalogEpoch: checkpoint.created.catalog_epoch,
    serverId: 'disposable-provider', admissionRevision: 1,
    settingsIdentity: 'qual-admin',
  };
  const fixture = database(path.join(config.work_dir, 'client-profile-resume-v2.sqlite'));
  try {
    fixture.native.prepare(`INSERT INTO published_waveform_profiles
      (source_key,track_id,sample_rate,duration_ms,ref_lufs,start_ramp,end_ramp,analyzer_ver,analyzed_at)
      VALUES (?,?,?,?,?,?,?,?,?)`).run(
      owner.key, 'track-b', 48000, 210, -9, Buffer.from('old-start'),
      Buffer.from('old-end'), 1, '2026-09-23T00:00:00Z',
    );
    const interrupted = accountClient();
    const create = interrupted.createProfileV2Session.bind(interrupted);
    interrupted.createProfileV2Session = (catalogId: string) => create(catalogId, 1);
    const page = interrupted.getProfileV2SnapshotPage.bind(interrupted);
    let pageCalls = 0;
    interrupted.getProfileV2SnapshotPage = async (...args) => {
      pageCalls += 1;
      if (pageCalls === 2) throw new Error('injected transport interruption');
      return page(...args);
    };
    await expect(runProfileV2Bootstrap(interrupted, fixture.db, owner,
      async () => undefined)).rejects.toThrow('injected transport interruption');
    expect(fixture.rows(owner.key)).toEqual([{ track_id: 'track-b', ref_lufs: -9 }]);
    expect((await getProfileV2State(fixture.db, owner))?.snapshot_rows_seen).toBe(1);
    expect(fixture.native.prepare('SELECT track_id FROM published_profile_staging').all())
      .toEqual([{ track_id: 'track-a' }]);

    fixture.reopen();
    const resumed = accountClient();
    await expect(runProfileV2Bootstrap(resumed, fixture.db, owner,
      async () => undefined)).resolves.toBe(3);
    expect(fixture.rows(owner.key).map((row) => row.track_id))
      .toEqual(['track-a', 'track-c', 'track-d']);
    expect(await getPublishedProfileCursor(fixture.db, owner)).toBeTruthy();
    expect(await getProfileV2State(fixture.db, owner)).toBeNull();
  } finally {
    fixture.native.close();
    restoreFetch();
  }
}, 30_000);

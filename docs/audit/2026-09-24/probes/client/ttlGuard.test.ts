import {
  decideProviderIdentityAdmission,
  providerInstanceFingerprint,
} from '@services/providerIdentityAdmission';
import { createProviderSourceProof } from '@services/providerSourceVerification';
import {
  resolveProfilePublicationSource,
  profilePublicationSourceStillAdmitted,
} from '@store/profilePublicationSource';
import { useProviderIdentityStore } from '@store/providerIdentityStore';
import { useSettingsStore } from '@store/settingsStore';
import md5 from 'js-md5';

const T0 = 1_800_000_000_000;

it('in-flight profile guard flips to stale after the 5-minute identity TTL', async () => {
  const now = jest.spyOn(Date, 'now').mockReturnValue(T0);
  const provider = { type: 'navidrome' as const, url: 'https://navi.example', username: 'u' };
  const analysis: any = {
    url: 'https://am.example', authMode: 'token', serverId: 'srv', catalogInstanceId: 'cat',
  };
  analysis.sourceProof = createProviderSourceProof(analysis, provider as never, ['lib']);
  useSettingsStore.setState({ providerConfig: provider, audioMuseConfig: analysis, demoMode: false } as never);
  const fingerprint = providerInstanceFingerprint(provider as never);
  const admission = decideProviderIdentityAdmission({
    previousServerVersion: '0.63.2', serverVersion: '0.63.2', serverType: 'navidrome',
    checkedAt: T0, pluginShieldAvailable: false, transition: null, audioMuseHealth: null,
    locallyAppliedTransitionIds: [],
  });
  useProviderIdentityStore.setState({
    activeFingerprint: fingerprint,
    observations: { [fingerprint]: {
      fingerprint, providerType: 'navidrome', serverType: 'navidrome',
      previousServerVersion: '0.63.2', currentServerVersion: '0.63.2', checkedAt: T0,
      admission, locallyAppliedTransitionIds: [],
    } },
  } as never);
  let sourceKey = '';
  const db: any = {
    getFirstAsync: async (sql: string) => {
      if (sql.includes('sync_stream_state')) return {
        catalog_instance_id: 'cat', current_server_id: 'srv', stream: 'catalog', schema_version: 1,
        epoch: 'e1', cursor: 'c', last_seq: 7, generation: 3, last_success_at: 'x',
      };
      if (sql.includes('profile_active_catalog')) return { source_key: sourceKey };
      return null;
    },
  };
  const settingsIdentity = (md5 as any)(JSON.stringify([fingerprint, 'https://am.example', 'token', 'installation-token', 'srv', 'cat']));
  sourceKey = (md5 as any)(`${settingsIdentity}\u0000e1`);
  const source = await resolveProfilePublicationSource(db, true);
  expect(source).not.toBeNull();
  expect(await profilePublicationSourceStillAdmitted(db, source!)).toBe(true);
  now.mockReturnValue(T0 + 4 * 60_000);
  expect(await profilePublicationSourceStillAdmitted(db, source!)).toBe(true);
  now.mockReturnValue(T0 + 5 * 60_000 + 1_000);
  // Nothing about the source changed; only wall-clock age of identity evidence.
  expect(await profilePublicationSourceStillAdmitted(db, source!)).toBe(false);
});

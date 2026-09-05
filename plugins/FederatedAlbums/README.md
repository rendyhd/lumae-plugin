# Friend Album Discovery

This plugin is in **private development** and excluded from the public manifest.
The sharing host must support scoped plugin bearer authentication through
`set_bearer_authenticator`. Missing support is reported by `/api/health`, and
pairing-token creation is disabled until that host extension is available.

Friend Album Discovery connects authenticated AudioMuse-AI instances and adds
two album-level features:

- three albums a listener does not own, matched to their Sonic Fingerprint;
- sonically similar albums across the local and connected friend catalogues.

The shared catalogue contains normal album metadata and a versioned Lumae Album
Dynamics fingerprint (album mean, up to three poles, spread, path, energy, and
mood statistics). It does not contain audio, filenames, listening history,
track identifiers, or individual track embeddings. Remote catalogue rows are
never re-exported, so federation does not become friend-of-a-friend sharing.

Album art is not stored in the catalogue. It is fetched on demand through a
size-limited authenticated proxy and remains subject to the media server's
existing access rights.

## Pairing

1. Install the plugin on both AudioMuse-AI instances.
2. On the sharing instance, create a pairing token from **Friend Albums** and
   copy it once.
3. On the receiving instance, enter the friend's AudioMuse base URL and token.
4. The connection returns HTTP 202. A durable background sync builds the cache;
   the connections view reports pending, running, complete or failed status.

Pairing tokens are revocable and accepted only for `GET` requests to the local
fingerprint catalogue and artwork endpoint. The database stores only their
SHA-256 hashes. The receiving instance stores the scoped read credential so it
can refresh later; it never stores the friend's password or global API token.

## Lumae

Lumae keeps its local Album Dynamics calculation on-device and persists each
fingerprint until the album's ordered track signature changes. If this plugin
is unavailable or the device is offline, local Similar Albums still works.
When online, friend candidates are appended with a friend badge. The three
friend recommendations use Lumae's native Sonic Fingerprint and send only its
200-dimensional centroid to the user's own AudioMuse-AI instance.

Connections and all derived artwork/catalogue operations are owner-scoped.
Search uses bounded SQL queries and similarity uses an approximate indexed
shortlist before the shared golden-tested scoring calculation.
See the [runtime and federation guide](../../runtime/README.md#friend-album-discovery)
for sync budgets, recovery and explicit Core 3 source selection.

# LUM-006: bounded backfill eligibility

## Decision

A `ready` profile with a NULL or empty stored media signature is eligible for a signature mismatch retry only when its current signature is known. For published catalogue rows, that requires a nonempty `media_fp`. Without one, `catalog-media:` is a placeholder, not evidence that the downloaded file changed. The SQL ready branch now requires a nonempty `media_fp`, and the Python predicate rejects the placeholder. Ready rows with matching signatures remain ineligible. Pending statuses, ordinary failed rows, skipped-no-file retry behavior, source selection, and the batch limit are unchanged.

## Evidence

A disposable PostgreSQL 17 regression places ready NULL and empty-signature rows ahead of a stale row with batch size one. With known fingerprints, it checks that each row is selected in order as its predecessor is marked processed. With NULL and empty fingerprints, it checks that the later stale row progresses first and that each earlier ready row becomes eligible when its fingerprint arrives. Both global and source-scoped profile selection run. Fake-row tests cover known signatures and reject the `catalog-media:` placeholder in both bounded and all-ID selection.

The first red run failed four targeted cases because Python discarded ready rows selected by SQL. After the initial predicate change, a second red run failed four targeted missing-fingerprint cases: two Python cases and two PostgreSQL selection modes. The final serial focused run uses the disposable `LUMAE_POSTGRES_TEST_DSN` and isolated `.pytest-tmp/lum006-sentinel-final` test paths.

The wider plugin suite and client behavior remain for the lead's gates.

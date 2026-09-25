# Representative-scale end-to-end gate (`scripts/e2e/`)

The server half of plan WP **P2-6** (`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`).
It serves the real plugin against the P0-3 representative fixture (94k
published profiles with real-size edges, 132k catalogue tracks) and drives it
with a protocol-faithful client, then asserts the gate criteria. The client
half (the real Auralscape sync code) is §H C-8. Results of each run go to
`docs/perf/E2E-<date>.md` and `.json`; run it on every release candidate.

These are harness scripts, not tests: pytest never collects `scripts/`.

| File | What it is |
|---|---|
| `seed_representative.py` | Seeds the fixture with the P0-3 seeder (`scripts/perf/seed.py`) and persists the journal retention limit, as plugin start-up maintenance does. `--host-schema stub` (default) for the local runner; `--host-schema audiomuse` seeds a database whose tables the stock host created. |
| `host_app.py` | A minimal host: the real plugin blueprint on the `scripts/perf/stub_host.py` stub, with per-request connections (like AudioMuse `database.get_db`), optional bearer auth and a harness-only `X-E2E-User` account header. |
| `run_server_matrix.py` | Starts gunicorn (`gthread`, 1 worker x 4 threads, the stock supervisord topology) with `host_app`, or targets the stock host (`--host docker`), runs the scenarios and writes JSON. |
| `client_sim.py` | The client: capability detection, v2 bootstrap (sliding expiry, `client_request_id`, `page_size` 50, gzip), catch-up, release, `/profiles/changes`, the legacy path, `Retry-After`, a SQLite "phone" with per-page checkpoints, edge digest and `boundaries` verification. Also a CLI (`sync`, `digest`). |
| `docker-compose.yml` | The stock AudioMuse-AI host pinned by digest (tree of 8aa1639c), PostgreSQL 15, the plugin bind-mounted, a Navidrome ping stub. `docker-compose.pg17.yml`: PostgreSQL 17. `docker-compose.external-db.yml`: an existing database, host network. |
| `host/install_plugin.py` | Runs in the host image: `init` creates the host schema, `register` adds the plugin row and runs its install hooks. |
| `host/navidrome_stub.py` | Answers the provider ping of `/api/catalog/health` like a LAN Navidrome. |
| `requirements-e2e.txt` | `gunicorn==25.3.0` (the image's release). |

## Local runner

Needs PostgreSQL 15/16/17 and `requirements-dev.txt` plus `requirements-e2e.txt`
(`pip install --ignore-installed blinker -r requirements-dev.txt -r scripts/e2e/requirements-e2e.txt`).
Disk: about 2.6 GB for the database, up to 0.7 GB per simulated device, and up
to 2 GB more for the capped catch-up.

```sh
psql -h 127.0.0.1 -p <port> -U postgres -c "create database e2e_rep owner lumae_test"
export LUMAE_E2E_DSN=postgresql://lumae_test@127.0.0.1:<port>/e2e_rep
python3 scripts/e2e/seed_representative.py --reset                       # ~4 min at scale 1
python3 scripts/e2e/run_server_matrix.py --scratch /var/tmp/e2e --out e2e.json
```

`--scenarios a,b` selects scenarios; `name#label` runs one again (for example
`first_load_v2#warm`). `--scale 0.02` seeds a quick fixture for development.
`--explain-analyze` records `EXPLAIN ANALYZE` (instead of `EXPLAIN`) of the v2
page query in each first load. The local gunicorn runs with
`--no-control-socket`, so several runners can share a machine.

A scenario ends PASS, FAIL, PENDING (only a criterion that waits for unbuilt
server work failed, e.g. K6/P3-2) or ERROR (it raised; its devices' v2
sessions are released and their SQLite files removed). Every data scenario
first asserts that the server publishes profiles and edges, so an empty
fixture cannot pass as 0 = 0.

## Stock host (Docker)

With the bundled PostgreSQL (15; add `-f docker-compose.pg17.yml` for 17):

```sh
cd scripts/e2e
docker compose up -d postgres navidrome-stub
docker compose run --rm lumae-install init
python3 seed_representative.py --host-schema audiomuse --navidrome-url http://navidrome-stub:4533 \
    --dsn postgresql://audiomuse:audiomusepassword@127.0.0.1:55433/audiomusedb
docker compose run --rm lumae-install register
docker compose up -d audiomuse-ai-flask
python3 run_server_matrix.py --host docker --compose docker-compose.yml \
    --dsn postgresql://audiomuse:audiomusepassword@127.0.0.1:55433/audiomusedb --out e2e-docker.json
```

Against an existing database (for example the one the local runner uses),
use `docker-compose.external-db.yml` with `E2E_PG_HOST`, `E2E_PG_PORT`,
`E2E_PG_USER`, `E2E_PG_PASSWORD` and `E2E_PG_DB`, seed with
`--navidrome-url http://127.0.0.1:4533`, and pass
`--compose docker-compose.yml,docker-compose.external-db.yml` to the runner.
The runner's kill points send SIGKILL to the host's gunicorn master and worker;
supervisord restarts it. `AUTH_ENABLED` is false by default (anonymous, like
contract §1.2); with `AUTH_ENABLED=true` pass the host's API token as
`--auth-token`.

## Scenarios

| Scenario | Asserts |
|---|---|
| `idle_routes` | Status routes over HTTP with nothing running (reference). |
| `first_load_v2`, `first_load_legacy` | A fresh device loads every waveform and edge profile once, in one run: one create request, one request per page, no restart, no retry or connection error, no 503, no invalid edge, the device dataset equals the server's (canonical digest of every profile and edge), health <= 50 ms and `/settings/status` <= 100 ms p95 while it runs. |
| `reanalysis_noop` | The analysis hook for every song (an AudioMuse re-analysis pass) and identical forced completions append 0 events, leave every stored edge row (keys and `updated_at`) and every published profile unchanged, and schedule or touch no edge job; the device's next delta downloads nothing. Loads its own device when `first_load_v2` did not run. |
| `concurrent_devices` | Two devices load at the same time (separate processes); both complete once and match the server. |
| `creators` | Two concurrent creators, same source and different sources: create p95 <= 5 s, no 503, no client timeout (P2-4). |
| `kill_restart` | kill -9 + restart at 8 points while the library changes: inside the create's snapshot capture and inside the first catch-up capture (the runner holds a lock the capture needs, waits until the capture blocks on it, then kills), after create, mid-snapshot, snapshot end, mid-catch-up, before release, during deltas. In-flight kills wait until the request is on the wire, and each cut-off request must fail on the client. The lost create is retried with the same `client_request_id` and its orphaned session replaced (K5); the device resumes from its checkpoint, never re-creates, and ends equal to the server. |
| `lum005_k6` | A full waveform republish reaches a device at <= 1 KB per track with K6 edge references. **Pending P3-2**: without `capabilities.profile_stream.edge_refs` it measures the no-K6 baseline and reports PENDING. |
| `capped_catchup` | First catch-up capture time, WAL, table size and server memory up to 4 x retention events (P2-4). |

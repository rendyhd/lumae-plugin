# Lumae Master Remediation — Astra Orchestration Brief

**Project:** Lumae / Auralscape  
**Repositories:**  
- `github.com/rendyhd/lumae-plugin` — server-side AudioMuse-AI plugin  
- `github.com/rendyhd/Auralscape` — Lumae client

**Purpose:** This document is the single source of truth for orchestrating the remediation discovered in the 22 September 2026 Lumae review.

---

# 1. Mission

Bring the Lumae server plugin and Auralscape integration to a reliability-qualified state by fixing the reviewed architecture, synchronization, concurrency, audio-analysis, performance, UI, and clarity issues without losing user data, weakening source-identity protections, or hiding incomplete qualification.

The work must be executed incrementally. Do not perform a broad rewrite.

Astra owns:
- the overall plan;
- dependency ordering;
- task decomposition;
- assignment to Terra or Sol;
- integration review;
- the live remediation ledger;
- cross-repository consistency;
- release readiness.

Terra should normally execute bounded implementation tasks.

Sol should be used selectively for:
- concurrency / transaction architecture;
- protocol and cursor correctness;
- audio measurement design;
- difficult RCA;
- independent review of high-risk changes;
- cases where two plausible designs need adjudication.

Do not use Sol for routine mechanical implementation if Terra can execute an already-decided plan.

---

# 2. Ground rules

1. **Verify before changing.**  
   The original review used plugin release 1.2.5 at commit `4276398c671f3e2713212488f4e72a32553697d7` and selected Auralscape source around `82f4917ef7d55971ff6a6e784b57bb076373f10b`. Current HEAD may already differ.

2. **Every finding begins with adjudication.**  
   Mark it:
   - PRESENT
   - PARTIALLY_FIXED
   - ALREADY_FIXED
   - NOT_REPRODUCED
   - BLOCKED_BY_ENVIRONMENT

3. **Every confirmed defect needs a regression.**  
   Prefer a failing test before the fix and a passing test after it.

4. **Do not call skipped tests passes.**

5. **Never trade correctness for performance.**
   Do not:
   - increase concurrency blindly;
   - greatly enlarge pages without profiling;
   - extend timeouts as a substitute for recovery;
   - disable provider/source identity safeguards;
   - reset user databases as a default recovery strategy.

6. **Preserve user data.**
   Shelves, collections, personal discovery, memories, order, undo state, outboxes, mutation receipts, and catalogue/source identity must survive compatible migrations.

7. **Respect source identity.**
   Navidrome is currently the supported provider. Catalogue instance identity and provider transition protection are non-negotiable.

8. **Version semantic changes.**
   Especially:
   - loudness / normalization;
   - MixRamp semantics if altered;
   - cursor/feed semantics;
   - public DTO/protocol changes.

9. **No production deployment during remediation.**
   Produce candidate commits and qualification evidence only unless the user explicitly asks for deployment.

10. **Keep one living overview.**
    Maintain `docs/remediation/LUMAE_REMEDIATION_LEDGER.md` (or the closest sensible repo-neutral location if cross-repo docs live elsewhere).

---

# 3. Required ledger format

Astra must maintain this table continuously:

| ID | Finding | Repo | Priority | Status | Owner | Dependency | Implementation SHA | Verification | Remaining gate |
|---|---|---|---|---|---|---|---|---|---|

Use these statuses:
- NOT_STARTED
- INVESTIGATING
- CONFIRMED
- IMPLEMENTING
- IMPLEMENTED_UNVERIFIED
- VERIFIED
- BLOCKED
- NO_LONGER_APPLICABLE

Below the table keep four sections:

## Active task
- exact scope;
- assigned model;
- branch / worktree;
- starting SHA;
- expected outputs;
- acceptance tests.

## Decisions
For each architectural choice:
- options considered;
- chosen design;
- why;
- compatibility implications;
- reviewer.

## Evidence
- test commands;
- benchmark commands;
- important results;
- fixture sizes;
- device / host assumptions.

## Deferred / unresolved
Anything not proven must remain visible here.

Astra must update the ledger after every delegated task.

---

# 4. Reviewed findings

## P1 / correctness and reliability

### LUM-001 — Profile journal sequence allocation race
Concurrent profile publishers can allocate the same stream sequence.

Goal:
- atomic, per-source publication ordering;
- profile row + change event + committed head are one transaction;
- legacy and edge-profile publication follow the same invariant.

Key validation:
- two independent PostgreSQL connections;
- different tracks;
- forced rollback;
- legacy vs edge publication.

---

### LUM-002 — Collection revision checks allow concurrent overwrite
Two requests can both read the same revision and both succeed.

Goal:
- true optimistic compare-and-swap or locked revision check;
- one success and one 409 for incompatible concurrent edits.

---

### LUM-003 — Collection idempotency receipt is not atomic with mutation
Mutation can commit before its receipt.

Goal:
- mutation and receipt share one transaction owner;
- idempotency key is bound to request fingerprint / operation context;
- replay is stable;
- restore cannot duplicate collections after a lost response.

---

### LUM-004 — Collection feed can miss a late-committing event
Sequence allocation order is not commit order.

Goal:
- a committed publication frontier or equivalent protocol;
- no client can advance beyond an event that may later commit;
- migration must account for old writers.

---

### LUM-005 — Loudness reference is not standards-qualified integrated LUFS
Observed issues include:
- fixed filter coefficients at arbitrary native sample rates;
- channel energy aggregation behavior;
- simplified gating;
- partial-window semantics.

Goal:
- first qualify a measurement contract;
- then implement a new version;
- preserve old profiles until new measurements are safely adopted;
- test normalization separately from relative MixRamp behavior.

---

### LUM-006 — Backfill SQL/Python eligibility mismatch can starve later work
Rows selected by SQL can be rejected by Python, while LIMIT prevents reaching later eligible rows.

Goal:
- one canonical eligibility truth table;
- bounded queries still make progress;
- missing signature semantics are explicit.

---

### LUM-009 — Late provider preflight turns productive sync into failure
Long sync completes useful work, then a later provider phase fails because verification expired or is still in flight.

Goal:
- renew or await admission immediately before protected late phases;
- never bypass genuine identity mismatch;
- successful streams remain successful;
- resume only unfinished phases.

---

### LUM-010 — Large profile bootstrap lacks durable page-level resume
Large imports can restart from scratch.

Goal:
- resumable server/client bootstrap contract;
- persisted client staging checkpoint;
- explicit epoch/source/schema validation;
- finite catch-up to a known stream head;
- safe restart after interruption.

---

## P2 / recovery, performance, UX, clarity

### LUM-007 — Failed profiles do not automatically recover on repaired media
Goal:
- failure taxonomy;
- bounded retries/backoff;
- media-revision change re-enables work;
- permanent bad media does not monopolize queue.

### LUM-008 — ready → pending → failed needs explicit published-state semantics
Goal:
- separate job state from published validity;
- old clients and clean bootstrap converge.

### LUM-011 — Catalogue health path is too expensive for routine admission
Goal:
- measure first;
- separate cheap committed summary from expensive diagnostic/probe work where safe;
- preserve freshness and source-safety semantics.

### LUM-012 — Analysis projection is memory/write heavy
Goal:
- benchmark no-change, small-delta, full rebuild;
- reduce demonstrated excess work without weakening immutable publication.

### LUM-013 — Workbench silently chooses one catalogue
Goal:
- explicit catalogue scope through browse/detail/save/playback.

### LUM-014 — Album browsing discards provider album identity
Goal:
- preserve catalogue-scoped album identity;
- same-name editions remain distinct.

### LUM-015 — Year sort exists while underlying year is NULL
Goal:
- populate authoritative year with defined semantics or remove/disable the option.

### LUM-016 — Search/count/OFFSET can be library-scale
Goal:
- EXPLAIN ANALYZE;
- indexed normalized search representation where justified;
- reduce repeated exact totals;
- stable pagination.

### LUM-017 — Settings readiness conflates usable data with enrichment
Goal:
- independent status for catalogue, analysis, profiles, relationships, and provider/personal state;
- targeted recovery actions.

### LUM-018 — Timeout arguments are misleading / not proven effective
Goal:
- identify actual host execution-limit mechanism;
- test cancellation, revocation, worker death, decoder stalls;
- remove fake guarantees.

### LUM-019 — Docs still teach removed Radio DJ behavior
Goal:
- release-specific capability matrix;
- remove current setup guidance for retired functionality;
- preserve historical changelog context.

### LUM-020 — Transaction ownership and state machines are fragmented
Goal:
- structural cleanup only after behavioral fixes;
- clarify API / orchestration / repository / job / presentation ownership;
- preserve queued task names and compatibility.

### LUM-021 — Diagnostics do not explain failures well enough
Goal:
- safe operation names;
- HTTP/error code;
- bootstrap/resume reason;
- per-stream outcome;
- transport/decode/stage/publish timing where measurable;
- precise definitions of imported vs changed vs published rows.

---

# 5. Incident evidence that must remain in scope

## Decoder incident
A worker failed inside PyAV while decoding a track:
`av.error.InvalidDataError` from `avcodec_send_packet()`.

Do not assume the adjacent log entry for a successfully downloaded track named "Brazil" identifies the failing media. It came from a different worker/track context.

The remediation should improve:
- safe decoder diagnostics;
- failure classification;
- retry behavior;
- revision-aware recovery;
- worker cleanup / timeout qualification.

Do not claim the exact production media root cause was reproduced unless a matching file is obtained.

## Long mobile sync incident
The supplied diagnostic showed:
- a very large analysis bootstrap;
- large profile import;
- useful data committed;
- expensive phone-side SQLite work;
- a later provider identity/admission failure;
- the overall run reported unsuccessful despite meaningful progress.

Therefore performance and reliability work must cover both:
- server/plugin behavior;
- Auralscape orchestration/local database behavior.

Do not describe this as a server-only performance problem.

---

# 6. Execution roadmap

Astra may adjust task boundaries after inspecting current HEAD, but must preserve dependencies and rationale.

## PHASE 0 — Baseline and observability

### Task 0A — Current-state adjudication
Repos: both

Deliver:
- current SHAs;
- clean/dirty status;
- disposition for all LUM findings;
- test environment inventory;
- PostgreSQL availability;
- host integration availability;
- Android/iOS/device gates;
- baseline suite results;
- skipped tests list.

Default executor: Terra  
Escalate to Sol only if current architecture differs materially from the reviewed design.

### Task 0B — Diagnostics
Repos: both

Implement LUM-021 before major remediation where feasible so subsequent evidence is easier to interpret.

---

# PHASE 1 — Data integrity

These precede performance concurrency changes.

### Task 1A — Profile journal publication
Finding: LUM-001  
Repo: plugin  
Default: Terra implementation  
Sol: design/review strongly recommended

### Task 1B — Collection transaction ownership
Findings: LUM-002 + LUM-003  
Repo: plugin  
Default: Terra  
Sol: review transaction semantics

### Task 1C — Collection committed feed
Finding: LUM-004  
Repo: plugin  
Default: Sol design → Terra implementation → Sol review

Do not mark this fixed merely because BIGSERIAL is unique.

---

# PHASE 2 — Profile lifecycle and recovery

### Task 2A — Backfill eligibility
Finding: LUM-006  
Repo: plugin  
Default: Terra

### Task 2B — Published profile vs job state
Findings: LUM-007 + LUM-008  
Repo: plugin  
Default: Sol design → Terra implementation

### Task 2C — Decoder / execution-limit qualification
Finding: LUM-018 + supplied PyAV incident  
Repo: plugin + verified AudioMuse host boundary  
Default: Terra investigation; Sol if hard process-isolation design is needed

---

# PHASE 3 — Sync reliability

### Task 3A — Late preflight
Finding: LUM-009  
Repo: Auralscape  
Default: Terra implementation; Sol review if provider-identity state machine changes

### Task 3B — Server resumable profile bootstrap contract
Finding: LUM-010  
Repo: plugin  
Default: Sol protocol design → Terra implementation

### Task 3C — Client resumable import
Finding: LUM-010  
Repo: Auralscape  
Default: Terra implementation

Required tests:
- process kill after many pages;
- resume;
- source/account change;
- epoch invalidation;
- replayed page token;
- concurrent server publication;
- unchanged second sync.

Only after resumability passes may Astra approve performance parallelism or prefetch changes.

---

# PHASE 4 — Performance

### Task 4A — Client SQLite performance
Repo: Auralscape  
Use collected diagnostics to profile:
- staging transactions;
- publication;
- `effective_embeddings`;
- cache invalidation;
- warmups;
- interactive queue waits.

Default: Terra  
Sol only for difficult RCA.

### Task 4B — Health/readiness path
Finding: LUM-011  
Repo: plugin

### Task 4C — Analysis projection
Finding: LUM-012  
Repo: plugin

### Task 4D — Workbench query performance
Finding: LUM-016  
Repo: plugin

All performance changes require before/after measurements with the same fixture and semantics.

Representative fixture target:
- ~69k analysis items;
- ~132k links;
- ~94k profiles;
- ~100k tracks for browser/search planning.

---

# PHASE 5 — Audio correctness

Keep this separate from ordinary bugfixes.

### Task 5A — Audio measurement qualification
Finding: LUM-005  
Repo: plugin with read access to Auralscape consumers  
Default: Sol

No production semantic change in this task.

Deliver a written audio contract covering:
- measurement method;
- sample-rate handling;
- channel weighting;
- mono/dual-mono policy;
- gating;
- tails;
- silence;
- finite values;
- measurement version;
- ramp version;
- compatibility;
- regeneration;
- fallback;
- activation;
- rollback limitations.

Use an independent reference such as FFmpeg ebur128 or another justified implementation, with exact version/options recorded.

### Task 5B — Versioned producer
Repo: plugin  
Default: Terra following Sol design

### Task 5C — Versioned consumer
Repo: Auralscape  
Default: Terra  
Sol reviews compatibility matrix and playback semantics.

Never silently reinterpret analyzer v1 data.

---

# PHASE 6 — Workbench identity and UX

### Task 6A — Source/album/year identity
Findings: LUM-013/014/015  
Repo: plugin  
Default: Terra; Sol review for migration ambiguity

Migration must never guess the source for an ambiguous historic item.

### Task 6B — Workbench search/pagination
Finding: LUM-016  
Repo: plugin  
Default: Terra

### Task 6C — Status/readiness UX
Finding: LUM-017  
Repos: plugin + Auralscape  
Default: Terra

Preserve current useful settings-page polling behavior:
- dirty fields;
- focus;
- selection;
- open `<details>`;
- scroll position;
- hidden-page polling pause.

Do not redesign the Album Shelf as part of this remediation.

---

# PHASE 7 — Security, docs, and structure

### Task 7A — Permission / privacy qualification
Repos: plugin + host/client checks

Test:
- anonymous;
- bearer;
- authenticated user;
- malformed session;
- admin/non-admin;
- cross-principal access;
- cross-source access;
- CSRF through host;
- request-size limits;
- malformed DTOs;
- nonfinite numbers;
- escaping.

The shared bearer principal is intentional behavior unless product requirements explicitly change it.

### Task 7B — Documentation cleanup
Finding: LUM-019  
Repo: plugin

### Task 7C — Structural consolidation
Finding: LUM-020  
Repo: plugin

Do this last.

Do not combine broad file moves with behavioral changes.

Preserve:
- endpoint names;
- task names;
- cron task types;
- dotted queued function paths;
- migration idempotence.

---

# 7. Model delegation policy

## Terra — default executor

Use Terra for:
- bounded code changes;
- test writing;
- migration implementation after design is decided;
- diagnostics;
- benchmark harnesses;
- UI implementation;
- documentation;
- query/index work when requirements are clear;
- client sync implementation after protocol design.

Terra task prompts must contain:
1. exact scope;
2. relevant finding IDs;
3. files/functions likely involved;
4. invariants that must not change;
5. tests that must fail before / pass after;
6. prohibited shortcuts;
7. expected handoff.

Terra must stop after its assigned task.

## Sol — targeted reasoning/review

Use Sol for:
- concurrency protocols;
- transaction/commit ordering;
- cursor correctness;
- protocol versioning;
- loudness qualification;
- complex state machines;
- root-cause analysis with multiple plausible causes;
- independent review of P1 changes;
- cross-repo compatibility decisions.

Sol should generally produce:
- decision;
- invariant;
- counterexamples;
- recommended design;
- acceptance tests;
rather than making large mechanical patches.

## Astra — orchestrator

Astra must:
- retain whole-project context;
- decide dependencies;
- avoid duplicate work;
- reconcile task outputs;
- update ledger;
- decide whether a Sol review is warranted;
- prevent one task from silently expanding into another;
- refuse to mark work complete when environment gates remain open.

---

# 8. Delegated task template

Astra should generate Terra/Sol prompts in this shape:

```text
You are working on Lumae remediation task <ID>.

Repository:
<repo>

Starting point:
<exact SHA/branch>

Relevant findings:
<LUM IDs>

Objective:
<one bounded outcome>

Current-source evidence:
<functions/files and why the finding is still present>

Invariants:
- preserve ...
- preserve ...
- do not ...

Required reproduction:
<deterministic failing case>

Implementation constraints:
<smallest safe change>

Required tests:
- ...
- ...
- ...

Compatibility/migration requirements:
- ...

Do not:
- broaden scope;
- reset user data;
- disable identity checks;
- introduce unbounded work;
- mark skipped tests as passed.

Return:
1. what you verified;
2. design used;
3. files changed;
4. test commands and results;
5. migration / compatibility impact;
6. benchmarks if relevant;
7. remaining gates;
8. exact commit SHA if committed.

Stop after this task.
```

---

# 9. Review loop

Every P1 implementation must receive an independent review before Astra advances its dependent tasks.

Reviewer prompt should challenge:
- whether the regression would fail on old code;
- all writers/readers, not only one code path;
- lock ordering;
- commit ordering;
- rollback;
- cancellation;
- restart;
- old worker compatibility;
- old/new client compatibility;
- source/principal isolation;
- migration;
- silent data loss;
- performance regressions.

Review verdict:
- PASS
- CHANGES_REQUIRED
- BLOCKED

Astra records the verdict in the ledger.

When CHANGES_REQUIRED:
- delegate only the correction;
- do not start the next dependent task.

---

# 10. Release gates

Astra may recommend a release candidate only when the selected release wave satisfies all relevant gates.

## Data-integrity gate
Must include:
- profile journal concurrency;
- collection concurrent revisions;
- idempotency crash/replay;
- late collection commit;
- migration from prior release.

## Profile-recovery gate
Must include:
- NULL/missing signature starvation;
- transient failure;
- permanent invalid media;
- repaired/replaced media;
- ready → pending → failed convergence.

## Sync gate
Must include:
- process interruption/resume;
- unchanged second sync;
- concurrent server publication during bootstrap;
- expired epoch/history;
- late provider preflight;
- real identity mismatch remains blocked.

## Performance gate
Must include comparable:
- cold;
- warm unchanged;
- small delta;
- resumed;
- browsing during sync;
- playback during sync.

Report:
- wall time;
- server query/serialization where measurable;
- bytes;
- rows;
- transaction duration;
- client SQLite time;
- interactive wait;
- memory where available.

Do not mix cellular and LAN measurements without labeling them.

## Audio gate
Before enabling new measurement semantics:
- reference fixtures pass;
- old/new producer-consumer matrix passes;
- old profiles remain usable until replacement is valid;
- normalization/headroom is tested;
- relative ramp behavior is separately qualified;
- rollback/disable path is documented.

## UI gate
At least:
- keyboard;
- focus;
- dialog escape/focus return;
- narrow layout;
- long titles;
- interrupted requests;
- high zoom;
- status partial-success cases.

## Security/privacy gate
Host-backed auth/authorization checks complete or explicitly BLOCKED.

---

# 11. Release strategy

Prefer small release waves.

Suggested sequence:

### Safety release
- LUM-001
- LUM-002
- LUM-003
- LUM-004
- LUM-006
- LUM-007
- LUM-008
- LUM-009
- LUM-018 where safe
- LUM-021

### Sync/performance release
- LUM-010
- LUM-011
- LUM-012
- LUM-016

### Workbench/UX release
- LUM-013
- LUM-014
- LUM-015
- LUM-017
- LUM-019

### Audio semantic release
- LUM-005 producer/consumer adoption

### Structural release
- LUM-020 only after preceding behavior is protected.

Astra may reorder based on current HEAD, but must document why.

---

# 12. Definition of done

The remediation is NOT complete merely because:
- current unit tests pass;
- CI is green;
- code was refactored;
- a benchmark improved once;
- a finding looks unlikely;
- an agent says it fixed the problem.

It is complete when:

1. every LUM finding has a final adjudication;
2. every confirmed P1 has an executable regression;
3. transaction/cursor invariants have concurrency tests;
4. long synchronization resumes safely;
5. partial success is represented correctly;
6. source identity remains fail-closed;
7. user-created data survives migrations;
8. performance has comparable measurements;
9. audio semantics are versioned and independently qualified;
10. UI status accurately distinguishes usable/current/preparing/deferred/failed;
11. security assumptions have been tested against the actual host where possible;
12. documentation matches shipped capabilities;
13. remaining physical-device or deployment gates are explicit;
14. the ledger contains exact SHAs and evidence for the final candidate.

---

# 13. Final Astra reporting format

At meaningful checkpoints Astra reports:

## Current position
- Phase:
- Active task:
- Completed:
- Blocked:
- Next dependency:

## Risk summary
- Critical correctness:
- Migration:
- Compatibility:
- Performance:
- Device/host qualification:

## Evidence added
- tests:
- benchmarks:
- architectural decisions:

## Changes since last checkpoint
Short, concrete list.

## Decision needed from user
Only include this if a genuinely product-level choice is required.
Do not ask the user to choose implementation details that Astra/Sol can resolve safely.

---

# 14. First action

Start with Phase 0.

Do not begin implementation immediately.

First:
1. inspect both repositories at current HEAD;
2. read relevant repository instructions;
3. record exact SHAs and worktree state;
4. compare current code to the reviewed findings;
5. create/update the remediation ledger;
6. run baseline tests and inventory environment gates;
7. return the initial orchestration dashboard;
8. then begin the first confirmed task.

The overarching objective is not "make the audit green."  
It is: **make Lumae's guarantees consistent, recoverable, measurable, and understandable while preserving the product's data and identity invariants.**

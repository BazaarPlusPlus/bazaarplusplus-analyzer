# Implementation plan: Analyzer V5 Rebuild (`bpp` CLI)

Working branch: `feat/v5-pipeline`. This plan is the phase map for implementing
the pipeline described in the two authority documents. Read both fully before
writing code:

- `docs/specs/2026-08-11-single-process-pipeline.md` — the spec: pipeline
  design, module interfaces, 17 checks, 12 acceptance criteria. **On any
  conflict between this plan and the spec, the spec wins.** If the spec is
  ambiguous or looks wrong, STOP and surface the question instead of guessing.
- `contracts/v5/` (README + 5 JSON Schemas) — the **frozen** publish contract.
  Every payload must validate against these schemas (spec check 9). Do not
  edit the schemas.

## Hard constraints

1. **Legacy repo `/Users/yxinyu/codes/workspaces/bpp/bazaarplusplus-analyzers`
   is strictly read-only.** It is the domain-semantics reference only: Bundle
   Server HTTP protocol and pagination, Bundle validation (sha256 / magic /
   segment digests), settled-hour rule, the five-table projection semantics,
   performance_rating / ghost / Wilson metric definitions. Understand the
   semantics, then rewrite from scratch. Do not copy its module structure, do
   not modify it, do not commit anything there.
2. **Never write to production R2** (any key under the `analyzer-v5/` prefix,
   including the pointer). The publish code path must exist and be complete,
   but is exercised only against a fake/local object store in tests. Real
   publishing is coordinated by the operator.
3. Configuration comes only from `.env` at the repo root (already in place:
   Bundle Server credentials, R2 credentials,
   `BPP_DATA_ROOT=/Users/yxinyu/bpp-state/analyzer-v5/data`). Ingesting from
   the real Bundle Server is allowed and encouraged (writes local facts only);
   start with conservative download concurrency.
4. All code, comments, docstrings in English. Conventional-commit style
   messages — but **git commits are performed by the dispatcher, not by the
   implementing agent** (the sandbox cannot take the parent repo's `.git`
   lock).

## Engineering conventions

- Python 3.13, package `bpp_analyzer`, distribution `bpp-analyzer`, CLI entry
  point `bpp`. A `.venv` with dependencies is pre-installed at the repo root;
  run everything through it (`.venv/bin/python`, `.venv/bin/pytest`,
  `.venv/bin/bpp`). If a new dependency is needed, add it to `pyproject.toml`
  and note it in the report — the dispatcher installs it.
- Deep-module style: behavior concentrates in the four module interfaces from
  the spec's Modules section (`bundle_source`, `fact_store`, `release`,
  `driver`); everything else is thin glue.
- Each of the 17 checks gets a test that fails if that check is removed
  (removal-detecting, not just happy-path).
- DuckDB builds are bit-deterministic per the spec's DuckDB rules; the
  "build the same window twice, compare bytes" test exists from day one of
  the build line.
- Tests must not require network; real-server runs are performed by the
  dispatcher. Use recorded fixtures / fakes for Bundle Server and object
  store.

## Phases

### Phase 1 — Ingest line

`bundle_source` + `fact_store` (commit/seal/abandon/verify) + lock + CLI
skeleton.

- `bundle_source.hour_index` / `stream` per the spec interface: full
  pagination, ordered deduplicated index, `raw_commit_sha256`, `HourExpired`,
  bounded download pool (conservative concurrency, look-ahead 8), per-Bundle
  sha256/magic/segment-digest verification, bundles in memory only.
- `fact_store.commit_hour` (stage → Parquet + `_commit.json` → atomic rename;
  identical re-commit → `reused`; differing re-commit → hard conflict),
  `seal_day`, `abandon_day`, `is_abandoned`, `seals()`, `hour_paths(days)`,
  `verify(day, deep)`.
- Projection of Bundle segments into the five hourly tables (`runs`,
  `battles`, `battle_cards`, `quality`, `quarantine`) — semantics from the
  legacy repo, schemas owned by this rebuild.
- `mkdir(.lock)` locking with heartbeat, stale takeover via atomic rename,
  ownership check, `BPP_MAX_RUN_SECONDS`.
- CLI skeleton: `bpp run` (heal+seal working; build/publish stubs OK in this
  phase), `bpp status --json`, `bpp verify`, exit codes 0/1/2/3/4.
- Driver heal loop: oldest-first over `--heal-days` (default 8), clamped at
  epoch 2026-08-07, settled-hour rule as a pure function, expired ⇒ abandon
  path, one failed hour ⇒ partial/exit 4.
- `status.json` + `runs.jsonl` writing (release fields may be placeholder
  until Phase 3).
- Checks 1–7 with removal-detecting tests.
- Acceptance targets this phase: #5 (hour-ingest RSS, asserted), #6 (no local
  bundle bytes), #7 (mutual exclusion), #11 (expired hour ⇒ abandoned).

### Phase 2 — Build line

DuckDB SQL + the five payload builders + `release.build`.

- One connection per build with the spec's settings; reads only via
  `read_parquet` on explicit `hour_paths`; every output surface totally
  ordered; float aggregates order-independent.
- Builders for `hero_daily`, `hero_window`, `builds`, `quality`,
  `release_manifest` producing canonical JSON bytes; `generated_at` derived
  from source time.
- Window rule (1–7 consecutive sealed days ending at anchor, never before
  epoch), `release_id` identity chain exactly as the spec's Identity section.
- Schema validation against `contracts/v5/` as a hard gate before the staging
  rename; stage → rename; existing release dir ⇒ reuse.
- Checks 8–11 with removal-detecting tests.
- Acceptance targets: #2 (contract validity), #4 (deterministic rebuild,
  threads > 1, byte-identical), #5 (7-day build RSS bound).

### Phase 3 — Publish line (fake store only)

- Object-store interface + fake/local implementation; real R2 client code may
  exist but no test or command may touch production R2.
- `publish` (per-key stat → PUT/skip/conflict, Cache-Control values, pointer
  last), `rollback` (+receipt +hold), hold semantics, `resume`.
- Checks 12–17 with removal-detecting tests.
- Acceptance targets: #1 (idempotent noop), #3 (crash convergence via fault
  injection at each seam), #8 (cheap no-op), #9 (late data ⇒ new release),
  #10 (rollback sticks).

### Phase 4 — Full acceptance + goldens + measurements

- All 12 acceptance criteria runnable (tests or scripts), whole suite green.
- After the dispatcher's real-data build: generate golden vectors into
  `contracts/v5/golden/` from the first full build and freeze them.
- Record any of the spec's "Unresolved, needs measurement" items observed
  along the way in `docs/measurements.md` — especially: does the Bundle
  Server return an error or an empty index for out-of-retention hours?

## Self-check

Before reporting a phase done, run from the repo root:

```
.venv/bin/python -m pytest -q
```

and for CLI-affecting phases also a smoke `bpp --help` plus the phase's
acceptance scripts. Report: what was done, acceptance-criteria status,
deviations from the spec (if any), open questions.

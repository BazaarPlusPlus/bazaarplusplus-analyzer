# Spec: Analyzer V5 Rebuild — Single-Process Pipeline and Publish Contract

A ground-up rewrite of Analyzer V5. One idempotent CLI converges "the newest
complete Source Day is published" with all state on disk and in R2, and
publishes a **new, two-layer consumer contract** designed from scratch. The V5
name and R2 prefix are kept; everything else is new — the rebuild lives in its
own repository (`bazaarplusplus-analyzer`) and carries over nothing from the
legacy stack (`bazaarplusplus-analyzers`) — not code, not payload shapes, not
data. It starts from an empty data root and bootstraps from the Bundle
Server's ~14-day retention. The **epoch day is 2026-08-07**: healing starts
there and no window ever reaches before it.

## Why this shape

Orchestrators (Dagster, Postgres, sensors, run keys, reapers) exist to track
progress that cannot be observed from outputs. Here every step's completion is
observable: an hourly fact partition exists and verifies, or it does not; a
release has its immutable objects in R2 with the public pointer at it, or it
does not. So the design rule is: **state lives only in completed, immutable,
content-addressed outputs, plus exactly one mutable pointer.**

**Red line**: if a mutable store (SQL upserts, row updates) ever enters the
publish path, either reduce it to a single-row pointer flip or bring
bookkeeping back deliberately, via ADR.

## Goals

- One command, `bpp run`, safe at any cadence, concurrently, and after
  `SIGKILL`.
- A publish contract where every file has one clear reason to exist: a
  mergeable **daily facts layer** and a self-contained **windowed products
  layer**. No consumer-named directories, no redundant projections.
- Peak memory bounded and declared. No full fact window in Python. Bundle
  bytes never touch local disk.
- Deterministic: identical committed inputs produce identical payload bytes,
  forever.

## Non-goals

- What invokes `bpp run` (launchd/cron/CI — anything satisfying the trigger
  contract).
- Compatibility with legacy payloads, golden vectors, or module boundaries.
  The legacy stack keeps publishing to `analyzer-v5/` untouched until the
  coordinated pointer flip (see Cutover); then it is deleted wholesale.
- Serving per-player profile queries. The public surface is static files; a
  queryable per-player API would be a different system.

---

# Part I — The publish contract

## Principles

1. **Name by content, not by consumer.** The legacy `web/`, `ladder/`, `mod/`
   directories encoded who read a file, which is why the same hero-day
   statistics existed twice and leaderboards three times. Files are named for
   what they contain; any consumer reads any file.
2. **Two layers.**
   - `daily/` — per-Source-Day facts: raw, mergeable counts and accumulators.
     Content depends only on that day, so the same day's file is byte-identical
     across releases and deduplicates in R2 automatically.
   - `window/` — products over the release window: anything that needs a
     full-window scan, an algorithm (scoring, percentiles), or a deliberate
     trim (privacy). Self-contained and presentation-ready; consumers never
     need to combine them with `daily/`.
3. **Counts in `daily/`, judgments in `window/`.** The daily layer carries
   integers that merge by addition (counts, sums, sum of squares) and no
   rates; the window layer carries derived values (rates, percentiles,
   scores) and is the only place they appear.
4. **Every payload is self-describing.** Common envelope on every file:

   ```json
   {"schema_version": 1, "kind": "<kind>", "generated_at": "<ISO-8601Z>",
    "day" | "window": ..., "params": {...}}
   ```

   `generated_at` derives from the Source Day / window end, never wall clock.
   `params` records every threshold and constant that shaped the payload
   (wilson_z, selection weights, coverage limits, …) so a payload can be
   interpreted without
   reading this spec. `schema_version` is an integer; breaking change = bump.
5. **Vocabulary** (used identically everywhere): `hero`, `segment`
   (`legend | non_legend` in `daily/`, where `all` would be a redundant sum;
   plus `all` in `window/`, where its order statistics are not derivable),
   `day` (`YYYY-MM-DD`, Source Day), `window` (`{start, end, days}`
   inclusive), `rank`, `decided`/`wins`/`losses` for battle outcomes, `runs`
   for run counts. Rates are `*_rate`, in [0,1], null when the denominator is
   0. Basis points, tenths and other scaled integers are allowed only in
   compact array encodings and must be named with their scale (`*_bps`,
   `*_tenth`). No field anywhere identifies a player.

## Release file set

```
analyzer-v5/releases/<release_id>/
    manifest.json                 file inventory + provenance
    quality.json                  check results + data-quality SLIs
    daily/<day>.json              one per window day (1–7 files)
    window/heroes.json            the complete per-hero dataset, one fetch
    window/builds.json            ten-win build catalog (compact encoding)
analyzer-v5/manifest.json         the pointer: byte-copy of the published
                                  release's manifest.json
```

Five kinds total. What the legacy contract published and this one does not,
and why:

- `ladder/<day>.json` and `web/<day>.json` → merged into `daily/<day>.json`;
  they were the same grain with inconsistent abstraction levels.
- `ladder/players.json`, `ladder/top_by_class.json`, `ladder/top_overall.json`
  → **dropped entirely. No per-player data is published at all.** Publishing
  player performance profiles as immutable, world-readable, forever-cached
  objects is a privacy decision, not a default; this contract's public
  surface contains zero player identifiers. Record in an ADR; if a
  leaderboard or player-profile feature is ever wanted, it is a new kind (or
  a different surface) and a new decision.
- Per-game-day battle splits (legacy `by_game_day`) → dropped by decision;
  the hourly facts retain the data, so the kind can be reintroduced without
  re-ingestion if it is ever missed.

## `daily/<day>.json` — kind `hero_daily`

Per hero × segment, one row per pair, plus day-level submitter counts.
Everything merges by addition across days — this file exists for trend views
and arbitrary client-side windows; the one-fetch hero surface is
`window/heroes.json`.

```json
{
  "schema_version": 1, "kind": "hero_daily", "generated_at": "…",
  "day": "2026-08-10",
  "submitters": {"all": 0, "by_hero": [{"hero": "…", "submitters": 0}]},
  "rows": [
    {
      "hero": "…", "segment": "legend",
      "runs": {
        "completed": 0,
        "results": {"flawless": 0, "ten_win": 0, "wins_7_9": 0, "wins_4_6": 0, "wins_0_3": 0}
      },
      "ten_win": {"runs": 0, "total_final_days": 0},
      "rating_delta": {"runs": 0, "total": 0},
      "rating": {"runs": 0, "sum": 0, "sum_sq": 0},
      "rank_runs": [{"rank": "…", "runs": 0}],
      "opponent_ranks": [{"rank": "…", "decided": 0, "wins": 0, "losses": 0}]
    }
  ]
}
```

Notes: segments here are `legend` and `non_legend` **only** — `all` is their
field-wise sum and publishing it would ship every number twice; clients that
want `all` add two rows. `rating` carries `sum`/`sum_sq` so any window's
mean/stddev is client-derivable; order statistics are not derivable and live
in `window/heroes.json`, as do windowed matchups.

## `window/heroes.json` — kind `hero_window`

**The complete hero dataset**: everything the site needs about heroes over the
window, in one fetch. Only trend-over-days views need `daily/` files. Segments
here are all three (`all`, `legend`, `non_legend`) — order statistics and
rates for `all` are not derivable from the other two, so the window layer
publishes them explicitly.

```json
{
  "schema_version": 1, "kind": "hero_window", "generated_at": "…",
  "window": {"start": "2026-08-04", "end": "2026-08-10", "days": 7},
  "params": {},
  "submitters": {"all": 0, "by_hero": [{"hero": "…", "submitters": 0}]},
  "rows": [
    {
      "hero": "…", "segment": "all",
      "runs": {"completed": 0,
               "results": {"flawless": 0, "ten_win": 0, "wins_7_9": 0, "wins_4_6": 0, "wins_0_3": 0}},
      "win_rate": null,
      "performance_rating": null,
      "avg_opponent_rating": null,
      "rating": {"runs": 0, "mean": null, "stddev": null,
                 "p10": null, "p50": null, "p90": null, "min": null, "max": null},
      "rating_delta": {"runs": 0, "net": 0},
      "ten_win": {"runs": 0, "avg_final_days": null},
      "rank_runs": [{"rank": "…", "runs": 0}],
      "opponent_ranks": [{"rank": "…", "decided": 0, "share": null, "win_rate": null}],
      "ghost": {"battles": 0, "win_rate": null},
      "matchups": [{"opponent_hero": "…", "decided": 0, "wins": 0, "losses": 0, "win_rate": null}]
    }
  ]
}
```

Contents are the legacy hero data carried forward — run result buckets, battle
`win_rate`, `performance_rating`, opponent strength, rating distribution (now
with order statistics), rank distribution, opponent-rank breakdown, ghost
baseline (a field, not a parallel array), rating delta, ten-win pace, and the
**windowed matchup matrix** — minus `by_game_day`, dropped by decision. With
tens of heroes the matchup matrix is O(heroes²) ≈ thousands of small rows per
segment: fine in one window file (it was only a size concern when repeated
per day). This is also why no separate daily matchups kind exists.

## `window/builds.json` — kind `builds`

The V5 `mod_tenwin_builds` v3 design is kept on merit — columnar array
encoding with a `schemas` block declaring tuple layouts, `card_index`,
Wilson-scored selection with coverage back-fill. Changes are naming only:
kind `builds`, envelope fields as above, selection thresholds moved into
`params`, and scaled-integer fields keep their `_bps`/`_tenth` suffixes.

## `manifest.json` and `quality.json`

```json
{
  "schema_version": 1, "kind": "release_manifest", "generated_at": "…",
  "release_id": "…", "window": {"start": "…", "end": "…", "days": 7},
  "builder_code_version": "…", "policy_version": "…",
  "files": [{"path": "daily/2026-08-10.json", "sha256": "…", "bytes": 0}]
}
```

`quality.json` merges V5's `checks.json` + `_sli.json`: the release-build
check results (each check id, pass, detail) and data-quality SLIs
(quarantine counts, timestamp-anomaly counts, per-day row counts). One file:
"should I trust this release" is one read.

The public pointer `analyzer-v5/manifest.json` is a byte-copy of the published
release's `manifest.json`. Consumers do: `GET` pointer → fetch files by path.
`Cache-Control`: `public,max-age=31536000,immutable` on release objects,
`public,max-age=60,must-revalidate` on the pointer.

Contracts live in `contracts/v5/*.schema.json` (JSON Schema 2020-12,
`additionalProperties: false` everywhere) with golden vectors under
`contracts/v5/golden/` generated once from the first production build and
frozen thereafter.

---

# Part II — The pipeline

## Trigger contract

1. Invoke `bpp run` periodically; any interval. Shorter = faster recovery.
2. Overlap is handled; never wait for the previous invocation.
3. Exit 3 (lock held) is normal.

Consequences that must hold: a no-op run costs at most one `stat` per healed
day plus one pointer `GET` — no Parquet read, no DuckDB, no R2 listing. No
wall-clock reasoning except "which Source Hours are settled". `generated_at`
derives from source time, so identical inputs give identical bytes.

## CLI surface

```
bpp run [--heal-days N] [--anchor-day DAY] [--no-publish] [--dry-run]
bpp status [--json]
bpp verify [--day DAY] [--deep]
bpp publish <release_id>
bpp rollback <release_id> --reason TEXT
bpp resume --reason TEXT
bpp show release <release_id>
```

- `run` — heal → seal → build → publish → report. Default `--heal-days 8`;
  healing is clamped at the epoch day 2026-08-07 and never reaches before it.
- `--anchor-day` — anchor the release at an explicit sealed day.
- `--no-publish` — everything except R2 writes.
- `--dry-run` — report the plan; write nothing; network = pointer `GET` only.
- `verify` — recompute Parquet hashes against `_commit.json`; `--deep` also
  revalidates local releases against `contracts/v5/`. Operator tool, not run
  by `run`; the runbook schedules it (weekly `--deep` is the default
  recommendation) — a check never run does not exist.
- `publish` — publish an already-built release; same code path as `run`.
- `rollback` — flip the pointer to an older release and write
  `publish-hold.json`; without the hold the next tick would re-publish the
  newest release and undo the rollback within one interval.
- `resume` — clear the hold with a recorded reason.

Exit codes: 0 success/no-op · 1 unexpected · 2 usage · 3 lock held ·
4 partial (retryable failures; next tick retries). An exit-3 attempt is not a
run: it writes nothing. Permanently unrecoverable conditions (abandoned days)
are recorded once and stop affecting exit codes. The single real alarm is the
age of `analyzer-v5/manifest.json`, monitored **off this machine**.

## Disk layout

`$BPP_DATA_ROOT`:

```
facts/hourly/source_hour=<key>/   runs battles battle_cards quality quarantine (.parquet) + _commit.json
facts/sealed/source_day=<day>.json
facts/sealed/source_day=<day>.abandoned.json
releases/<release_id>/            the release file set (Part I)
receipts/<receipt_id>.json        rollback receipts
publish-hold.json                 present ⇒ no publishing
duckdb-tmp/                       disposable spill
logs/<utc-timestamp>-<pid>.log
runs.jsonl                        append-only run summaries
status.json                       atomic health snapshot
.lock/                            mkdir lock + heartbeat
```

`facts/hourly/` — five tables per hour (`runs`, `battles`, `battle_cards`,
`quality`, `quarantine`) plus `_commit.json`. The five-table split is kept
from the legacy stack on merit, but the Parquet schemas are owned by this
rebuild and free to change until the first production commit; after that,
committed hours are read, never rewritten.

`$BPP_DATA_ROOT` starts empty and lives outside the repo:
`/Users/yxinyu/bpp-state/analyzer-v5/data`. This is a fresh tree — the legacy
stack's `~/bpp-state/analyzers-v5/` is a different directory and is deleted
with the legacy stack at cutover step 5. The rebuild runs directly on the
host (no container), so `$BPP_DATA_ROOT` is the only path configuration — no
host/container mount mapping.

**Local retention**: end of each successful run, prune `releases/<id>/` not
(published ∨ hold target ∨ newest `BPP_KEEP_RELEASES`, default 3); prune
`logs/` >10 days. `facts/` is never pruned. R2 objects are never deleted
(accepted cost, ADR). `release_id` embeds `builder_code_version`, so deploys
create new releases even with identical payload bytes — that churn is why
retention exists.

## Identity

All hashes: sha256 over canonical JSON (`sort_keys`, `(",",":")` separators,
`ensure_ascii=False`, trailing newline, UTF-8).

```
fact_commit_sha256 = sha256(canonical(_commit.json))
day_seal_sha256    = sha256(canonical(seal body minus this field))
release_id         = "<anchor_day>-" + sha256(canonical({
                         anchor_day, day_seal_sha256s, hourly_fact_commit_sha256s,
                         builder_code_version, policy_version }))[:16]
```

Shape `^\d{4}-\d{2}-\d{2}-[0-9a-f]{16}$`. Seals are derived from the hourly
hashes; including both is deliberate redundancy so a seal-writing bug cannot
produce a stale identity. Because identity is content-derived, late data
inside the window changes the seals → new `release_id` → a corrected release
publishes automatically; the anti-regression check therefore compares `>=`,
not `>`.

## Sealing, expired hours, abandoned days

`facts/sealed/source_day=<day>.json`: `schema_version`, `source_day`,
`hourly_fact_commits` (exactly 24, hours 00–23, `{source_hour,
fact_commit_sha256}`), summed `row_counts`, `day_seal_sha256`. Written
atomically. Sealing verifies 24 `_commit.json`s and their recorded
`file_sha256s` against file presence and size — sub-second, no Parquet reads.
(Presence+size, not re-hashing, is a deliberate speed trade; silent
post-commit corruption is caught only by scheduled `verify`.)

The Bundle Server retains ~14 days. An uncommitted hour older than
`BPP_BUNDLE_RETENTION_DAYS` (default 10, deliberately conservative against
the server's ~14) is **expired** — a permanent failure wearing a retryable
costume. Two rules:

1. **Empty ≠ expired.** `hour_index` must distinguish "server answered, zero
   Bundles" (commit a zero-row hour) from "outside retention". If the API
   cannot express the difference, age decides: an empty index past the
   threshold is expired, and a zero-row commit for it is refused. Otherwise an
   expired hour silently seals an empty day and publishes data loss.
2. **Abandonment is terminal.** A day with an expired uncommitted hour gets
   `source_day=<day>.abandoned.json` (missing hours, reason, timestamp).
   Abandoned days are excluded from healing, never seal, never anchor, are
   loud in logs and `status.json`, and stop producing exit 4.

`--heal-days 8 < 10` keeps normal operation clear of the expiry edge; an
outage longer than ~2 days starts eating the margin, and abandonment is what
makes long outages survivable instead of infinitely retried.

## `status.json`, `runs.jsonl`, heartbeat

`status.json` (atomic, rewritten every run including failures):
`facts` {newest_sealed_day, sealed_days, incomplete_days (with missing hours,
settled flag), abandoned_days}, `release` {publish_hold, local_newest_release_id,
published_release_id, published_window_end, published_manifest_age_seconds},
`last_run` {run_id, timings, outcome ∈ ok|noop|partial|error, exit_code,
hours_ingested, days_sealed, release_built, release_published, failures[]},
`disk` free-bytes, `peak_rss_bytes`. `published_*` always comes from the
pointer `GET` — R2 is the authority; a manual pointer change cannot desync.

`runs.jsonl`: one line per run, same object as `last_run`, append-only.
`.lock/heartbeat`: `{run_id, pid, started_at, hostname}`.

## Locking

`mkdir(.lock)` — atomic on every filesystem. On success write `heartbeat`,
touch every 30s from a daemon thread. On `FileExistsError` read it:

- mtime within 300s → live; exit 3.
- older → stale. Take over **atomically**: rename `.lock/` →
  `.lock.stale-<own_run_id>/` (one contender's rename wins; the loser retries
  acquisition), log the stale `run_id`, `mkdir(.lock)` fresh, proceed. Never
  replace a heartbeat in place — that lets two contenders both conclude they
  won.

Stale ≠ dead — the old process may be paused and resume later. Guards:

- **Ownership check** on every heartbeat touch and on the `finally` release:
  act only if `heartbeat.run_id` is your own; on mismatch or missing lock dir,
  abort the whole process. A resumed zombie must never write after losing the
  lock, nor delete the new holder's lock.
- **`BPP_MAX_RUN_SECONDS`** (default 7200): exceed it → stop heartbeating,
  abort. A hung-but-alive run must not hold the lock forever.

The lock serializes this machine only — see the single-writer assumption.

## Modules

Deep modules, thin plumbing. Interfaces, not inherited code:

**`bundle_source`** — `hour_index(source_hour) -> RawHourIndex` (walks
pagination to completion; ordered, deduplicated `(bundle_id, sha256, bytes)`
plus derived `raw_commit_sha256`; raises `HourExpired` past retention) and
`stream(index) -> Iterator[Bundle]` (bounded download pool, look-ahead 8,
yields in index order, per-Bundle sha256/magic/segment-digest verification).
Bundles are ≤~3 MiB and live only in memory.

**`fact_store`** — `commit_hour` (stage → write Parquet + `_commit.json` →
atomic rename; identical re-commit returns `reused`), `seal_day`,
`abandon_day`, `is_abandoned`, `seals()`, `hour_paths(days)` (the **only**
read surface: Parquet paths for SQL), `verify(day, deep)`. Nothing
materializes fact rows as Python objects.

**`release`** — `build(anchor_day, seals)`: window via the growing-window rule
(1–7 consecutive sealed days ending at anchor; never before the epoch day
2026-08-07),
compute `release_id`; if `releases/<release_id>/` exists, return it — a
directory at its final path is valid by construction (staged then renamed);
the reuse path may parse `manifest.json` and nothing more. Otherwise build via
DuckDB, stage, rename. `publish(release)`: per key `stat` → absent: `PUT` +
confirm; present with matching sha256/bytes: skip; present differing:
`ImmutableObjectConflict`, hard stop. The `stat` is not bookkeeping — it is
the only thing that makes the immutability check enforceable; a blind re-`PUT`
can only overwrite the object it conflicts with. Pointer written last.
`rollback(to, reason)` / hold / `resume` as in the CLI section; hold file and
pointer flips take the run lock briefly.

**Single-writer assumption**: exactly one machine runs this pipeline against
the bucket. Pointer update is read-then-write, no CAS; the local lock is what
makes the publish checks sound. Manual pointer surgery only under a hold.
Record in ADR.

**`driver`** —

```
with lock():
    for day in last_n_utc_days(heal_days):            # oldest first
        if store.has_seal(day) or store.is_abandoned(day): continue
        for hour in settled_missing_hours(day):
            index = bundles.hour_index(hour)          # HourExpired ⇒ abandon path
            store.commit_hour(project(bundles.stream(index), index))
        if all 24 committed: store.seal_day(day)
        elif any missing hour expired: store.abandon_day(day, missing, reason)

    seals  = store.seals()
    anchor = anchor_day or newest_sealed_day(seals)
    if anchor is None: return noop
    local  = release.build(anchor, window_seals(seals, anchor))
    if publish and not publish_hold() and local.release_id != published_release_id():
        release.publish(local)
    prune_releases(); write_status(); append_runs_jsonl()
```

One failed hour fails that hour only: log, continue, `partial`, exit 4; the
unsealed day retries next tick. `settled_missing_hours` keeps the settled-hour
rule (an hour is ingestible only once the server can no longer add to it) as
a pure function — the one surviving piece of wall-clock reasoning.

## DuckDB rules

One connection per build: `memory_limit` (`BPP_DUCKDB_MEMORY_LIMIT`, default
8GB), `temp_directory=duckdb-tmp`, `threads` (default 8),
`preserve_insertion_order=false`.

- Read only via `read_parquet([explicit paths from hour_paths])`. No globs —
  the window is defined by seals, not by what is on disk.
- All filtering/joining/aggregation in SQL. `fetchall()` only on aggregated
  results; anything that can exceed ~10k rows is a defect.
- Spill to `duckdb-tmp` is fine.
- **Bit-determinism is a contract**: same committed window ⇒ same bytes,
  forever. With unordered parallel execution, result order and float summation
  order both drift — so every output surface gets an explicit total
  `ORDER BY`, and any float aggregate reaching a payload must be
  order-independent (canonical-order aggregation, or integer/fixed-point
  arithmetic — prefer scaled integers as in `builds.json`). Nondeterminism is
  not cosmetic: rebuilding an already-published `release_id` with different
  bytes trips the immutability check and wedges the pipeline on its own
  output.

The analytics builders take `(connection, hour_paths, day_or_window,
generated_at)` and return payload bytes. This is the largest work item: three
V5 builders' logic re-expressed in SQL against the new contract.

## Checks

Inline preconditions that fail their step; each needs a test that fails when
the check is removed.

**Ingestion**
1. Every Bundle's declared sha256 matches its bytes; magic and segment digests
   validate; failures go to `quarantine` with a reason, never dropped.
2. The hour index is complete and deduplicated before projection begins.
3. `_commit.json` `file_sha256s` match the Parquet actually written, before
   the rename.
4. A re-commit whose canonical bytes differ from the existing commit is a hard
   conflict, never an overwrite.

**Sealing**
5. Exactly 24 hours, 00–23, each independently verified.
6. Client-side timestamp anomalies count into `quality`; they never move a
   Bundle between partitions.
7. An empty hour index past the retention threshold is expired, not empty: no
   zero-row commit, day abandoned — silent data loss becomes a loud terminal
   state.

**Release build**
8. Window: 1–7 consecutive sealed days ending at a sealed anchor, none before
   the epoch day 2026-08-07.
9. Every payload validates against `contracts/v5/*.schema.json` before the
   staging rename.
10. `manifest.json` `files[]` sha256/bytes match disk; file set exact.
11. `release_id.startswith(f"{anchor_day}-")`.

**Publish**
12. The current pointer parses; its `release_id` matches the frozen pattern
    and is prefixed by its own window end. Unparseable pointer blocks
    publication.
13. Anti-regression: new window end `>=` published window end (`>=` so a
    corrected same-anchor release ships). Only `rollback` moves backward.
14. Immutable-key conflict (differing bytes at an existing release key) is a
    hard error, detected by the per-key `stat`.
15. `Cache-Control` exactly as specified in Part I.
16. Pointer written last, after every artifact `PUT` succeeded.
17. `publish-hold.json` present ⇒ no publish, from `run` or `bpp publish`;
    only `resume` clears it.

## Acceptance criteria

1. **Idempotence** — second consecutive `run` exits 0, `outcome: noop`,
   mutates nothing under `facts/` or `releases/` (mtime+size snapshot).
2. **Contract validity** — every payload validates against `contracts/v5/`;
   golden vectors generated from the first production build are frozen and
   pass thereafter.
3. **Crash convergence** — kill at each fault-injection seam; re-run converges
   to the same final state as an uninterrupted run.
4. **Deterministic rebuild** — delete `releases/<id>/`, rebuild the same
   window twice with `threads > 1`: byte-identical both times.
5. **Memory** — peak RSS < 1 GiB per hour ingest, < 8 GiB per 7-day build;
   recorded in `status.json`, asserted in a test.
6. **No local Bundle bytes** — one Source Day of ingest grows the data root by
   committed Parquet size + ≤1%.
7. **Mutual exclusion** — concurrent `run` exits 3 within 1s writing nothing;
   stale-lock race has exactly one winner; a resumed former holder neither
   writes nor removes the new holder's lock.
8. **Cheap no-op** — zero Parquet reads, no DuckDB connection, exactly one R2
   request.
9. **Late data** — committing a missing hour inside the published window
   yields a new `release_id` published on the next run.
10. **Rollback sticks** — rollback moves the pointer, writes receipt + hold;
    next `run` builds but does not publish; after `resume`, it publishes.
11. **Expired hour** — an empty index past retention abandons the day (no
    zero-row commit, no seal), visibly in `status.json`; subsequent runs
    neither retry nor exit 4 for it.
12. **All 17 checks** have a removal-detecting test.

## Cutover

The rebuild reuses the `analyzer-v5/` prefix, so the pointer flip **is** the
consumer-breaking moment and must be coordinated — unlike a new prefix, there
is no gradual migration. The legacy stack keeps running untouched until step 5.

1. Contracts first: author `contracts/v5/` schemas in the new repo (the
   legacy repo is not touched at all); review the shapes with both consumers
   (site, mod) before any pipeline code.
2. Build the pipeline against the new contracts, in a fresh data root. The
   first `bpp run` heals as far back as it can reach (up to the epoch floor
   2026-08-07); days already past the expiry guard by then are simply
   abandoned and the effective history starts later. This is acceptable, not
   a deadline: the release window is at most 7 days, so early history's only
   value is widening the first few windows — after one week of running, the
   bootstrap date is invisible. Generate golden vectors from the first full
   build; freeze them.
3. Run `bpp run --no-publish` alongside the legacy stack for several days:
   sanity-check the new numbers against legacy output where windows overlap
   (semantic spot-checks, not byte comparison — there is nothing to be
   byte-equal to).
4. The flip, as one coordinated change: consumers deploy support for the new
   payloads, the legacy scheduler is stopped, and the first `bpp run` publish
   replaces `analyzer-v5/manifest.json` with the new-format pointer. Legacy
   immutable release objects stay readable at their old keys (never-delete);
   `rollback` cannot cross the flip — the last legacy release is not a valid
   rollback target for the new pointer format.
5. Retire the legacy stack wholesale: stop and remove the Docker deployment
   (Dagster, Postgres, compose), archive the `bazaarplusplus-analyzers` repo,
   and delete the legacy data root `~/bpp-state/analyzers-v5/`. Write the
   ADRs in the new repo (single-process idempotent pipeline + mutable-store
   red line; Bundle bytes never persisted, hourly facts as first recovery
   authority; Day Seal; zero per-player data published; R2 never-delete;
   single-writer) and write the runbooks fresh.

## Unresolved, needs measurement

- Hourly Parquet size ⇒ steady-state `facts/` footprint and real disk need.
- Wall-clock + peak RSS of a 7-day build ⇒ `BPP_DUCKDB_MEMORY_LIMIT`, publish
  latency.
- Tolerated download concurrency ⇒ pool size, look-ahead depth.
- Cold full-day ingest wall-clock ⇒ manifest-age alarm threshold.
- Bundle Server behavior for out-of-retention hours (error vs empty) ⇒ can
  the expired-hour age guard be simplified?
- Release payload sizes under the new contract — chiefly `window/heroes.json`
  (the matchup matrix and per-rank arrays × 3 segments). Expected small;
  measure before freezing contracts.
- Offsite backup for `facts/`: it is the only copy of history beyond server
  retention on one machine's disk. Small; an rclone-to-R2 sync after each run
  may suffice, but it must be decided, not defaulted.

# Measurements against the spec's "Unresolved, needs measurement" list

Measured on 2026-08-11 during the first real-server bootstrap
(`bpp run --no-publish --heal-days 8`), on the production Bundle Server and
the operator's machine (macOS, Apple Silicon).

## Bundle Server behavior for out-of-retention hours — RESOLVED

Direct probe of `GET /bundles` (read-only):

| Window | Response |
| --- | --- |
| ~41 days old (2026-07-01T00Z) | **HTTP 410** `{"error":{"code":"window_expired","message":"Bundle window is outside R2 retention","retryable":false}}` |
| ~20 days old | **HTTP 410** `window_expired`, `retryable: false` |
| in-retention hour with no data | **HTTP 200** `{"window":…,"items":[],"next_after":null}` |

The server distinguishes "expired" from "empty" explicitly: expiry is a
non-retryable 410 error, never an empty index. `bundle_source` maps 410 to
`HourExpired` directly. Consequence for the spec's question: the age-based
expired guard (`BPP_BUNDLE_RETENTION_DAYS`) is not the only line of defense
and could in principle be simplified; it is kept as a conservative local
backstop so a server-side regression (410 turning into an empty 200) cannot
silently seal an empty day (spec check 7 still enforces the age rule).

## Cold full-day ingest wall clock — measured during bootstrap

First-run pace: roughly 5–6 minutes per Source Hour at the initial
conservative download concurrency, i.e. a cold full day ≈ 2–2.5 hours.
Implication for the manifest-age alarm: bootstrap-scale healing is hours, but
steady-state (1–2 new settled hours per tick) is minutes per tick.
Concurrency headroom exists; raising the pool size is the lever if bootstrap
pace ever matters again.

## Hourly Parquet size ⇒ `facts/` footprint — measured

Busy daytime hours: ~2,400 bundles ⇒ ~17–22 MiB of Parquet per hour
(battle_cards dominates at ~18 MiB). Five sealed days (120 hours) occupy
~1.9 GiB. Steady state grows ≈ 0.3–0.5 GiB/day of never-pruned history;
a year is roughly 120–180 GiB.

## Real release build wall clock + peak RSS — measured

First real build (5-day window, 2.79 M battles, 13.8 k submitters): **4 s
wall clock**, ~0.9 GiB peak build-phase RSS after the builds-payload memory
fix (pre-fix the build OOMed the 8 GB DuckDB limit). Projected 7-day peak
< 2 GiB — comfortably inside the 8 GB contract.

## Tolerated download concurrency — measured

The workload is latency-bound (CPU < 1%, per-request round trip ≈ 3 s on the
operator's network path): 4 → 8 concurrent downloads scaled near-linearly
(~5.5 → ~2.5 min per busy hour). Above ~20, the default httpx keepalive pool
was the hidden cap (fixed: pool now sized to the concurrency). Concurrency 64
produced no server-side pushback (no load-attributable 429/5xx); daytime
congestion on the operator's path, not the server, set the effective floor.
Bootstrap settings: `BPP_DOWNLOAD_CONCURRENCY=64`, `BPP_DOWNLOAD_LOOKAHEAD=128`.

## Release payload sizes — measured

Whole release (5-day window): **1.0 MiB** total; `window/heroes.json`
(3 segments × 8 heroes, full matchup matrix and rank arrays) and
`window/builds.json` (4,600 builds, 1,026-card index) are each a few hundred
KiB. Size is a non-issue; no compact-encoding pressure on `heroes.json`.

## Ingest peak RSS — measured; fix in progress

Multi-hour heal runs peaked at ~5 GiB RSS with the original whole-hour
Python-dict projection (busy hours materialize millions of battle_cards rows
before a single Parquet write), violating acceptance #5's 1 GiB/hour bound on
real data even though the synthetic-fixture test passed. Remediation:
projection is being converted to stream bounded row batches through
`pyarrow.parquet.ParquetWriter`.

## Still open

- Offsite backup decision for `facts/` (rclone-to-R2 after each run remains
  the candidate; must be decided, not defaulted).

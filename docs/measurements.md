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

## Still open (to be filled as observed)

- Hourly Parquet size ⇒ steady-state `facts/` footprint.
- Wall clock + peak RSS of a real 7-day build.
- Tolerated download concurrency (current conservative setting untested
  upward).
- Release payload sizes under the new contract (chiefly
  `window/heroes.json`).
- Offsite backup decision for `facts/`.

# Analyzer V5 publish contract

The consumer-facing contract for `analyzer-v5/` on R2. Five kinds; every
payload validates against its schema here before a release may be staged
(spec check 9). Golden vectors under `golden/` are generated once from the
first production build and frozen thereafter.

## File set

```
analyzer-v5/releases/<release_id>/
    manifest.json          release_manifest — file inventory + provenance
    quality.json           quality — check results + data-quality SLIs
    daily/<day>.json       hero_daily — one per window day (1-7 files)
    window/heroes.json     hero_window — the complete per-hero dataset
    window/builds.json     builds — ten-win build catalog (compact encoding)
analyzer-v5/manifest.json  the pointer: byte-copy of the published manifest.json
```

Consumers do: `GET` the pointer, then fetch files by `files[].path`.
`Cache-Control` is `public,max-age=31536000,immutable` on release objects and
`public,max-age=60,must-revalidate` on the pointer.

## Envelope

Every payload starts with `schema_version` (integer; breaking change = bump),
`kind`, `generated_at` (derived from source time, never wall clock — identical
inputs give identical bytes), `day` or `window` (`{start, end, days}`,
inclusive), and `params` (every threshold/constant that shaped the payload).

## Layering

- `daily/` is the mergeable facts layer: integer counts and accumulators only
  (`sum`/`sum_sq`), no rates. Segments are `legend` and `non_legend` only —
  `all` is their field-wise sum, done client-side. Any client-side window is a
  fold over these files.
- `window/` is the products layer: self-contained, presentation-ready, and
  the only place derived values (rates, percentiles, scores) appear. Segments
  include `all`, whose order statistics are not derivable from the other two.
- No payload anywhere contains a player identifier.

## Vocabulary

`hero`, `segment`, `day` (`YYYY-MM-DD`, Source Day), `rank`,
`decided`/`wins`/`losses` (battle outcomes), `runs` (run counts). Rates are
`*_rate` in [0,1], `null` when the denominator is 0. Scaled integers appear
only inside the `builds` compact encoding and carry their scale in the name
(`*_bps`, `*_tenth`).

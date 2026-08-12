# Analyzer V5 implementation map

## Modules

- `bundle_source` enumerates immutable hourly Bundle identities and streams
  bounded verified downloads.
- `projection` enforces Accepted Run admission and cascades rejected Runs out
  of Battles and Battle Cards.
- `fact_store` atomically commits hourly Parquet and seals only verified
  24-hour Source Days.
- `publication` selects a one-to-seven-day consecutive Analysis Window, builds the two
  consumer payloads, validates their schemas and semantic invariants, and
  replaces one fixed latest key at a time.
- `driver` owns locking, recovery, product failure isolation, structured Run
  Reports, and local status/log updates.
- `cli` exposes the three operator workflows: run, status, and verify.

## Persistent state

```text
<data-root>/
├── facts/
│   ├── hourly/source_hour=YYYY-MM-DDTHH/
│   └── sealed/source_day=YYYY-MM-DD.json
├── snapshots/
│   ├── heroes/latest.json
│   └── builds/latest.json
├── logs/
├── status.json
└── runs.jsonl
```

Local snapshots are mutable recovery/inspection artifacts. The remote object
set has the same two products under `analyzer-v5/` and no additional object.

## Verification

The suite tests admission cascade, bounded growing-window selection, Source
Epoch exclusion, additive Hero
denominators, Matchups, Build eligibility and ranking, Representative Layout,
P75, Wilson score, Top-500 coverage, card indices, deterministic content,
fixed public keys, independent failure behavior, download-failure reporting,
locking, crash-safe fact commits, and bounded-memory ingestion.

Run:

```bash
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
```

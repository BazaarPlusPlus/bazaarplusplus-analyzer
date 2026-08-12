# BazaarPlusPlus Analyzer

The analyzer converts verified Bundle V5 deliveries into local hourly facts
and exactly two current consumer snapshots:

```text
analyzer-v5/heroes/latest.json
analyzer-v5/builds/latest.json
```

The Web snapshot contains one to seven newest-first daily partitions of
additive integer Hero metrics for `legend` and `non_legend`. The Mod snapshot
contains the current schema-2 Ten-Win Build corpus with deterministic lookup
tables and card indices. Both use the latest consecutive Complete Source Days,
up to seven, and are calculated, validated, and replaced independently. One
complete day is sufficient to publish.

## Operator commands

```bash
uv run bpp run
uv run bpp run --no-publish
uv run bpp status --json
uv run bpp verify --deep
```

Configuration is read from the repository `.env`. Optional
`BPP_SOURCE_EPOCH=YYYY-MM-DD` excludes every earlier UTC Source Day from
healing, sealing, and analysis. `--no-publish` does not construct an R2
adapter. Tests use only fake/local object stores.

The authoritative domain vocabulary is in `CONTEXT.md`; the wire and metric
contract is in `docs/specs/consumer-data-contract.md`; strict schemas are in
`contracts/v5/`.

## Quality gate

```bash
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
```

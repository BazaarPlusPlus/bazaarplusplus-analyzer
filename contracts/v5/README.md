# Analyzer V5 consumer schemas

`contracts/v5/` contains the complete public contract. It has exactly two
strict Draft 2020-12 JSON Schemas:

- `heroes.schema.json` validates `analyzer-v5/heroes/latest.json`;
- `builds.schema.json` validates `analyzer-v5/builds/latest.json`.

Both public objects are mutable current snapshots. There is no manifest,
release directory, public daily partition, quality object, or public history.
The semantic invariants that JSON Schema cannot express—seven consecutive
days, the canonical Hero × Segment row set, additive denominators, layout
occupancy, and bidirectional card indices—are enforced by
`bppanalyzer.publication.validate_snapshot` before an object is replaced.

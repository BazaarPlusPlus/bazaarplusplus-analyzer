# Frozen V5 golden releases

This directory contains exact release payloads frozen from the first real
Analyzer V5 production build. Until that build is ready, this README is the
only committed file here and the golden-vector pytest skips cleanly.

Each frozen set preserves the local release layout:

```text
contracts/v5/golden/<release_id>/
    manifest.json
    quality.json
    daily/<day>.json
    window/heroes.json
    window/builds.json
```

Freeze the first full real release from the repository root:

```bash
.venv/bin/python scripts/freeze_v5_goldens.py \
  /absolute/path/to/data/releases/<release_id>
```

The command validates every source payload against `contracts/v5/*.schema.json`,
verifies the manifest's exact file set, sha256 values, and byte sizes, then
atomically copies it here. It refuses to overwrite an existing release. Review
the new files and commit them once; after that they are frozen contract vectors.

Validate all frozen sets with:

```bash
.venv/bin/python -m pytest -q \
  tests/test_golden_workflow.py::test_frozen_golden_vectors_match_schemas_and_manifest_inventory
```

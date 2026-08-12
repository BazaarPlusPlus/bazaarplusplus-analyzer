# Analyzer V5 single-process pipeline

## Contract authority

The domain definitions in `CONTEXT.md` and the wire contract in
`docs/specs/consumer-data-contract.md` are authoritative. The only public
objects are:

```text
analyzer-v5/heroes/latest.json
analyzer-v5/builds/latest.json
```

## Pipeline

One locked invocation performs four phases:

1. enumerate settled Source Hours and stream verified Bundles;
2. project accepted Runs into hourly Parquet and seal a Source Day only after
   all hours `00` through `23` verify;
3. select the latest exact seven-day consecutive Analysis Window;
4. independently build, validate, save, and optionally replace each consumer
   snapshot.

Hourly facts, commit records, day seals, status, logs, and the directory lock
are durable local recovery state. Raw Bundle bytes are never persisted.

## Fact admission

Projection normalizes the `Hero8` alias to `TheDragons`. A decoded Run is
accepted only when its normalized hero and final rank are canonical. An
unaccepted Run emits one operational quarantine record with separate
`discarded_unknown_hero` and `discarded_unknown_final_rank` flags. It emits no
Run, Battle, or Battle Card fact rows.

Download or Bundle validation failure leaves the Source Hour uncommitted.
Missing or corrupt hourly facts prevent a day seal. A day seal contains and
verifies exactly 24 immutable hourly commit identities.

## Analysis and validation

`SnapshotBuilder` owns the DuckDB analysis interface. It reads only explicit
hourly paths obtained through verified day seals. Heroes and Builds use
separate connections so either calculation can fail without blocking the
other.

`validate_snapshot` applies the strict schema for the selected product and
then checks semantic invariants not expressible in JSON Schema. Validated bytes
are canonical JSON. Each product is atomically written to one local
`snapshots/<product>/latest.json` file.

## Publication

`LatestPublisher.replace` accepts one validated snapshot and one object-store
adapter. It can address only the two fixed latest keys. The replacement uses:

```text
Cache-Control: public,max-age=60,must-revalidate
Content-Type: application/json
```

The object store's PUT is the atomic consumer-visible operation. An existing
object is untouched until the replacement payload passes all validation.
There are no release IDs, manifests, immutable public directories, quality
objects, holds, rollback pointers, daily objects, or remote bookkeeping.

## Local report

Every invocation stores the Run Report defined by the consumer contract in
`status.json`, appends it through `runs.jsonl`, and records it in the run log.
Failed download counts remain visible even when no Analysis Window exists.
The report is never sent to the public object store.

## Operator interface

The CLI exposes `run`, `status`, and `verify`. `run --no-publish` creates and
validates the two local latest snapshots without constructing an R2 adapter.
`run --dry-run` reads or writes neither local pipeline state nor external
state. `verify --deep` validates any local consumer snapshots in addition to
rehashing fact state.

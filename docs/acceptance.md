# Analyzer V5 acceptance evidence

This record maps the frozen acceptance criteria and pipeline checks in
`docs/specs/2026-08-11-single-process-pipeline.md` to offline, runnable
evidence. All tests use temporary data roots and fake or in-process external
boundaries. They do not read `BPP_DATA_ROOT` or contact the Bundle Server or
R2.

## Verification commands

Run the complete offline evidence set:

```bash
.venv/bin/python -m pytest -q
```

On 2026-08-11, every selector cited in the two matrices below was run
directly: 39 passed and the unpopulated real-golden selector was the only
skip. The complete offline suite then passed with 57 passed and that same one
expected skip.

Freeze the first full real release when the dispatcher declares it ready:

```bash
.venv/bin/python scripts/freeze_v5_goldens.py \
  /absolute/path/to/data/releases/<release_id>
.venv/bin/python -m pytest -q \
  tests/test_golden_workflow.py::test_frozen_golden_vectors_match_schemas_and_manifest_inventory
```

The freeze command defaults to `contracts/v5/golden/<release_id>/`, validates
the source before copying, promotes the copy atomically, and refuses to
overwrite an existing golden set. The checked-in structure and freeze policy
are documented in `contracts/v5/golden/README.md`.

## Acceptance criteria

| ID | Criterion | Runnable evidence | Status |
| --- | --- | --- | --- |
| 1 | Idempotent second run | `tests/test_phase3_acceptance.py::test_acceptance_1_and_8_second_run_is_a_cheap_nonmutating_noop` | Pass: exit 0/noop and facts/releases mtime+size snapshot unchanged. |
| 2 | Contract validity and frozen goldens | `tests/test_phase2_acceptance.py::test_every_payload_matches_the_frozen_schema_and_source_time`; `tests/test_golden_workflow.py::test_freeze_script_copies_an_exact_valid_release_and_refuses_overwrite`; `tests/test_golden_workflow.py::test_golden_validator_rejects_manifest_digest_or_size_drift`; `tests/test_golden_workflow.py::test_frozen_golden_vectors_match_schemas_and_manifest_inventory` | Pending-freeze: synthetic releases pass every schema and inventory gate; the final node skips until the first real golden set is frozen. |
| 3 | Crash convergence at every fault seam | `tests/test_phase3_crash_convergence.py::test_acceptance_3_commit_and_seal_crashes_converge_to_uninterrupted_facts[before_precommit_verify]`; `tests/test_phase3_crash_convergence.py::test_acceptance_3_commit_and_seal_crashes_converge_to_uninterrupted_facts[before_hour_promote]`; `tests/test_phase3_crash_convergence.py::test_acceptance_3_commit_and_seal_crashes_converge_to_uninterrupted_facts[before_seal_promote]`; `tests/test_phase3_crash_convergence.py::test_acceptance_3_release_stage_crash_converges_to_uninterrupted_release[after_payloads_written]`; `tests/test_phase3_crash_convergence.py::test_acceptance_3_release_stage_crash_converges_to_uninterrupted_release[after_manifest_written]`; `tests/test_phase3_crash_convergence.py::test_acceptance_3_publish_crash_converges_to_uninterrupted_remote[after_artifact_confirmed]`; `tests/test_phase3_crash_convergence.py::test_acceptance_3_publish_crash_converges_to_uninterrupted_remote[before_pointer_put]` | Pass: all seven implemented seams converge. |
| 4 | Bit-deterministic parallel rebuild | `tests/test_phase2_acceptance.py::test_seven_day_parallel_build_is_byte_deterministic_and_records_bounded_rss` | Pass: a deleted seven-day release rebuilt with four threads is byte-identical. |
| 5 | Peak RSS limits and status recording | `tests/test_phase1_acceptance.py::test_hour_ingest_records_and_stays_below_the_one_gib_peak_rss_limit`; `tests/test_phase2_acceptance.py::test_seven_day_parallel_build_is_byte_deterministic_and_records_bounded_rss` | Pass: hour ingest is below 1 GiB; seven-day build is below 8 GiB; both assert recorded status. |
| 6 | No local Bundle bytes | `tests/test_phase1_acceptance.py::test_one_source_day_persists_only_parquet_plus_at_most_one_percent_metadata` | Pass: data-root growth is Parquet plus at most 1%, with no raw/Bundle files. |
| 7 | Mutual exclusion and stale ownership | `tests/test_locking.py::test_live_lock_rejects_a_concurrent_holder_within_one_second_without_writes`; `tests/test_locking.py::test_stale_lock_race_has_exactly_one_winner`; `tests/test_locking.py::test_resumed_stale_holder_neither_writes_nor_removes_the_new_lock` | Pass. |
| 8 | Cheap no-op | `tests/test_phase3_acceptance.py::test_acceptance_1_and_8_second_run_is_a_cheap_nonmutating_noop` | Pass: zero Parquet reads, no DuckDB connection, exactly one fake-store pointer GET. |
| 9 | Late data changes release identity and publishes | `tests/test_phase3_acceptance.py::test_acceptance_9_late_seal_inside_lookback_changes_identity_and_publishes` | Pass: a late-completed day fills the anchor lookback, changes the seal identity chain, and publishes a corrected same-anchor release. |
| 10 | Rollback sticks until resume | `tests/test_phase3_acceptance.py::test_acceptance_10_rollback_hold_sticks_through_run_then_resume_publishes` | Pass: pointer, receipt, hold, held run, and resumed publication are asserted. |
| 11 | Expired hour abandonment | `tests/test_driver.py::test_expired_hour_abandons_day_visibly_and_subsequent_runs_do_not_retry_or_exit_partial`; `tests/test_bundle_source.py::test_empty_index_past_retention_is_expired_instead_of_a_zero_row_hour` | Pass: no commit/seal, visible terminal status, and no subsequent retry or exit 4. |
| 12 | All 17 checks have removal-detecting tests | The check matrix below; each row names its direct negative or boundary test. | Pass. |

## Pipeline checks

| Check | Required invariant | Removal-detecting test node ID |
| --- | --- | --- |
| 1 | Bundle sha256, magic, and segment digests; failures quarantined | `tests/test_projection.py::test_bundle_digest_magic_and_segment_failures_are_quarantined_without_dropping_valid_data` |
| 2 | Complete, deduplicated hour index before projection | `tests/test_bundle_source.py::test_hour_index_exhausts_keyset_pages_and_returns_ordered_deduplicated_index` |
| 3 | Written Parquet matches pre-promotion `_commit.json` hashes | `tests/test_fact_store.py::test_commit_refuses_promotion_when_a_written_parquet_differs_from_its_recorded_hash` |
| 4 | Differing canonical re-commit is a hard conflict | `tests/test_fact_store.py::test_identical_recommit_is_reused_but_different_canonical_commit_is_a_hard_conflict` |
| 5 | Seal requires exactly 24 independently verified hours | `tests/test_fact_store.py::test_seal_requires_exactly_24_independently_verified_hours` |
| 6 | Timestamp anomalies count into quality without repartitioning | `tests/test_projection.py::test_client_timestamp_anomalies_are_quality_rows_and_never_repartition_a_bundle` |
| 7 | Past-retention empty hour is expired and abandoned, never committed empty | `tests/test_bundle_source.py::test_empty_index_past_retention_is_expired_instead_of_a_zero_row_hour`; `tests/test_driver.py::test_expired_hour_abandons_day_visibly_and_subsequent_runs_do_not_retry_or_exit_partial` |
| 8 | Window is 1–7 consecutive sealed days, anchored and epoch-clamped | `tests/test_release.py::test_check_8_window_is_consecutive_bounded_anchored_and_epoch_clamped` |
| 9 | Every staged payload passes its frozen schema | `tests/test_release.py::test_check_9_invalid_payload_never_promotes_the_staging_directory` |
| 10 | Manifest sha256/bytes and file set exactly match disk | `tests/test_release.py::test_check_10_manifest_hash_size_and_exact_file_set_are_verified` |
| 11 | Release ID is prefixed by its anchor day | `tests/test_release.py::test_check_11_manifest_release_identity_must_start_with_anchor` |
| 12 | Pointer parses and its identity matches its own window end | `tests/test_publish.py::test_check_12_unparseable_or_self_inconsistent_pointer_blocks_publish` |
| 13 | Normal publish never regresses the window end | `tests/test_publish.py::test_check_13_blocks_older_window_but_allows_corrected_same_anchor` |
| 14 | Existing differing immutable object is a hard conflict | `tests/test_publish.py::test_check_14_existing_different_immutable_object_is_a_hard_conflict` |
| 15 | Release and pointer Cache-Control values are exact | `tests/test_publish.py::test_checks_15_and_16_cache_headers_are_exact_and_pointer_put_is_last` |
| 16 | Pointer is last and artifact failure never exposes the release | `tests/test_publish.py::test_checks_15_and_16_cache_headers_are_exact_and_pointer_put_is_last`; `tests/test_publish.py::test_check_16_artifact_failure_or_crash_before_pointer_never_exposes_release` |
| 17 | Hold blocks run and explicit publish; only reasoned resume clears it | `tests/test_publish_operations.py::test_check_17_rollback_hold_blocks_publish_until_reasoned_resume`; `tests/test_phase2_cli.py::test_publish_rollback_and_resume_commands_use_the_locked_publish_flow`; `tests/test_phase3_acceptance.py::test_acceptance_10_rollback_hold_sticks_through_run_then_resume_publishes` |

## Measurement boundary

Real-data measurements remain recorded in `docs/measurements.md`. This
offline evidence phase neither reads the in-progress real ingest nor creates
goldens from synthetic fixtures. Criterion 2 becomes fully complete only when
the dispatcher freezes and commits the first full real release and the
currently skipped golden-vector node passes.

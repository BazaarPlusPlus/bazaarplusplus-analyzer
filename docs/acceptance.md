# Analyzer V5 acceptance evidence

All evidence runs offline against temporary fact roots and the local
object-store adapter. Tests never contact the Bundle Server or R2.

| Contract behavior | Runnable evidence |
| --- | --- |
| Unknown hero/rank cascade removes Run, Battles, and Cards | `tests/test_projection.py::test_unaccepted_run_is_discarded_with_all_battles_and_cards` |
| Complete window is consecutive and exactly seven days | `tests/test_publication.py::test_analysis_window_is_exactly_seven_consecutive_complete_source_days` |
| Fewer than seven days never publishes | `tests/test_driver.py::test_fewer_than_seven_complete_days_never_publish_a_growing_window` |
| Hero rows are canonical, daily, additive, and preserve Matchups | `tests/test_publication.py::test_heroes_snapshot_is_daily_additive_and_matches_the_strict_contract` |
| No `battle_days` or precomputed rate fields | strict `heroes.schema.json` plus the Hero payload test |
| Build wire shape matches schema 2 | `tests/test_publication.py::test_builds_snapshot_matches_mod_schema_and_has_bidirectional_card_index` |
| Final-layout eligibility rejects each invalid boundary | `tests/test_publication.py::test_build_eligibility_rejects_every_incomplete_final_layout_boundary` |
| Representative Layout, nearest-rank P75, and Wilson score | `tests/test_publication.py::test_representative_layout_mode_tie_break_and_nearest_rank_p75` |
| Top 500 plus highest-ranked uncovered-card backfill | `tests/test_publication.py::test_top_500_then_coverage_appends_only_highest_ranked_uncovered_build` |
| Card index agrees with emitted Builds in both directions | strict semantic validation in all Builds tests |
| Public writes are only the two latest keys with exact headers | `tests/test_publication.py::test_latest_publisher_validates_and_only_writes_the_two_public_keys` |
| One invalid product remains unchanged while the other updates | `tests/test_driver.py::test_one_product_failure_preserves_it_but_the_other_product_still_updates` |
| Download failure is reported and prevents hourly completion | `tests/test_driver.py::test_failed_bundle_is_reported_and_its_source_hour_remains_incomplete` |
| Structured report is stored in status, JSONL, and logs | `tests/test_driver.py::test_driver_publishes_exactly_two_objects_and_records_the_structured_run_report` |
| Same inputs have stable ordering and business content | `tests/test_phase2_acceptance.py::test_same_input_produces_stable_ordering_and_business_content` |
| Hour commit/seal integrity and crash convergence | `tests/test_fact_store.py` |
| Lock ownership and stale takeover safety | `tests/test_locking.py` |
| Bounded-memory streaming ingest | `tests/test_phase1_acceptance.py` |

The complete gate is `uv run pytest` with branch coverage enforced by
`pyproject.toml`.

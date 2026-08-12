from dataclasses import replace
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path

import pytest

import bpp_analyzer.release as release_module
from bpp_analyzer.fact_store import DaySeal, canonical_json
from bpp_analyzer.release import (
    ContractViolation,
    ManifestMismatch,
    ReleaseBuilder,
    ReleaseIdentityError,
    compute_release_id,
    window_seals,
)
from tests.release_fixtures import sealed_store


class _SyntheticSpillStore:
    def __init__(self, paths: dict[str, Path], seal: DaySeal) -> None:
        self._paths = paths
        self._seal = seal

    def seals(self) -> tuple[DaySeal, ...]:
        return (self._seal,)

    def hour_paths(self, days) -> dict[str, tuple[Path, ...]]:
        assert tuple(days) == (self._seal.source_day,)
        return {name: (path,) for name, path in self._paths.items()}


def _spill_stress_store(root: Path, *, non_final_battles: int) -> _SyntheticSpillStore:
    paths = {
        name: root / f"{name}.parquet"
        for name in ("runs", "battles", "battle_cards")
    }
    connection = release_module.duckdb.connect(database=":memory:")
    try:
        connection.execute(
            f"""
            COPY (
              SELECT '2026-08-07T00' AS source_hour,
                     '2026-08-07' AS source_day,
                     1::BIGINT AS available_at_ms,
                     'bundle' AS bundle_id,
                     'run-0' AS run_id,
                     'account-0' AS player_account_id,
                     'Dooley' AS hero,
                     'completed' AS status,
                     10::BIGINT AS run_day,
                     10::BIGINT AS victories,
                     0::BIGINT AS losses,
                     'Legendary' AS final_rank,
                     1300::BIGINT AS final_rating,
                     10::BIGINT AS final_rating_delta,
                     'battle-00000000' AS final_battle_id
            ) TO {release_module._sql_string(str(paths['runs']))}
              (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        connection.execute(
            f"""
            COPY (
              SELECT '2026-08-07T00' AS source_hour,
                     '2026-08-07' AS source_day,
                     battle::BIGINT AS available_at_ms,
                     'bundle' AS bundle_id,
                     'run-0' AS run_id,
                     printf('battle-%08d', battle) AS battle_id,
                     battle=0 AS is_final_battle,
                     'account-0' AS player_account_id,
                     'Dooley' AS player_hero,
                     'ghost' AS opponent_account_id,
                     'Jules' AS opponent_hero,
                     'Gold' AS opponent_rank,
                     1000::BIGINT AS opponent_rating,
                     'account-0' AS winner_combatant_id,
                     'ghost' AS loser_combatant_id,
                     'player' AS winner_side,
                     'Dooley' AS winner_hero
              FROM range({non_final_battles + 1}) AS values(battle)
            ) TO {release_module._sql_string(str(paths['battles']))}
              (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        connection.execute(
            f"""
            COPY (
              SELECT '2026-08-07T00' AS source_hour,
                     '2026-08-07' AS source_day,
                     battle::BIGINT AS available_at_ms,
                     'bundle' AS bundle_id,
                     'run-0' AS run_id,
                     printf('battle-%08d', battle) AS battle_id,
                     'player_hand' AS card_set_label,
                     'present' AS card_set_status,
                     'player' AS owner_side,
                     'item' AS card_kind,
                     slot::BIGINT AS slot_index,
                     printf('instance-%08d-%02d', battle, slot) AS instance_id,
                     printf('00000000-0000-0000-0000-%012d', slot + 1) AS template_id,
                     1::BIGINT AS size,
                     slot::BIGINT AS socket,
                     'Gold' AS tier,
                     NULL::VARCHAR AS enchantment
              FROM range({non_final_battles + 1}) AS battles(battle)
              CROSS JOIN range(10) AS slots(slot)
            ) TO {release_module._sql_string(str(paths['battle_cards']))}
              (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        connection.close()
    seal = _seal(date(2026, 8, 7))
    return _SyntheticSpillStore(paths, seal)


def _seal(source_day: date) -> DaySeal:
    digest = hashlib.sha256(source_day.isoformat().encode()).hexdigest()
    return DaySeal(
        source_day.isoformat(),
        tuple(
            {
                "source_hour": f"{source_day.isoformat()}T{hour:02d}",
                "fact_commit_sha256": hashlib.sha256(
                    f"{source_day}:{hour}".encode()
                ).hexdigest(),
            }
            for hour in range(24)
        ),
        {name: 0 for name in ("runs", "battles", "battle_cards", "quality", "quarantine")},
        digest,
    )


def test_check_8_window_is_consecutive_bounded_anchored_and_epoch_clamped() -> None:
    epoch = date(2026, 8, 7)
    seals = tuple(_seal(epoch + timedelta(days=offset)) for offset in range(-3, 10))

    selected = window_seals(seals, date(2026, 8, 16))

    assert [item.source_day for item in selected] == [
        (date(2026, 8, 10) + timedelta(days=offset)).isoformat()
        for offset in range(7)
    ]
    with_gap = tuple(item for item in seals if item.source_day != "2026-08-15")
    assert [item.source_day for item in window_seals(with_gap, "2026-08-16")] == [
        "2026-08-16"
    ]
    with pytest.raises(ReleaseIdentityError, match="sealed anchor"):
        window_seals(seals, "2026-08-20")
    with pytest.raises(ReleaseIdentityError, match="epoch"):
        window_seals(seals, "2026-08-06")


def test_release_id_uses_the_exact_canonical_identity_chain() -> None:
    seals = (_seal(date(2026, 8, 7)), _seal(date(2026, 8, 8)))
    identity = {
        "anchor_day": "2026-08-08",
        "day_seal_sha256s": [item.day_seal_sha256 for item in seals],
        "hourly_fact_commit_sha256s": [
            hourly["fact_commit_sha256"]
            for item in seals
            for hourly in item.hourly_fact_commits
        ],
        "builder_code_version": "builder-test",
        "policy_version": "policy-test",
    }
    expected = "2026-08-08-" + hashlib.sha256(canonical_json(identity)).hexdigest()[:16]

    assert compute_release_id(
        "2026-08-08",
        seals,
        builder_code_version="builder-test",
        policy_version="policy-test",
    ) == expected


def test_release_derives_hero_outcomes_from_raw_side_name_facts(
    tmp_path: Path,
) -> None:
    store = sealed_store(tmp_path, 1)

    release = ReleaseBuilder(tmp_path, store=store).build(
        "2026-08-07", store.seals()
    )

    hero_window = json.loads((release.path / "window/heroes.json").read_bytes())
    dooley = next(
        row
        for row in hero_window["rows"]
        if row["hero"] == "Dooley" and row["segment"] == "all"
    )
    assert dooley["win_rate"] == 0.6
    assert dooley["matchups"] == [
        {
            "opponent_hero": "Dooley",
            "decided": 50,
            "wins": 30,
            "losses": 20,
            "win_rate": 0.6,
        }
    ]
    assert dooley["ghost"] == {"battles": 50, "win_rate": 0.4}


def test_duckdb_candidate_ranking_uses_the_payload_score() -> None:
    cases = (
        (1, 1, 0, 10, 1, 0, 1),
        (10, 1, 1, 13, 1, 2, 1),
        (100, 20, 5, 230, 20, 17, 20),
        (10_000, 4_321, 987, 48_765, 4_000, 5_432, 4_321),
    )
    values = ",".join(f"({','.join(map(str, case))})" for case in cases)
    connection = release_module.duckdb.connect(database=":memory:")
    try:
        observed = connection.execute(
            f"""
            WITH aggregated(
              completed, ten_win, legend_ten_win, day_sum, day_count,
              loss_sum, loss_count
            ) AS (VALUES {values})
            SELECT completed, ten_win, legend_ten_win, day_sum, day_count,
                   loss_sum, loss_count, {release_module._candidate_score_sql()}
            FROM aggregated
            ORDER BY completed, ten_win, legend_ten_win
            """
        ).fetchall()
    finally:
        connection.close()

    expected = []
    for case in sorted(cases):
        (
            completed,
            ten_win,
            legend_ten_win,
            day_sum,
            day_count,
            loss_sum,
            loss_count,
        ) = case
        expected.append(
            (
                completed,
                ten_win,
                legend_ten_win,
                day_sum,
                day_count,
                loss_sum,
                loss_count,
                release_module.candidate_score(
                    completed=completed,
                    ten_win=ten_win,
                    legend_ten_win=legend_ten_win,
                    average_day=day_sum / day_count if day_count else None,
                    average_losses=loss_sum / loss_count if loss_count else None,
                ),
            )
        )
    assert observed == expected


def test_check_9_invalid_payload_never_promotes_the_staging_directory(
    tmp_path: Path,
) -> None:
    store = sealed_store(tmp_path, 1)

    def corrupt_payload(seam: str, stage: Path) -> None:
        if seam != "after_payloads_written":
            return
        path = stage / "daily/2026-08-07.json"
        value = json.loads(path.read_bytes())
        value["not_in_the_frozen_contract"] = True
        path.write_bytes(canonical_json(value))

    builder = ReleaseBuilder(tmp_path, store=store, fault_injector=corrupt_payload)
    with pytest.raises(ContractViolation, match="hero_daily"):
        builder.build("2026-08-07", store.seals())

    assert not any(path.name.startswith("2026-08-07-") for path in (tmp_path / "releases").iterdir())


def test_check_10_manifest_hash_size_and_exact_file_set_are_verified(
    tmp_path: Path,
) -> None:
    store = sealed_store(tmp_path, 1)

    def corrupt_after_inventory(seam: str, stage: Path) -> None:
        if seam == "after_manifest_written":
            quality = stage / "quality.json"
            quality.write_bytes(quality.read_bytes() + b" ")

    builder = ReleaseBuilder(
        tmp_path, store=store, fault_injector=corrupt_after_inventory
    )
    with pytest.raises(ManifestMismatch, match="quality.json"):
        builder.build("2026-08-07", store.seals())

    assert not any(path.name.startswith("2026-08-07-") for path in (tmp_path / "releases").iterdir())


def test_check_11_manifest_release_identity_must_start_with_anchor(
    tmp_path: Path,
) -> None:
    store = sealed_store(tmp_path, 1)

    def change_identity(seam: str, stage: Path) -> None:
        if seam != "after_manifest_written":
            return
        path = stage / "manifest.json"
        value = json.loads(path.read_bytes())
        value["release_id"] = "2026-08-08-0000000000000000"
        path.write_bytes(canonical_json(value))

    builder = ReleaseBuilder(tmp_path, store=store, fault_injector=change_identity)
    with pytest.raises(ReleaseIdentityError, match="anchor"):
        builder.build("2026-08-07", store.seals())


def test_existing_release_is_reused_by_reading_only_its_manifest(
    tmp_path: Path, monkeypatch
) -> None:
    store = sealed_store(tmp_path, 1)
    builder = ReleaseBuilder(tmp_path, store=store, threads=4)
    first = builder.build("2026-08-07", store.seals())

    def no_connection(**_kwargs):
        raise AssertionError("Release reuse must not open DuckDB")

    monkeypatch.setattr(release_module.duckdb, "connect", no_connection)

    reused = builder.build("2026-08-07", tuple(replace(item) for item in store.seals()))

    assert reused.release_id == first.release_id
    assert reused.path == first.path
    assert reused.reused is True


def test_build_uses_one_configured_connection_and_only_explicit_hour_paths(
    tmp_path: Path, monkeypatch
) -> None:
    store = sealed_store(tmp_path, 1)
    real_connect = release_module.duckdb.connect
    statements: list[str] = []
    connection_count = 0

    class RecordingConnection:
        def __init__(self) -> None:
            self.inner = real_connect(database=":memory:")

        def execute(self, sql, parameters=()):
            statements.append(sql)
            return self.inner.execute(sql, parameters)

        def close(self) -> None:
            self.inner.close()

    def recording_connect(**_kwargs):
        nonlocal connection_count
        connection_count += 1
        return RecordingConnection()

    monkeypatch.setattr(release_module.duckdb, "connect", recording_connect)

    ReleaseBuilder(tmp_path, store=store, memory_limit="512MB", threads=4).build(
        "2026-08-07", store.seals()
    )

    assert connection_count == 1
    normalized = [" ".join(statement.split()) for statement in statements]
    assert "SET memory_limit='512MB'" in normalized
    assert any(statement.startswith("SET temp_directory='") for statement in normalized)
    assert any(statement == "SET threads=4" for statement in normalized)
    assert any(statement == "SET preserve_insertion_order=false" for statement in normalized)
    parquet_reads = [statement for statement in normalized if "read_parquet([" in statement]
    assert len(parquet_reads) == 3
    assert all(statement.count(".parquet'") == 24 for statement in parquet_reads)
    assert all("*.parquet" not in statement for statement in parquet_reads)


@pytest.mark.timeout(10)
def test_release_build_spills_under_a_256mb_memory_limit(tmp_path: Path) -> None:
    store = _spill_stress_store(tmp_path, non_final_battles=50_000)
    output = tmp_path / "output"

    release = ReleaseBuilder(
        output,
        store=store,
        memory_limit="256MB",
        threads=8,
    ).build("2026-08-07", store.seals())

    builds = json.loads((release.path / "window/builds.json").read_bytes())
    assert builds["heroes"][0]["candidate_build_count"] == 1
    assert builds["heroes"][0]["included_build_count"] == 1

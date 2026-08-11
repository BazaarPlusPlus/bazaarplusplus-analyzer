from __future__ import annotations

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

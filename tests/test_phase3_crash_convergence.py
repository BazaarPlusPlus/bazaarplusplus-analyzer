from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from bpp_analyzer.fact_store import FactStore
from bpp_analyzer.object_store import LocalObjectStore
from bpp_analyzer.release import POINTER_KEY, ReleaseBuilder, ReleasePublisher
from tests.release_fixtures import commit_sealed_day, sealed_store


NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


def _bytes(root: Path) -> dict[str, bytes]:
    if not root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize("fault_seam", ["before_hour_promote", "before_seal_promote"])
def test_acceptance_3_commit_and_seal_crashes_converge_to_uninterrupted_facts(
    tmp_path: Path, fault_seam: str
) -> None:
    source_day = date(2026, 8, 7)
    expected_root = tmp_path / "expected"
    commit_sealed_day(FactStore(expected_root), source_day)
    crashed_root = tmp_path / "crashed"
    fired = False

    def crash(seam: str, path: Path) -> None:
        nonlocal fired
        if seam == fault_seam and not fired:
            fired = True
            raise RuntimeError(f"crash at {fault_seam}: {path.name}")

    with pytest.raises(RuntimeError, match=fault_seam):
        commit_sealed_day(
            FactStore(crashed_root, fault_injector=crash),
            source_day,
        )

    commit_sealed_day(FactStore(crashed_root), source_day)
    assert _bytes(crashed_root / "facts") == _bytes(expected_root / "facts")


def test_acceptance_3_release_stage_crash_converges_to_uninterrupted_release(
    tmp_path: Path,
) -> None:
    expected_root = tmp_path / "expected"
    expected_facts = sealed_store(expected_root, 1)
    expected = ReleaseBuilder(expected_root, store=expected_facts).build(
        "2026-08-07", expected_facts.seals()
    )
    crashed_root = tmp_path / "crashed"
    crashed_facts = sealed_store(crashed_root, 1)

    def crash(seam: str, _path: Path) -> None:
        if seam == "after_manifest_written":
            raise RuntimeError("crash mid-stage")

    with pytest.raises(RuntimeError, match="mid-stage"):
        ReleaseBuilder(
            crashed_root,
            store=crashed_facts,
            fault_injector=crash,
        ).build("2026-08-07", crashed_facts.seals())

    converged = ReleaseBuilder(crashed_root, store=crashed_facts).build(
        "2026-08-07", crashed_facts.seals()
    )
    assert converged.release_id == expected.release_id
    assert _bytes(converged.path) == _bytes(expected.path)


def test_acceptance_3_pre_pointer_publish_crash_converges_to_uninterrupted_remote(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    facts = sealed_store(root, 1)
    release = ReleaseBuilder(root, store=facts).build("2026-08-07", facts.seals())
    expected_objects = LocalObjectStore(tmp_path / "expected-objects", clock=lambda: NOW)
    ReleasePublisher(root, expected_objects, clock=lambda: NOW).publish(release)
    crashed_objects = LocalObjectStore(tmp_path / "crashed-objects", clock=lambda: NOW)

    def crash(seam: str, _key: str) -> None:
        if seam == "before_pointer_put":
            raise RuntimeError("crash before pointer")

    with pytest.raises(RuntimeError, match="before pointer"):
        ReleasePublisher(
            root,
            crashed_objects,
            clock=lambda: NOW,
            fault_injector=crash,
        ).publish(release)
    assert crashed_objects.get(POINTER_KEY) is None

    ReleasePublisher(root, crashed_objects, clock=lambda: NOW).publish(release)
    assert _bytes(crashed_objects.root) == _bytes(expected_objects.root)

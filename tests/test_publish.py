from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from bpp_analyzer.fact_store import canonical_json
from bpp_analyzer.object_store import LocalObjectStore
from bpp_analyzer.release import (
    IMMUTABLE_CACHE_CONTROL,
    POINTER_CACHE_CONTROL,
    POINTER_KEY,
    AntiRegressionError,
    ImmutableObjectConflict,
    InvalidPointer,
    ReleaseBuilder,
    ReleasePublisher,
)
from tests.release_fixtures import sealed_store


NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


def _releases(root: Path):
    facts = sealed_store(root, 2)
    builder = ReleaseBuilder(root, store=facts, threads=4)
    older = builder.build("2026-08-07", facts.seals())
    newer = builder.build("2026-08-08", facts.seals())
    return facts, older, newer


def _release_keys(release) -> set[str]:
    return {
        f"analyzer-v5/releases/{release.release_id}/{path.relative_to(release.path).as_posix()}"
        for path in release.path.rglob("*")
        if path.is_file()
    }


@pytest.mark.parametrize(
    "pointer_bytes",
    [
        b"not-json\n",
        canonical_json(
            {
                "schema_version": 1,
                "kind": "release_manifest",
                "generated_at": "2026-08-09T00:00:00Z",
                "release_id": "2026-08-07-0000000000000000",
                "window": {"start": "2026-08-08", "end": "2026-08-08", "days": 1},
                "builder_code_version": "fixture",
                "policy_version": "fixture",
                "files": [
                    {"path": "quality.json", "sha256": "0" * 64, "bytes": 1},
                    {"path": "window/builds.json", "sha256": "1" * 64, "bytes": 1},
                    {"path": "window/heroes.json", "sha256": "2" * 64, "bytes": 1},
                ],
            }
        ),
    ],
)
def test_check_12_unparseable_or_self_inconsistent_pointer_blocks_publish(
    tmp_path: Path, pointer_bytes: bytes
) -> None:
    _facts, _older, newer = _releases(tmp_path / "data")
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)
    objects.put(POINTER_KEY, pointer_bytes, cache_control=POINTER_CACHE_CONTROL)
    objects.clear_requests()

    with pytest.raises(InvalidPointer):
        ReleasePublisher(tmp_path / "data", objects, clock=lambda: NOW).publish(newer)

    assert [(item.operation, item.key) for item in objects.requests] == [
        ("get", POINTER_KEY)
    ]


def test_check_13_blocks_older_window_but_allows_corrected_same_anchor(
    tmp_path: Path,
) -> None:
    facts, older, newer = _releases(tmp_path / "data")
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)
    publisher = ReleasePublisher(tmp_path / "data", objects, clock=lambda: NOW)
    publisher.publish(newer)

    with pytest.raises(AntiRegressionError):
        publisher.publish(older)
    assert publisher.current_pointer().release_id == newer.release_id

    corrected = ReleaseBuilder(
        tmp_path / "data",
        store=facts,
        builder_code_version="0.2.0-correction",
        threads=4,
    ).build("2026-08-08", facts.seals())
    publisher.publish(corrected)
    assert publisher.current_pointer().release_id == corrected.release_id


def test_check_14_existing_different_immutable_object_is_a_hard_conflict(
    tmp_path: Path,
) -> None:
    _facts, _older, newer = _releases(tmp_path / "data")
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)
    conflict_key = f"analyzer-v5/releases/{newer.release_id}/manifest.json"
    objects.put(conflict_key, b"different\n", cache_control=IMMUTABLE_CACHE_CONTROL)

    with pytest.raises(ImmutableObjectConflict, match="manifest.json"):
        ReleasePublisher(tmp_path / "data", objects, clock=lambda: NOW).publish(newer)

    assert objects.get(conflict_key).body == b"different\n"
    assert objects.get(POINTER_KEY) is None


def test_checks_15_and_16_cache_headers_are_exact_and_pointer_put_is_last(
    tmp_path: Path,
) -> None:
    _facts, _older, newer = _releases(tmp_path / "data")
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)

    result = ReleasePublisher(
        tmp_path / "data", objects, clock=lambda: NOW
    ).publish(newer)

    assert result.release_id == newer.release_id
    expected_keys = _release_keys(newer)
    for key in expected_keys:
        assert objects.stat(key).cache_control == IMMUTABLE_CACHE_CONTROL
    pointer = objects.get(POINTER_KEY)
    assert pointer is not None
    assert pointer.body == (newer.path / "manifest.json").read_bytes()
    assert pointer.stat.cache_control == POINTER_CACHE_CONTROL
    put_keys = [item.key for item in objects.requests if item.operation == "put"]
    assert set(put_keys[:-1]) == expected_keys
    assert put_keys[-1] == POINTER_KEY


def test_check_16_artifact_failure_or_crash_before_pointer_never_exposes_release(
    tmp_path: Path,
) -> None:
    _facts, _older, newer = _releases(tmp_path / "data")
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)

    def crash(seam: str, _key: str) -> None:
        if seam == "before_pointer_put":
            raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        ReleasePublisher(
            tmp_path / "data",
            objects,
            clock=lambda: NOW,
            fault_injector=crash,
        ).publish(newer)

    assert objects.get(POINTER_KEY) is None
    assert all(objects.stat(key) is not None for key in _release_keys(newer))

    ReleasePublisher(tmp_path / "data", objects, clock=lambda: NOW).publish(newer)
    assert json.loads(objects.get(POINTER_KEY).body)["release_id"] == newer.release_id

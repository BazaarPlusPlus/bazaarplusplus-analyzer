import json
import subprocess
import sys
from pathlib import Path

import pytest

from bppanalyzer.goldens import validate_golden_release
from bppanalyzer.release import RELEASE_ID_PATTERN, ManifestMismatch, ReleaseBuilder
from tests.release_fixtures import sealed_store

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FREEZE_SCRIPT = REPOSITORY_ROOT / "scripts/freeze_v5_goldens.py"
FROZEN_ROOT = REPOSITORY_ROOT / "contracts/v5/golden"


def _file_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _freeze(release_path: Path, output_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(FREEZE_SCRIPT),
            str(release_path),
            "--output-root",
            str(output_root),
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_freeze_script_copies_an_exact_valid_release_and_refuses_overwrite(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    store = sealed_store(data_root, 1)
    release = ReleaseBuilder(data_root, store=store).build("2026-08-07", store.seals())
    output_root = tmp_path / "golden"

    first = _freeze(release.path, output_root)

    assert first.returncode == 0, first.stderr
    frozen = output_root / release.release_id
    assert _file_bytes(frozen) == _file_bytes(release.path)
    assert json.loads((frozen / "manifest.json").read_bytes())["release_id"] == release.release_id
    before = _file_bytes(frozen)

    second = _freeze(release.path, output_root)

    assert second.returncode != 0
    assert "already exists" in second.stderr
    assert _file_bytes(frozen) == before


def test_golden_validator_rejects_manifest_digest_or_size_drift(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    store = sealed_store(data_root, 1)
    release = ReleaseBuilder(data_root, store=store).build("2026-08-07", store.seals())
    output_root = tmp_path / "golden"
    result = _freeze(release.path, output_root)
    assert result.returncode == 0, result.stderr
    frozen = output_root / release.release_id
    validate_golden_release(frozen)
    manifest = json.loads((frozen / "manifest.json").read_bytes())
    payload = frozen / manifest["files"][0]["path"]
    payload.write_bytes(payload.read_bytes() + b" ")

    with pytest.raises(ManifestMismatch):
        validate_golden_release(frozen)


def test_frozen_golden_vectors_match_schemas_and_manifest_inventory() -> None:
    releases = sorted(path for path in FROZEN_ROOT.glob("*") if path.is_dir())
    if not releases:
        pytest.skip("pending freeze from the first real production build")

    for release in releases:
        assert RELEASE_ID_PATTERN.fullmatch(release.name), (
            f"Unexpected golden release directory: {release.name}"
        )
        validate_golden_release(release)

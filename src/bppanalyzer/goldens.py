"""One-time freezing and offline verification for V5 golden releases."""

import os
import shutil
import tempfile
from pathlib import Path

from bppanalyzer.release import validate_release


class GoldenFreezeError(RuntimeError):
    """A release cannot be frozen without risking an existing golden."""


def freeze_release(
    release_dir: str | Path,
    *,
    output_root: str | Path | None = None,
) -> Path:
    """Validate and atomically copy one immutable release into the golden tree."""
    source = Path(release_dir).resolve()
    if not source.is_dir():
        raise GoldenFreezeError(f"Release directory does not exist: {source}")
    validate_release(source)
    release_id = source.name
    root = Path(output_root).resolve() if output_root is not None else _default_golden_root()
    target = root / release_id
    if target.exists():
        raise GoldenFreezeError(f"Golden release already exists: {target}")

    root.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(tempfile.mkdtemp(prefix=".freeze-v5-", dir=root))
    staged = temporary_root / release_id
    try:
        shutil.copytree(source, staged, copy_function=shutil.copyfile)
        validate_golden_release(staged)
        if target.exists():
            raise GoldenFreezeError(f"Golden release already exists: {target}")
        os.rename(staged, target)
        _fsync_directory(root)
        return target
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)


def validate_golden_release(
    release_dir: str | Path,
    *,
    contracts_dir: str | Path | None = None,
) -> None:
    """Validate every payload schema and the manifest's exact byte inventory."""
    validate_release(release_dir, contracts_dir)


def _default_golden_root() -> Path:
    return Path(__file__).resolve().parents[2] / "contracts/v5/golden"


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

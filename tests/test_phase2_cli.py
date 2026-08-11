from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

import bpp_analyzer.cli as cli
from bpp_analyzer.config import Config
from bpp_analyzer.release import ReleaseBuilder
from tests.release_fixtures import sealed_store


def test_show_release_emits_the_local_manifest(tmp_path: Path, monkeypatch) -> None:
    store = sealed_store(tmp_path, 1)
    release = ReleaseBuilder(tmp_path, store=store).build(
        "2026-08-07", store.seals()
    )
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda **_kwargs: Config(tmp_path, None, None),
    )

    result = CliRunner().invoke(cli.main, ["show", "release", release.release_id])

    assert result.exit_code == 0
    assert json.loads(result.output)["release_id"] == release.release_id

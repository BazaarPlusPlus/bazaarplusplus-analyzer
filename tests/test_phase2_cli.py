from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

import bpp_analyzer.cli as cli
from bpp_analyzer.config import Config
from bpp_analyzer.locking import DirectoryLock
from bpp_analyzer.object_store import LocalObjectStore
from bpp_analyzer.release import POINTER_KEY, ReleaseBuilder
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


def test_publish_rollback_and_resume_commands_use_the_locked_publish_flow(
    tmp_path: Path, monkeypatch
) -> None:
    facts = sealed_store(tmp_path, 2)
    builder = ReleaseBuilder(tmp_path, store=facts)
    older = builder.build("2026-08-07", facts.seals())
    newer = builder.build("2026-08-08", facts.seals())
    objects = LocalObjectStore(tmp_path / "fake-r2")
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: Config(tmp_path, None, None))
    monkeypatch.setattr(cli, "_object_store", lambda _config: objects, raising=False)
    runner = CliRunner()

    assert runner.invoke(cli.main, ["publish", newer.release_id]).exit_code == 0
    assert json.loads(objects.get(POINTER_KEY).body)["release_id"] == newer.release_id

    locked = DirectoryLock(tmp_path, "operator-lock-test", heartbeat_interval=60)
    locked.acquire()
    try:
        blocked = runner.invoke(
            cli.main,
            ["rollback", older.release_id, "--reason", "fixture rollback"],
        )
    finally:
        locked.release()
    assert blocked.exit_code == 3
    assert not (tmp_path / "publish-hold.json").exists()

    assert runner.invoke(
        cli.main,
        ["rollback", older.release_id, "--reason", "fixture rollback"],
    ).exit_code == 0
    assert runner.invoke(cli.main, ["publish", newer.release_id]).exit_code == 1
    assert json.loads(objects.get(POINTER_KEY).body)["release_id"] == older.release_id

    assert runner.invoke(
        cli.main, ["resume", "--reason", "fixture corrected"]
    ).exit_code == 0
    assert runner.invoke(cli.main, ["publish", newer.release_id]).exit_code == 0
    assert json.loads(objects.get(POINTER_KEY).body)["release_id"] == newer.release_id

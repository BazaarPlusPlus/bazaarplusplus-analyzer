from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

import bpp_analyzer.cli as cli
from bpp_analyzer.bundle_source import RetryableSourceError
from bpp_analyzer.config import Config
from bpp_analyzer.locking import DirectoryLock


class FailedContextSource:
    def __init__(self, **_kwargs) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        pass

    def hour_index(self, _source_hour):
        raise RetryableSourceError("fixture_retryable", "fixture failed")

    def stream(self, _index):
        raise AssertionError("A failed index must never be streamed")


def _config(root: Path) -> Config:
    return Config(root, "https://api.invalid", "test-token")


def test_cli_maps_success_usage_lock_and_partial_outcomes_to_frozen_exit_codes(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(tmp_path))
    monkeypatch.setattr(cli, "BundleSource", FailedContextSource)
    runner = CliRunner()

    assert runner.invoke(cli.main, ["status", "--json"]).exit_code == 0
    assert runner.invoke(cli.main, ["publish", "release-id"]).exit_code == 1
    assert runner.invoke(cli.main, ["run", "--heal-days", "0"]).exit_code == 2

    holder = DirectoryLock(tmp_path, "held-by-test", heartbeat_interval=60)
    holder.acquire()
    try:
        locked = runner.invoke(cli.main, ["run"])
    finally:
        holder.release()
    assert locked.exit_code == 3
    assert not (tmp_path / "status.json").exists()
    assert not (tmp_path / "runs.jsonl").exists()

    partial = runner.invoke(cli.main, ["run", "--heal-days", "1"])
    assert partial.exit_code == 4

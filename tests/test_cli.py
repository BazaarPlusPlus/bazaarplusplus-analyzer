from datetime import UTC, datetime
from functools import partial
import json
from pathlib import Path

from click.testing import CliRunner

import bpp_analyzer.cli as cli
from bpp_analyzer.bundle_source import RawHourIndex, RetryableSourceError, raw_commit_sha256
from bpp_analyzer.config import Config
from bpp_analyzer.driver import PipelineDriver
from bpp_analyzer.locking import DirectoryLock
from bpp_analyzer.object_store import LocalObjectStore
from bpp_analyzer.release import (
    POINTER_CACHE_CONTROL,
    POINTER_KEY,
    ReleaseBuilder,
    ReleasePublisher,
)
from tests.release_fixtures import sealed_store


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


class EmptyContextSource:
    def __init__(self, **_kwargs) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        pass

    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        return RawHourIndex(source_hour, (), raw_commit_sha256(()), 1)

    def stream(self, _index):
        return iter(())


class NeverContextSource(EmptyContextSource):
    def hour_index(self, _source_hour):
        raise AssertionError("Sealed fixture days must not reach the Bundle Server")


def _config(root: Path) -> Config:
    return Config(root, "https://api.invalid", "test-token")


def test_cli_maps_success_usage_lock_and_partial_outcomes_to_frozen_exit_codes(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(tmp_path))
    monkeypatch.setattr(cli, "BundleSource", FailedContextSource)
    monkeypatch.setattr(cli, "_object_store", lambda _config: LocalObjectStore(tmp_path / "r2"))
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


def test_cli_dry_run_uses_only_the_fake_pointer_get(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    objects = LocalObjectStore(tmp_path / "fake-r2")
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(data_root))
    monkeypatch.setattr(cli, "_object_store", lambda _config: objects)

    result = CliRunner().invoke(cli.main, ["run", "--dry-run"])

    assert result.exit_code == 0
    assert json.loads(result.output)["published_release_id"] is None
    assert [(item.operation, item.key) for item in objects.requests] == [
        ("get", "analyzer-v5/manifest.json")
    ]
    assert not data_root.exists()


def test_cli_run_streams_the_heal_plan_and_zero_row_hour_progress(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(tmp_path))
    monkeypatch.setattr(cli, "BundleSource", EmptyContextSource)
    monkeypatch.setattr(
        cli,
        "PipelineDriver",
        partial(PipelineDriver, clock=lambda: now),
    )
    monkeypatch.setattr(
        cli,
        "_object_store",
        lambda _config: LocalObjectStore(tmp_path / "fake-r2"),
    )

    result = CliRunner().invoke(cli.main, ["run", "--heal-days", "1"])

    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert lines[0] == "heal plan: days=1 missing_settled_hours=1"
    assert lines[1] == "hour started: source_hour=2026-08-07T00 [1/1]"
    assert lines[2].startswith(
        "hour indexed: source_hour=2026-08-07T00 bundles=0 pages=1 elapsed="
    )
    assert lines[3] == (
        "hour ingest started: source_hour=2026-08-07T00 bundles=0"
    )
    assert lines[4].startswith(
        "healed 2026-08-07T00 bundles=0 rows=0 bytes="
    )
    assert lines[4].endswith("[1/1]")
    assert lines[-1] == "ok: 1 hours ingested, 0 days sealed, 0 days abandoned"


def test_cli_run_quiet_suppresses_progress_but_keeps_the_final_summary(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(tmp_path))
    monkeypatch.setattr(cli, "BundleSource", EmptyContextSource)
    monkeypatch.setattr(
        cli,
        "PipelineDriver",
        partial(PipelineDriver, clock=lambda: now),
    )
    monkeypatch.setattr(
        cli,
        "_object_store",
        lambda _config: LocalObjectStore(tmp_path / "fake-r2"),
    )

    result = CliRunner().invoke(
        cli.main,
        ["run", "--heal-days", "1", "--quiet"],
    )

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "ok: 1 hours ingested, 0 days sealed, 0 days abandoned"
    ]


def test_cli_noop_prints_no_per_hour_progress_and_uses_one_pointer_get(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 8, 7, 23, 59, tzinfo=UTC)
    facts = sealed_store(tmp_path, 1)
    local = ReleaseBuilder(tmp_path, store=facts).build(
        "2026-08-07",
        facts.seals(),
    )
    objects = LocalObjectStore(tmp_path / "fake-r2", clock=lambda: now)
    ReleasePublisher(tmp_path, objects, clock=lambda: now).publish(local)
    objects.clear_requests()
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(tmp_path))
    monkeypatch.setattr(cli, "BundleSource", NeverContextSource)
    monkeypatch.setattr(
        cli,
        "PipelineDriver",
        partial(PipelineDriver, clock=lambda: now),
    )
    monkeypatch.setattr(cli, "_object_store", lambda _config: objects)

    result = CliRunner().invoke(
        cli.main,
        ["run", "--heal-days", "1", "--anchor-day", "2026-08-07"],
    )

    assert result.exit_code == 0
    assert not any(
        line.startswith("healed ") for line in result.output.splitlines()
    )
    assert "already published" in result.output
    assert [(request.operation, request.key) for request in objects.requests] == [
        ("get", "analyzer-v5/manifest.json")
    ]


def test_cli_run_records_an_invalid_pointer_as_error_exit_one(
    tmp_path: Path, monkeypatch
) -> None:
    data_root = tmp_path / "data"
    now = datetime(2026, 8, 7, 23, 59, tzinfo=UTC)
    sealed_store(data_root, 1)
    objects = LocalObjectStore(tmp_path / "fake-r2", clock=lambda: now)
    invalid_pointer = b"not-json\n"
    objects.put(
        POINTER_KEY,
        invalid_pointer,
        cache_control=POINTER_CACHE_CONTROL,
    )
    objects.clear_requests()
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(data_root))
    monkeypatch.setattr(cli, "BundleSource", NeverContextSource)
    monkeypatch.setattr(
        cli,
        "PipelineDriver",
        partial(PipelineDriver, clock=lambda: now),
    )
    monkeypatch.setattr(cli, "_object_store", lambda _config: objects)

    result = CliRunner().invoke(
        cli.main,
        ["run", "--heal-days", "1", "--anchor-day", "2026-08-07"],
    )

    assert result.exit_code == 1
    assert "Public pointer does not match the frozen manifest contract" in result.output
    status = json.loads((data_root / "status.json").read_bytes())
    assert status["last_run"]["outcome"] == "error"
    assert status["last_run"]["exit_code"] == 1
    assert status["last_run"]["release_built"] is not None
    assert status["last_run"]["release_published"] is None
    assert status["release"]["pointer_state"] == "invalid"
    assert status["release"]["published_release_id"] is None
    assert objects.get(POINTER_KEY).body == invalid_pointer
    assert [(request.operation, request.key) for request in objects.requests[:-1]] == [
        ("get", POINTER_KEY)
    ]


def test_cli_run_quiet_still_prints_retryable_hour_errors(
    tmp_path: Path, monkeypatch
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(cli, "load_config", lambda **_kwargs: _config(tmp_path))
    monkeypatch.setattr(cli, "BundleSource", FailedContextSource)
    monkeypatch.setattr(
        cli,
        "PipelineDriver",
        partial(PipelineDriver, clock=lambda: now),
    )
    monkeypatch.setattr(
        cli,
        "_object_store",
        lambda _config: LocalObjectStore(tmp_path / "fake-r2"),
    )

    result = CliRunner().invoke(
        cli.main,
        ["run", "--heal-days", "1", "--quiet"],
    )

    assert result.exit_code == 4
    assert result.output.splitlines() == [
        "source hour failed: 2026-08-07T00 (fixture_retryable)",
        "partial: 0 hours ingested, 0 days sealed, 0 days abandoned",
    ]


def test_cli_status_text_shows_live_current_run_progress(
    tmp_path: Path, monkeypatch
) -> None:
    status = {
        "facts": {"newest_sealed_day": None, "abandoned_days": []},
        "current_run": {
            "run_id": "live-run",
            "phase": "heal",
            "step": "ingest",
            "current_hour": "2026-08-07T00",
            "hours_done": 1,
            "hours_planned": 2,
            "bundles": {"done": 250, "total": 2000},
            "started_at": "2026-08-07T02:01:00Z",
            "updated_at": "2026-08-07T02:03:00Z",
        },
        "last_run": {"outcome": "ok"},
    }
    (tmp_path / "status.json").write_text(json.dumps(status))
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda **_kwargs: Config(tmp_path, None, None),
    )

    result = CliRunner().invoke(cli.main, ["status"])

    assert result.exit_code == 0
    assert (
        "current run: live-run phase=heal step=ingest hour=2026-08-07T00 "
        "hours=1/2 bundles=250/2000 updated=2026-08-07T02:03:00Z "
        "started=2026-08-07T02:01:00Z"
    ) in result.output

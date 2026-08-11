from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import time

import pytest

from bpp_analyzer.bundle_source import (
    HourExpired,
    RawHourIndex,
    RetryableSourceError,
    raw_commit_sha256,
)
from bpp_analyzer.driver import PipelineDriver
from bpp_analyzer.fact_store import FactStore
from bpp_analyzer.locking import LockOwnershipLost, MaximumRunTimeExceeded


class ExpiredSource:
    def __init__(self) -> None:
        self.calls: list[datetime] = []

    def hour_index(self, source_hour: datetime):
        self.calls.append(source_hour)
        raise HourExpired("source_hour_expired", "fixture expired")

    def stream(self, _index):
        raise AssertionError("An expired index must never be streamed")


class FailedSource:
    def hour_index(self, source_hour: datetime):
        raise RetryableSourceError("fixture_retryable", "fixture failed")

    def stream(self, _index):
        raise AssertionError("A failed index must never be streamed")


class NeverSource:
    def hour_index(self, _source_hour):
        raise AssertionError("No Source Hour is settled")

    def stream(self, _index):
        raise AssertionError("No Source Hour is settled")


class SlowEmptySource:
    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        time.sleep(0.1)
        return RawHourIndex(source_hour, (), raw_commit_sha256(()), 1)

    def stream(self, _index):
        return iter(())


class UnexpectedSecondHourSource:
    def __init__(self) -> None:
        self.calls = 0

    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("unexpected mid-run failure")
        return RawHourIndex(source_hour, (), raw_commit_sha256(()), 1)

    def stream(self, _index):
        return iter(())


class OwnershipReplacingSource:
    def __init__(self, root: Path) -> None:
        self.root = root

    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        heartbeat = self.root / ".lock" / "heartbeat"
        value = json.loads(heartbeat.read_bytes())
        value["run_id"] = "replacement-run"
        heartbeat.write_text(json.dumps(value))
        return RawHourIndex(source_hour, (), raw_commit_sha256(()), 1)

    def stream(self, _index):
        return iter(())


def _hours(day: date) -> tuple[datetime, ...]:
    return tuple(
        datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=value)
        for value in range(24)
    )


def test_expired_hour_abandons_day_visibly_and_subsequent_runs_do_not_retry_or_exit_partial(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 18, 12, tzinfo=UTC)
    target = date(2026, 8, 8)
    store = FactStore(tmp_path, clock=lambda: now)
    for offset in range(11):
        day = date(2026, 8, 8) + timedelta(days=offset)
        if day != target:
            store.abandon_day(day, _hours(day), "test setup")
    source = ExpiredSource()
    driver = PipelineDriver(tmp_path, source=source, clock=lambda: now)

    first = driver.run(heal_days=11)

    assert first.exit_code == 0
    assert first.outcome == "ok"
    assert source.calls == [datetime(2026, 8, 8, tzinfo=UTC)]
    assert store.is_abandoned(target)
    assert not store.has_seal(target)
    assert not any(
        path.name.startswith("source_hour=2026-08-08")
        for path in (tmp_path / "facts/hourly").glob("*")
    )
    status = json.loads((tmp_path / "status.json").read_text())
    abandoned = {item["source_day"] for item in status["facts"]["abandoned_days"]}
    assert "2026-08-08" in abandoned
    assert "source day abandoned: 2026-08-08" in "".join(
        path.read_text() for path in (tmp_path / "logs").glob("*.log")
    )

    second = driver.run(heal_days=11)

    assert second.exit_code == 0
    assert second.outcome == "noop"
    assert source.calls == [datetime(2026, 8, 8, tzinfo=UTC)]


def test_one_failed_hour_is_partial_exit_four_and_remains_visible_for_retry(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)

    result = PipelineDriver(
        tmp_path, source=FailedSource(), clock=lambda: now
    ).run(heal_days=8)

    assert result.outcome == "partial"
    assert result.exit_code == 4
    assert result.failures == (
        {
            "scope": "source_hour",
            "source_hour": "2026-08-07T00",
            "reason": "fixture_retryable",
        },
    )
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["facts"]["incomplete_days"][0]["source_day"] == "2026-08-07"
    assert "2026-08-07T00" in status["facts"]["incomplete_days"][0]["missing_hours"]
    lines = (tmp_path / "runs.jsonl").read_text().splitlines()
    assert json.loads(lines[-1]) == status["last_run"]


def test_maximum_run_time_mid_heal_records_the_failed_run(tmp_path: Path) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    previous_run = {"run_id": "previous-run"}
    (tmp_path / "status.json").write_text(json.dumps({"last_run": previous_run}))
    (tmp_path / "runs.jsonl").write_text(json.dumps(previous_run) + "\n")

    with pytest.raises(MaximumRunTimeExceeded, match="Maximum run time exceeded"):
        PipelineDriver(
            tmp_path,
            source=SlowEmptySource(),
            clock=lambda: now,
            max_run_seconds=0.05,
            heartbeat_interval=60,
        ).run(heal_days=1)

    status = json.loads((tmp_path / "status.json").read_bytes())
    last_run = status["last_run"]
    assert last_run["outcome"] == "error"
    assert last_run["exit_code"] == 1
    assert last_run["hours_ingested"] == 0
    assert last_run["days_sealed"] == 0
    assert last_run["timings"]["total_seconds"] >= 0.05
    assert last_run["timings"]["heal_seconds"] >= 0.05
    assert last_run["failures"] == [
        {"scope": "run", "reason": "Maximum run time exceeded"}
    ]
    lines = (tmp_path / "runs.jsonl").read_text().splitlines()
    assert json.loads(lines[0]) == previous_run
    assert json.loads(lines[-1]) == last_run
    assert len(lines) == 2
    assert not (tmp_path / ".lock").exists()


def test_unexpected_mid_run_exception_records_progress_before_escaping(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 7, 2, 1, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="unexpected mid-run failure"):
        PipelineDriver(
            tmp_path,
            source=UnexpectedSecondHourSource(),
            clock=lambda: now,
            heartbeat_interval=60,
        ).run(heal_days=1)

    status = json.loads((tmp_path / "status.json").read_bytes())
    last_run = status["last_run"]
    assert last_run["outcome"] == "error"
    assert last_run["exit_code"] == 1
    assert last_run["hours_ingested"] == 1
    assert last_run["days_sealed"] == 0
    assert last_run["failures"] == [
        {"scope": "run", "reason": "unexpected mid-run failure"}
    ]
    lines = (tmp_path / "runs.jsonl").read_text().splitlines()
    assert json.loads(lines[-1]) == last_run


def test_ownership_lost_mid_run_does_not_write_run_reports(tmp_path: Path) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    status_before = b'{"last_run":{"run_id":"previous-run"}}\n'
    runs_before = b'{"run_id":"previous-run"}\n'
    (tmp_path / "status.json").write_bytes(status_before)
    (tmp_path / "runs.jsonl").write_bytes(runs_before)

    with pytest.raises(LockOwnershipLost, match="ownership was replaced"):
        PipelineDriver(
            tmp_path,
            source=OwnershipReplacingSource(tmp_path),
            clock=lambda: now,
            heartbeat_interval=60,
        ).run(heal_days=1)

    assert (tmp_path / "status.json").read_bytes() == status_before
    assert (tmp_path / "runs.jsonl").read_bytes() == runs_before
    heartbeat = json.loads((tmp_path / ".lock" / "heartbeat").read_bytes())
    assert heartbeat["run_id"] == "replacement-run"


def test_reporting_failure_does_not_mask_the_original_run_error(
    tmp_path: Path,
) -> None:
    (tmp_path / "status.json").mkdir()
    now = datetime(2026, 8, 7, 2, 1, tzinfo=UTC)

    with pytest.raises(RuntimeError, match="unexpected mid-run failure"):
        PipelineDriver(
            tmp_path,
            source=UnexpectedSecondHourSource(),
            clock=lambda: now,
            heartbeat_interval=60,
        ).run(heal_days=1)

    lines = (tmp_path / "runs.jsonl").read_text().splitlines()
    last_run = json.loads(lines[-1])
    assert last_run["exit_code"] == 1
    assert last_run["failures"][-1]["reason"] == "unexpected mid-run failure"


def test_stale_takeover_records_the_displaced_run_identity(tmp_path: Path) -> None:
    lock_dir = tmp_path / ".lock"
    lock_dir.mkdir()
    heartbeat = lock_dir / "heartbeat"
    heartbeat.write_text(
        json.dumps(
            {
                "run_id": "displaced-run",
                "pid": 1,
                "started_at": "2026-08-01T00:00:00Z",
                "hostname": "fixture",
            }
        )
    )
    stale = time.time() - 600
    os.utime(heartbeat, (stale, stale))

    result = PipelineDriver(
        tmp_path,
        source=NeverSource(),
        clock=lambda: datetime(2026, 8, 7, tzinfo=UTC),
        heartbeat_interval=60,
    ).run(heal_days=8)

    assert result.outcome == "noop"
    log = "".join(path.read_text() for path in (tmp_path / "logs").glob("*.log"))
    assert "stale lock taken over: displaced-run" in log

import json
import os
import shutil
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from bppanalyzer.bundle_source import (
    Bundle,
    BundleRef,
    HourExpired,
    RawHourIndex,
    RetryableSourceError,
    raw_commit_sha256,
)
from bppanalyzer.driver import PipelineDriver
from bppanalyzer.fact_store import FactStore
from bppanalyzer.locking import LockOwnershipLost, MaximumRunTimeExceeded
from bppanalyzer.object_store import LocalObjectStore
from bppanalyzer.projection import project_hour
from bppanalyzer.release import ReleaseBuilder


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


class EmptySource:
    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        return RawHourIndex(source_hour, (), raw_commit_sha256(()), 1)

    def stream(self, _index):
        return iter(())


class QuarantinedBundleSource:
    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        base_ms = int(source_hour.timestamp() * 1_000)
        items = tuple(
            BundleRef(
                bundle_id=f"bundle-{offset}",
                available_at_ms=base_ms + offset,
                download_url=f"https://download.invalid/{offset}",
                download_expires_at_ms=base_ms + 7_200_000,
                sha256=None,
                bytes=None,
            )
            for offset in range(2)
        )
        return RawHourIndex(source_hour, items, raw_commit_sha256(items), 1)

    def stream(self, index: RawHourIndex):
        return iter(Bundle(item, None, None, 0, "fixture_invalid") for item in index.items)


class SlowPointerStore(LocalObjectStore):
    def get(self, key: str):
        time.sleep(0.1)
        return super().get(key)


class StatusObservingSource(EmptySource):
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls = 0
        self.observed_status: dict | None = None

    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        self.calls += 1
        if self.calls == 2:
            self.observed_status = json.loads((self.root / "status.json").read_bytes())
        return super().hour_index(source_hour)


class FirstIndexStatusSource(EmptySource):
    def __init__(self, root: Path) -> None:
        self.root = root
        self.observed_status: dict | None = None

    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        self.observed_status = json.loads((self.root / "status.json").read_bytes())
        return super().hour_index(source_hour)


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
    events: list[str] = []
    abandoned_status: list[dict] = []

    def observe(event: str) -> None:
        events.append(event)
        if event.startswith("abandoned "):
            abandoned_status.append(json.loads((tmp_path / "status.json").read_bytes()))

    first = driver.run(heal_days=11, progress_callback=observe)

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
    assert next(event for event in events if event.startswith("abandoned ")).startswith(
        "abandoned 2026-08-08 missing=24 reason=source_hour_expired elapsed="
    )
    assert abandoned_status[0]["current_run"] == {
        "run_id": first.run_id,
        "phase": "heal",
        "step": "abandoned",
        "current_hour": "2026-08-08T00",
        "hours_done": 0,
        "hours_planned": 24,
        "started_at": "2026-08-18T12:00:00Z",
        "updated_at": "2026-08-18T12:00:00Z",
    }

    second = driver.run(heal_days=11)

    assert second.exit_code == 0
    assert second.outcome == "noop"
    assert source.calls == [datetime(2026, 8, 8, tzinfo=UTC)]


def test_one_failed_hour_is_partial_exit_four_and_remains_visible_for_retry(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)

    result = PipelineDriver(tmp_path, source=FailedSource(), clock=lambda: now).run(heal_days=8)

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
    assert last_run["failures"] == [{"scope": "run", "reason": "Maximum run time exceeded"}]
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
    assert last_run["failures"] == [{"scope": "run", "reason": "unexpected mid-run failure"}]
    lines = (tmp_path / "runs.jsonl").read_text().splitlines()
    assert json.loads(lines[-1]) == last_run


def test_ownership_lost_mid_run_keeps_the_last_owned_checkpoint_only(
    tmp_path: Path,
) -> None:
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

    status = json.loads((tmp_path / "status.json").read_bytes())
    assert status["last_run"] == {"run_id": "previous-run"}
    assert status["current_run"]["step"] == "index"
    assert status["current_run"]["current_hour"] == "2026-08-07T00"
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


def test_hour_seal_and_release_build_progress_is_mirrored_to_the_run_log(
    tmp_path: Path,
) -> None:
    source_day = date(2026, 8, 7)
    now = datetime(2026, 8, 8, 0, 1, tzinfo=UTC)
    store = FactStore(tmp_path, clock=lambda: now)
    for hour in _hours(source_day)[:-1]:
        index = RawHourIndex(hour, (), raw_commit_sha256(()), 1)
        store.commit_hour(project_hour(index, ()))
    events: list[str] = []

    result = PipelineDriver(
        tmp_path,
        source=EmptySource(),
        clock=lambda: now,
    ).run(heal_days=2, progress_callback=events.append)

    assert result.exit_code == 0
    assert events[0] == "heal plan: days=2 missing_settled_hours=1"
    healed = next(event for event in events if event.startswith("healed "))
    sealed = next(event for event in events if event.startswith("sealed "))
    build_started = next(event for event in events if event.startswith("release build started:"))
    build_done = next(event for event in events if event.startswith("release build done:"))
    assert healed.startswith("healed 2026-08-07T23 bundles=0 rows=0 bytes=")
    assert sealed.startswith("sealed 2026-08-07 rows=0 elapsed=")
    assert build_started.startswith("release build started: release_id=2026-08-07-")
    assert build_done.startswith("release build done: release_id=2026-08-07-")
    assert "reused=false" in build_done
    log = "".join(path.read_text() for path in (tmp_path / "logs").glob("*.log"))
    for event in events:
        assert event in log


def test_hour_progress_reports_index_and_streamed_bundle_counts(tmp_path: Path) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    events: list[str] = []

    result = PipelineDriver(
        tmp_path,
        source=QuarantinedBundleSource(),
        clock=lambda: now,
    ).run(heal_days=1, progress_callback=events.append)

    assert result.exit_code == 0
    assert any(
        event.startswith("hour indexed: source_hour=2026-08-07T00 bundles=2 pages=1 elapsed=")
        for event in events
    )
    assert any(
        event.startswith("hour ingest progress: source_hour=2026-08-07T00 bundles=1/2 elapsed=")
        for event in events
    )
    assert any(
        event.startswith("hour ingest progress: source_hour=2026-08-07T00 bundles=2/2 elapsed=")
        for event in events
    )


def test_run_summary_phase_timings_do_not_include_later_phases(tmp_path: Path) -> None:
    source_day = date(2026, 8, 7)
    now = datetime(2026, 8, 8, 0, 1, tzinfo=UTC)
    store = FactStore(tmp_path, clock=lambda: now)
    for hour in _hours(source_day):
        index = RawHourIndex(hour, (), raw_commit_sha256(()), 1)
        store.commit_hour(project_hour(index, ()))
    store.seal_day(source_day)
    ReleaseBuilder(tmp_path, store=store).build(source_day, store.seals())

    result = PipelineDriver(
        tmp_path,
        source=NeverSource(),
        clock=lambda: now,
        object_store=SlowPointerStore(tmp_path / "objects"),
    ).run(heal_days=2)

    assert result.timings["publish_seconds"] >= 0.1
    assert result.timings["build_seconds"] < 0.1


def test_status_exposes_current_run_after_each_hour_commit_and_clears_at_end(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 7, 2, 1, tzinfo=UTC)
    source = StatusObservingSource(tmp_path)

    result = PipelineDriver(
        tmp_path,
        source=source,
        clock=lambda: now,
    ).run(heal_days=1)

    assert source.observed_status is not None
    current = source.observed_status["current_run"]
    assert current == {
        "run_id": result.run_id,
        "phase": "heal",
        "step": "index",
        "current_hour": "2026-08-07T01",
        "hours_done": 1,
        "hours_planned": 2,
        "started_at": "2026-08-07T02:01:00Z",
        "updated_at": "2026-08-07T02:01:00Z",
    }
    final_status = json.loads((tmp_path / "status.json").read_bytes())
    assert final_status["current_run"] is None
    assert final_status["last_run"]["run_id"] == result.run_id


def test_status_exposes_the_active_hour_before_source_indexing_starts(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)
    source = FirstIndexStatusSource(tmp_path)

    result = PipelineDriver(
        tmp_path,
        source=source,
        clock=lambda: now,
    ).run(heal_days=1)

    assert source.observed_status is not None
    assert source.observed_status["current_run"] == {
        "run_id": result.run_id,
        "phase": "heal",
        "step": "index",
        "current_hour": "2026-08-07T00",
        "hours_done": 0,
        "hours_planned": 1,
        "started_at": "2026-08-07T01:01:00Z",
        "updated_at": "2026-08-07T01:01:00Z",
    }


def test_hour_progress_marks_an_identical_commit_reused(tmp_path: Path) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)

    def install_identical_commit(seam: str, stage: Path) -> None:
        if seam != "before_hour_promote":
            return
        final_name = stage.name.removeprefix(".").split(".tmp-", 1)[0]
        shutil.copytree(stage, stage.parent / final_name)

    events: list[str] = []
    result = PipelineDriver(
        tmp_path,
        source=EmptySource(),
        clock=lambda: now,
        fact_fault_injector=install_identical_commit,
    ).run(heal_days=1, progress_callback=events.append)

    assert result.exit_code == 0
    healed = next(event for event in events if event.startswith("healed "))
    assert healed.endswith("reused [1/1]")

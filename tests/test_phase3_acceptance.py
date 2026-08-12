from __future__ import annotations

from datetime import UTC, date, datetime
import json
import os
from pathlib import Path

import bpp_analyzer.driver as driver_module
import bpp_analyzer.release as release_module
from bpp_analyzer.driver import PipelineDriver
from bpp_analyzer.fact_store import FactStore
from bpp_analyzer.object_store import LocalObjectStore, StoreRequest
from bpp_analyzer.release import POINTER_KEY, ReleaseBuilder, ReleasePublisher
from tests.release_fixtures import commit_sealed_day, sealed_store


NOW = datetime(2026, 8, 13, 23, 59, tzinfo=UTC)


class NeverSource:
    def hour_index(self, _source_hour):
        raise AssertionError("Sealed fixture days must not reach the Bundle Server")

    def stream(self, _index):
        raise AssertionError("Sealed fixture days must not reach the Bundle Server")


def _snapshot(*roots: Path) -> dict[str, tuple[int, int]]:
    result: dict[str, tuple[int, int]] = {}
    for root in roots:
        if not root.exists():
            continue
        for path in [root, *sorted(root.rglob("*"))]:
            stat = path.stat()
            result[f"{root.name}/{path.relative_to(root).as_posix()}"] = (
                stat.st_mtime_ns,
                stat.st_size,
            )
    return result


def test_acceptance_1_and_8_second_run_is_a_cheap_nonmutating_noop(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "data"
    sealed_store(root, 1)
    run_now = datetime(2026, 8, 7, 23, 59, tzinfo=UTC)
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: run_now)
    driver = PipelineDriver(
        root,
        source=NeverSource(),
        object_store=objects,
        clock=lambda: run_now,
        duckdb_threads=4,
    )
    first_events: list[str] = []
    first = driver.run(
        heal_days=1,
        anchor_day="2026-08-07",
        progress_callback=first_events.append,
    )
    assert first.exit_code == 0
    assert first.release_published == first.release_built
    assert first_events[-4] == "publish pointer check started"
    assert first_events[-3].startswith(
        "publish pointer check done: published_release_id=none elapsed="
    )
    assert first_events[-2] == (
        f"publish started: release_id={first.release_published}"
    )
    assert first_events[-1].startswith(
        f"publish done: release_id={first.release_published} uploaded="
    )
    assert objects.requests[-1] == StoreRequest("get", POINTER_KEY)
    before = _snapshot(root / "facts", root / "releases")

    def no_duckdb(**_kwargs):
        raise AssertionError("A no-op run must not open DuckDB")

    def no_parquet_paths(self, _days):
        raise AssertionError("A no-op run must not ask for Parquet paths")

    monkeypatch.setattr(release_module.duckdb, "connect", no_duckdb)
    monkeypatch.setattr(FactStore, "hour_paths", no_parquet_paths)
    objects.clear_requests()

    second_events: list[str] = []
    second = driver.run(
        heal_days=1,
        anchor_day="2026-08-07",
        progress_callback=second_events.append,
    )

    assert second.exit_code == 0
    assert second.outcome == "noop"
    assert _snapshot(root / "facts", root / "releases") == before
    assert [(item.operation, item.key) for item in objects.requests] == [
        ("get", POINTER_KEY)
    ]
    assert not any(event.startswith("healed ") for event in second_events)
    assert second_events[-1] == (
        f"publish skipped: release_id={first.release_published} already published"
    )
    status = json.loads((root / "status.json").read_bytes())
    assert status["release"]["published_release_id"] == first.release_published
    assert status["release"]["published_window_end"] == "2026-08-07"
    assert status["release"]["published_manifest_age_seconds"] == 0.0
    assert status["release"]["pointer_state"] == "ok"


def test_no_publish_builds_locally_and_performs_no_object_store_write(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    sealed_store(root, 1)
    run_now = datetime(2026, 8, 7, 23, 59, tzinfo=UTC)
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: run_now)

    result = PipelineDriver(
        root,
        source=NeverSource(),
        object_store=objects,
        clock=lambda: run_now,
    ).run(heal_days=1, anchor_day="2026-08-07", publish=False)

    assert result.release_built is not None
    assert result.release_published is None
    status = json.loads((root / "status.json").read_bytes())
    assert status["release"]["pointer_state"] == "absent"
    assert status["release"]["published_release_id"] is None
    assert [(item.operation, item.key) for item in objects.requests] == [
        ("get", POINTER_KEY)
    ]


def test_status_fallback_retains_authoritative_pointer_fields(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "data"
    sealed_store(root, 1)
    current_time = [datetime(2026, 8, 7, 23, 59, tzinfo=UTC)]
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: current_time[0])
    driver = PipelineDriver(
        root,
        source=NeverSource(),
        object_store=objects,
        clock=lambda: current_time[0],
    )
    first = driver.run(heal_days=1, anchor_day="2026-08-07")
    current_time[0] = datetime(2026, 8, 8, 0, 59, tzinfo=UTC)

    def fail_status(*_args, **_kwargs):
        raise RuntimeError("status fixture failure")

    monkeypatch.setattr(driver_module, "build_status", fail_status)
    second = driver.run(heal_days=1, anchor_day="2026-08-07")

    assert second.exit_code == 1
    status = json.loads((root / "status.json").read_bytes())
    assert status["release"] == {
        "local_newest_release_id": first.release_published,
        "pointer_state": "ok",
        "publish_hold": False,
        "published_manifest_age_seconds": 3600.0,
        "published_release_id": first.release_published,
        "published_window_end": "2026-08-07",
    }


def test_acceptance_9_late_seal_inside_lookback_changes_identity_and_publishes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    facts = FactStore(root)
    commit_sealed_day(facts, date(2026, 8, 7), day_offset=0)
    commit_sealed_day(facts, date(2026, 8, 9), day_offset=2)
    initial = ReleaseBuilder(root, store=facts).build("2026-08-09", facts.seals())
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)
    ReleasePublisher(root, objects, clock=lambda: NOW).publish(initial)

    commit_sealed_day(facts, date(2026, 8, 8), day_offset=1)
    result = PipelineDriver(
        root,
        source=NeverSource(),
        object_store=objects,
        clock=lambda: datetime(2026, 8, 9, 23, 59, tzinfo=UTC),
    ).run(heal_days=1, anchor_day="2026-08-09")

    assert result.release_built is not None
    assert result.release_built != initial.release_id
    assert result.release_published == result.release_built
    assert ReleasePublisher(root, objects).current_pointer().release_id == result.release_built


def test_acceptance_10_rollback_hold_sticks_through_run_then_resume_publishes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    facts = sealed_store(root, 2)
    older = ReleaseBuilder(root, store=facts).build("2026-08-07", facts.seals())
    current_time = [datetime(2026, 8, 8, 23, 59, tzinfo=UTC)]
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: current_time[0])
    driver = PipelineDriver(
        root,
        source=NeverSource(),
        object_store=objects,
        clock=lambda: current_time[0],
        keep_releases=3,
    )
    published = driver.run(heal_days=1, anchor_day="2026-08-08")
    assert published.release_published is not None
    publisher = ReleasePublisher(root, objects, clock=lambda: current_time[0])
    publisher.rollback(older, "rollback fixture")

    old_log = root / "logs/20000101T000000.000000Z-fixture.log"
    old_log.write_text("old\n")
    old = current_time[0].timestamp() - 11 * 24 * 60 * 60
    os.utime(old_log, (old, old))
    commit_sealed_day(facts, date(2026, 8, 9), day_offset=2)
    current_time[0] = datetime(2026, 8, 9, 23, 59, tzinfo=UTC)
    held_driver = PipelineDriver(
        root,
        source=NeverSource(),
        object_store=objects,
        clock=lambda: current_time[0],
        keep_releases=1,
    )
    held = held_driver.run(heal_days=1, anchor_day="2026-08-09")

    assert held.release_built is not None
    assert held.release_published is None
    assert publisher.current_pointer().release_id == older.release_id
    assert (root / "publish-hold.json").is_file()
    assert not old_log.exists()
    kept = {path.name for path in (root / "releases").iterdir() if path.is_dir()}
    assert kept == {older.release_id, held.release_built}

    publisher.resume("rollback issue corrected")
    resumed = held_driver.run(heal_days=1, anchor_day="2026-08-09")

    assert resumed.release_published == held.release_built
    assert publisher.current_pointer().release_id == held.release_built

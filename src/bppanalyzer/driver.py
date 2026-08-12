"""Oldest-first heal/seal convergence and local health reporting."""

import json
import os
import resource
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol

from bppanalyzer.bundle_source import HourExpired, RawHourIndex, RetryableSourceError
from bppanalyzer.fact_store import FactStore, parse_source_day
from bppanalyzer.locking import (
    DirectoryLock,
    LockOwnershipLost,
    MaximumRunTimeExceeded,
)
from bppanalyzer.object_store import ObjectStore
from bppanalyzer.projection import project_hour
from bppanalyzer.release import (
    EPOCH_DAY,
    RELEASE_ID_PATTERN,
    InvalidPointer,
    PublishedPointer,
    ReleaseBuilder,
    ReleasePublisher,
    compute_release_id,
    local_newest_release_id,
    window_seals,
)

DEFAULT_HEAL_DAYS = 8
DEFAULT_SETTLE_LAG = timedelta(seconds=60)
BUNDLE_PROGRESS_EVERY = 250
POINTER_STATE_OK = "ok"
POINTER_STATE_INVALID = "invalid"
POINTER_STATE_ABSENT = "absent"


class Source(Protocol):
    def hour_index(self, source_hour: datetime) -> RawHourIndex: ...
    def stream(self, index: RawHourIndex): ...


@dataclass(frozen=True, slots=True)
class RunSummary:
    run_id: str
    started_at: str
    finished_at: str
    timings: dict[str, float]
    outcome: str
    exit_code: int
    hours_ingested: int
    days_sealed: int
    days_abandoned: int
    release_built: str | None
    release_published: str | None
    failures: tuple[dict[str, str], ...]
    peak_rss_bytes: int

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["failures"] = list(self.failures)
        return value


@dataclass(slots=True)
class _RunProgress:
    hours_ingested: int = 0
    hours_planned: int = 0
    days_sealed: int = 0
    days_abandoned: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)
    changed: bool = False
    heal_started_monotonic: float | None = None


class PipelineDriver:
    """Converge recoverable Source Hours into immutable local facts."""

    def __init__(
        self,
        data_root: str | Path,
        *,
        source: Source,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        settle_lag: timedelta = DEFAULT_SETTLE_LAG,
        heartbeat_interval: float = 30,
        stale_after: float = 300,
        max_run_seconds: float = 7200,
        duckdb_memory_limit: str = "8GB",
        duckdb_threads: int = 8,
        object_store: ObjectStore | None = None,
        keep_releases: int = 3,
        fact_fault_injector: Callable[[str, Path], None] | None = None,
        release_fault_injector: Callable[[str, Path], None] | None = None,
        publish_fault_injector: Callable[[str, str], None] | None = None,
    ) -> None:
        if settle_lag <= timedelta():
            raise ValueError("Settle lag must be positive")
        if (
            not isinstance(keep_releases, int)
            or isinstance(keep_releases, bool)
            or keep_releases < 1
        ):
            raise ValueError("keep_releases must be a positive integer")
        self.data_root = Path(data_root)
        self.source = source
        self.clock = clock
        self.settle_lag = settle_lag
        self.heartbeat_interval = heartbeat_interval
        self.stale_after = stale_after
        self.max_run_seconds = max_run_seconds
        self.duckdb_memory_limit = duckdb_memory_limit
        self.duckdb_threads = duckdb_threads
        self.object_store = object_store
        self.keep_releases = keep_releases
        self.fact_fault_injector = fact_fault_injector
        self.release_fault_injector = release_fault_injector
        self.publish_fault_injector = publish_fault_injector

    def run(
        self,
        *,
        heal_days: int = DEFAULT_HEAL_DAYS,
        anchor_day: date | str | None = None,
        publish: bool = True,
        dry_run: bool = False,
        progress_callback: Callable[[str], None] | None = None,
        error_callback: Callable[[str], None] | None = None,
    ) -> RunSummary:
        if not isinstance(heal_days, int) or isinstance(heal_days, bool) or heal_days < 1:
            raise ValueError("heal_days must be a positive integer")
        parsed_anchor = parse_source_day(anchor_day) if anchor_day is not None else None
        now = _aware_utc(self.clock())
        run_id = uuid.uuid4().hex
        if dry_run:
            if self.object_store is not None:
                ReleasePublisher(self.data_root, self.object_store).current_pointer()
            return _summary(
                run_id,
                now,
                now,
                outcome="noop",
                exit_code=0,
                hours_ingested=0,
                days_sealed=0,
                days_abandoned=0,
                failures=(),
                elapsed=0.0,
            )

        lock = DirectoryLock(
            self.data_root,
            run_id,
            heartbeat_interval=self.heartbeat_interval,
            stale_after=self.stale_after,
            max_run_seconds=self.max_run_seconds,
        )
        with lock:
            started_monotonic = time.monotonic()
            progress = _RunProgress()
            summary: RunSummary | None = None
            published_pointer: PublishedPointer | None = None
            pointer_state: str | None = None
            run_log: _RunLog | None = None
            pending_error: BaseException | None = None
            pending_traceback = None
            store = FactStore(
                self.data_root,
                clock=self.clock,
                ownership_check=lock.assert_owned,
                fault_injector=self.fact_fault_injector,
            )
            try:
                publisher = (
                    ReleasePublisher(
                        self.data_root,
                        self.object_store,
                        clock=self.clock,
                        ownership_check=lock.assert_owned,
                        fault_injector=self.publish_fault_injector,
                    )
                    if self.object_store is not None
                    else None
                )
                run_log = _RunLog(
                    self.data_root,
                    run_id,
                    clock=self.clock,
                    ownership_check=lock.assert_owned,
                )
                run_log.write("run started")
                if lock.stale_run_id is not None:
                    run_log.write(f"stale lock taken over: {lock.stale_run_id}")

                def report(message: str) -> None:
                    run_log.write(message)
                    if progress_callback is not None:
                        progress_callback(message)

                def report_error(message: str) -> None:
                    run_log.write(message)
                    if error_callback is not None:
                        error_callback(message)

                def checkpoint(
                    phase: str,
                    current_hour: str | None,
                    *,
                    step: str,
                    bundles_done: int | None = None,
                    bundles_total: int | None = None,
                ) -> None:
                    try:
                        _write_current_status(
                            self.data_root,
                            store,
                            run_id=run_id,
                            phase=phase,
                            step=step,
                            current_hour=current_hour,
                            hours_done=progress.hours_ingested,
                            hours_planned=progress.hours_planned,
                            bundles_done=bundles_done,
                            bundles_total=bundles_total,
                            started_at=now,
                            now=_aware_utc(self.clock()),
                            considered_days=healing_days(now, heal_days),
                            published_pointer=published_pointer,
                            pointer_state=pointer_state,
                            ownership_check=lock.assert_owned,
                        )
                    except LockOwnershipLost:
                        raise
                    except BaseException as error:
                        _try_log(
                            run_log,
                            f"live status refresh failed: {_error_reason(error)}",
                        )

                summary = self._heal(
                    store,
                    lock,
                    run_id=run_id,
                    now=now,
                    heal_days=heal_days,
                    started_monotonic=started_monotonic,
                    progress=progress,
                    log=run_log.write,
                    report=report,
                    report_error=report_error,
                    checkpoint=checkpoint,
                )
                build_started = time.monotonic()
                seals = store.seals()
                selected_anchor = parsed_anchor or (
                    parse_source_day(seals[-1].source_day) if seals else None
                )
                local = None
                if selected_anchor is not None:
                    builder = ReleaseBuilder(
                        self.data_root,
                        store=store,
                        memory_limit=self.duckdb_memory_limit,
                        threads=self.duckdb_threads,
                        ownership_check=lock.assert_owned,
                        fault_injector=self.release_fault_injector,
                    )
                    release_id = compute_release_id(
                        selected_anchor,
                        window_seals(seals, selected_anchor),
                        builder_code_version=builder.builder_code_version,
                        policy_version=builder.policy_version,
                    )
                    checkpoint("build", None, step="build")
                    report(f"release build started: release_id={release_id}")
                    local = builder.build(selected_anchor, seals)
                    report(
                        f"release build done: release_id={local.release_id} "
                        f"elapsed={_format_elapsed(time.monotonic() - build_started)} "
                        f"reused={str(local.reused).lower()}"
                    )
                    if not local.reused:
                        summary = replace(
                            summary,
                            outcome="ok" if summary.outcome == "noop" else summary.outcome,
                            release_built=local.release_id,
                        )
                build_seconds = time.monotonic() - build_started
                publish_started = time.monotonic()
                if publisher is not None:
                    checkpoint("publish", None, step="pointer_check")
                    pointer_started = time.monotonic()
                    report("publish pointer check started")
                    hold = (self.data_root / "publish-hold.json").is_file()
                    publish_candidate = local is not None and publish and not hold
                    try:
                        published_pointer = publisher.current_pointer()
                    except InvalidPointer:
                        pointer_state = POINTER_STATE_INVALID
                        report(
                            "publish pointer check done: published_release_id=none "
                            f"elapsed={_format_elapsed(time.monotonic() - pointer_started)} "
                            f"pointer_state={pointer_state}"
                        )
                        raise
                    else:
                        pointer_state = (
                            POINTER_STATE_OK
                            if published_pointer is not None
                            else POINTER_STATE_ABSENT
                        )
                        report(
                            "publish pointer check done: published_release_id="
                            f"{published_pointer.release_id if published_pointer else 'none'} "
                            f"elapsed={_format_elapsed(time.monotonic() - pointer_started)} "
                            f"pointer_state={pointer_state}"
                        )
                    if publish_candidate and (
                        published_pointer is None
                        or local.release_id != published_pointer.release_id
                    ):
                        publish_action_started = time.monotonic()
                        checkpoint("publish", None, step="upload")
                        report(f"publish started: release_id={local.release_id}")
                        published = publisher.publish(
                            local,
                            current=published_pointer,
                        )
                        published_pointer = published.pointer
                        pointer_state = POINTER_STATE_OK
                        report(
                            f"publish done: release_id={local.release_id} "
                            f"uploaded={published.uploaded} skipped={published.skipped} "
                            f"elapsed={_format_elapsed(time.monotonic() - publish_action_started)}"
                        )
                        summary = replace(
                            summary,
                            outcome="ok" if summary.outcome == "noop" else summary.outcome,
                            release_published=local.release_id,
                        )
                    elif local is None:
                        report("publish skipped: no local release")
                    elif not publish:
                        report(f"publish skipped: release_id={local.release_id} --no-publish")
                    elif hold:
                        report(f"publish skipped: release_id={local.release_id} hold active")
                    else:
                        report(f"publish skipped: release_id={local.release_id} already published")
                publish_seconds = time.monotonic() - publish_started
                if summary.exit_code == 0:
                    _prune_releases(
                        self.data_root,
                        keep=self.keep_releases,
                        published_release_id=(
                            published_pointer.release_id if published_pointer else None
                        ),
                        ownership_check=lock.assert_owned,
                    )
                    _prune_logs(self.data_root, self.clock(), lock.assert_owned)
                summary = replace(
                    summary,
                    finished_at=_aware_utc(self.clock()).isoformat().replace("+00:00", "Z"),
                    timings={
                        **summary.timings,
                        "total_seconds": round(max(time.monotonic() - started_monotonic, 0.0), 6),
                        "build_seconds": round(max(build_seconds, 0.0), 6),
                        "publish_seconds": round(max(publish_seconds, 0.0), 6),
                    },
                    peak_rss_bytes=peak_rss_bytes(),
                )
                run_log.write(f"run finished: {summary.outcome}")
            except BaseException as error:
                if isinstance(error, LockOwnershipLost) and not isinstance(
                    error, MaximumRunTimeExceeded
                ):
                    raise
                pending_error = error
                pending_traceback = error.__traceback__
                summary = _failed_summary(
                    summary,
                    progress,
                    error,
                    run_id=run_id,
                    started_at=now,
                    started_monotonic=started_monotonic,
                    finished_at=_aware_utc(self.clock()),
                )
                _try_log(run_log, f"run failed: {_error_reason(error)}")
            finalization_ownership_check = (
                lock.assert_current_owner
                if isinstance(pending_error, MaximumRunTimeExceeded)
                else lock.assert_owned
            )
            while True:
                try:
                    finalization_ownership_check()
                    summary, reporting_error = _write_run_reports(
                        self.data_root,
                        store,
                        summary,
                        now=_aware_utc(self.clock()),
                        considered_days=healing_days(now, heal_days),
                        published_pointer=published_pointer,
                        pointer_state=pointer_state,
                        ownership_check=finalization_ownership_check,
                        run_log=run_log,
                    )
                    break
                except MaximumRunTimeExceeded as error:
                    if not any(
                        failure.get("reason") == _error_reason(error)
                        for failure in summary.failures
                    ):
                        summary = _failed_summary(
                            summary,
                            progress,
                            error,
                            run_id=run_id,
                            started_at=now,
                            started_monotonic=started_monotonic,
                            finished_at=_aware_utc(self.clock()),
                        )
                    if pending_error is None:
                        pending_error = error
                        pending_traceback = error.__traceback__
                    finalization_ownership_check = lock.assert_current_owner
                except LockOwnershipLost:
                    if pending_error is not None:
                        raise pending_error.with_traceback(pending_traceback)
                    raise
                except BaseException:
                    if pending_error is not None:
                        raise pending_error.with_traceback(pending_traceback)
                    raise
            if pending_error is not None:
                raise pending_error.with_traceback(pending_traceback)
            if reporting_error is not None:
                raise reporting_error
            return summary

    def _heal(
        self,
        store: FactStore,
        lock: DirectoryLock,
        *,
        run_id: str,
        now: datetime,
        heal_days: int,
        started_monotonic: float,
        progress: _RunProgress,
        log: Callable[[str], None],
        report: Callable[[str], None],
        report_error: Callable[[str], None],
        checkpoint: Callable[..., None],
    ) -> RunSummary:
        heal_started = time.monotonic()
        progress.heal_started_monotonic = heal_started

        days = healing_days(now, heal_days)
        planned_by_day: dict[date, tuple[datetime, ...]] = {}
        for day in days:
            if store.has_seal(day) or store.is_abandoned(day):
                continue
            planned_by_day[day] = settled_missing_hours(
                day,
                now,
                store.missing_hours(day),
                settle_lag=self.settle_lag,
            )
        hours_planned = sum(len(hours) for hours in planned_by_day.values())
        progress.hours_planned = hours_planned
        report(f"heal plan: days={len(days)} missing_settled_hours={hours_planned}")

        hours_started = 0
        for day, planned_hours in planned_by_day.items():
            lock.assert_owned()
            day_started = time.monotonic()
            expired_reason: str | None = None
            for hour in planned_hours:
                lock.assert_owned()
                hours_started += 1
                try:
                    hour_started = time.monotonic()
                    hour_key = hour.strftime("%Y-%m-%dT%H")
                    report(
                        f"hour started: source_hour={hour_key} [{hours_started}/{hours_planned}]"
                    )
                    checkpoint("heal", hour_key, step="index")
                    index_started = time.monotonic()
                    index = self.source.hour_index(hour)
                    report(
                        f"hour indexed: source_hour={hour_key} "
                        f"bundles={len(index.items)} pages={index.pages} "
                        f"elapsed={_format_elapsed(time.monotonic() - index_started)}"
                    )
                    report(
                        f"hour ingest started: source_hour={hour_key} bundles={len(index.items)}"
                    )
                    checkpoint(
                        "heal",
                        hour_key,
                        step="ingest",
                        bundles_done=0,
                        bundles_total=len(index.items),
                    )
                    ingest_started = time.monotonic()

                    def observed_bundles():
                        for completed, bundle in enumerate(self.source.stream(index), start=1):
                            yield bundle
                            if (
                                completed == 1
                                or completed == len(index.items)
                                or completed % BUNDLE_PROGRESS_EVERY == 0
                            ):
                                report(
                                    f"hour ingest progress: source_hour={hour_key} "
                                    f"bundles={completed}/{len(index.items)} "
                                    f"elapsed={_format_elapsed(time.monotonic() - ingest_started)}"
                                )
                                checkpoint(
                                    "heal",
                                    hour_key,
                                    step="ingest",
                                    bundles_done=completed,
                                    bundles_total=len(index.items),
                                )

                    projected = project_hour(index, observed_bundles())
                    commit = store.commit_hour(projected)
                    progress.hours_ingested += 1
                    progress.changed = True
                    checkpoint(
                        "heal",
                        commit.source_hour,
                        step="complete",
                        bundles_done=commit.bundle_count,
                        bundles_total=len(index.items),
                    )
                    reused = " reused" if commit.reused else ""
                    report(
                        f"healed {commit.source_hour} bundles={commit.bundle_count} "
                        f"rows={sum(commit.row_counts.values())} "
                        f"bytes={_format_bytes(sum(commit.file_bytes.values()))} "
                        f"elapsed={_format_elapsed(time.monotonic() - hour_started)}"
                        f"{reused} [{progress.hours_ingested}/{hours_planned}]"
                    )
                except HourExpired as error:
                    expired_reason = error.reason
                    report_error(
                        f"source hour expired: {hour.strftime('%Y-%m-%dT%H')} ({error.reason})"
                    )
                    break
                except RetryableSourceError as error:
                    progress.failures.append(
                        {
                            "scope": "source_hour",
                            "source_hour": hour.strftime("%Y-%m-%dT%H"),
                            "reason": str(getattr(error, "reason", type(error).__name__)),
                        }
                    )
                    report_error(
                        f"source hour failed: {hour.strftime('%Y-%m-%dT%H')} "
                        f"({getattr(error, 'reason', type(error).__name__)})"
                    )
                    continue
                except Exception:
                    raise

            if expired_reason is not None:
                remaining = store.missing_hours(day)
                store.abandon_day(day, remaining, expired_reason)
                log(f"source day abandoned: {day.isoformat()} ({expired_reason})")
                progress.days_abandoned += 1
                progress.changed = True
                checkpoint(
                    "heal",
                    hour.strftime("%Y-%m-%dT%H"),
                    step="abandoned",
                )
                report(
                    f"abandoned {day.isoformat()} missing={len(remaining)} "
                    f"reason={expired_reason} "
                    f"elapsed={_format_elapsed(time.monotonic() - day_started)}"
                )
                continue
            try:
                if not store.missing_hours(day):
                    seal = store.seal_day(day)
                    progress.days_sealed += 1
                    progress.changed = True
                    checkpoint(
                        "heal",
                        f"{day.isoformat()}T23",
                        step="seal",
                    )
                    report(
                        f"sealed {day.isoformat()} rows={sum(seal.row_counts.values())} "
                        f"elapsed={_format_elapsed(time.monotonic() - day_started)}"
                    )
            except Exception as error:
                progress.failures.append(
                    {
                        "scope": "source_day",
                        "source_day": day.isoformat(),
                        "reason": str(getattr(error, "reason", type(error).__name__)),
                    }
                )
                report_error(
                    f"source day failed: {day.isoformat()} "
                    f"({getattr(error, 'reason', type(error).__name__)})"
                )

        finished = _aware_utc(self.clock())
        outcome = "partial" if progress.failures else "ok" if progress.changed else "noop"
        return _summary(
            run_id,
            now,
            finished,
            outcome=outcome,
            exit_code=4 if progress.failures else 0,
            hours_ingested=progress.hours_ingested,
            days_sealed=progress.days_sealed,
            days_abandoned=progress.days_abandoned,
            failures=tuple(progress.failures),
            elapsed=time.monotonic() - started_monotonic,
            heal_seconds=time.monotonic() - heal_started,
        )


def _format_bytes(value: int) -> str:
    return f"{value / (1024 * 1024):.1f}MiB"


def _format_elapsed(value: float) -> str:
    return f"{max(round(value), 0)}s"


def healing_days(now: datetime, count: int) -> tuple[date, ...]:
    """Return the clamped oldest-first set of UTC days considered by a run."""
    current = _aware_utc(now).date()
    first = max(EPOCH_DAY, current - timedelta(days=count - 1))
    if first > current:
        return ()
    return tuple(first + timedelta(days=offset) for offset in range((current - first).days + 1))


def is_hour_settled(
    source_hour: datetime, now: datetime, *, settle_lag: timedelta = DEFAULT_SETTLE_LAG
) -> bool:
    """Pure settled-hour rule: the fixed hour ended and the server lag elapsed."""
    hour = _aware_utc(source_hour)
    if hour.minute or hour.second or hour.microsecond:
        raise ValueError("Source Hour must align to the hour")
    return _aware_utc(now) >= hour + timedelta(hours=1) + settle_lag


def settled_missing_hours(
    source_day: date,
    now: datetime,
    missing: tuple[datetime, ...],
    *,
    settle_lag: timedelta = DEFAULT_SETTLE_LAG,
) -> tuple[datetime, ...]:
    if any(hour.date() != source_day for hour in missing):
        raise ValueError("Missing Source Hours must belong to the Source Day")
    return tuple(
        sorted(hour for hour in missing if is_hour_settled(hour, now, settle_lag=settle_lag))
    )


def build_status(
    data_root: str | Path,
    store: FactStore,
    last_run: RunSummary | None,
    *,
    now: datetime,
    considered_days: tuple[date, ...] | None = None,
    published_pointer: PublishedPointer | None = None,
    pointer_state: str | None = None,
) -> dict[str, Any]:
    root = Path(data_root)
    seals = store.seals()
    abandoned = store.abandoned_days()
    if considered_days is None:
        candidate_days = {date.fromisoformat(value[:10]) for value in store.committed_hours()}
        candidate_days.update(date.fromisoformat(item.source_day) for item in abandoned)
        considered_days = tuple(sorted(candidate_days))
    incomplete = []
    sealed_days = {item.source_day for item in seals}
    abandoned_days = {item.source_day for item in abandoned}
    for day in considered_days:
        if day.isoformat() in sealed_days or day.isoformat() in abandoned_days:
            continue
        missing = store.missing_hours(day)
        if missing:
            incomplete.append(
                {
                    "source_day": day.isoformat(),
                    "missing_hours": [hour.strftime("%Y-%m-%dT%H") for hour in missing],
                    "settled": is_hour_settled(
                        datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=23),
                        now,
                    ),
                }
            )
    disk_root = _existing_ancestor(root)
    checked_at = _aware_utc(now)
    published_age = (
        max(0.0, (checked_at - published_pointer.stat.last_modified).total_seconds())
        if published_pointer is not None
        else None
    )
    observed_pointer_state = pointer_state or (
        POINTER_STATE_OK if published_pointer is not None else POINTER_STATE_ABSENT
    )
    return {
        "facts": {
            "newest_sealed_day": max(sealed_days, default=None),
            "sealed_days": sorted(sealed_days),
            "incomplete_days": incomplete,
            "abandoned_days": [
                {
                    "source_day": item.source_day,
                    "missing_hours": list(item.missing_hours),
                    "reason": item.reason,
                    "abandoned_at": item.abandoned_at,
                }
                for item in abandoned
            ],
        },
        "release": {
            "publish_hold": (root / "publish-hold.json").is_file(),
            "local_newest_release_id": local_newest_release_id(root),
            "pointer_state": observed_pointer_state,
            "published_release_id": (
                published_pointer.release_id if published_pointer is not None else None
            ),
            "published_window_end": (
                published_pointer.window_end if published_pointer is not None else None
            ),
            "published_manifest_age_seconds": published_age,
        },
        "last_run": last_run.to_dict() if last_run is not None else None,
        "current_run": None,
        "disk": {"free_bytes": shutil.disk_usage(disk_root).free},
        "peak_rss_bytes": peak_rss_bytes(),
    }


def read_status(data_root: str | Path) -> dict[str, Any]:
    path = Path(data_root) / "status.json"
    try:
        value = json.loads(path.read_bytes())
    except FileNotFoundError:
        return build_status(
            data_root,
            FactStore(data_root),
            None,
            now=datetime.now(UTC),
        )
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RuntimeError("status.json is unreadable") from error
    if not isinstance(value, dict):
        raise RuntimeError("status.json must contain an object")
    return value


def peak_rss_bytes() -> int:
    observed = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return observed if sys.platform == "darwin" else observed * 1024


def _write_current_status(
    root: Path,
    store: FactStore,
    *,
    run_id: str,
    phase: str,
    step: str,
    current_hour: str | None,
    hours_done: int,
    hours_planned: int,
    bundles_done: int | None,
    bundles_total: int | None,
    started_at: datetime,
    now: datetime,
    considered_days: tuple[date, ...],
    published_pointer: PublishedPointer | None,
    pointer_state: str | None,
    ownership_check: Callable[[], None],
) -> None:
    previous: dict[str, Any] = {}
    try:
        observed = json.loads((root / "status.json").read_bytes())
        if isinstance(observed, dict):
            previous = observed
    except FileNotFoundError, OSError, json.JSONDecodeError, UnicodeDecodeError:
        pass

    status = build_status(
        root,
        store,
        None,
        now=now,
        considered_days=considered_days,
        published_pointer=published_pointer,
        pointer_state=pointer_state,
    )
    status["last_run"] = previous.get("last_run")
    if (
        pointer_state is None
        and published_pointer is None
        and isinstance(previous.get("release"), dict)
    ):
        for field in (
            "pointer_state",
            "published_release_id",
            "published_window_end",
            "published_manifest_age_seconds",
        ):
            status["release"][field] = previous["release"].get(field)
    current_run: dict[str, Any] = {
        "run_id": run_id,
        "phase": phase,
        "step": step,
        "current_hour": current_hour,
        "hours_done": hours_done,
        "hours_planned": hours_planned,
        "started_at": _aware_utc(started_at).isoformat().replace("+00:00", "Z"),
        "updated_at": _aware_utc(now).isoformat().replace("+00:00", "Z"),
    }
    if bundles_total is not None:
        current_run["bundles"] = {
            "done": bundles_done or 0,
            "total": bundles_total,
        }
    status["current_run"] = current_run
    _write_status(root, status, ownership_check)


def _error_reason(error: BaseException) -> str:
    reason = getattr(error, "reason", None)
    if isinstance(reason, str) and reason:
        return reason
    message = str(error)
    return message if message else type(error).__name__


def _failed_summary(
    summary: RunSummary | None,
    progress: _RunProgress,
    error: BaseException,
    *,
    run_id: str,
    started_at: datetime,
    started_monotonic: float,
    finished_at: datetime,
) -> RunSummary:
    failure = {
        "scope": (
            "run" if summary is None or isinstance(error, MaximumRunTimeExceeded) else "release"
        ),
        "reason": _error_reason(error),
    }
    elapsed = time.monotonic() - started_monotonic
    if summary is None:
        heal_seconds = (
            time.monotonic() - progress.heal_started_monotonic
            if progress.heal_started_monotonic is not None
            else 0.0
        )
        return _summary(
            run_id,
            started_at,
            finished_at,
            outcome="error",
            exit_code=1,
            hours_ingested=progress.hours_ingested,
            days_sealed=progress.days_sealed,
            days_abandoned=progress.days_abandoned,
            failures=tuple(progress.failures) + (failure,),
            elapsed=elapsed,
            heal_seconds=heal_seconds,
        )
    return replace(
        summary,
        finished_at=finished_at.isoformat().replace("+00:00", "Z"),
        timings={
            **summary.timings,
            "total_seconds": round(max(elapsed, 0.0), 6),
        },
        outcome="error",
        exit_code=1,
        failures=summary.failures + (failure,),
        peak_rss_bytes=peak_rss_bytes(),
    )


def _try_log(run_log: "_RunLog | None", message: str) -> None:
    if run_log is None:
        return
    try:
        run_log.write(message)
    except BaseException:
        pass


def _write_run_reports(
    root: Path,
    store: FactStore,
    summary: RunSummary,
    *,
    now: datetime,
    considered_days: tuple[date, ...],
    published_pointer: PublishedPointer | None,
    pointer_state: str | None,
    ownership_check: Callable[[], None],
    run_log: "_RunLog | None",
) -> tuple[RunSummary, BaseException | None]:
    reporting_error: BaseException | None = None
    status: dict[str, Any] | None = None
    try:
        status = build_status(
            root,
            store,
            summary,
            now=now,
            considered_days=considered_days,
            published_pointer=published_pointer,
            pointer_state=pointer_state,
        )
    except LockOwnershipLost:
        raise
    except BaseException as error:
        reason = _error_reason(error)
        summary = replace(
            summary,
            finished_at=now.isoformat().replace("+00:00", "Z"),
            outcome="error",
            exit_code=1,
            failures=summary.failures + ({"scope": "status", "reason": reason},),
            peak_rss_bytes=peak_rss_bytes(),
        )
        _try_log(run_log, f"status collection failed: {reason}")
        try:
            status = _error_status(
                root,
                summary,
                published_pointer,
                pointer_state,
                now=now,
            )
        except LockOwnershipLost:
            raise
        except BaseException as fallback_error:
            reporting_error = fallback_error
        if not isinstance(error, Exception) and reporting_error is None:
            reporting_error = error

    if status is not None:
        try:
            _write_status(root, status, ownership_check)
        except LockOwnershipLost:
            raise
        except BaseException as error:
            reporting_error = reporting_error or error
    try:
        _append_run(root, summary.to_dict(), ownership_check)
    except LockOwnershipLost:
        raise
    except BaseException as error:
        reporting_error = reporting_error or error
    return summary, reporting_error


def _error_status(
    root: Path,
    summary: RunSummary,
    published_pointer: PublishedPointer | None = None,
    pointer_state: str | None = None,
    *,
    now: datetime,
) -> dict[str, Any]:
    published_age = (
        max(
            0.0,
            (_aware_utc(now) - published_pointer.stat.last_modified).total_seconds(),
        )
        if published_pointer is not None
        else None
    )
    return {
        "facts": {
            "newest_sealed_day": None,
            "sealed_days": [],
            "incomplete_days": [],
            "abandoned_days": [],
        },
        "release": {
            "publish_hold": (root / "publish-hold.json").is_file(),
            "local_newest_release_id": local_newest_release_id(root),
            "pointer_state": pointer_state
            or (POINTER_STATE_OK if published_pointer is not None else POINTER_STATE_ABSENT),
            "published_release_id": (
                published_pointer.release_id if published_pointer is not None else None
            ),
            "published_window_end": (
                published_pointer.window_end if published_pointer is not None else None
            ),
            "published_manifest_age_seconds": published_age,
        },
        "last_run": summary.to_dict(),
        "current_run": None,
        "disk": {"free_bytes": shutil.disk_usage(_existing_ancestor(root)).free},
        "peak_rss_bytes": peak_rss_bytes(),
    }


def _summary(
    run_id: str,
    started: datetime,
    finished: datetime,
    *,
    outcome: str,
    exit_code: int,
    hours_ingested: int,
    days_sealed: int,
    days_abandoned: int,
    failures: tuple[dict[str, str], ...],
    elapsed: float,
    heal_seconds: float = 0.0,
) -> RunSummary:
    return RunSummary(
        run_id=run_id,
        started_at=started.isoformat().replace("+00:00", "Z"),
        finished_at=finished.isoformat().replace("+00:00", "Z"),
        timings={
            "total_seconds": round(max(elapsed, 0.0), 6),
            "heal_seconds": round(max(heal_seconds, 0.0), 6),
        },
        outcome=outcome,
        exit_code=exit_code,
        hours_ingested=hours_ingested,
        days_sealed=days_sealed,
        days_abandoned=days_abandoned,
        release_built=None,
        release_published=None,
        failures=failures,
        peak_rss_bytes=peak_rss_bytes(),
    )


def _write_status(root: Path, value: dict[str, Any], ownership_check: Callable[[], None]) -> None:
    ownership_check()
    root.mkdir(parents=True, exist_ok=True)
    for stale in root.glob(".status.json.tmp-*"):
        if stale.is_file():
            stale.unlink(missing_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".status.json.tmp-", dir=root)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(_canonical_json(value))
            stream.flush()
            os.fsync(stream.fileno())
        ownership_check()
        os.replace(temporary, root / "status.json")
        _fsync_directory(root)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _append_run(root: Path, value: dict[str, Any], ownership_check: Callable[[], None]) -> None:
    ownership_check()
    with (root / "runs.jsonl").open("ab") as stream:
        stream.write(_canonical_json(value))
        stream.flush()
        os.fsync(stream.fileno())


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Pipeline clock must be timezone-aware")
    return value.astimezone(UTC)


def _existing_ancestor(path: Path) -> Path:
    candidate = path
    while not candidate.exists():
        if candidate.parent == candidate:
            return Path("/")
        candidate = candidate.parent
    return candidate


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _RunLog:
    def __init__(
        self,
        root: Path,
        run_id: str,
        *,
        clock: Callable[[], datetime],
        ownership_check: Callable[[], None],
    ) -> None:
        self._clock = clock
        self._ownership_check = ownership_check
        started = _aware_utc(clock())
        directory = root / "logs"
        ownership_check()
        directory.mkdir(parents=True, exist_ok=True)
        stamp = started.strftime("%Y%m%dT%H%M%S.%fZ")
        self._path = directory / f"{stamp}-{os.getpid()}-{run_id[:8]}.log"

    def write(self, message: str) -> None:
        self._ownership_check()
        timestamp = _aware_utc(self._clock()).isoformat().replace("+00:00", "Z")
        with self._path.open("a", encoding="utf-8") as stream:
            stream.write(f"{timestamp} {message}\n")
            stream.flush()
            os.fsync(stream.fileno())


def _prune_releases(
    root: Path,
    *,
    keep: int,
    published_release_id: str | None,
    ownership_check: Callable[[], None],
) -> None:
    releases = root / "releases"
    if not releases.is_dir():
        return
    release_ids = sorted(
        (
            path.name
            for path in releases.iterdir()
            if path.is_dir() and RELEASE_ID_PATTERN.fullmatch(path.name)
        ),
        reverse=True,
    )
    protected = set(release_ids[:keep])
    if published_release_id is not None:
        protected.add(published_release_id)
    hold_path = root / "publish-hold.json"
    try:
        hold = json.loads(hold_path.read_bytes())
    except FileNotFoundError, OSError, json.JSONDecodeError, UnicodeDecodeError:
        hold = None
    if isinstance(hold, dict):
        target = hold.get("target_release_id")
        if isinstance(target, str) and RELEASE_ID_PATTERN.fullmatch(target):
            protected.add(target)
    removed = False
    for release_id in release_ids:
        if release_id in protected:
            continue
        ownership_check()
        shutil.rmtree(releases / release_id)
        removed = True
    if removed:
        _fsync_directory(releases)


def _prune_logs(root: Path, now: datetime, ownership_check: Callable[[], None]) -> None:
    cutoff = _aware_utc(now).timestamp() - 10 * 24 * 60 * 60
    directory = root / "logs"
    if not directory.is_dir():
        return
    for path in directory.iterdir():
        if not path.is_file() or path.suffix != ".log":
            continue
        try:
            expired = path.stat().st_mtime < cutoff
        except OSError:
            continue
        if expired:
            ownership_check()
            path.unlink(missing_ok=True)

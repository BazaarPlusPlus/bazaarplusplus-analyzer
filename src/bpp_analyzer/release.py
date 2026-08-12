"""Deterministic DuckDB analytics and atomic immutable local releases."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any
import uuid

import duckdb
from jsonschema import Draft202012Validator

from bpp_analyzer.fact_store import DaySeal, FactStore, TABLES, canonical_json, parse_source_day
from bpp_analyzer.object_store import ObjectStat, ObjectStore, StoredObject


EPOCH_DAY = date(2026, 8, 7)
BUILDER_CODE_VERSION = "0.3.1"
POLICY_VERSION = "v5-contract-1"
MAX_FETCH_ROWS = 10_000
MIN_RATED_BATTLES = 50

CORE_BUILD_LIMIT_PER_HERO = 500
MIN_TEN_WIN_RUNS = 1
WILSON_Z = 1.96
EVIDENCE_WEIGHT = 1000
LEGEND_WEIGHT = 250
SPEED_WEIGHT = 100
STABILITY_WEIGHT = 50

OBJECT_PREFIX = "analyzer-v5"
POINTER_KEY = f"{OBJECT_PREFIX}/manifest.json"
IMMUTABLE_CACHE_CONTROL = "public,max-age=31536000,immutable"
POINTER_CACHE_CONTROL = "public,max-age=60,must-revalidate"

CANONICAL_HEROES = (
    "Dooley",
    "Jules",
    "Karnok",
    "Mak",
    "Pygmalien",
    "Stelle",
    "TheDragons",
    "Vanessa",
)
RELEASE_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}-[0-9a-f]{16}$")

BUILD_SCHEMA = ["card_refs", "layout", "stats", "selection"]
LAYOUT_SCHEMA = ["card_ref", "slot", "tier", "enchant_ref", "size"]
STATS_SCHEMA = [
    "completed_run_count",
    "ten_win_run_count",
    "ten_win_rate_bps",
    "avg_ten_win_final_day_tenth",
    "p75_ten_win_final_day",
    "avg_ten_win_final_losses_tenth",
    "legend_completed_run_count",
    "legend_ten_win_run_count",
    "legend_ten_win_rate_bps",
    "legend_avg_ten_win_final_day_tenth",
    "score",
]
SELECTION_SCHEMA = ["reason", "covered_card_ref"]
SELECTION_REASONS = ["core", "coverage"]

_SCHEMA_FILES = {
    "hero_daily": "hero-daily.schema.json",
    "hero_window": "hero-window.schema.json",
    "builds": "builds.schema.json",
    "quality": "quality.schema.json",
    "release_manifest": "release-manifest.schema.json",
}
_UUID_PATTERN = (
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


class ReleaseBuildError(RuntimeError):
    """A local immutable release could not be constructed safely."""


class ReleaseIdentityError(ReleaseBuildError):
    """A window or release identity violates the frozen rules."""


class ContractViolation(ReleaseBuildError):
    """A staged payload does not validate against its frozen JSON Schema."""


class ManifestMismatch(ReleaseBuildError):
    """A manifest inventory differs from the staged bytes or file set."""


class PublishError(RuntimeError):
    """A release cannot be safely exposed through the public pointer."""


class InvalidPointer(PublishError):
    """The current public pointer violates its frozen self-identity."""


class AntiRegressionError(PublishError):
    """A normal publish attempted to move the window backward."""


class ImmutableObjectConflict(PublishError):
    """An immutable release key already contains different bytes or metadata."""


class PublishConfirmationError(PublishError):
    """An object did not confirm with the exact bytes and Cache-Control."""


class PublishHold(PublishError):
    """A local publish hold blocks normal publication."""


@dataclass(frozen=True, slots=True)
class LocalRelease:
    release_id: str
    path: Path
    manifest: Mapping[str, Any]
    reused: bool


@dataclass(frozen=True, slots=True)
class PublishedPointer:
    release_id: str
    window_end: str
    manifest: Mapping[str, Any]
    stat: ObjectStat


@dataclass(frozen=True, slots=True)
class PublishResult:
    release_id: str
    uploaded: int
    skipped: int
    pointer: PublishedPointer


@dataclass(frozen=True, slots=True)
class OperatorResult:
    action: str
    release_id: str
    receipt_path: Path
    pointer: PublishedPointer | None


_CURRENT_UNSET = object()


def window_seals(
    seals: Iterable[DaySeal], anchor_day: date | str
) -> tuple[DaySeal, ...]:
    """Select the maximal 1–7-day consecutive sealed window ending at anchor."""
    anchor = parse_source_day(anchor_day)
    if anchor < EPOCH_DAY:
        raise ReleaseIdentityError(
            f"Release anchor cannot precede epoch {EPOCH_DAY.isoformat()}"
        )
    by_day: dict[date, DaySeal] = {}
    for seal in seals:
        day = parse_source_day(seal.source_day)
        if day in by_day:
            raise ReleaseIdentityError(f"Duplicate Source Day Seal: {day.isoformat()}")
        by_day[day] = seal
    if anchor not in by_day:
        raise ReleaseIdentityError(f"Release requires a sealed anchor: {anchor.isoformat()}")
    selected = [by_day[anchor]]
    candidate = anchor - timedelta(days=1)
    while len(selected) < 7 and candidate >= EPOCH_DAY and candidate in by_day:
        selected.append(by_day[candidate])
        candidate -= timedelta(days=1)
    selected.reverse()
    _check_window(tuple(selected), anchor)
    return tuple(selected)


def compute_release_id(
    anchor_day: date | str,
    seals: Sequence[DaySeal],
    *,
    builder_code_version: str = BUILDER_CODE_VERSION,
    policy_version: str = POLICY_VERSION,
) -> str:
    """Apply the spec's complete canonical identity chain."""
    anchor = parse_source_day(anchor_day)
    _check_window(tuple(seals), anchor)
    identity = {
        "anchor_day": anchor.isoformat(),
        "day_seal_sha256s": [seal.day_seal_sha256 for seal in seals],
        "hourly_fact_commit_sha256s": [
            hourly["fact_commit_sha256"]
            for seal in seals
            for hourly in seal.hourly_fact_commits
        ],
        "builder_code_version": builder_code_version,
        "policy_version": policy_version,
    }
    suffix = hashlib.sha256(canonical_json(identity)).hexdigest()[:16]
    release_id = f"{anchor.isoformat()}-{suffix}"
    _check_release_prefix(release_id, anchor)
    return release_id


def performance_rating(
    opponent_rating_sum: int, rated_battles: int, wins: int, losses: int
) -> float | None:
    """Legacy-authoritative FIDE rating with Laplace-smoothed win share."""
    if rated_battles <= 0 or wins + losses <= 0:
        return None
    average = opponent_rating_sum / rated_battles
    probability = (wins + 1) / (wins + losses + 2)
    return round(average + 400 * math.log10(probability / (1 - probability)), 1)


def candidate_score(
    *,
    completed: int,
    ten_win: int,
    legend_ten_win: int,
    average_day: float | None,
    average_losses: float | None,
) -> int:
    """Wilson-scored build selection policy expressed from integer aggregates."""
    success = _wilson_lower_bound(ten_win, completed, WILSON_Z)
    evidence = math.log1p(ten_win)
    legend = math.log1p(legend_ten_win)
    speed = 10.0 / max(average_day or 10.0, 10.0)
    stability = 1.0 / (1.0 + (average_losses or 0.0))
    return _round_half_up(
        EVIDENCE_WEIGHT * evidence * success
        + LEGEND_WEIGHT * legend
        + SPEED_WEIGHT * speed
        + STABILITY_WEIGHT * stability
    )


def _candidate_score_sql() -> str:
    proportion = "(ten_win::DOUBLE / completed)"
    z_squared = WILSON_Z * WILSON_Z
    wilson = (
        f"(({proportion} + {z_squared!r} / (2 * completed) "
        f"- {WILSON_Z!r} * sqrt({proportion} * (1 - {proportion}) / completed "
        f"+ {z_squared!r} / (4 * completed * completed))) "
        f"/ (1 + {z_squared!r} / completed))"
    )
    average_day = (
        "CASE WHEN day_count=0 OR day_sum=0 THEN 10.0 "
        "ELSE day_sum::DOUBLE / day_count END"
    )
    average_losses = (
        "CASE WHEN loss_count=0 OR loss_sum=0 THEN 0.0 "
        "ELSE loss_sum::DOUBLE / loss_count END"
    )
    return (
        "CAST(floor("
        f"{EVIDENCE_WEIGHT} * ln(1 + ten_win) * {wilson} "
        f"+ {LEGEND_WEIGHT} * ln(1 + legend_ten_win) "
        f"+ {SPEED_WEIGHT} * 10.0 / greatest({average_day}, 10.0) "
        f"+ {STABILITY_WEIGHT} / (1.0 + {average_losses}) "
        "+ 0.5) AS BIGINT)"
    )


class ReleaseBuilder:
    """Own one DuckDB build connection and the releases/ promotion boundary."""

    def __init__(
        self,
        data_root: str | Path,
        *,
        store: FactStore | None = None,
        memory_limit: str = "8GB",
        threads: int = 8,
        builder_code_version: str = BUILDER_CODE_VERSION,
        policy_version: str = POLICY_VERSION,
        contracts_dir: str | Path | None = None,
        ownership_check: Callable[[], None] = lambda: None,
        fault_injector: Callable[[str, Path], None] | None = None,
    ) -> None:
        if not isinstance(threads, int) or isinstance(threads, bool) or threads < 1:
            raise ValueError("DuckDB threads must be a positive integer")
        if not memory_limit:
            raise ValueError("DuckDB memory limit is required")
        if not builder_code_version or not policy_version:
            raise ValueError("Builder and policy versions are required")
        self.root = Path(data_root)
        self.store = store or FactStore(self.root, ownership_check=ownership_check)
        self.memory_limit = memory_limit
        self.threads = threads
        self.builder_code_version = builder_code_version
        self.policy_version = policy_version
        self.contracts_dir = Path(contracts_dir) if contracts_dir else _default_contracts_dir()
        self._ownership_check = ownership_check
        self._fault = fault_injector or (lambda _seam, _path: None)

    def build(
        self, anchor_day: date | str, seals: Iterable[DaySeal] | None = None
    ) -> LocalRelease:
        anchor = parse_source_day(anchor_day)
        selected = window_seals(self.store.seals() if seals is None else seals, anchor)
        release_id = compute_release_id(
            anchor,
            selected,
            builder_code_version=self.builder_code_version,
            policy_version=self.policy_version,
        )
        releases = self.root / "releases"
        final = releases / release_id
        if final.is_dir():
            return self._reuse(final, release_id)
        if final.exists():
            raise ReleaseBuildError(f"Release final path is not a directory: {release_id}")

        days = tuple(seal.source_day for seal in selected)
        hour_paths = self.store.hour_paths(days)
        window = {"start": days[0], "end": days[-1], "days": len(days)}
        window_generated_at = _generated_at(days[-1])
        self._ownership_check()
        (self.root / "duckdb-tmp").mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(database=":memory:")
        try:
            _configure_connection(
                connection,
                memory_limit=self.memory_limit,
                temp_directory=self.root / "duckdb-tmp",
                threads=self.threads,
            )
            analytics = _Analytics(connection, hour_paths, window, window_generated_at)
            daily_bytes = {
                day: analytics.hero_daily(day, _generated_at(day)) for day in days
            }
            heroes_bytes = analytics.hero_window()
            builds_bytes = analytics.builds()
        finally:
            connection.close()

        heroes = _decode_object(heroes_bytes, "hero_window")
        builds = _decode_object(builds_bytes, "builds")
        quality_bytes = _quality_payload(
            release_id,
            window,
            window_generated_at,
            selected,
            heroes,
            builds,
        )
        payloads: dict[str, bytes] = {
            **{f"daily/{day}.json": content for day, content in daily_bytes.items()},
            "quality.json": quality_bytes,
            "window/builds.json": builds_bytes,
            "window/heroes.json": heroes_bytes,
        }

        self._ownership_check()
        releases.mkdir(parents=True, exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix=f".{release_id}.tmp-", dir=releases))
        try:
            for relative in sorted(payloads):
                self._ownership_check()
                _durable_write(stage / relative, payloads[relative])
            self._fault("after_payloads_written", stage)
            _validate_payload_files(stage, payloads, self.contracts_dir)

            inventory = [_inventory_entry(stage, relative) for relative in sorted(payloads)]
            manifest = {
                "schema_version": 1,
                "kind": "release_manifest",
                "generated_at": window_generated_at,
                "release_id": release_id,
                "window": window,
                "builder_code_version": self.builder_code_version,
                "policy_version": self.policy_version,
                "files": inventory,
            }
            _durable_write(stage / "manifest.json", canonical_json(manifest))
            self._fault("after_manifest_written", stage)
            _validate_release_schemas(stage, self.contracts_dir)
            verified_manifest = _verify_manifest(stage)
            _check_release_prefix(str(verified_manifest.get("release_id")), anchor)
            _fsync_directory(stage)
            self._ownership_check()
            try:
                os.rename(stage, final)
            except FileExistsError:
                if final.is_dir():
                    return self._reuse(final, release_id)
                raise
            _fsync_directory(releases)
            return LocalRelease(release_id, final, manifest, reused=False)
        finally:
            if stage.exists():
                shutil.rmtree(stage, ignore_errors=True)

    def show(self, release_id: str) -> Mapping[str, Any]:
        return self.local(release_id).manifest

    def local(self, release_id: str) -> LocalRelease:
        if RELEASE_ID_PATTERN.fullmatch(release_id) is None:
            raise ReleaseIdentityError("Release ID is invalid")
        return self._reuse(self.root / "releases" / release_id, release_id)

    def _reuse(self, path: Path, release_id: str) -> LocalRelease:
        try:
            manifest = _decode_object((path / "manifest.json").read_bytes(), "release_manifest")
        except OSError as error:
            raise ReleaseBuildError(f"Existing release manifest is unreadable: {release_id}") from error
        if manifest.get("release_id") != release_id:
            raise ReleaseIdentityError(f"Existing release identity differs: {release_id}")
        return LocalRelease(release_id, path, manifest, reused=True)


class ReleasePublisher:
    """Enforce immutable release objects and one mutable public pointer."""

    def __init__(
        self,
        data_root: str | Path,
        object_store: ObjectStore,
        *,
        contracts_dir: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
        ownership_check: Callable[[], None] = lambda: None,
        fault_injector: Callable[[str, str], None] | None = None,
    ) -> None:
        self.root = Path(data_root)
        self.object_store = object_store
        self.contracts_dir = Path(contracts_dir) if contracts_dir else _default_contracts_dir()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._ownership_check = ownership_check
        self._fault = fault_injector or (lambda _seam, _key: None)

    def current_pointer(self) -> PublishedPointer | None:
        observed = self.object_store.get(POINTER_KEY)
        if observed is None:
            return None
        return _parse_pointer(observed, self.contracts_dir)

    def publish(
        self,
        release: LocalRelease,
        *,
        current: PublishedPointer | None | object = _CURRENT_UNSET,
    ) -> PublishResult:
        if (self.root / "publish-hold.json").is_file():
            raise PublishHold("publish-hold.json blocks publication")
        pointer = self.current_pointer() if current is _CURRENT_UNSET else current
        if pointer is not None and not isinstance(pointer, PublishedPointer):
            raise TypeError("Current pointer must be PublishedPointer or None")
        if pointer is not None and pointer.release_id == release.release_id:
            return PublishResult(release.release_id, 0, 0, pointer)
        target_end = parse_source_day(release.manifest["window"]["end"])
        if pointer is not None and target_end < parse_source_day(pointer.window_end):
            raise AntiRegressionError(
                f"Release window {target_end.isoformat()} precedes published "
                f"window {pointer.window_end}"
            )

        validate_release(release.path, self.contracts_dir)
        uploaded, skipped = self._ensure_artifacts(release)
        published = self._put_pointer(release)
        return PublishResult(release.release_id, uploaded, skipped, published)

    def rollback(self, release: LocalRelease, reason: str) -> OperatorResult:
        reason = _required_reason(reason)
        current = self.current_pointer()
        if current is None:
            raise PublishError("Cannot rollback without a published pointer")
        if current.release_id == release.release_id:
            raise PublishError("Rollback target is already published")
        target_end = parse_source_day(release.manifest["window"]["end"])
        if target_end > parse_source_day(current.window_end):
            raise PublishError("Rollback target cannot be newer than the current pointer")
        validate_release(release.path, self.contracts_dir)
        self._ensure_artifacts(release)
        created_at = self._now_string()
        hold = {
            "schema_version": 1,
            "target_release_id": release.release_id,
            "previous_release_id": current.release_id,
            "reason": reason,
            "created_at": created_at,
        }
        self._ownership_check()
        _atomic_replace_file(self.root / "publish-hold.json", canonical_json(hold))
        pointer = self._put_pointer(release)
        receipt = {
            "schema_version": 1,
            "action": "rollback",
            "from_release_id": current.release_id,
            "to_release_id": release.release_id,
            "reason": reason,
            "created_at": created_at,
        }
        path = self._write_receipt(receipt, created_at)
        return OperatorResult("rollback", release.release_id, path, pointer)

    def resume(self, reason: str) -> OperatorResult:
        reason = _required_reason(reason)
        hold_path = self.root / "publish-hold.json"
        try:
            hold = _decode_object(hold_path.read_bytes(), "publish-hold.json")
        except OSError as error:
            raise PublishHold("No publish hold exists to resume") from error
        target = hold.get("target_release_id")
        if not isinstance(target, str) or RELEASE_ID_PATTERN.fullmatch(target) is None:
            raise PublishHold("publish-hold.json is invalid")
        created_at = self._now_string()
        receipt = {
            "schema_version": 1,
            "action": "resume",
            "target_release_id": target,
            "reason": reason,
            "created_at": created_at,
        }
        path = self._write_receipt(receipt, created_at)
        self._ownership_check()
        hold_path.unlink()
        _fsync_directory(self.root)
        return OperatorResult("resume", target, path, None)

    def _ensure_artifacts(self, release: LocalRelease) -> tuple[int, int]:
        uploaded = 0
        skipped = 0
        for relative, content in _release_artifacts(release):
            key = f"{OBJECT_PREFIX}/releases/{release.release_id}/{relative}"
            self._ownership_check()
            observed = self.object_store.stat(key)
            if observed is None:
                self.object_store.put(
                    key,
                    content,
                    cache_control=IMMUTABLE_CACHE_CONTROL,
                )
                confirmed = self.object_store.stat(key)
                _confirm_object(
                    key,
                    confirmed,
                    content,
                    IMMUTABLE_CACHE_CONTROL,
                    conflict=False,
                )
                uploaded += 1
            else:
                _confirm_object(
                    key,
                    observed,
                    content,
                    IMMUTABLE_CACHE_CONTROL,
                    conflict=True,
                )
                skipped += 1
            self._fault("after_artifact_confirmed", key)
        return uploaded, skipped

    def _put_pointer(self, release: LocalRelease) -> PublishedPointer:
        manifest_bytes = (release.path / "manifest.json").read_bytes()
        self._fault("before_pointer_put", POINTER_KEY)
        self._ownership_check()
        self.object_store.put(
            POINTER_KEY,
            manifest_bytes,
            cache_control=POINTER_CACHE_CONTROL,
        )
        confirmed = self.object_store.stat(POINTER_KEY)
        _confirm_object(
            POINTER_KEY,
            confirmed,
            manifest_bytes,
            POINTER_CACHE_CONTROL,
            conflict=False,
        )
        observed = self.object_store.get(POINTER_KEY)
        if observed is None or observed.body != manifest_bytes:
            raise PublishConfirmationError("Pointer GET differs after publication")
        _confirm_object(
            POINTER_KEY,
            observed.stat,
            manifest_bytes,
            POINTER_CACHE_CONTROL,
            conflict=False,
        )
        return _parse_pointer(observed, self.contracts_dir)

    def _write_receipt(self, receipt: Mapping[str, Any], created_at: str) -> Path:
        receipts = self.root / "receipts"
        self._ownership_check()
        receipts.mkdir(parents=True, exist_ok=True)
        stamp = created_at.replace("-", "").replace(":", "").replace(".", "")
        path = receipts / f"{stamp}-{uuid.uuid4().hex}.json"
        _durable_write(path, canonical_json(receipt))
        _fsync_directory(receipts)
        return path

    def _now_string(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Publisher clock must be timezone-aware")
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build(
    data_root: str | Path,
    anchor_day: date | str,
    seals: Iterable[DaySeal] | None = None,
    **settings: Any,
) -> LocalRelease:
    """Build or reuse a local release through the module's public interface."""
    return ReleaseBuilder(data_root, **settings).build(anchor_day, seals)


def validate_release(path: str | Path, contracts_dir: str | Path | None = None) -> None:
    """Deep-verify one local release against schemas and its manifest inventory."""
    release_path = Path(path)
    _validate_release_schemas(
        release_path, Path(contracts_dir) if contracts_dir else _default_contracts_dir()
    )
    manifest = _verify_manifest(release_path)
    release_id = str(manifest.get("release_id"))
    if release_path.name != release_id:
        raise ReleaseIdentityError("Release directory and manifest identities differ")
    _check_release_prefix(release_id, parse_source_day(manifest["window"]["end"]))


def validate_local_releases(
    data_root: str | Path, contracts_dir: str | Path | None = None
) -> int:
    releases = Path(data_root) / "releases"
    if not releases.is_dir():
        return 0
    count = 0
    for path in sorted(releases.iterdir()):
        if path.is_dir() and RELEASE_ID_PATTERN.fullmatch(path.name):
            validate_release(path, contracts_dir)
            count += 1
    return count


def local_newest_release_id(data_root: str | Path) -> str | None:
    releases = Path(data_root) / "releases"
    if not releases.is_dir():
        return None
    return max(
        (
            path.name
            for path in releases.iterdir()
            if path.is_dir() and RELEASE_ID_PATTERN.fullmatch(path.name)
        ),
        default=None,
    )


class _Analytics:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        hour_paths: Mapping[str, Sequence[Path]],
        window: Mapping[str, Any],
        generated_at: str,
    ) -> None:
        self.connection = connection
        self.window = dict(window)
        self.generated_at = generated_at
        for table in ("runs", "battles", "battle_cards"):
            paths = hour_paths.get(table)
            if not paths:
                raise ReleaseBuildError(f"No explicit hourly paths for {table}")
            explicit = ",".join(_sql_string(str(Path(path))) for path in paths)
            connection.execute(
                f"CREATE TEMP VIEW {table}_fact AS "
                f"SELECT * FROM read_parquet([{explicit}], hive_partitioning=false)"
            )
        heroes = ",".join(_sql_string(hero) for hero in CANONICAL_HEROES)
        connection.execute(
            "CREATE TEMP VIEW analytic_runs AS "
            "SELECT *, CASE WHEN trim(hero)='Hero8' THEN 'TheDragons' "
            "ELSE trim(hero) END AS hero_norm FROM runs_fact"
        )
        connection.execute(
            "CREATE TEMP VIEW analytic_battles AS "
            "SELECT * EXCLUDE (winner_side,winner_hero), "
            "CASE WHEN winner_combatant_id='Player' THEN 'player' "
            "WHEN winner_combatant_id='Opponent' THEN 'opponent' "
            "WHEN winner_combatant_id IS NOT NULL "
            "AND winner_combatant_id=player_account_id THEN 'player' "
            "WHEN winner_combatant_id IS NOT NULL "
            "AND winner_combatant_id=opponent_account_id THEN 'opponent' "
            "ELSE NULL END AS winner_side, "
            "CASE WHEN winner_combatant_id='Player' "
            "OR (winner_combatant_id IS NOT NULL "
            "AND winner_combatant_id=player_account_id) "
            "THEN CASE WHEN trim(player_hero)='Hero8' THEN 'TheDragons' "
            "ELSE trim(player_hero) END "
            "WHEN winner_combatant_id='Opponent' "
            "OR (winner_combatant_id IS NOT NULL "
            "AND winner_combatant_id=opponent_account_id) "
            "THEN CASE WHEN trim(opponent_hero)='Hero8' THEN 'TheDragons' "
            "ELSE trim(opponent_hero) END "
            "ELSE NULL END AS winner_hero, "
            "CASE WHEN trim(player_hero)='Hero8' THEN 'TheDragons' "
            "ELSE trim(player_hero) END AS player_hero_norm, "
            "CASE WHEN trim(opponent_hero)='Hero8' THEN 'TheDragons' "
            "ELSE trim(opponent_hero) END AS opponent_hero_norm FROM battles_fact"
        )
        connection.execute(
            "CREATE TEMP VIEW run_segments AS "
            "SELECT *, 'all' AS segment FROM analytic_runs "
            f"WHERE hero_norm IN ({heroes}) UNION ALL "
            "SELECT *, CASE WHEN final_rank='Legendary' THEN 'legend' "
            "ELSE 'non_legend' END AS segment FROM analytic_runs "
            f"WHERE hero_norm IN ({heroes}) AND final_rank IS NOT NULL"
        )
        connection.execute(
            "CREATE TEMP VIEW battle_segments AS "
            "SELECT b.*, r.final_rank, 'all' AS segment FROM analytic_battles b "
            "JOIN analytic_runs r ON r.run_id=b.run_id AND r.source_day=b.source_day "
            f"WHERE b.player_hero_norm IN ({heroes}) UNION ALL "
            "SELECT b.*, r.final_rank, CASE WHEN r.final_rank='Legendary' "
            "THEN 'legend' ELSE 'non_legend' END AS segment "
            "FROM analytic_battles b JOIN analytic_runs r "
            "ON r.run_id=b.run_id AND r.source_day=b.source_day "
            f"WHERE b.player_hero_norm IN ({heroes}) AND r.final_rank IS NOT NULL"
        )

    def hero_daily(self, source_day: str, generated_at: str) -> bytes:
        keys = self._rows(
            """
            WITH keys AS (
              SELECT hero_norm AS hero, segment
              FROM run_segments
              WHERE source_day=? AND lower(status)='completed' AND segment <> 'all'
              UNION
              SELECT player_hero_norm AS hero,
                     CASE WHEN final_rank='Legendary' THEN 'legend' ELSE 'non_legend' END AS segment
              FROM battle_segments
              WHERE source_day=? AND segment <> 'all' AND winner_side IN ('player','opponent')
            )
            SELECT hero, segment FROM keys
            ORDER BY CASE segment WHEN 'legend' THEN 0 ELSE 1 END, hero
            """,
            [source_day, source_day],
        )
        run_values = {
            (row[0], row[1]): row[2:]
            for row in self._rows(
                """
                SELECT hero_norm AS hero, segment,
                       count(*) AS completed,
                       count(*) FILTER (WHERE victories=10 AND losses=0) AS flawless,
                       count(*) FILTER (WHERE victories=10 AND losses>=1) AS ten_win,
                       count(*) FILTER (WHERE victories BETWEEN 7 AND 9) AS wins_7_9,
                       count(*) FILTER (WHERE victories BETWEEN 4 AND 6) AS wins_4_6,
                       count(*) FILTER (WHERE victories BETWEEN 0 AND 3) AS wins_0_3,
                       count(*) FILTER (WHERE victories=10 AND losses>=0 AND run_day IS NOT NULL) AS ten_runs,
                       coalesce(sum(run_day) FILTER (WHERE victories=10 AND losses>=0),0) AS ten_days,
                       count(final_rating_delta) AS delta_runs,
                       coalesce(sum(final_rating_delta),0) AS delta_total,
                       count(final_rating) AS rating_runs,
                       coalesce(sum(final_rating),0) AS rating_sum,
                       coalesce(sum(final_rating::HUGEINT * final_rating::HUGEINT),0) AS rating_sum_sq
                FROM run_segments
                WHERE source_day=? AND lower(status)='completed' AND segment <> 'all'
                GROUP BY hero_norm, segment
                ORDER BY CASE segment WHEN 'legend' THEN 0 ELSE 1 END, hero
                """,
                [source_day],
            )
        }
        rank_values: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for hero, segment, rank, runs in self._rows(
            """
            SELECT hero_norm AS hero, segment, final_rank, count(*)
            FROM run_segments
            WHERE source_day=? AND lower(status)='completed' AND segment <> 'all'
            GROUP BY hero_norm, segment, final_rank
            ORDER BY CASE segment WHEN 'legend' THEN 0 ELSE 1 END, hero,
                     CASE final_rank WHEN 'Bronze' THEN 0 WHEN 'Silver' THEN 1
                       WHEN 'Gold' THEN 2 WHEN 'Diamond' THEN 3 WHEN 'Legendary' THEN 4 ELSE 5 END,
                     final_rank
            """,
            [source_day],
        ):
            rank_values.setdefault((hero, segment), []).append(
                {"rank": rank, "runs": int(runs)}
            )
        opponent_values: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for hero, segment, rank, decided, wins, losses in self._rows(
            """
            SELECT player_hero_norm, segment, opponent_rank, count(*),
                   count(*) FILTER (WHERE winner_side='player'),
                   count(*) FILTER (WHERE winner_side='opponent')
            FROM battle_segments
            WHERE source_day=? AND segment <> 'all'
              AND winner_side IN ('player','opponent') AND opponent_rank IS NOT NULL
            GROUP BY player_hero_norm, segment, opponent_rank
            ORDER BY CASE segment WHEN 'legend' THEN 0 ELSE 1 END, player_hero_norm,
                     CASE opponent_rank WHEN 'Bronze' THEN 0 WHEN 'Silver' THEN 1
                       WHEN 'Gold' THEN 2 WHEN 'Diamond' THEN 3 WHEN 'Legendary' THEN 4 ELSE 5 END,
                     opponent_rank
            """,
            [source_day],
        ):
            opponent_values.setdefault((hero, segment), []).append(
                {
                    "rank": rank,
                    "decided": int(decided),
                    "wins": int(wins),
                    "losses": int(losses),
                }
            )
        rows = []
        for hero, segment in keys:
            values = run_values.get((hero, segment), (0,) * 13)
            rows.append(
                {
                    "hero": hero,
                    "segment": segment,
                    "runs": {
                        "completed": int(values[0]),
                        "results": {
                            "flawless": int(values[1]),
                            "ten_win": int(values[2]),
                            "wins_7_9": int(values[3]),
                            "wins_4_6": int(values[4]),
                            "wins_0_3": int(values[5]),
                        },
                    },
                    "ten_win": {"runs": int(values[6]), "total_final_days": int(values[7])},
                    "rating_delta": {"runs": int(values[8]), "total": int(values[9])},
                    "rating": {
                        "runs": int(values[10]),
                        "sum": int(values[11]),
                        "sum_sq": int(values[12]),
                    },
                    "rank_runs": rank_values.get((hero, segment), []),
                    "opponent_ranks": opponent_values.get((hero, segment), []),
                }
            )
        return canonical_json(
            {
                "schema_version": 1,
                "kind": "hero_daily",
                "generated_at": generated_at,
                "day": source_day,
                "params": {},
                "submitters": self._submitters(source_day),
                "rows": rows,
            }
        )

    def hero_window(self) -> bytes:
        keys = self._rows(
            """
            WITH keys AS (
              SELECT hero_norm AS hero, segment FROM run_segments
              WHERE lower(status)='completed'
              UNION
              SELECT player_hero_norm AS hero, segment FROM battle_segments
              WHERE winner_side IN ('player','opponent')
            )
            SELECT hero, segment FROM keys
            ORDER BY CASE segment WHEN 'all' THEN 0 WHEN 'legend' THEN 1 ELSE 2 END, hero
            """
        )
        run_values = {
            (row[0], row[1]): row[2:]
            for row in self._rows(
                """
                SELECT hero_norm, segment, count(*),
                       count(*) FILTER (WHERE victories=10 AND losses=0),
                       count(*) FILTER (WHERE victories=10 AND losses>=1),
                       count(*) FILTER (WHERE victories BETWEEN 7 AND 9),
                       count(*) FILTER (WHERE victories BETWEEN 4 AND 6),
                       count(*) FILTER (WHERE victories BETWEEN 0 AND 3),
                       count(final_rating), coalesce(sum(final_rating),0),
                       coalesce(sum(final_rating::HUGEINT * final_rating::HUGEINT),0),
                       quantile_cont(final_rating,0.1), quantile_cont(final_rating,0.5),
                       quantile_cont(final_rating,0.9), min(final_rating), max(final_rating),
                       count(final_rating_delta), coalesce(sum(final_rating_delta),0),
                       count(*) FILTER (WHERE victories=10 AND losses>=0 AND run_day IS NOT NULL),
                       coalesce(sum(run_day) FILTER (WHERE victories=10 AND losses>=0),0)
                FROM run_segments
                WHERE lower(status)='completed'
                GROUP BY hero_norm, segment
                ORDER BY CASE segment WHEN 'all' THEN 0 WHEN 'legend' THEN 1 ELSE 2 END, hero_norm
                """
            )
        }
        battle_values = {
            (row[0], row[1]): row[2:]
            for row in self._rows(
                """
                SELECT player_hero_norm, segment,
                       count(*) FILTER (WHERE opponent_rating IS NOT NULL),
                       count(*) FILTER (WHERE opponent_rating IS NOT NULL AND winner_side='player'),
                       count(*) FILTER (WHERE opponent_rating IS NOT NULL AND winner_side='opponent'),
                       coalesce(sum(opponent_rating),0)
                FROM battle_segments
                WHERE winner_side IN ('player','opponent')
                GROUP BY player_hero_norm, segment
                ORDER BY CASE segment WHEN 'all' THEN 0 WHEN 'legend' THEN 1 ELSE 2 END,
                         player_hero_norm
                """
            )
        }
        ranks: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for hero, segment, rank, count in self._rows(
            """
            SELECT hero_norm, segment, final_rank, count(*) FROM run_segments
            WHERE lower(status)='completed' AND final_rank IS NOT NULL
            GROUP BY hero_norm, segment, final_rank
            ORDER BY CASE segment WHEN 'all' THEN 0 WHEN 'legend' THEN 1 ELSE 2 END, hero_norm,
                     CASE final_rank WHEN 'Bronze' THEN 0 WHEN 'Silver' THEN 1
                       WHEN 'Gold' THEN 2 WHEN 'Diamond' THEN 3 WHEN 'Legendary' THEN 4 ELSE 5 END,
                     final_rank
            """
        ):
            ranks.setdefault((hero, segment), []).append({"rank": rank, "runs": int(count)})
        opponent_ranks: dict[tuple[str, str], list[dict[str, Any]]] = {}
        opponent_totals: dict[tuple[str, str], int] = {}
        opponent_rows = self._rows(
            """
            SELECT player_hero_norm, segment, opponent_rank, count(*),
                   count(*) FILTER (WHERE winner_side='player')
            FROM battle_segments
            WHERE winner_side IN ('player','opponent') AND opponent_rank IS NOT NULL
            GROUP BY player_hero_norm, segment, opponent_rank
            ORDER BY CASE segment WHEN 'all' THEN 0 WHEN 'legend' THEN 1 ELSE 2 END,
                     player_hero_norm,
                     CASE opponent_rank WHEN 'Bronze' THEN 0 WHEN 'Silver' THEN 1
                       WHEN 'Gold' THEN 2 WHEN 'Diamond' THEN 3 WHEN 'Legendary' THEN 4 ELSE 5 END,
                     opponent_rank
            """
        )
        for hero, segment, _rank, decided, _wins in opponent_rows:
            key = (hero, segment)
            opponent_totals[key] = opponent_totals.get(key, 0) + int(decided)
        for hero, segment, rank, decided, wins in opponent_rows:
            key = (hero, segment)
            opponent_ranks.setdefault(key, []).append(
                {
                    "rank": rank,
                    "decided": int(decided),
                    "share": _rate(int(decided), opponent_totals[key]),
                    "win_rate": _rate(int(wins), int(decided)),
                }
            )
        matchups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for hero, segment, opponent, decided, wins, losses in self._rows(
            """
            SELECT player_hero_norm, segment, opponent_hero_norm, count(*),
                   count(*) FILTER (WHERE winner_side='player'),
                   count(*) FILTER (WHERE winner_side='opponent')
            FROM battle_segments
            WHERE winner_side IN ('player','opponent')
              AND opponent_hero_norm IN ('Dooley','Jules','Karnok','Mak','Pygmalien','Stelle','TheDragons','Vanessa')
            GROUP BY player_hero_norm, segment, opponent_hero_norm
            ORDER BY CASE segment WHEN 'all' THEN 0 WHEN 'legend' THEN 1 ELSE 2 END,
                     player_hero_norm, count(*) DESC, opponent_hero_norm
            """
        ):
            matchups.setdefault((hero, segment), []).append(
                {
                    "opponent_hero": opponent,
                    "decided": int(decided),
                    "wins": int(wins),
                    "losses": int(losses),
                    "win_rate": _rate(int(wins), int(decided)),
                }
            )
        ghosts = {
            hero: {"battles": int(battles), "win_rate": _rate(int(wins), int(battles))}
            for hero, battles, wins in self._rows(
                """
                SELECT opponent_hero_norm, count(*),
                       count(*) FILTER (WHERE winner_side='opponent')
                FROM analytic_battles
                WHERE winner_side IN ('player','opponent')
                  AND opponent_account_id IS NOT NULL
                  AND opponent_hero_norm IN ('Dooley','Jules','Karnok','Mak','Pygmalien','Stelle','TheDragons','Vanessa')
                GROUP BY opponent_hero_norm
                ORDER BY opponent_hero_norm
                """
            )
        }
        rows = []
        for hero, segment in keys:
            run = run_values.get((hero, segment), (0,) * 18)
            battle = battle_values.get((hero, segment), (0,) * 4)
            rating_runs = int(run[6])
            rating_sum = int(run[7])
            rating_sum_sq = int(run[8])
            mean = rating_sum / rating_runs if rating_runs else None
            variance = (
                rating_sum_sq / rating_runs - mean * mean if mean is not None else None
            )
            rated, rated_wins, rated_losses, opponent_sum = map(int, battle)
            ten_runs = int(run[16])
            rows.append(
                {
                    "hero": hero,
                    "segment": segment,
                    "runs": {
                        "completed": int(run[0]),
                        "results": {
                            "flawless": int(run[1]),
                            "ten_win": int(run[2]),
                            "wins_7_9": int(run[3]),
                            "wins_4_6": int(run[4]),
                            "wins_0_3": int(run[5]),
                        },
                    },
                    "win_rate": _rate(rated_wins, rated),
                    "performance_rating": (
                        performance_rating(opponent_sum, rated, rated_wins, rated_losses)
                        if rated >= MIN_RATED_BATTLES
                        else None
                    ),
                    "avg_opponent_rating": _one_decimal(opponent_sum / rated if rated else None),
                    "rating": {
                        "runs": rating_runs,
                        "mean": _one_decimal(mean),
                        "stddev": _one_decimal(
                            math.sqrt(max(variance, 0.0)) if variance is not None else None
                        ),
                        "p10": _one_decimal(run[9]),
                        "p50": _one_decimal(run[10]),
                        "p90": _one_decimal(run[11]),
                        "min": int(run[12]) if run[12] is not None else None,
                        "max": int(run[13]) if run[13] is not None else None,
                    },
                    "rating_delta": {"runs": int(run[14]), "net": int(run[15])},
                    "ten_win": {
                        "runs": ten_runs,
                        "avg_final_days": _one_decimal(int(run[17]) / ten_runs if ten_runs else None),
                    },
                    "rank_runs": ranks.get((hero, segment), []),
                    "opponent_ranks": opponent_ranks.get((hero, segment), []),
                    "ghost": ghosts.get(hero, {"battles": 0, "win_rate": None}),
                    "matchups": matchups.get((hero, segment), []),
                }
            )
        return canonical_json(
            {
                "schema_version": 1,
                "kind": "hero_window",
                "generated_at": self.generated_at,
                "window": self.window,
                "params": {},
                "submitters": self._submitters(None),
                "rows": rows,
            }
        )

    def builds(self) -> bytes:
        temporary_tables = ("build_pool", "build_candidates", "build_runs")
        try:
            self.connection.execute(
                f"""
                CREATE TEMP TABLE build_runs AS
                WITH final_battles AS (
                  SELECT run_id, source_day, min(battle_id) AS battle_id,
                         count(*) AS final_count
                  FROM battles_fact
                  WHERE is_final_battle
                  GROUP BY run_id, source_day
                ), layout_keys AS (
                  SELECT c.run_id, c.source_day, c.battle_id,
                         string_agg(lower(c.template_id), '|'
                           ORDER BY lower(c.template_id)) AS build_key
                  FROM battle_cards_fact c
                  JOIN final_battles f
                    ON f.run_id=c.run_id AND f.source_day=c.source_day
                   AND f.battle_id=c.battle_id AND f.final_count=1
                  WHERE c.owner_side='player' AND c.card_kind='item'
                    AND c.card_set_label='player_hand'
                    AND lower(coalesce(c.card_set_status,'')) <> 'missing'
                  GROUP BY c.run_id, c.source_day, c.battle_id
                  HAVING count(*) > 0 AND count(c.size)=count(*) AND min(c.size)>0
                     AND sum(c.size)=10 AND count(c.template_id)=count(*)
                     AND count(coalesce(c.socket,c.slot_index))=count(*)
                     AND bool_and(regexp_full_match(
                       lower(c.template_id), '{_UUID_PATTERN}'))
                )
                SELECT r.run_id, r.source_day, r.hero_norm AS hero,
                       r.available_at_ms, r.final_battle_id, r.victories,
                       r.losses, r.run_day, r.final_rank, l.build_key
                FROM analytic_runs r
                JOIN layout_keys l
                  ON l.run_id=r.run_id AND l.source_day=r.source_day
                 AND l.battle_id=r.final_battle_id
                WHERE lower(r.status)='completed'
                  AND r.hero_norm IN (
                    'Dooley','Jules','Karnok','Mak','Pygmalien','Stelle',
                    'TheDragons','Vanessa'
                  )
                """
            )
            self.connection.execute(
                f"""
                CREATE TEMP TABLE build_candidates AS
                WITH aggregated AS (
                  SELECT hero, build_key, count(*) AS completed,
                         count(*) FILTER (
                           WHERE victories=10 AND losses>=0
                         ) AS ten_win,
                         coalesce(sum(run_day) FILTER (
                           WHERE victories=10 AND losses>=0
                             AND run_day IS NOT NULL
                         ),0) AS day_sum,
                         count(run_day) FILTER (
                           WHERE victories=10 AND losses>=0
                         ) AS day_count,
                         quantile_cont(run_day,0.75) FILTER (
                           WHERE victories=10 AND losses>=0
                         ) AS p75_day,
                         coalesce(sum(losses) FILTER (
                           WHERE victories=10 AND losses>=0
                         ),0) AS loss_sum,
                         count(losses) FILTER (
                           WHERE victories=10 AND losses>=0
                         ) AS loss_count,
                         count(*) FILTER (
                           WHERE final_rank='Legendary'
                         ) AS legend_completed,
                         count(*) FILTER (
                           WHERE final_rank='Legendary'
                             AND victories=10 AND losses>=0
                         ) AS legend_ten_win,
                         coalesce(sum(run_day) FILTER (
                           WHERE final_rank='Legendary'
                             AND victories=10 AND losses>=0
                             AND run_day IS NOT NULL
                         ),0) AS legend_day_sum,
                         count(run_day) FILTER (
                           WHERE final_rank='Legendary'
                             AND victories=10 AND losses>=0
                         ) AS legend_day_count,
                         arg_min(
                           struct_pack(
                             run_id := run_id, source_day := source_day,
                             battle_id := final_battle_id
                           ),
                           struct_pack(
                             null_day := run_day IS NULL,
                             run_day := coalesce(run_day,0),
                             available_at_ms := available_at_ms,
                             source_day := source_day,
                             final_battle_id := final_battle_id,
                             run_id := run_id
                           )
                         ) FILTER (
                           WHERE victories=10 AND losses>=0
                         ) AS representative
                  FROM build_runs
                  GROUP BY hero, build_key
                  HAVING count(*) FILTER (
                    WHERE victories=10 AND losses>=0
                  ) >= {MIN_TEN_WIN_RUNS}
                )
                SELECT *, {_candidate_score_sql()} AS score
                FROM aggregated
                """
            )
            self.connection.execute(
                f"""
                CREATE TEMP TABLE build_pool AS
                WITH ranked AS (
                  SELECT hero, build_key,
                         row_number() OVER (
                           PARTITION BY hero ORDER BY score DESC, build_key
                         ) AS core_rank
                  FROM build_candidates
                ), candidate_cards AS (
                  SELECT hero, build_key, score,
                         unnest(string_split(build_key, '|')) AS card_id
                  FROM build_candidates
                ), best_by_card AS (
                  SELECT hero, build_key
                  FROM (
                    SELECT hero, build_key,
                           row_number() OVER (
                             PARTITION BY hero, card_id
                             ORDER BY score DESC, build_key
                           ) AS card_rank
                    FROM candidate_cards
                  )
                  WHERE card_rank=1
                )
                SELECT hero, build_key FROM ranked
                WHERE core_rank <= {CORE_BUILD_LIMIT_PER_HERO}
                UNION
                SELECT hero, build_key FROM best_by_card
                """
            )

            candidate_counts = {
                hero: int(count)
                for hero, count in self._rows(
                    """
                    SELECT hero, count(*) FROM build_candidates
                    GROUP BY hero ORDER BY hero
                    """
                )
            }
            cards_by_hero: dict[str, set[str]] = {}
            for hero, card in self._rows(
                """
                SELECT hero, card_id
                FROM build_candidates,
                     unnest(string_split(build_key, '|')) AS cards(card_id)
                GROUP BY hero, card_id
                ORDER BY hero, card_id
                """
            ):
                cards_by_hero.setdefault(hero, set()).add(card)

            candidates = self._rows(
                """
                SELECT c.hero, c.build_key, c.completed, c.ten_win,
                       c.day_sum, c.day_count, c.p75_day,
                       c.loss_sum, c.loss_count, c.legend_completed,
                       c.legend_ten_win, c.legend_day_sum,
                       c.legend_day_count, c.score
                FROM build_candidates c
                JOIN build_pool p USING (hero, build_key)
                ORDER BY c.hero, c.score DESC, c.build_key
                """
            )
            layout_values = {
                (hero, build_key): layout
                for hero, build_key, layout in self._rows(
                    """
                    SELECT p.hero, p.build_key,
                           list(struct_pack(
                             template_id := lower(c.template_id),
                             slot := coalesce(c.socket,c.slot_index),
                             tier := c.tier, enchantment := c.enchantment,
                             size := c.size
                           ) ORDER BY coalesce(c.socket,c.slot_index),
                                      c.slot_index,lower(c.template_id),
                                      c.instance_id,c.tier,c.enchantment,c.size)
                    FROM build_pool p
                    JOIN build_candidates b USING (hero, build_key)
                    JOIN battle_cards_fact c
                      ON c.run_id=b.representative.run_id
                     AND c.source_day=b.representative.source_day
                     AND c.battle_id=b.representative.battle_id
                    WHERE c.owner_side='player' AND c.card_kind='item'
                      AND c.card_set_label='player_hand'
                      AND lower(coalesce(c.card_set_status,'')) <> 'missing'
                    GROUP BY p.hero, p.build_key
                    ORDER BY p.hero, p.build_key
                    """
                )
            }
        finally:
            for table in temporary_tables:
                self.connection.execute(f"DROP TABLE IF EXISTS {table}")

        scored_by_hero: dict[str, list[dict[str, Any]]] = {}
        for row in candidates:
            (
                hero,
                build_key,
                completed,
                ten_win,
                day_sum,
                day_count,
                p75_day,
                loss_sum,
                loss_count,
                legend_completed,
                legend_ten_win,
                legend_day_sum,
                legend_day_count,
                sql_score,
            ) = row
            average_day = int(day_sum) / int(day_count) if day_count else None
            average_losses = int(loss_sum) / int(loss_count) if loss_count else None
            candidate = {
                "hero": hero,
                "card_ids": tuple(build_key.split("|")),
                "completed": int(completed),
                "ten_win": int(ten_win),
                "average_day": average_day,
                "p75_day": float(p75_day) if p75_day is not None else None,
                "average_losses": average_losses,
                "legend_completed": int(legend_completed),
                "legend_ten_win": int(legend_ten_win),
                "legend_average_day": (
                    int(legend_day_sum) / int(legend_day_count) if legend_day_count else None
                ),
                "layout": layout_values[(hero, build_key)],
            }
            candidate["score"] = candidate_score(
                completed=candidate["completed"],
                ten_win=candidate["ten_win"],
                legend_ten_win=candidate["legend_ten_win"],
                average_day=average_day,
                average_losses=average_losses,
            )
            if candidate["score"] != int(sql_score):
                raise ReleaseBuildError("DuckDB and Python candidate scores differ")
            scored_by_hero.setdefault(hero, []).append(candidate)
        for values in scored_by_hero.values():
            values.sort(key=lambda item: (-item["score"], item["card_ids"]))

        selected_by_hero: dict[str, list[tuple[dict[str, Any], int, str | None]]] = {}
        for hero in sorted(scored_by_hero):
            scored = scored_by_hero[hero]
            selected = [(candidate, 0, None) for candidate in scored[:CORE_BUILD_LIMIT_PER_HERO]]
            selected_ids = {candidate["card_ids"] for candidate, _reason, _card in selected}
            pool_cards = cards_by_hero[hero]
            covered = {card for candidate, _reason, _card in selected for card in candidate["card_ids"]}
            for card in sorted(pool_cards - covered):
                if card in covered:
                    continue
                candidate = next(item for item in scored if card in item["card_ids"])
                if candidate["card_ids"] not in selected_ids:
                    selected.append((candidate, 1, card))
                    selected_ids.add(candidate["card_ids"])
                    covered.update(candidate["card_ids"])
            selected_by_hero[hero] = selected

        card_table = sorted(
            {card for values in cards_by_hero.values() for card in values}
        )
        card_ref = {card: index for index, card in enumerate(card_table)}
        enchantment_names = sorted(
            {
                cleaned
                for selected in selected_by_hero.values()
                for candidate, _reason, _covered in selected
                for card in candidate["layout"]
                if (cleaned := _clean_enchantment(card["enchantment"])) is not None
            }
        )
        enchantments: list[str | None] = [None, *enchantment_names]
        enchant_ref = {name: index + 1 for index, name in enumerate(enchantment_names)}
        heroes = []
        for hero in sorted(scored_by_hero):
            scored = scored_by_hero[hero]
            selected = selected_by_hero[hero]
            pool_cards = cards_by_hero[hero]
            covered = {card for candidate, _reason, _card in selected for card in candidate["card_ids"]}
            build_rows = [
                _build_row(candidate, reason, covered_card, card_ref, enchant_ref)
                for candidate, reason, covered_card in selected
            ]
            index: dict[int, list[int]] = {}
            for build_id, (candidate, _reason, _card) in enumerate(selected):
                for card in sorted(set(candidate["card_ids"])):
                    index.setdefault(card_ref[card], []).append(build_id)
            heroes.append(
                {
                    "hero": hero,
                    "candidate_build_count": candidate_counts[hero],
                    "candidate_card_count": len(pool_cards),
                    "included_build_count": len(selected),
                    "covered_card_count": len(covered & pool_cards),
                    "coverage": {
                        "uncovered_card_refs": sorted(card_ref[card] for card in pool_cards - covered)
                    },
                    "builds": build_rows,
                    "card_index": [[ref, index[ref]] for ref in sorted(index)],
                }
            )
        return canonical_json(
            {
                "schema_version": 1,
                "kind": "builds",
                "generated_at": self.generated_at,
                "window": self.window,
                "params": {
                    "pool": "window_completed_runs",
                    "segment_definition": "rank_legendary",
                    "core_build_limit_per_hero": CORE_BUILD_LIMIT_PER_HERO,
                    "min_ten_win_runs": MIN_TEN_WIN_RUNS,
                    "wilson_z": WILSON_Z,
                    "evidence_weight": EVIDENCE_WEIGHT,
                    "legend_weight": LEGEND_WEIGHT,
                    "speed_weight": SPEED_WEIGHT,
                    "stability_weight": STABILITY_WEIGHT,
                    "coverage_policy": "append_best_build_per_uncovered_card",
                },
                "cards": card_table,
                "enchantments": enchantments,
                "schemas": {
                    "build": BUILD_SCHEMA,
                    "layout": LAYOUT_SCHEMA,
                    "stats": STATS_SCHEMA,
                    "selection": SELECTION_SCHEMA,
                },
                "selection_reasons": SELECTION_REASONS,
                "heroes": heroes,
            }
        )

    def _submitters(self, source_day: str | None) -> dict[str, Any]:
        predicate = "WHERE source_day=?" if source_day is not None else ""
        params = [source_day] if source_day is not None else []
        all_count = self._rows(
            f"SELECT count(DISTINCT player_account_id) FROM analytic_runs {predicate} "
            "ORDER BY count(DISTINCT player_account_id)",
            params,
        )[0][0]
        by_hero = [
            {"hero": hero, "submitters": int(count)}
            for hero, count in self._rows(
                f"SELECT hero_norm, count(DISTINCT player_account_id) FROM analytic_runs {predicate} "
                + ("AND " if predicate else "WHERE ")
                + "hero_norm IN ('Dooley','Jules','Karnok','Mak','Pygmalien','Stelle','TheDragons','Vanessa') "
                "GROUP BY hero_norm ORDER BY hero_norm",
                params,
            )
        ]
        return {"all": int(all_count), "by_hero": by_hero}

    def _rows(self, sql: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        rows = self.connection.execute(sql, parameters).fetchall()
        if len(rows) > MAX_FETCH_ROWS:
            raise ReleaseBuildError(
                f"DuckDB query exceeded bounded aggregate output: {len(rows)} rows"
            )
        return rows


def _quality_payload(
    release_id: str,
    window: Mapping[str, Any],
    generated_at: str,
    seals: Sequence[DaySeal],
    heroes: Mapping[str, Any],
    builds: Mapping[str, Any],
) -> bytes:
    days = [
        {"day": seal.source_day, "row_counts": {name: int(seal.row_counts[name]) for name in TABLES}}
        for seal in seals
    ]
    totals = {name: sum(item["row_counts"][name] for item in days) for name in TABLES}
    build_heroes = builds.get("heroes", [])
    checks = [
        {"id": "8", "passed": True, "detail": "window is consecutive, anchored, and epoch-clamped"},
        {"id": "9", "passed": True, "detail": "all five payload kinds validated before promotion"},
        {"id": "10", "passed": True, "detail": "manifest inventory matches the exact staged file set"},
        {"id": "11", "passed": True, "detail": "release identity is prefixed by the anchor day"},
    ]
    return canonical_json(
        {
            "schema_version": 1,
            "kind": "quality",
            "generated_at": generated_at,
            "release_id": release_id,
            "window": dict(window),
            "checks": {"all_passed": True, "items": checks},
            "sli": {
                "days": days,
                "totals": totals,
                "hero_count": len({row["hero"] for row in heroes.get("rows", [])}),
                "build_count": sum(int(item["included_build_count"]) for item in build_heroes),
                "card_count": len(builds.get("cards", [])),
                "uncovered_card_count": sum(
                    len(item["coverage"]["uncovered_card_refs"]) for item in build_heroes
                ),
            },
        }
    )


def _build_row(
    candidate: Mapping[str, Any],
    reason: int,
    covered_card: str | None,
    card_ref: Mapping[str, int],
    enchant_ref: Mapping[str, int],
) -> list[Any]:
    layout = [
        [
            card_ref[card["template_id"]],
            int(card["slot"]),
            _tier_value(card["tier"]),
            enchant_ref.get(_clean_enchantment(card["enchantment"]), 0),
            int(card["size"]),
        ]
        for card in candidate["layout"]
    ]
    stats = [
        candidate["completed"],
        candidate["ten_win"],
        _bps(candidate["ten_win"], candidate["completed"]),
        _tenth(candidate["average_day"]),
        _round_half_up(candidate["p75_day"]) if candidate["p75_day"] is not None else None,
        _tenth(candidate["average_losses"]),
        candidate["legend_completed"],
        candidate["legend_ten_win"],
        _bps(candidate["legend_ten_win"], candidate["legend_completed"]),
        _tenth(candidate["legend_average_day"]),
        candidate["score"],
    ]
    return [
        sorted(card_ref[card] for card in candidate["card_ids"]),
        layout,
        stats,
        [reason, card_ref[covered_card] if covered_card is not None else None],
    ]


def _configure_connection(
    connection: duckdb.DuckDBPyConnection,
    *,
    memory_limit: str,
    temp_directory: Path,
    threads: int,
) -> None:
    connection.execute(f"SET memory_limit={_sql_string(memory_limit)}")
    connection.execute(f"SET temp_directory={_sql_string(str(temp_directory))}")
    connection.execute(f"SET threads={threads}")
    connection.execute("SET preserve_insertion_order=false")


def _validate_payload_files(
    stage: Path, payloads: Mapping[str, bytes], contracts_dir: Path
) -> None:
    for relative in sorted(payloads):
        _validate_payload(stage / relative, contracts_dir)


def _validate_release_schemas(stage: Path, contracts_dir: Path) -> None:
    for path in sorted(item for item in stage.rglob("*.json") if item.is_file()):
        _validate_payload(path, contracts_dir)


def _validate_payload(path: Path, contracts_dir: Path) -> None:
    try:
        value = _decode_object(path.read_bytes(), path.as_posix())
    except OSError as error:
        raise ContractViolation(f"Payload is unreadable: {path.name}") from error
    _validate_payload_value(value, contracts_dir)


def _validate_payload_value(value: Mapping[str, Any], contracts_dir: Path) -> None:
    kind = value.get("kind")
    schema_name = _SCHEMA_FILES.get(str(kind))
    if schema_name is None:
        raise ContractViolation(f"Payload kind is unknown: {kind}")
    try:
        schema = _decode_object((contracts_dir / schema_name).read_bytes(), schema_name)
    except OSError as error:
        raise ContractViolation(f"Frozen schema is unreadable: {schema_name}") from error
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(value), key=lambda item: list(item.absolute_path))
    if errors:
        location = "/".join(str(item) for item in errors[0].absolute_path) or "<root>"
        raise ContractViolation(f"{kind} violates its frozen contract at {location}: {errors[0].message}")


def _parse_pointer(observed: StoredObject, contracts_dir: Path) -> PublishedPointer:
    try:
        manifest = _decode_object(observed.body, "public pointer")
        _validate_payload_value(manifest, contracts_dir)
        release_id = manifest.get("release_id")
        window = manifest.get("window")
        window_end = window.get("end") if isinstance(window, dict) else None
        end = parse_source_day(window_end) if isinstance(window_end, str) else None
    except (ReleaseBuildError, TypeError, ValueError) as error:
        raise InvalidPointer("Public pointer does not match the frozen manifest contract") from error
    if (
        end is None
        or not isinstance(release_id, str)
        or RELEASE_ID_PATTERN.fullmatch(release_id) is None
        or not release_id.startswith(f"{end.isoformat()}-")
        or canonical_json(manifest) != observed.body
        or observed.stat.cache_control != POINTER_CACHE_CONTROL
    ):
        raise InvalidPointer("Public pointer identity or Cache-Control is invalid")
    return PublishedPointer(release_id, end.isoformat(), manifest, observed.stat)


def _release_artifacts(release: LocalRelease) -> list[tuple[str, bytes]]:
    return [
        (path.relative_to(release.path).as_posix(), path.read_bytes())
        for path in sorted(release.path.rglob("*"))
        if path.is_file()
    ]


def _confirm_object(
    key: str,
    observed: ObjectStat | None,
    content: bytes,
    cache_control: str,
    *,
    conflict: bool,
) -> None:
    digest = hashlib.sha256(content).hexdigest()
    matches = (
        observed is not None
        and observed.sha256 == digest
        and observed.bytes == len(content)
        and observed.cache_control == cache_control
    )
    if matches:
        return
    if conflict:
        raise ImmutableObjectConflict(f"Immutable object differs: {key}")
    raise PublishConfirmationError(f"Object confirmation differs: {key}")


def _verify_manifest(stage: Path) -> Mapping[str, Any]:
    manifest = _decode_object((stage / "manifest.json").read_bytes(), "release_manifest")
    declared = manifest.get("files")
    if not isinstance(declared, list):
        raise ManifestMismatch("manifest.json files is not an array")
    expected_paths = [item.get("path") if isinstance(item, dict) else None for item in declared]
    actual_paths = sorted(
        path.relative_to(stage).as_posix()
        for path in stage.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    if expected_paths != actual_paths:
        raise ManifestMismatch("manifest.json file set is not exact")
    for item in declared:
        relative = item["path"]
        path = stage / relative
        size = path.stat().st_size
        digest = _sha256_file(path)
        if item.get("bytes") != size or item.get("sha256") != digest:
            raise ManifestMismatch(f"manifest.json digest or size differs: {relative}")
    return manifest


def _inventory_entry(stage: Path, relative: str) -> dict[str, Any]:
    path = stage / relative
    return {"path": relative, "sha256": _sha256_file(path), "bytes": path.stat().st_size}


def _check_window(seals: Sequence[DaySeal], anchor: date) -> None:
    if not 1 <= len(seals) <= 7:
        raise ReleaseIdentityError("Release window must contain between 1 and 7 sealed days")
    days = [parse_source_day(seal.source_day) for seal in seals]
    if days[-1] != anchor:
        raise ReleaseIdentityError("Release window must end at its sealed anchor")
    if days[0] < EPOCH_DAY:
        raise ReleaseIdentityError(f"Release window cannot precede epoch {EPOCH_DAY.isoformat()}")
    expected = [days[0] + timedelta(days=offset) for offset in range(len(days))]
    if days != expected:
        raise ReleaseIdentityError("Release window must contain consecutive Source Days")


def _check_release_prefix(release_id: str, anchor: date) -> None:
    if RELEASE_ID_PATTERN.fullmatch(release_id) is None or not release_id.startswith(
        f"{anchor.isoformat()}-"
    ):
        raise ReleaseIdentityError("Release identity is not prefixed by its anchor day")


def _decode_object(content: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ContractViolation(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ContractViolation(f"{label} must contain a JSON object")
    return value


def _generated_at(source_day: date | str) -> str:
    return f"{(parse_source_day(source_day) + timedelta(days=1)).isoformat()}T00:00:00Z"


def _required_reason(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Operator reason is required")
    return value.strip()


def _default_contracts_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "contracts" / "v5"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _durable_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _atomic_replace_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def _one_decimal(value: Any | None) -> float | None:
    return round(float(value), 1) if value is not None else None


def _wilson_lower_bound(successes: int, total: int, z: float) -> float:
    if total <= 0:
        return 0.0
    proportion = successes / total
    denominator = 1 + z * z / total
    center = proportion + z * z / (2 * total)
    margin = z * math.sqrt(
        proportion * (1 - proportion) / total + z * z / (4 * total * total)
    )
    return (center - margin) / denominator


def _round_half_up(value: float) -> int:
    return math.floor(float(value) + 0.5)


def _bps(numerator: int, denominator: int) -> int | None:
    return _round_half_up(10_000 * numerator / denominator) if denominator else None


def _tenth(value: float | None) -> int | None:
    return _round_half_up(10 * value) if value is not None else None


def _clean_enchantment(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return None if not cleaned or cleaned.lower() == "none" else cleaned


def _tier_value(value: str | None) -> int | None:
    if value is None:
        return None
    return {
        "bronze": 1,
        "silver": 2,
        "gold": 3,
        "diamond": 4,
        "legendary": 5,
    }.get(value.strip().lower())

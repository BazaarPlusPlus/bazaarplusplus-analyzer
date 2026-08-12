from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import shutil

from jsonschema import Draft202012Validator

from bpp_analyzer.driver import PipelineDriver
from bpp_analyzer.fact_store import canonical_json
from bpp_analyzer.release import ReleaseBuilder
from tests.release_fixtures import sealed_store


class NeverSource:
    def hour_index(self, _source_hour):
        raise AssertionError("Sealed fixture days must not reach the Bundle Server")

    def stream(self, _index):
        raise AssertionError("Sealed fixture days must not reach the Bundle Server")


def _release_bytes(path: Path) -> dict[str, bytes]:
    return {
        item.relative_to(path).as_posix(): item.read_bytes()
        for item in sorted(path.rglob("*"))
        if item.is_file()
    }


def test_every_payload_matches_the_frozen_schema_and_source_time(tmp_path: Path) -> None:
    store = sealed_store(tmp_path, 2)
    release = ReleaseBuilder(tmp_path, store=store, threads=4).build(
        "2026-08-08", store.seals()
    )
    contracts = Path(__file__).resolve().parents[1] / "contracts/v5"
    schema_for_path = {
        "manifest.json": "release-manifest.schema.json",
        "quality.json": "quality.schema.json",
        "window/heroes.json": "hero-window.schema.json",
        "window/builds.json": "builds.schema.json",
        "daily/2026-08-07.json": "hero-daily.schema.json",
        "daily/2026-08-08.json": "hero-daily.schema.json",
    }

    for relative, schema_name in schema_for_path.items():
        content = (release.path / relative).read_bytes()
        payload = json.loads(content)
        schema = json.loads((contracts / schema_name).read_bytes())
        Draft202012Validator(schema).validate(payload)
        assert content == canonical_json(payload)
        source_day = payload.get("day", "2026-08-08")
        expected_generated_at = (
            date.fromisoformat(source_day) + timedelta(days=1)
        ).isoformat() + "T00:00:00Z"
        assert payload["generated_at"] == expected_generated_at

    heroes = json.loads((release.path / "window/heroes.json").read_bytes())
    all_dooley = next(
        row for row in heroes["rows"] if row["hero"] == "Dooley" and row["segment"] == "all"
    )
    assert all_dooley["performance_rating"] == 1067.7
    assert all_dooley["ghost"] == {"battles": 51, "win_rate": 0.3922}
    assert [item["rank"] for item in all_dooley["opponent_ranks"]] == ["Gold"]

    builds = json.loads((release.path / "window/builds.json").read_bytes())
    assert builds["heroes"][0]["builds"][0][2][-1] == 389


def test_seven_day_parallel_build_is_byte_deterministic_and_records_bounded_rss(
    tmp_path: Path,
) -> None:
    store = sealed_store(tmp_path, 7)
    driver = PipelineDriver(
        tmp_path,
        source=NeverSource(),
        clock=lambda: datetime(2026, 8, 13, 23, 59, tzinfo=UTC),
        duckdb_threads=4,
    )

    summary = driver.run(heal_days=7, anchor_day="2026-08-13", publish=False)

    assert summary.release_built is not None
    assert summary.peak_rss_bytes < 8 * 1024**3
    status = json.loads((tmp_path / "status.json").read_bytes())
    assert status["peak_rss_bytes"] < 8 * 1024**3
    assert status["last_run"]["release_built"] == summary.release_built
    release_path = tmp_path / "releases" / summary.release_built
    first = _release_bytes(release_path)

    shutil.rmtree(release_path)
    rebuilt = ReleaseBuilder(tmp_path, store=store, threads=4).build(
        "2026-08-13", store.seals()
    )
    second = _release_bytes(rebuilt.path)

    assert first == second

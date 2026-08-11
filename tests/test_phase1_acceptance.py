from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa

from bpp_analyzer.bundle_source import (
    Bundle,
    BundleRef,
    RawHourIndex,
    raw_commit_sha256,
)
from bpp_analyzer.driver import EPOCH_DAY, PipelineDriver, healing_days, is_hour_settled
from bpp_analyzer.fact_store import FactStore
from bpp_analyzer.projection import HourProjection, table_schemas
from tests.bundle_fixtures import bundle_bytes


class OneBundleSource:
    def __init__(self) -> None:
        self.content = bundle_bytes("bundle-a")

    def hour_index(self, source_hour: datetime) -> RawHourIndex:
        ref = BundleRef(
            bundle_id="bundle-a",
            available_at_ms=int(source_hour.timestamp() * 1_000),
            download_url="https://download.invalid/bundle-a",
            download_expires_at_ms=int(source_hour.timestamp() * 1_000) + 60_000,
            sha256=hashlib.sha256(self.content).hexdigest(),
            bytes=len(self.content),
        )
        return RawHourIndex(source_hour, (ref,), raw_commit_sha256((ref,)), 1)

    def stream(self, index: RawHourIndex):
        ref = index.items[0]
        yield Bundle(
            ref,
            self.content,
            hashlib.sha256(self.content).hexdigest(),
            len(self.content),
        )


def _empty_projection(hour: datetime) -> HourProjection:
    return HourProjection(
        hour,
        hashlib.sha256(hour.isoformat().encode()).hexdigest(),
        {
            name: pa.Table.from_pylist([], schema=schema)
            for name, schema in table_schemas().items()
        },
    )


def test_hour_ingest_records_and_stays_below_the_one_gib_peak_rss_limit(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 7, 1, 1, tzinfo=UTC)

    summary = PipelineDriver(
        tmp_path, source=OneBundleSource(), clock=lambda: now
    ).run(heal_days=8)

    status = json.loads((tmp_path / "status.json").read_text())
    assert summary.hours_ingested == 1
    assert summary.peak_rss_bytes < 1024**3
    assert status["peak_rss_bytes"] < 1024**3


def test_one_source_day_persists_only_parquet_plus_at_most_one_percent_metadata(
    tmp_path: Path,
) -> None:
    store = FactStore(tmp_path)
    schema = table_schemas()
    day = date(2026, 8, 10)
    for hour_number in range(24):
        hour = datetime.combine(day, datetime.min.time(), UTC) + timedelta(
            hours=hour_number
        )
        projection = _empty_projection(hour)
        if hour_number == 0:
            quarantine = pa.Table.from_pylist(
                [
                    {
                        "source_hour": hour.strftime("%Y-%m-%dT%H"),
                        "source_day": day.isoformat(),
                        "bundle_id": "fixture-bundle",
                        "run_id": None,
                        "stage": "bundle_validation",
                        "reason_code": "fixture",
                        "first_seen_at": "2026-08-10T01:00:00Z",
                        "decoder_code_version": "fixture",
                        "diagnostic_json": os.urandom(6 * 1024 * 1024).hex(),
                    }
                ],
                schema=schema["quarantine"],
            )
            projection = HourProjection(
                hour,
                projection.raw_commit_sha256,
                {**projection.tables, "quarantine": quarantine},
            )
        store.commit_hour(projection)
    store.seal_day(day)

    files = [path for path in tmp_path.rglob("*") if path.is_file()]
    parquet_bytes = sum(path.stat().st_size for path in files if path.suffix == ".parquet")
    total_bytes = sum(path.stat().st_size for path in files)
    assert parquet_bytes > 0
    assert total_bytes <= parquet_bytes * 1.01
    assert not list(tmp_path.rglob("*.bundle"))
    assert not (tmp_path / "raw").exists()


def test_healing_is_epoch_clamped_oldest_first_and_settlement_is_a_pure_boundary() -> None:
    now = datetime(2026, 8, 9, 1, 1, tzinfo=UTC)

    assert healing_days(now, 30) == (EPOCH_DAY, date(2026, 8, 8), date(2026, 8, 9))
    hour = datetime(2026, 8, 9, tzinfo=UTC)
    assert not is_hour_settled(hour, datetime(2026, 8, 9, 1, 0, 59, tzinfo=UTC))
    assert is_hour_settled(hour, datetime(2026, 8, 9, 1, 1, tzinfo=UTC))

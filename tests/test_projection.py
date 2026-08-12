from __future__ import annotations

from datetime import UTC, datetime
import hashlib

import httpx
import pytest

from bpp_analyzer.bundle_source import (
    Bundle,
    BundleRef,
    BundleSource,
    RawHourIndex,
    raw_commit_sha256,
)
from bpp_analyzer.projection import project_hour
from tests.bundle_fixtures import SOURCE_HOUR, bundle_bytes, payload


def _project_payload(run_payload: bytes):
    content = bundle_bytes("bundle-a", run_payload=run_payload)
    ref = BundleRef(
        bundle_id="bundle-a",
        available_at_ms=int(SOURCE_HOUR.timestamp() * 1_000),
        download_url="https://download.invalid/a",
        download_expires_at_ms=int(SOURCE_HOUR.timestamp() * 1_000) + 60_000,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    index = RawHourIndex(
        source_hour=SOURCE_HOUR,
        items=(ref,),
        raw_commit_sha256=raw_commit_sha256((ref,)),
        pages=1,
    )
    return project_hour(
        index,
        [Bundle(ref, content, hashlib.sha256(content).hexdigest(), len(content))],
    )


@pytest.mark.parametrize(
    ("winner_id", "loser_id", "winner_side", "winner_hero"),
    (
        ("Player", "Opponent", "player", "Vanessa"),
        ("Opponent", "Player", "opponent", "Pygmalien"),
    ),
)
def test_battle_outcome_uses_bundle_side_names(
    winner_id: str,
    loser_id: str,
    winner_side: str,
    winner_hero: str,
) -> None:
    projected = _project_payload(
        payload(
            winner_combatant_id=winner_id,
            loser_combatant_id=loser_id,
            victories=int(winner_side == "player"),
            losses=int(winner_side == "opponent"),
        )
    )

    battle = projected.tables["battles"].to_pylist()[0]
    run = projected.tables["runs"].to_pylist()[0]
    assert battle["winner_combatant_id"] == winner_id
    assert battle["loser_combatant_id"] == loser_id
    assert battle["winner_side"] == winner_side
    assert battle["winner_hero"] == winner_hero
    assert run["battle_decided_count"] == 1
    assert run["battle_player_win_count"] == (winner_side == "player")
    assert run["battle_player_loss_count"] == (winner_side == "opponent")
    assert "run_outcome_count_mismatch" not in {
        row["code"] for row in projected.tables["quality"].to_pylist()
    }


@pytest.mark.parametrize(
    ("winner_id", "loser_id", "winner_side", "winner_hero"),
    (
        ("account-1", "account-2", "player", "Vanessa"),
        ("account-2", "account-1", "opponent", "Pygmalien"),
    ),
)
def test_battle_outcome_falls_back_to_participant_account_ids(
    winner_id: str,
    loser_id: str,
    winner_side: str,
    winner_hero: str,
) -> None:
    battle = _project_payload(
        payload(
            winner_combatant_id=winner_id,
            loser_combatant_id=loser_id,
        )
    ).tables["battles"].to_pylist()[0]

    assert battle["winner_side"] == winner_side
    assert battle["winner_hero"] == winner_hero


def test_battle_outcome_keeps_an_unknown_combatant_undecided() -> None:
    projected = _project_payload(
        payload(
            winner_combatant_id="unknown-combatant",
            loser_combatant_id="another-unknown-combatant",
        )
    )

    battle = projected.tables["battles"].to_pylist()[0]
    run = projected.tables["runs"].to_pylist()[0]
    assert battle["winner_side"] is None
    assert battle["winner_hero"] is None
    assert run["battle_decided_count"] == 0


def test_bundle_digest_magic_and_segment_failures_are_quarantined_without_dropping_valid_data(
) -> None:
    valid = bundle_bytes("bundle-a")
    wrong_declared_digest = bundle_bytes("bundle-b")
    corrupt_magic = b"NOTBNDL5" + bundle_bytes("bundle-c")[8:]
    corrupt_segment = bytearray(bundle_bytes("bundle-d"))
    corrupt_segment[-1] ^= 1
    contents = {
        "bundle-a": valid,
        "bundle-b": wrong_declared_digest,
        "bundle-c": corrupt_magic,
        "bundle-d": bytes(corrupt_segment),
    }

    def server(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.invalid":
            items = []
            for offset, (bundle_id, content) in enumerate(contents.items()):
                digest = hashlib.sha256(content).hexdigest()
                if bundle_id == "bundle-b":
                    digest = "0" * 64
                items.append(
                    {
                        "bundle_id": bundle_id,
                        "available_at_ms": int(SOURCE_HOUR.timestamp() * 1_000) + offset,
                        "download_url": f"https://download.invalid/{bundle_id}",
                        "download_expires_at_ms": int(SOURCE_HOUR.timestamp() * 1_000) + 60_000,
                        "sha256": digest,
                        "bytes": len(content),
                    }
                )
            return httpx.Response(
                200,
                json={
                    "window": {
                        "available_from_ms": int(SOURCE_HOUR.timestamp() * 1_000),
                        "available_before_ms": int(SOURCE_HOUR.timestamp() * 1_000) + 3_600_000,
                    },
                    "items": items,
                    "next_after": None,
                },
            )
        return httpx.Response(200, content=contents[request.url.path.removeprefix("/")])

    source = BundleSource(
        api_base_url="https://api.invalid",
        sync_token="test-token",
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: datetime(2026, 8, 11, tzinfo=UTC),
    )

    index = source.hour_index(SOURCE_HOUR)
    projected = project_hour(index, source.stream(index))

    assert projected.tables["runs"].num_rows == 1
    quarantine = projected.tables["quarantine"].to_pylist()
    assert [row["bundle_id"] for row in quarantine] == [
        "bundle-b",
        "bundle-c",
        "bundle-d",
    ]
    assert [row["reason_code"] for row in quarantine] == [
        "bundle_sha256_mismatch",
        "invalid_prefix",
        "segment_digest_mismatch",
    ]
    assert sum(table.num_rows for table in projected.tables.values()) >= 4


def test_client_timestamp_anomalies_are_quality_rows_and_never_repartition_a_bundle() -> None:
    content = bundle_bytes(
        "bundle-a",
        run_payload=payload(
            started_at="2026-08-11T01:00:00Z",
            battle_at="2026-08-09T23:00:00Z",
        ),
        created_at_ms=int(datetime(2026, 8, 11, tzinfo=UTC).timestamp() * 1_000),
    )
    ref = BundleRef(
        bundle_id="bundle-a",
        available_at_ms=int(SOURCE_HOUR.timestamp() * 1_000),
        download_url="https://download.invalid/a",
        download_expires_at_ms=int(SOURCE_HOUR.timestamp() * 1_000) + 60_000,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    index = RawHourIndex(
        source_hour=SOURCE_HOUR,
        items=(ref,),
        raw_commit_sha256=raw_commit_sha256((ref,)),
        pages=1,
    )

    projected = project_hour(
        index,
        [Bundle(ref, content, hashlib.sha256(content).hexdigest(), len(content))],
    )

    quality_codes = {row["code"] for row in projected.tables["quality"].to_pylist()}
    assert {"client_clock_future", "client_clock_before_run"} <= quality_codes
    for table_name in ("runs", "battles", "battle_cards", "quality"):
        table = projected.tables[table_name]
        assert set(table.column("source_hour").to_pylist()) == {"2026-08-10T12"}
        assert set(table.column("source_day").to_pylist()) == {"2026-08-10"}

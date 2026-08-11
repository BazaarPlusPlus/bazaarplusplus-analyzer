from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import threading
import time

import httpx
import pytest

from bpp_analyzer.bundle_source import BundleSource, HourExpired
from tests.bundle_fixtures import bundle_bytes


HOUR = datetime(2026, 8, 10, 12, tzinfo=UTC)


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def test_hour_index_exhausts_keyset_pages_and_returns_ordered_deduplicated_index() -> None:
    first = b"first"
    second = b"second"
    requests: list[httpx.Request] = []

    def server(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        cursor = request.url.params.get("after_bundle_id")
        common = {
            "window": {
                "available_from_ms": int(HOUR.timestamp() * 1_000),
                "available_before_ms": int(HOUR.timestamp() * 1_000) + 3_600_000,
            }
        }
        if cursor is None:
            return httpx.Response(
                200,
                json={
                    **common,
                    "items": [
                        {
                            "bundle_id": "bundle-a",
                            "available_at_ms": int(HOUR.timestamp() * 1_000) + 1,
                            "download_url": "https://download.invalid/a",
                            "download_expires_at_ms": int(HOUR.timestamp() * 1_000) + 9_000,
                            "sha256": _digest(first),
                            "bytes": len(first),
                        }
                    ],
                    "next_after": {
                        "available_at_ms": int(HOUR.timestamp() * 1_000) + 1,
                        "bundle_id": "bundle-a",
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                **common,
                "items": [
                    {
                        "bundle_id": "bundle-a",
                        "available_at_ms": int(HOUR.timestamp() * 1_000) + 1,
                        "download_url": "https://download.invalid/a-refreshed",
                        "download_expires_at_ms": int(HOUR.timestamp() * 1_000) + 10_000,
                        "sha256": _digest(first),
                        "bytes": len(first),
                    },
                    {
                        "bundle_id": "bundle-b",
                        "available_at_ms": int(HOUR.timestamp() * 1_000) + 2,
                        "download_url": "https://download.invalid/b",
                        "download_expires_at_ms": int(HOUR.timestamp() * 1_000) + 9_000,
                        "sha256": _digest(second),
                        "bytes": len(second),
                    },
                ],
                "next_after": None,
            },
        )

    source = BundleSource(
        api_base_url="https://api.invalid",
        sync_token="test-token",
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: datetime(2026, 8, 11, tzinfo=UTC),
    )

    index = source.hour_index(HOUR)

    assert [item.bundle_id for item in index.items] == ["bundle-a", "bundle-b"]
    assert index.pages == 2
    assert len(requests) == 2
    assert requests[1].url.params["after_available_at_ms"] == str(
        int(HOUR.timestamp() * 1_000) + 1
    )
    assert requests[1].url.params["after_bundle_id"] == "bundle-a"
    expected_identity = [
        {
            "available_at_ms": int(HOUR.timestamp() * 1_000) + 1,
            "bundle_id": "bundle-a",
            "bytes": len(first),
            "sha256": _digest(first),
        },
        {
            "available_at_ms": int(HOUR.timestamp() * 1_000) + 2,
            "bundle_id": "bundle-b",
            "bytes": len(second),
            "sha256": _digest(second),
        },
    ]
    canonical = (
        json.dumps(expected_identity, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    assert index.raw_commit_sha256 == hashlib.sha256(canonical).hexdigest()


def test_empty_index_past_retention_is_expired_instead_of_a_zero_row_hour() -> None:
    def empty(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "window": {
                    "available_from_ms": int(HOUR.timestamp() * 1_000),
                    "available_before_ms": int(HOUR.timestamp() * 1_000) + 3_600_000,
                },
                "items": [],
                "next_after": None,
            },
        )

    source = BundleSource(
        api_base_url="https://api.invalid",
        sync_token="test-token",
        client=httpx.Client(transport=httpx.MockTransport(empty)),
        clock=lambda: datetime(2026, 8, 21, tzinfo=UTC),
        retention_days=10,
    )

    with pytest.raises(HourExpired, match="past retention"):
        source.hour_index(HOUR)


def test_expired_download_capability_reenumerates_the_same_fixed_hour() -> None:
    content = bundle_bytes("bundle-a")
    enumerations = 0

    def server(request: httpx.Request) -> httpx.Response:
        nonlocal enumerations
        if request.url.host == "api.invalid":
            enumerations += 1
            return httpx.Response(
                200,
                json={
                    "window": {
                        "available_from_ms": int(HOUR.timestamp() * 1_000),
                        "available_before_ms": int(HOUR.timestamp() * 1_000) + 3_600_000,
                    },
                    "items": [
                        {
                            "bundle_id": "bundle-a",
                            "available_at_ms": int(HOUR.timestamp() * 1_000) + 1,
                            "download_url": (
                                "https://download.invalid/expired"
                                if enumerations == 1
                                else "https://download.invalid/refreshed"
                            ),
                            "download_expires_at_ms": int(HOUR.timestamp() * 1_000) + 60_000,
                            "sha256": hashlib.sha256(content).hexdigest(),
                            "bytes": len(content),
                        }
                    ],
                    "next_after": None,
                },
            )
        if request.url.path == "/expired":
            return httpx.Response(403)
        return httpx.Response(200, content=content)

    source = BundleSource(
        api_base_url="https://api.invalid",
        sync_token="test-token",
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: datetime(2026, 8, 11, tzinfo=UTC),
    )

    index = source.hour_index(HOUR)
    downloaded = list(source.stream(index))

    assert enumerations == 2
    assert downloaded[0].validation_error is None
    assert downloaded[0].content == content


def test_stream_never_downloads_beyond_its_eight_bundle_lookahead() -> None:
    contents = {
        f"bundle-{number:02d}": bundle_bytes(f"bundle-{number:02d}")
        for number in range(20)
    }
    gate = threading.Event()
    eight_started = threading.Event()
    counter_lock = threading.Lock()
    started = 0

    def server(request: httpx.Request) -> httpx.Response:
        nonlocal started
        if request.url.host == "api.invalid":
            return httpx.Response(
                200,
                json={
                    "window": {
                        "available_from_ms": int(HOUR.timestamp() * 1_000),
                        "available_before_ms": int(HOUR.timestamp() * 1_000) + 3_600_000,
                    },
                    "items": [
                        {
                            "bundle_id": bundle_id,
                            "available_at_ms": int(HOUR.timestamp() * 1_000) + number,
                            "download_url": f"https://download.invalid/{bundle_id}",
                            "download_expires_at_ms": int(HOUR.timestamp() * 1_000) + 60_000,
                            "sha256": hashlib.sha256(content).hexdigest(),
                            "bytes": len(content),
                        }
                        for number, (bundle_id, content) in enumerate(contents.items())
                    ],
                    "next_after": None,
                },
            )
        with counter_lock:
            started += 1
            if started == 8:
                eight_started.set()
        assert gate.wait(2)
        return httpx.Response(200, content=contents[request.url.path.removeprefix("/")])

    source = BundleSource(
        api_base_url="https://api.invalid",
        sync_token="test-token",
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: datetime(2026, 8, 11, tzinfo=UTC),
        download_concurrency=16,
        lookahead=8,
    )
    index = source.hour_index(HOUR)
    stream = source.stream(index)
    result: list[object] = []
    worker = threading.Thread(target=lambda: result.append(next(stream)))
    worker.start()

    assert eight_started.wait(1)
    time.sleep(0.02)
    assert started == 8
    gate.set()
    worker.join(2)
    assert len(result) == 1
    stream.close()

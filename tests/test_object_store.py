from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from pathlib import Path

from bpp_analyzer.object_store import LocalObjectStore


def test_local_object_store_exposes_stat_get_put_as_the_offline_system_boundary(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 11, 12, 30, tzinfo=UTC)
    store = LocalObjectStore(tmp_path, clock=lambda: now)
    content = b'{"hello":"world"}\n'

    assert store.stat("fixture/data.json") is None
    store.put(
        "fixture/data.json",
        content,
        cache_control="public,max-age=60,must-revalidate",
    )

    observed = store.get("fixture/data.json")
    assert observed is not None
    assert observed.body == content
    assert observed.stat.sha256 == hashlib.sha256(content).hexdigest()
    assert observed.stat.bytes == len(content)
    assert observed.stat.cache_control == "public,max-age=60,must-revalidate"
    assert observed.stat.last_modified == now
    assert [(item.operation, item.key) for item in store.requests] == [
        ("stat", "fixture/data.json"),
        ("put", "fixture/data.json"),
        ("get", "fixture/data.json"),
    ]


import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from bppanalyzer.object_store import LocalObjectStore
from bppanalyzer.release import (
    POINTER_KEY,
    PublishHold,
    ReleaseBuilder,
    ReleasePublisher,
)
from tests.release_fixtures import sealed_store

NOW = datetime(2026, 8, 11, 15, tzinfo=UTC)


def test_check_17_rollback_hold_blocks_publish_until_reasoned_resume(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    facts = sealed_store(root, 2)
    builder = ReleaseBuilder(root, store=facts, threads=4)
    older = builder.build("2026-08-07", facts.seals())
    newer = builder.build("2026-08-08", facts.seals())
    objects = LocalObjectStore(tmp_path / "objects", clock=lambda: NOW)
    publisher = ReleasePublisher(root, objects, clock=lambda: NOW)
    publisher.publish(newer)

    rollback = publisher.rollback(older, "bad upstream data")

    assert publisher.current_pointer().release_id == older.release_id
    hold = json.loads((root / "publish-hold.json").read_bytes())
    assert hold["target_release_id"] == older.release_id
    assert hold["reason"] == "bad upstream data"
    rollback_receipt = json.loads(rollback.receipt_path.read_bytes())
    assert rollback_receipt["action"] == "rollback"
    assert rollback_receipt["from_release_id"] == newer.release_id
    assert rollback_receipt["to_release_id"] == older.release_id

    objects.clear_requests()
    with pytest.raises(PublishHold):
        publisher.publish(newer)
    assert not any(request.operation == "put" for request in objects.requests)
    assert publisher.current_pointer().release_id == older.release_id

    resume = publisher.resume("upstream corrected")

    assert not (root / "publish-hold.json").exists()
    resume_receipt = json.loads(resume.receipt_path.read_bytes())
    assert resume_receipt["action"] == "resume"
    assert resume_receipt["reason"] == "upstream corrected"
    publisher.publish(newer)
    assert json.loads(objects.get(POINTER_KEY).body)["release_id"] == newer.release_id


def test_publish_hold_and_operator_actions_require_nonempty_reasons(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    facts = sealed_store(root, 2)
    builder = ReleaseBuilder(root, store=facts)
    older = builder.build("2026-08-07", facts.seals())
    newer = builder.build("2026-08-08", facts.seals())
    publisher = ReleasePublisher(
        root,
        LocalObjectStore(tmp_path / "objects", clock=lambda: NOW),
        clock=lambda: NOW,
    )
    publisher.publish(newer)

    with pytest.raises(ValueError, match="reason"):
        publisher.rollback(older, "  ")
    with pytest.raises(ValueError, match="reason"):
        publisher.resume("")

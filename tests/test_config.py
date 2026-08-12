from pathlib import Path

import pytest

from bpp_analyzer.config import ConfigurationError, load_config


def _write_env(root: Path, extra: str = "") -> None:
    (root / ".env").write_text(
        "BPP_DATA_ROOT=fixture-data\n"
        "BPP_V5_API_BASE_URL=https://api.invalid\n"
        "BPP_BUNDLE_SYNC_TOKEN=fixture-sync-token\n"
        f"{extra}"
    )


def test_object_store_configuration_is_complete_and_credentials_are_repr_safe(
    tmp_path: Path,
) -> None:
    _write_env(
        tmp_path,
        "BPP_METRICS_R2_ACCOUNT_ID=fixture-account\n"
        "BPP_METRICS_R2_BUCKET=fixture-bucket\n"
        "BPP_METRICS_R2_ACCESS_KEY_ID=fixture-access\n"
        "BPP_METRICS_R2_SECRET_ACCESS_KEY=fixture-secret\n"
        "BPP_KEEP_RELEASES=5\n",
    )

    config = load_config(root=tmp_path, require_object_store=True)

    assert config.data_root == tmp_path / "fixture-data"
    assert config.keep_releases == 5
    assert config.r2_bucket == "fixture-bucket"
    assert "fixture-access" not in repr(config)
    assert "fixture-secret" not in repr(config)


def test_object_store_configuration_rejects_partial_r2_credentials(
    tmp_path: Path,
) -> None:
    _write_env(tmp_path, "BPP_METRICS_R2_BUCKET=fixture-bucket\n")

    with pytest.raises(ConfigurationError, match="object-store configuration"):
        load_config(root=tmp_path, require_object_store=True)

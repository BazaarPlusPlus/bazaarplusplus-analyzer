"""Strict repository ``.env`` configuration without ambient-env fallback."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re

from dotenv import dotenv_values


class ConfigurationError(ValueError):
    """Required pipeline configuration is missing or invalid."""


@dataclass(frozen=True, slots=True)
class Config:
    data_root: Path
    api_base_url: str | None
    sync_token: str | None = field(repr=False)
    bundle_retention_days: int = 10
    download_concurrency: int = 4
    download_lookahead: int = 8
    max_run_seconds: int = 7200
    duckdb_memory_limit: str = "8GB"
    duckdb_threads: int = 8


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_config(*, require_source: bool = True, root: Path | None = None) -> Config:
    repo = (root or repository_root()).resolve()
    env_file = repo / ".env"
    if not env_file.is_file():
        raise ConfigurationError("Repository .env file is missing")
    values = dotenv_values(env_file, interpolate=False)
    data_root_value = values.get("BPP_DATA_ROOT")
    if not isinstance(data_root_value, str) or not data_root_value.strip():
        raise ConfigurationError("BPP_DATA_ROOT is missing from repository .env")
    data_root = Path(data_root_value)
    if not data_root.is_absolute():
        data_root = repo / data_root
    api_base_url = _optional(values.get("BPP_V5_API_BASE_URL"))
    sync_token = _optional(values.get("BPP_BUNDLE_SYNC_TOKEN"))
    if require_source and (api_base_url is None or sync_token is None):
        raise ConfigurationError("Bundle Server configuration is incomplete")
    return Config(
        data_root=data_root,
        api_base_url=api_base_url,
        sync_token=sync_token,
        bundle_retention_days=_positive_int(
            values.get("BPP_BUNDLE_RETENTION_DAYS"), 10, "BPP_BUNDLE_RETENTION_DAYS"
        ),
        download_concurrency=_positive_int(
            values.get("BPP_DOWNLOAD_CONCURRENCY"), 4, "BPP_DOWNLOAD_CONCURRENCY"
        ),
        download_lookahead=_positive_int(
            values.get("BPP_DOWNLOAD_LOOKAHEAD"), 8, "BPP_DOWNLOAD_LOOKAHEAD"
        ),
        max_run_seconds=_positive_int(
            values.get("BPP_MAX_RUN_SECONDS"), 7200, "BPP_MAX_RUN_SECONDS"
        ),
        duckdb_memory_limit=_memory_limit(values.get("BPP_DUCKDB_MEMORY_LIMIT")),
        duckdb_threads=_positive_int(
            values.get("BPP_DUCKDB_THREADS"), 8, "BPP_DUCKDB_THREADS"
        ),
    )


def _optional(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _positive_int(value: object, default: int, name: str) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(str(value))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a positive integer") from error
    if parsed < 1:
        raise ConfigurationError(f"{name} must be a positive integer")
    return parsed


def _memory_limit(value: object) -> str:
    if value is None or value == "":
        return "8GB"
    parsed = str(value).strip().upper()
    if re.fullmatch(r"\d+(?:\.\d+)?(?:KB|MB|GB|TB)", parsed) is None:
        raise ConfigurationError(
            "BPP_DUCKDB_MEMORY_LIMIT must be a positive size such as 8GB"
        )
    if float(parsed[:-2]) <= 0:
        raise ConfigurationError(
            "BPP_DUCKDB_MEMORY_LIMIT must be a positive size such as 8GB"
        )
    return parsed

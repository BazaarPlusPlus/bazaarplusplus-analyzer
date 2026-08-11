"""Operator CLI for healing, sealing, status, and local verification."""

from __future__ import annotations

import json

import click

from bpp_analyzer.bundle_source import BundleSource
from bpp_analyzer.config import ConfigurationError, load_config
from bpp_analyzer.driver import PipelineDriver, read_status
from bpp_analyzer.fact_store import FactStore, FactStoreError, parse_source_day
from bpp_analyzer.locking import LockHeld, LockOwnershipLost
from bpp_analyzer.release import (
    ReleaseBuildError,
    ReleaseBuilder,
    validate_local_releases,
)


@click.group()
def main() -> None:
    """bpp — Analyzer V5 pipeline."""


@main.command("run")
@click.option("--heal-days", type=click.IntRange(min=1), default=8, show_default=True)
@click.option("--anchor-day", type=str)
@click.option("--no-publish", is_flag=True, help="Build locally without publishing.")
@click.option("--dry-run", is_flag=True, help="Report without writing local state.")
def run_command(
    heal_days: int,
    anchor_day: str | None,
    no_publish: bool,
    dry_run: bool,
) -> None:
    """Heal Source Hours, seal complete Source Days, then report."""
    try:
        if anchor_day is not None:
            parse_source_day(anchor_day)
        config = load_config(require_source=not dry_run)
        if dry_run:
            click.echo(
                json.dumps(
                    {
                        "dry_run": True,
                        "heal_days": heal_days,
                        "anchor_day": anchor_day,
                        "publish": not no_publish,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return
        assert config.api_base_url is not None and config.sync_token is not None
        with BundleSource(
            api_base_url=config.api_base_url,
            sync_token=config.sync_token,
            retention_days=config.bundle_retention_days,
            download_concurrency=config.download_concurrency,
            lookahead=config.download_lookahead,
        ) as source:
            summary = PipelineDriver(
                config.data_root,
                source=source,
                max_run_seconds=config.max_run_seconds,
                duckdb_memory_limit=config.duckdb_memory_limit,
                duckdb_threads=config.duckdb_threads,
            ).run(
                heal_days=heal_days,
                anchor_day=anchor_day,
                publish=not no_publish,
            )
        click.echo(
            f"{summary.outcome}: {summary.hours_ingested} hours ingested, "
            f"{summary.days_sealed} days sealed, "
            f"{summary.days_abandoned} days abandoned"
        )
        if summary.exit_code:
            raise click.exceptions.Exit(summary.exit_code)
    except LockHeld:
        raise click.exceptions.Exit(3) from None
    except (ConfigurationError, ValueError) as error:
        raise click.UsageError(str(error)) from None
    except LockOwnershipLost as error:
        raise click.ClickException(str(error)) from None
    except ReleaseBuildError as error:
        raise click.ClickException(str(error)) from None


@main.command("status")
@click.option("json_output", "--json", is_flag=True, help="Emit machine-readable JSON.")
def status_command(json_output: bool) -> None:
    """Show the last atomic local health snapshot."""
    try:
        config = load_config(require_source=False)
        value = read_status(config.data_root)
    except ConfigurationError as error:
        raise click.UsageError(str(error)) from None
    except Exception as error:
        raise click.ClickException(str(error)) from None
    if json_output:
        click.echo(json.dumps(value, sort_keys=True, separators=(",", ":")))
        return
    facts = value["facts"]
    last = value.get("last_run")
    click.echo(f"newest sealed day: {facts.get('newest_sealed_day') or '-'}")
    click.echo(f"abandoned days: {len(facts.get('abandoned_days', []))}")
    click.echo(f"last outcome: {last.get('outcome') if isinstance(last, dict) else '-'}")


@main.command("verify")
@click.option("--day", type=str, help="Verify one Source Day (YYYY-MM-DD).")
@click.option("--deep", is_flag=True, help="Also verify local release contracts.")
def verify_command(day: str | None, deep: bool) -> None:
    """Re-hash committed Parquet and validate local immutable state."""
    try:
        if day is not None:
            parse_source_day(day)
        config = load_config(require_source=False)
        report = FactStore(config.data_root).verify(day, deep=deep)
        releases_verified = (
            validate_local_releases(config.data_root) if deep else 0
        )
    except (ConfigurationError, ValueError) as error:
        raise click.UsageError(str(error)) from None
    except (FactStoreError, ReleaseBuildError) as error:
        raise click.ClickException(str(error)) from None
    suffix = f" and {releases_verified} releases" if deep else ""
    click.echo(
        f"verified {report.hours_verified} hours and {report.files_verified} Parquet files{suffix}"
    )


@main.command("publish")
@click.argument("release_id")
def publish_command(release_id: str) -> None:
    """Publish an already-built release (available in Phase 3)."""
    del release_id
    raise click.ClickException("Release publishing is not implemented until Phase 3")


@main.command("rollback")
@click.argument("release_id")
@click.option("--reason", required=True)
def rollback_command(release_id: str, reason: str) -> None:
    """Roll back the public pointer (available after the publish phase)."""
    del release_id, reason
    raise click.ClickException("Release rollback is not implemented until Phase 3")


@main.command("resume")
@click.option("--reason", required=True)
def resume_command(reason: str) -> None:
    """Clear a publish hold (available after the publish phase)."""
    del reason
    raise click.ClickException("Release resume is not implemented until Phase 3")


@main.group("show")
def show_command() -> None:
    """Show local immutable objects."""


@show_command.command("release")
@click.argument("release_id")
def show_release_command(release_id: str) -> None:
    """Show a local release manifest."""
    try:
        config = load_config(require_source=False)
        manifest = ReleaseBuilder(
            config.data_root,
            memory_limit=config.duckdb_memory_limit,
            threads=config.duckdb_threads,
        ).show(release_id)
    except ConfigurationError as error:
        raise click.UsageError(str(error)) from None
    except ReleaseBuildError as error:
        raise click.ClickException(str(error)) from None
    click.echo(json.dumps(manifest, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()

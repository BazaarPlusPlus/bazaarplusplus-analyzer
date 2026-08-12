"""Operator CLI for healing, sealing, status, and local verification."""

import json
import uuid

import click

from bpp_analyzer.bundle_source import BundleSource
from bpp_analyzer.config import ConfigurationError, load_config
from bpp_analyzer.driver import PipelineDriver, read_status
from bpp_analyzer.fact_store import FactStore, FactStoreError, parse_source_day
from bpp_analyzer.locking import DirectoryLock, LockHeld, LockOwnershipLost
from bpp_analyzer.object_store import ObjectStoreError, R2ObjectStore
from bpp_analyzer.release import (
    PublishError,
    ReleaseBuildError,
    ReleaseBuilder,
    ReleasePublisher,
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
@click.option("--quiet", is_flag=True, help="Suppress progress; keep the final summary.")
def run_command(
    heal_days: int,
    anchor_day: str | None,
    no_publish: bool,
    dry_run: bool,
    quiet: bool,
) -> None:
    """Heal Source Hours, seal complete Source Days, then report."""
    try:
        if anchor_day is not None:
            parse_source_day(anchor_day)
        config = load_config(
            require_source=not dry_run,
            require_object_store=True,
        )
        object_store = _object_store(config)
        if dry_run:
            pointer = ReleasePublisher(
                config.data_root,
                object_store,
            ).current_pointer()
            click.echo(
                json.dumps(
                    {
                        "dry_run": True,
                        "heal_days": heal_days,
                        "anchor_day": anchor_day,
                        "publish": not no_publish,
                        "published_release_id": (
                            pointer.release_id if pointer is not None else None
                        ),
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
                object_store=object_store,
                keep_releases=config.keep_releases,
            ).run(
                heal_days=heal_days,
                anchor_day=anchor_day,
                publish=not no_publish,
                progress_callback=None if quiet else click.echo,
                error_callback=lambda message: click.echo(message, err=True),
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
    except (ReleaseBuildError, PublishError, ObjectStoreError) as error:
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
    current = value.get("current_run")
    if isinstance(current, dict):
        bundles = current.get("bundles")
        fields = [
            f"current run: {current.get('run_id') or '-'}",
            f"phase={current.get('phase') or '-'}",
            f"step={current.get('step') or '-'}",
            f"hour={current.get('current_hour') or '-'}",
            f"hours={current.get('hours_done', 0)}/{current.get('hours_planned', 0)}",
        ]
        if isinstance(bundles, dict):
            fields.append(
                f"bundles={bundles.get('done', 0)}/{bundles.get('total', 0)}"
            )
        fields.extend(
            [
                f"updated={current.get('updated_at') or '-'}",
                f"started={current.get('started_at') or '-'}",
            ]
        )
        click.echo(" ".join(fields))
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
    """Publish an already-built release."""
    try:
        config = load_config(require_source=False, require_object_store=True)
        with _operator_lock(config) as lock:
            builder = _release_builder(config)
            result = ReleasePublisher(
                config.data_root,
                _object_store(config),
                ownership_check=lock.assert_owned,
            ).publish(builder.local(release_id))
    except LockHeld:
        raise click.exceptions.Exit(3) from None
    except ConfigurationError as error:
        raise click.UsageError(str(error)) from None
    except (ReleaseBuildError, PublishError, ObjectStoreError) as error:
        raise click.ClickException(str(error)) from None
    click.echo(f"published {result.release_id}")


@main.command("rollback")
@click.argument("release_id")
@click.option("--reason", required=True)
def rollback_command(release_id: str, reason: str) -> None:
    """Roll back the public pointer and establish a publish hold."""
    try:
        config = load_config(require_source=False, require_object_store=True)
        with _operator_lock(config) as lock:
            builder = _release_builder(config)
            result = ReleasePublisher(
                config.data_root,
                _object_store(config),
                ownership_check=lock.assert_owned,
            ).rollback(builder.local(release_id), reason)
    except LockHeld:
        raise click.exceptions.Exit(3) from None
    except (ConfigurationError, ValueError) as error:
        raise click.UsageError(str(error)) from None
    except (ReleaseBuildError, PublishError, ObjectStoreError) as error:
        raise click.ClickException(str(error)) from None
    click.echo(f"rolled back to {result.release_id}")


@main.command("resume")
@click.option("--reason", required=True)
def resume_command(reason: str) -> None:
    """Clear a publish hold with a recorded reason."""
    try:
        config = load_config(require_source=False, require_object_store=True)
        with _operator_lock(config) as lock:
            result = ReleasePublisher(
                config.data_root,
                _object_store(config),
                ownership_check=lock.assert_owned,
            ).resume(reason)
    except LockHeld:
        raise click.exceptions.Exit(3) from None
    except (ConfigurationError, ValueError) as error:
        raise click.UsageError(str(error)) from None
    except (PublishError, ObjectStoreError) as error:
        raise click.ClickException(str(error)) from None
    click.echo(f"resumed publishing from hold target {result.release_id}")


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


def _release_builder(config):
    return ReleaseBuilder(
        config.data_root,
        memory_limit=config.duckdb_memory_limit,
        threads=config.duckdb_threads,
    )


def _operator_lock(config):
    return DirectoryLock(
        config.data_root,
        f"operator-{uuid.uuid4().hex}",
        max_run_seconds=config.max_run_seconds,
    )


def _object_store(config):
    assert config.r2_account_id is not None
    assert config.r2_bucket is not None
    assert config.r2_access_key_id is not None
    assert config.r2_secret_access_key is not None
    return R2ObjectStore(
        account_id=config.r2_account_id,
        bucket=config.r2_bucket,
        access_key_id=config.r2_access_key_id,
        secret_access_key=config.r2_secret_access_key,
    )


if __name__ == "__main__":
    main()

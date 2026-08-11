"""CLI entry point skeleton; commands are implemented by the pipeline phases."""

import click


@click.group()
def main() -> None:
    """bpp — Analyzer V5 pipeline."""


if __name__ == "__main__":
    main()

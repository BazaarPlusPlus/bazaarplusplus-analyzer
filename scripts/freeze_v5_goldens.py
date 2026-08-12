#!/usr/bin/env python3
"""Freeze a validated local V5 release as immutable contract goldens."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from bpp_analyzer.goldens import GoldenFreezeError, freeze_release
from bpp_analyzer.release import ReleaseBuildError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Copy one validated local release into contracts/v5/golden/."
    )
    parser.add_argument("release_dir", type=Path)
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Override the golden root (intended for offline workflow tests).",
    )
    args = parser.parse_args(argv)
    try:
        target = freeze_release(args.release_dir, output_root=args.output_root)
    except (GoldenFreezeError, ReleaseBuildError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"frozen {target.name} at {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

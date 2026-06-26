"""Data generator CLI (doc 04 §1).

Implemented so far: ``golden`` (normalization slice, Phase 3) and ``watchlists``
(Phase 4). Later phases extend this with ``profiles``, ``updates``, ``all``, and
``verify`` subcommands.

    python -m tools.datagen golden     --seed 42 --out data/golden/
    python -m tools.datagen watchlists --seed 42 --out data/watchlists/
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

from tools.datagen import (
    decisions_golden,
    matching_golden,
    normalization_golden,
    watchlists,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tools.datagen")
    sub = parser.add_subparsers(dest="command", required=True)

    golden = sub.add_parser("golden", help="generate golden expected-output datasets")
    golden.add_argument("--seed", type=int, default=42)
    golden.add_argument("--out", type=Path, default=Path("data/golden/"))
    golden.add_argument(
        "--set",
        choices=["normalization", "matching", "decisions", "all"],
        default="all",
        help="which golden set to generate",
    )

    wl = sub.add_parser("watchlists", help="generate provider watchlists + manifest")
    wl.add_argument("--seed", type=int, default=42)
    wl.add_argument("--out", type=Path, default=Path("data/watchlists/"))

    args = parser.parse_args(argv)

    if args.command == "golden":
        # Seed every RNG for determinism (hard rule #1); the golden slices are
        # fully deterministic already, but we honor the contract uniformly.
        random.seed(args.seed)
        if args.set in ("normalization", "all"):
            normalization_golden.generate(args.out)
        if args.set in ("matching", "all"):
            matching_golden.generate(args.out)
        if args.set in ("decisions", "all"):
            decisions_golden.generate(args.out)
        return 0

    if args.command == "watchlists":
        watchlists.generate(args.out, seed=args.seed)
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

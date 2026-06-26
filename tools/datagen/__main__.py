"""Data generator CLI (doc 04 §1).

Phase 3 implements only the normalization golden slice; the ``golden`` command
below is intentionally scoped to it. Later phases extend this with
``watchlists``, ``profiles``, ``updates``, ``all``, and ``verify`` subcommands.

    python -m tools.datagen golden --seed 42 --out data/golden/
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

from tools.datagen import normalization_golden


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tools.datagen")
    sub = parser.add_subparsers(dest="command", required=True)

    golden = sub.add_parser("golden", help="generate golden expected-output datasets")
    golden.add_argument("--seed", type=int, default=42)
    golden.add_argument("--out", type=Path, default=Path("data/golden/"))

    args = parser.parse_args(argv)

    if args.command == "golden":
        # Seed every RNG for determinism (hard rule #1); the normalization slice
        # is fully deterministic already, but we honor the contract uniformly.
        random.seed(args.seed)
        normalization_golden.generate(args.out)
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
